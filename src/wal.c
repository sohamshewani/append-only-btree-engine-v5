#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

#define WAL_PAGE_SIZE 4096
#define WAL_MAGIC 0x57414C31 /* "WAL1" */

typedef uint64_t lsn_t;
typedef uint64_t tx_id_t;
typedef uint32_t page_id_t;

typedef enum {
    WAL_OP_BEGIN = 1,
    WAL_OP_COMMIT,
    WAL_OP_ABORT,
    WAL_OP_INSERT,
    WAL_OP_UPDATE,
    WAL_OP_DELETE,
    WAL_OP_CHECKPOINT
} wal_op_type_t;

typedef struct __attribute__((__packed__)) {
    uint32_t magic;
    uint32_t crc32;
    lsn_t lsn;
    tx_id_t tx_id;
    uint32_t op_type;
    page_id_t page_id;
    uint32_t payload_len;
} wal_record_header_t;

struct wal_manager {
    int fd;
    pthread_mutex_t mutex;
    lsn_t next_lsn;
    lsn_t flushed_lsn;
    char path[1024];
    
    /* Internal buffer for group commit / buffering */
    uint8_t *buffer;
    size_t buf_capacity;
    size_t buf_used;
};

static uint32_t calculate_crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc = crc >> 1;
            }
        }
    }
    return ~crc;
}

static int wal_flush_unlocked(struct wal_manager *wm) {
    if (wm->buf_used == 0) {
        return 0;
    }

    ssize_t written = 0;
    size_t to_write = wm->buf_used;
    uint8_t *ptr = wm->buffer;

    while (to_write > 0) {
        ssize_t n = write(wm->fd, ptr, to_write);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        ptr += n;
        to_write -= n;
        written += n;
    }

    if (fsync(wm->fd) < 0) {
        return -1;
    }

    /* Update flushed LSN based on the last record written in this batch */
    /* For simplicity, we track that everything in the buffer is now flushed */
    wm->flushed_lsn = wm->next_lsn - 1;
    wm->buf_used = 0;
    return 0;
}

struct wal_manager* wal_init(const char *log_path) {
    struct wal_manager *wm = (struct wal_manager *)malloc(sizeof(struct wal_manager));
    if (!wm) {
        return NULL;
    }

    strncpy(wm->path, log_path, sizeof(wm->path) - 1);
    wm->path[sizeof(wm->path) - 1] = '\0';

    wm->fd = open(wm->path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (wm->fd < 0) {
        free(wm);
        return NULL;
    }

    if (pthread_mutex_init(&wm->mutex, NULL) != 0) {
        close(wm->fd);
        free(wm);
        return NULL;
    }

    wm->next_lsn = 1;
    wm->flushed_lsn = 0;
    wm->buf_capacity = WAL_PAGE_SIZE * 4;
    
    if (posix_memalign((void **)&wm->buffer, WAL_PAGE_SIZE, wm->buf_capacity) != 0) {
        pthread_mutex_destroy(&wm->mutex);
        close(wm->fd);
        free(wm);
        return NULL;
    }
    
    wm->buf_used = 0;

    /* Scan existing file to determine last LSN if recovering */
    struct stat st;
    if (fstat(wm->fd, &st) == 0 && st.st_size > 0) {
        /* Seek to end or read back to find last LSN */
        off_t current = lseek(wm->fd, 0, SEEK_END);
        if (current > 0) {
            /* Basic estimation, real implementation would scan records */
            wm->next_lsn = (current / 64) + 1;
            wm->flushed_lsn = wm->next_lsn - 1;
        }
    }

    return wm;
}

lsn_t wal_log(struct wal_manager *wm, tx_id_t tx_id, wal_op_type_t op_type, 
              page_id_t page_id, const uint8_t *payload, uint32_t payload_len) {
    pthread_mutex_lock(&wm->mutex);

    size_t total_record_size = sizeof(wal_record_header_t) + payload_len;
    
    if (total_record_size > WAL_PAGE_SIZE) {
        pthread_mutex_unlock(&wm->mutex);
        return 0; /* Payload exceeds page limit */
    }

    lsn_t current_lsn = wm->next_lsn++;

    wal_record_header_t header;
    header.magic = WAL_MAGIC;
    header.lsn = current_lsn;
    header.tx_id = tx_id;
    header.op_type = (uint32_t)op_type;
    header.page_id = page_id;
    header.payload_len = payload_len;

    /* Calculate CRC over header (excluding magic and crc itself) + payload */
    /* For simplicity, compute over entire header fields + payload */
    size_t crc_data_len = sizeof(wal_record_header_t) - offsetof(wal_record_header_t, lsn) + payload_len;
    uint8_t *crc_buf = malloc(crc_data_len);
    if (!crc_buf) {
        pthread_mutex_unlock(&wm->mutex);
        return 0;
    }

    memcpy(crc_buf, &header.lsn, sizeof(wal_record_header_t) - offsetof(wal_record_header_t, lsn));
    if (payload && payload_len > 0) {
        memcpy(crc_buf + (sizeof(wal_record_header_t) - offsetof(wal_record_header_t, lsn)), payload, payload_len);
    }
    
    header.crc32 = calculate_crc32(crc_buf, crc_data_len);
    free(crc_buf);

    if (wm->buf_used + total_record_size > wm->buf_capacity) {
        if (wal_flush_unlocked(wm) < 0) {
            pthread_mutex_unlock(&wm->mutex);
            return 0;
        }
    }

    memcpy(wm->buffer + wm->buf_used, &header, sizeof(wal_record_header_t));
    wm->buf_used += sizeof(wal_record_header_t);

    if (payload && payload_len > 0) {
        memcpy(wm->buffer + wm->buf_used, payload, payload_len);
        wm->buf_used += payload_len;
    }

    pthread_mutex_unlock(&wm->mutex);
    return current_lsn;
}

int wal_flush(struct wal_manager *wm, lsn_t target_lsn) {
    pthread_mutex_lock(&wm->mutex);
    if (target_lsn <= wm->flushed_lsn) {
        pthread_mutex_unlock(&wm->mutex);
        return 0;
    }
    int rc = wal_flush_unlocked(wm);
    pthread_mutex_unlock(&wm->mutex);
    return rc;
}

void wal_close(struct wal_manager *wm) {
    if (!wm) {
        return;
    }

    pthread_mutex_lock(&wm->mutex);
    wal_flush_unlocked(wm);
    close(wm->fd);
    pthread_mutex_unlock(&wm->mutex);

    pthread_mutex_destroy(&wm->mutex);
    free(wm->buffer);
    free(wm);
}

int wal_recover(struct wal_manager *wm, void (*apply_redo)(lsn_t lsn, tx_id_t tx_id, wal_op_type_t op, page_id_t page, const uint8_t *payload, uint32_t len),
                void (*apply_undo)(lsn_t lsn, tx_id_t tx_id, wal_op_type_t op, page_id_t page, const uint8_t *payload, uint32_t len)) {
    if (!wm) {
        return -1;
    }

    pthread_mutex_lock(&wm->mutex);

    if (lseek(wm->fd, 0, SEEK_SET) < 0) {
        pthread_mutex_unlock(&wm->mutex);
        return -1;
    }

    uint8_t read_buf[WAL_PAGE_SIZE];
    ssize_t bytes_read;

    while ((bytes_read = read(wm->fd, read_buf, sizeof(read_buf))) > 0) {
        size_t offset = 0;
        while (offset + sizeof(wal_record_header_t) <= (size_t)bytes_read) {
            wal_record_header_t *header = (wal_record_header_t *)(read_buf + offset);
            
            if (header->magic != WAL_MAGIC) {
                break;
            }

            if (offset + sizeof(wal_record_header_t) + header->payload_len > (size_t)bytes_read) {
                /* Partial record at end of block, requires more sophisticated reading */
                break;
            }

            uint8_t *payload = read_buf + offset + sizeof(wal_record_header_t);

            /* Validate CRC */
            size_t crc_data_len = sizeof(wal_record_header_t) - offsetof(wal_record_header_t, lsn) + header->payload_len;
            uint8_t *crc_buf = malloc(crc_data_len);
            if (!crc_buf) {
                pthread_mutex_unlock(&wm->mutex);
                return -1;
            }

            memcpy(crc_buf, &header->lsn, sizeof(wal_record_header_t) - offsetof(wal_record_header_t, lsn));
            if (header->payload_len > 0) {
                memcpy(crc_buf + (sizeof(wal_record_header_t) - offsetof(wal_record_header_t, lsn)), payload, header->payload_len);
            }

            uint32_t calculated_crc = calculate_crc32(crc_buf, crc_data_len);
            free(crc_buf);

            if (calculated_crc != header->crc32) {
                pthread_mutex_unlock(&wm->mutex);
                return -1; /* Corrupted WAL record */
            }

            if (apply_redo) {
                apply_redo(header->lsn, header->tx_id, (wal_op_type_t)header->op_type, header->page_id, payload, header->payload_len);
            }

            offset += sizeof(wal_record_header_t) + header->payload_len;
        }
    }

    pthread_mutex_unlock(&wm->mutex);
    return 0;
}