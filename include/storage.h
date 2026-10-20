#ifndef STORAGE_H
#define STORAGE_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#define PAGE_SIZE 4096
#define MAX_BUFFER_POOL_FRAMES 1024
#define HASH_TABLE_BUCKETS 256
#define MAX_KEY_SIZE 256
#define MAX_VAL_SIZE 1024

typedef uint32_t page_id_t;
typedef uint64_t lsn_t;
typedef uint64_t tx_id_t;

#define INVALID_PAGE_ID ((page_id_t)0xFFFFFFFF)
#define INVALID_LSN     ((lsn_t)0)
#define INVALID_TX_ID   ((tx_id_t)0)

typedef enum {
    NODE_TYPE_INTERNAL = 0x01,
    NODE_TYPE_LEAF     = 0x02
} node_type_t;

typedef enum {
    OP_INSERT = 1,
    OP_UPDATE,
    OP_DELETE,
    OP_BEGIN,
    OP_COMMIT,
    OP_ABORT
} log_op_type_t;

#pragma pack(push, 1)
typedef struct {
    uint8_t type;         
    uint16_t key_count;   
    uint16_t lower_offset;
    uint16_t upper_offset;
    page_id_t next_page;  
    page_id_t prev_page;  
    lsn_t page_lsn;       
} btree_node_header_t;

typedef struct {
    uint16_t offset;
    uint16_t length;
} slot_t;

typedef struct {
    lsn_t lsn;
    tx_id_t tx_id;
    uint8_t op_type;
    page_id_t page_id;
    uint32_t payload_len;
} wal_record_header_t;
#pragma pack(pop)

typedef struct {
    page_id_t page_id;
    int frame_id;
    int pin_count;
    bool is_dirty;
    bool reference_bit;
    uint32_t usage_count;
    pthread_rwlock_t rwlock;
    pthread_mutex_t frame_mutex;
    uint8_t data[PAGE_SIZE];
} frame_t;

typedef struct hash_bucket {
    page_id_t page_id;
    int frame_id;
    struct hash_bucket* next;
} hash_bucket_t;

typedef struct {
    hash_bucket_t* buckets[HASH_TABLE_BUCKETS];
    pthread_rwlock_t bucket_locks[HASH_TABLE_BUCKETS];
} buffer_pool_hash_t;

typedef struct {
    int fd;
    size_t num_pages;
    pthread_rwlock_t file_lock;
} disk_manager_t;

typedef struct {
    frame_t frames[MAX_BUFFER_POOL_FRAMES];
    buffer_pool_hash_t page_table;
    disk_manager_t* disk_manager;
    int free_list[MAX_BUFFER_POOL_FRAMES];
    int free_list_head;
    int clock_hand;
    pthread_mutex_t bp_mutex;
    
    pthread_t flusher_thread;
    pthread_cond_t flusher_cond;
    pthread_mutex_t flusher_mutex;
    bool shutdown_flag;
} buffer_pool_manager_t;

typedef struct {
    int wal_fd;
    lsn_t current_lsn;
    lsn_t flushed_lsn;
    pthread_mutex_t wal_mutex;
    char wal_path[512];
} log_manager_t;

typedef struct {
    disk_manager_t* disk_mgr;
    buffer_pool_manager_t* bpm;
    log_manager_t* log_mgr;
} storage_engine_t;

disk_manager_t* disk_manager_init(const char* db_file);
void disk_manager_close(disk_manager_t* dm);
void disk_manager_read_page(disk_manager_t* dm, page_id_t page_id, uint8_t* page_data);
void disk_manager_write_page(disk_manager_t* dm, page_id_t page_id, const uint8_t* page_data);
page_id_t disk_manager_allocate_page(disk_manager_t* dm);

log_manager_t* log_manager_init(const char* wal_file);
void log_manager_close(log_manager_t* lm);
lsn_t log_manager_append(log_manager_t* lm, tx_id_t tx_id, uint8_t op_type, page_id_t page_id, const void* payload, uint32_t payload_len);
void log_manager_flush(log_manager_t* lm, lsn_t lsn);

buffer_pool_manager_t* buffer_pool_manager_init(disk_manager_t* dm, log_manager_t* lm);
void buffer_pool_manager_close(buffer_pool_manager_t* bpm);
frame_t* buffer_pool_fetch_page(buffer_pool_manager_t* bpm, page_id_t page_id);
void buffer_pool_unpin_page(buffer_pool_manager_t* bpm, page_id_t page_id, bool is_dirty);
bool buffer_pool_flush_page(buffer_pool_manager_t* bpm, page_id_t page_id);
void buffer_pool_flush_all(buffer_pool_manager_t* bpm);

storage_engine_t* storage_engine_init(const char* db_file, const char* wal_file);
void storage_engine_shutdown(storage_engine_t* engine);

void btree_node_init(uint8_t* page_data, node_type_t type);
bool btree_node_insert(uint8_t* page_data, const void* key, uint16_t key_len, const void* val, uint16_t val_len, lsn_t lsn);
bool btree_node_search(const uint8_t* page_data, const void* key, uint16_t key_len, void* val_out, uint16_t* val_len_out);

#ifdef STORAGE_IMPLEMENTATION

static void* background_flusher_routine(void* arg) {
    buffer_pool_manager_t* bpm = (buffer_pool_manager_t*)arg;
    while (true) {
        pthread_mutex_lock(&bpm->flusher_mutex);
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1; 
        pthread_cond_timedwait(&bpm->flusher_cond, &bpm->flusher_mutex, &ts);
        
        if (bpm->shutdown_flag) {
            pthread_mutex_unlock(&bpm->flusher_mutex);
            break;
        }
        pthread_mutex_unlock(&bpm->flusher_mutex);

        buffer_pool_flush_all(bpm);
    }
    return NULL;
}

disk_manager_t* disk_manager_init(const char* db_file) {
    disk_manager_t* dm = (disk_manager_t*)malloc(sizeof(disk_manager_t));
    if (!dm) return NULL;

    dm->fd = open(db_file, O_RDWR | O_CREAT, 0644);
    if (dm->fd == -1) {
        free(dm);
        return NULL;
    }

    struct stat st;
    if (fstat(dm->fd, &st) == 0) {
        dm->num_pages = st.st_size / PAGE_SIZE;
    } else {
        dm->num_pages = 0;
    }

    pthread_rwlock_init(&dm->file_lock, NULL);
    return dm;
}

void disk_manager_close(disk_manager_t* dm) {
    if (!dm) return;
    pthread_rwlock_wrlock(&dm->file_lock);
    close(dm->fd);
    pthread_rwlock_unlock(&dm->file_lock);
    pthread_rwlock_destroy(&dm->file_lock);
    free(dm);
}

void disk_manager_read_page(disk_manager_t* dm, page_id_t page_id, uint8_t* page_data) {
    pthread_rwlock_rdlock(&dm->file_lock);
    off_t offset = (off_t)page_id * PAGE_SIZE;
    ssize_t bytes_read = pread(dm->fd, page_data, PAGE_SIZE, offset);
    if (bytes_read < PAGE_SIZE) {
        memset(page_data + (bytes_read < 0 ? 0 : bytes_read), 0, PAGE_SIZE - (bytes_read < 0 ? 0 : bytes_read));
    }
    pthread_rwlock_unlock(&dm->file_lock);
}

void disk_manager_write_page(disk_manager_t* dm, page_id_t page_id, const uint8_t* page_data) {
    pthread_rwlock_wrlock(&dm->file_lock);
    off_t offset = (off_t)page_id * PAGE_SIZE;
    ssize_t bytes_written = pwrite(dm->fd, page_data, PAGE_SIZE, offset);
    if (bytes_written != PAGE_SIZE) {
        perror("disk_manager_write_page pwrite failed");
    }
    fsync(dm->fd);
    pthread_rwlock_unlock(&dm->file_lock);
}

page_id_t disk_manager_allocate_page(disk_manager_t* dm) {
    pthread_rwlock_wrlock(&dm->file_lock);
    page_id_t new_page_id = (page_id_t)dm->num_pages++;
    uint8_t zero_page[PAGE_SIZE];
    memset(zero_page, 0, PAGE_SIZE);
    off_t offset = (off_t)new_page_id * PAGE_SIZE;
    pwrite(dm->fd, zero_page, PAGE_SIZE, offset);
    pthread_rwlock_unlock(&dm->file_lock);
    return new_page_id;
}

log_manager_t* log_manager_init(const char* wal_file) {
    log_manager_t* lm = (log_manager_t*)malloc(sizeof(log_manager_t));
    if (!lm) return NULL;

    strncpy(lm->wal_path, wal_file, sizeof(lm->wal_path) - 1);
    lm->wal_fd = open(lm->wal_path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (lm->wal_fd == -1) {
        free(lm);
        return NULL;
    }

    lm->current_lsn = 1;
    lm->flushed_lsn = 0;
    pthread_mutex_init(&lm->wal_mutex, NULL);
    return lm;
}

void log_manager_close(log_manager_t* lm) {
    if (!lm) return;
    pthread_mutex_lock(&lm->wal_mutex);
    fsync(lm->wal_fd);
    close(lm->wal_fd);
    pthread_mutex_unlock(&lm->wal_mutex);
    pthread_mutex_destroy(&lm->wal_mutex);
    free(lm);
}

lsn_t log_manager_append(log_manager_t* lm, tx_id_t tx_id, uint8_t op_type, page_id_t page_id, const void* payload, uint32_t payload_len) {
    pthread_mutex_lock(&lm->wal_mutex);
    lsn_t assigned_lsn = lm->current_lsn++;

    wal_record_header_t header;
    header.lsn = assigned_lsn;
    header.tx_id = tx_id;
    header.op_type = op_type;
    header.page_id = page_id;
    header.payload_len = payload_len;

    write(lm->wal_fd, &header, sizeof(wal_record_header_t));
    if (payload_len > 0 && payload != NULL) {
        write(lm->wal_fd, payload, payload_len);
    }
    
    pthread_mutex_unlock(&lm->wal_mutex);
    return assigned_lsn;
}

void log_manager_flush(log_manager_t* lm, lsn_t lsn) {
    pthread_mutex_lock(&lm->wal_mutex);
    if (lsn > lm->flushed_lsn) {
        fsync(lm->wal_fd);
        lm->flushed_lsn = lsn;
    }
    pthread_mutex_unlock(&lm->wal_mutex);
}

buffer_pool_manager_t* buffer_pool_manager_init(disk_manager_t* dm, log_manager_t* lm) {
    buffer_pool_manager_t* bpm = (buffer_pool_manager_t*)malloc(sizeof(buffer_pool_manager_t));
    if (!bpm) return NULL;

    bpm->disk_manager = dm;
    bpm->shutdown_flag = false;
    bpm->clock_hand = 0;
    pthread_mutex_init(&bpm->bp_mutex, NULL);
    pthread_mutex_init(&bpm->flusher_mutex, NULL);
    pthread_cond_init(&bpm->flusher_cond, NULL);

    for (int i = 0; i < HASH_TABLE_BUCKETS; i++) {
        bpm->page_table.buckets[i] = NULL;
        pthread_rwlock_init(&bpm->page_table.bucket_locks[i], NULL);
    }

    for (int i = 0; i < MAX_BUFFER_POOL_FRAMES; i++) {
        bpm->frames[i].frame_id = i;
        bpm->frames[i].page_id = INVALID_PAGE_ID;
        bpm->frames[i].pin_count = 0;
        bpm->frames[i].is_dirty = false;
        bpm->frames[i].reference_bit = false;
        bpm->frames[i].usage_count = 0;
        pthread_rwlock_init(&bpm->frames[i].rwlock, NULL);
        pthread_mutex_init(&bpm->frames[i].frame_mutex, NULL);
        memset(bpm->frames[i].data, 0, PAGE_SIZE);

        bpm->free_list[i] = i;
    }
    bpm->free_list_head = 0;

    pthread_create(&bpm->flusher_thread, NULL, background_flusher_routine, bpm);
    return bpm;
}

void buffer_pool_manager_close(buffer_pool_manager_t* bpm) {
    if (!bpm) return;

    pthread_mutex_lock(&bpm->flusher_mutex);
    bpm->shutdown_flag = true;
    pthread_cond_signal(&bpm->flusher_cond);
    pthread_mutex_unlock(&bpm->flusher_mutex);

    pthread_join(bpm->flusher_thread, NULL);

    buffer_pool_flush_all(bpm);

    for (int i = 0; i < MAX_BUFFER_POOL_FRAMES; i++) {
        pthread_rwlock_destroy(&bpm->frames[i].rwlock);
        pthread_mutex_destroy(&bpm->frames[i].frame_mutex);
    }

    for (int i = 0; i < HASH_TABLE_BUCKETS; i++) {
        pthread_rwlock_destroy(&bpm->page_table.bucket_locks[i]);
        hash_bucket_t* curr = bpm->page_table.buckets[i];
        while (curr) {
            hash_bucket_t* next = curr->next;
            free(curr);
            curr = next;
        }
    }

    pthread_