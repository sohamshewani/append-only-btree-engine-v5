#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <assert.h>

#define PAGE_SIZE 4096
#define MAX_SLOTS 256
#define BUFFER_POOL_SIZE 64
#define HASH_BUCKET_COUNT 16
#define MAX_KEY_SIZE 256
#define MAX_VAL_SIZE 1024

typedef uint64_t lsn_t;
typedef uint64_t page_id_t;
typedef uint64_t tx_id_t;

typedef enum {
    NODE_TYPE_INTERNAL = 0x01,
    NODE_TYPE_LEAF = 0x02
} node_type_t;

typedef enum {
    OP_INSERT = 1,
    OP_UPDATE,
    OP_DELETE,
    OP_NEW_PAGE
} op_type_t;

typedef struct __attribute__((__packed__)) {
    node_type_t type;
    uint16_t key_count;
    uint16_t lower_offset;
    uint16_t upper_offset;
    page_id_t next_page_id;
    page_id_t prev_page_id;
    lsn_t page_lsn;
} btree_node_header_t;

typedef struct __attribute__((__packed__)) {
    uint16_t offset;
    uint16_t length;
} slot_t;

typedef struct __attribute__((__packed__)) {
    lsn_t lsn;
    tx_id_t tx_id;
    op_type_t op_type;
    page_id_t page_id;
    uint32_t payload_len;
} wal_record_header_t;

typedef struct {
    page_id_t page_id;
    int pin_count;
    bool is_dirty;
    bool ref_bit;
    pthread_rwlock_t rwlock;
    char data[PAGE_SIZE];
} frame_meta_t;

typedef struct hash_node {
    page_id_t page_id;
    int frame_id;
    struct hash_node* next;
} hash_node_t;

typedef struct {
    hash_node_t* buckets[HASH_BUCKET_COUNT];
    pthread_rwlock_t locks[HASH_BUCKET_COUNT];
} page_table_t;

typedef struct {
    int db_fd;
    int wal_fd;
    lsn_t next_lsn;
    pthread_mutex_t wal_mutex;
    
    frame_meta_t frames[BUFFER_POOL_SIZE];
    int clock_hand;
    pthread_mutex_t bp_mutex;
    
    page_table_t page_table;
    
    pthread_t flusher_thread;
    bool flusher_running;
    pthread_cond_t flusher_cond;
    pthread_mutex_t flusher_mutex;
} db_engine_t;

static db_engine_t* g_engine = NULL;

static void panic_error(const char* msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static lsn_t wal_append(db_engine_t* engine, tx_id_t tx_id, op_type_t op_type, page_id_t page_id, const void* payload, uint32_t payload_len) {
    pthread_mutex_lock(&engine->wal_mutex);
    lsn_t current_lsn = ++engine->next_lsn;
    
    wal_record_header_t header;
    header.lsn = current_lsn;
    header.tx_id = tx_id;
    header.op_type = op_type;
    header.page_id = page_id;
    header.payload_len = payload_len;
    
    if (write(engine->wal_fd, &header, sizeof(header)) != sizeof(header)) {
        panic_error("WAL write header failed");
    }
    if (payload_len > 0 && payload != NULL) {
        if (write(engine->wal_fd, payload, payload_len) != (ssize_t)payload_len) {
            panic_error("WAL write payload failed");
        }
    }
    if (fsync(engine->wal_fd) < 0) {
        panic_error("WAL fsync failed");
    }
    
    pthread_mutex_unlock(&engine->wal_mutex);
    return current_lsn;
}

static int hash_func(page_id_t page_id) {
    return (int)(page_id % HASH_BUCKET_COUNT);
}

static int page_table_lookup(page_table_t* pt, page_id_t page_id) {
    int b = hash_func(page_id);
    pthread_rwlock_rdlock(&pt->locks[b]);
    hash_node_t* curr = pt->buckets[b];
    int frame_id = -1;
    while (curr) {
        if (curr->page_id == page_id) {
            frame_id = curr->frame_id;
            break;
        }
        curr = curr->next;
    }
    pthread_rwlock_unlock(&pt->locks[b]);
    return frame_id;
}

static void page_table_insert(page_table_t* pt, page_id_t page_id, int frame_id) {
    int b = hash_func(page_id);
    pthread_rwlock_wrlock(&pt->locks[b]);
    hash_node_t* new_node = (hash_node_t*)malloc(sizeof(hash_node_t));
    if (!new_node) panic_error("malloc hash_node failed");
    new_node->page_id = page_id;
    new_node->frame_id = frame_id;
    new_node->next = pt->buckets[b];
    pt->buckets[b] = new_node;
    pthread_rwlock_unlock(&pt->locks[b]);
}

static void page_table_remove(page_table_t* pt, page_id_t page_id) {
    int b = hash_func(page_id);
    pthread_rwlock_wrlock(&pt->locks[b]);
    hash_node_t* curr = pt->buckets[b];
    hash_node_t* prev = NULL;
    while (curr) {
        if (curr->page_id == page_id) {
            if (prev) {
                prev->next = curr->next;
            } else {
                pt->buckets[b] = curr->next;
            }
            free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_rwlock_unlock(&pt->locks[b]);
}

static int clock_evict(db_engine_t* engine) {
    pthread_mutex_lock(&engine->bp_mutex);
    int scanned = 0;
    while (scanned < BUFFER_POOL_SIZE * 2) {
        int fid = engine->clock_hand;
        engine->clock_hand = (engine->clock_hand + 1) % BUFFER_POOL_SIZE;
        
        frame_meta_t* frame = &engine->frames[fid];
        if (frame->pin_count > 0) {
            scanned++;
            continue;
        }
        if (frame->ref_bit) {
            frame->ref_bit = false;
            scanned++;
            continue;
        }
        
        if (frame->page_id != (page_id_t)-1) {
            if (frame->is_dirty) {
                off_t offset = (off_t)frame->page_id * PAGE_SIZE;
                if (pwrite(engine->db_fd, frame->data, PAGE_SIZE, offset) != PAGE_SIZE) {
                    panic_error("pwrite failed during eviction");
                }
                frame->is_dirty = false;
            }
            page_table_remove(&engine->page_table, frame->page_id);
        }
        pthread_mutex_unlock(&engine->bp_mutex);
        return fid;
    }
    pthread_mutex_unlock(&engine->bp_mutex);
    panic_error("No available frames for eviction (all pinned)");
    return -1;
}

static int buffer_pool_fetch(db_engine_t* engine, page_id_t page_id) {
    int fid = page_table_lookup(&engine->page_table, page_id);
    if (fid != -1) {
        pthread_mutex_lock(&engine->bp_mutex);
        engine->frames[fid].pin_count++;
        engine->frames[fid].ref_bit = true;
        pthread_mutex_unlock(&engine->bp_mutex);
        return fid;
    }
    
    fid = clock_evict(engine);
    
    pthread_mutex_lock(&engine->bp_mutex);
    frame_meta_t* frame = &engine->frames[fid];
    frame->page_id = page_id;
    frame->pin_count = 1;
    frame->is_dirty = false;
    frame->ref_bit = true;
    
    off_t offset = (off_t)page_id * PAGE_SIZE;
    struct stat st;
    fstat(engine->db_fd, &st);
    if (offset < st.st_size) {
        if (pread(engine->db_fd, frame->data, PAGE_SIZE, offset) != PAGE_SIZE) {
            panic_error("pread failed");
        }
    } else {
        memset(frame->data, 0, PAGE_SIZE);
    }
    pthread_mutex_unlock(&engine->bp_mutex);
    
    page_table_insert(&engine->page_table, page_id, fid);
    return fid;
}

static void buffer_pool_unpin(db_engine_t* engine, int frame_id, bool is_dirty) {
    pthread_mutex_lock(&engine->bp_mutex);
    if (is_dirty) {
        engine->frames[frame_id].is_dirty = true;
    }
    engine->frames[frame_id].pin_count--;
    if (engine->frames[frame_id].pin_count < 0) {
        panic_error("Page pin count dropped below zero");
    }
    pthread_mutex_unlock(&engine->bp_mutex);
}

static void* background_flusher_main(void* arg) {
    db_engine_t* engine = (db_engine_t*)arg;
    while (true) {
        pthread_mutex_lock(&engine->flusher_mutex);
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&engine->flusher_cond, &engine->flusher_mutex, &ts);
        if (!engine->flusher_running) {
            pthread_mutex_unlock(&engine->flusher_mutex);
            break;
        }
        pthread_mutex_unlock(&engine->flusher_mutex);
        
        pthread_mutex_lock(&engine->bp_mutex);
        for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
            frame_meta_t* frame = &engine->frames[i];
            if (frame->page_id != (page_id_t)-1 && frame->is_dirty && frame->pin_count == 0) {
                off_t offset = (off_t)frame->page_id * PAGE_SIZE;
                if (pwrite(engine->db_fd, frame->data, PAGE_SIZE, offset) != PAGE_SIZE) {
                    // Log error or panic
                } else {
                    frame->is_dirty = false;
                }
            }
        }
        fsync(engine->db_fd);
        pthread_mutex_unlock(&engine->bp_mutex);
    }
    return NULL;
}

db_engine_t* engine_init(const char* db_path, const char* wal_path) {
    db_engine_t* engine = (db_engine_t*)malloc(sizeof(db_engine_t));
    if (!engine) panic_error("engine malloc failed");
    
    engine->db_fd = open(db_path, O_RDWR | O_CREAT, 0666);
    if (engine->db_fd < 0) panic_error("open db_path failed");
    
    engine->wal_fd = open(wal_path, O_RDWR | O_CREAT | O_APPEND, 0666);
    if (engine->wal_fd < 0) panic_error("open wal_path failed");
    
    engine->next_lsn = 0;
    pthread_mutex_init(&engine->wal_mutex, NULL);
    pthread_mutex_init(&engine->bp_mutex, NULL);
    
    for (int i = 0; i < HASH_BUCKET_COUNT; i++) {
        engine->page_table.buckets[i] = NULL;
        pthread_rwlock_init(&engine->page_table.locks[i], NULL);
    }
    
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        engine->frames[i].page_id = (page_id_t)-1;
        engine->frames[i].pin_count = 0;
        engine->frames[i].is_dirty = false;
        engine->frames[i].ref_bit = false;
        pthread_rwlock_init(&engine->frames[i].rwlock, NULL);
    }
    engine->clock_hand = 0;
    
    engine->flusher_running = true;
    pthread_cond_init(&engine->flusher_cond, NULL);
    pthread_mutex_init(&engine->flusher_mutex, NULL);
    pthread_create(&engine->flusher_thread, NULL, background_flusher_main, engine);
    
    g_engine = engine;
    return engine;
}

void engine_shutdown(db_engine_t* engine) {
    pthread_mutex_lock(&engine->flusher_mutex);
    engine->flusher_running = false;
    pthread_cond_signal(&engine->flusher_cond);
    pthread_mutex_unlock(&engine->flusher_mutex);
    
    pthread_join(engine->flusher_thread, NULL);
    
    pthread_mutex_lock(&engine->bp_mutex);
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        frame_meta_t* frame = &engine->frames[i];
        if (frame->page_id != (page_id_t)-1 && frame->is_dirty) {
            off_t offset = (off_t)frame->page_id * PAGE_SIZE;
            pwrite(engine->db_fd, frame->data, PAGE_SIZE, offset);
            frame->is_dirty = false;
        }
        pthread_rwlock_destroy(&frame->rwlock);
    }
    fsync(engine->db_fd);
    pthread_mutex_unlock(&engine->bp_mutex);
    
    for (int i = 0; i < HASH_BUCKET_COUNT; i++) {
        hash_node_t* curr = engine->page_table.buckets[i];
        while (curr) {
            hash_node_t* next = curr->next;
            free(curr);
            curr = next;
        }
        pthread_rwlock_destroy(&engine->page_table.locks[i]);
    }
    
    pthread_mutex_destroy(&engine->wal_mutex);
    pthread_mutex_destroy(&engine->bp_mutex);
    pthread_cond_destroy(&engine->flusher_cond);
    pthread_mutex_destroy(&engine->flusher_mutex);
    
    close(engine->db_fd);
    close(engine->wal_fd);
    free(engine);
}

page_id_t btree_create_node(db_engine_t* engine, node_type_t type, tx_id_t tx_id) {
    struct stat st;
    fstat(engine->db_fd, &