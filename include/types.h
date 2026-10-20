#ifndef INCLUDE_TYPES_H
#define INCLUDE_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/types.h>

#define PAGE_SIZE 4096
#define MAX_KEY_SIZE 512
#define MAX_VALUE_SIZE 1024

typedef uint64_t page_id_t;
typedef uint64_t lsn_t;
typedef uint64_t tx_id_t;
typedef uint16_t slot_offset_t;

#define INVALID_PAGE_ID ((page_id_t)0)
#define INVALID_LSN     ((lsn_t)0)
#define INVALID_TX_ID   ((tx_id_t)0)

typedef enum {
    NODE_TYPE_INVALID = 0,
    NODE_TYPE_INTERNAL = 1,
    NODE_TYPE_LEAF = 2
} node_type_t;

typedef enum {
    OP_INVALID = 0,
    OP_INSERT = 1,
    OP_UPDATE = 2,
    OP_DELETE = 3,
    OP_BEGIN = 4,
    OP_COMMIT = 5,
    OP_ABORT = 6,
    OP_CHECKPOINT = 7
} operation_type_t;

typedef struct __attribute__((__packed__)) {
    node_type_t type;
    uint16_t key_count;
    slot_offset_t lower_offset;
    slot_offset_t upper_offset;
    page_id_t right_sibling_id;
    page_id_t left_sibling_id;
    lsn_t page_lsn;
} btree_node_header_t;

typedef struct __attribute__((__packed__)) {
    slot_offset_t offset;
    uint16_t length;
} slot_t;

typedef struct __attribute__((__packed__)) {
    btree_node_header_t header;
    /* Slotted array of slots growing downwards, payloads growing upwards */
    uint8_t data[PAGE_SIZE - sizeof(btree_node_header_t)];
} disk_page_t;

typedef struct {
    page_id_t page_id;
    uint32_t frame_id;
    int32_t pin_count;
    bool is_dirty;
    bool referenced;
    uint32_t usage_count; /* For clock-sweep / LRU-K approximation */
    pthread_rwlock_t rwlock;
    pthread_mutex_t frame_mutex;
} frame_meta_t;

typedef struct __attribute__((__packed__)) {
    lsn_t lsn;
    lsn_t prev_lsn;
    tx_id_t tx_id;
    operation_type_t op_type;
    page_id_t page_id;
    uint32_t undo_next_lsn;
    uint32_t payload_length;
} wal_record_header_t;

typedef struct __attribute__((__packed__)) {
    wal_record_header_t header;
    uint8_t payloads[PAGE_SIZE - sizeof(wal_record_header_t)];
} wal_record_t;

typedef struct {
    int log_fd;
    lsn_t committed_lsn;
    lsn_t flushed_lsn;
    pthread_mutex_t log_mutex;
} wal_manager_t;

typedef struct {
    disk_page_t *pages; /* Array of frames allocated via posix_memalign */
    frame_meta_t *metas;
    uint32_t pool_size;
    pthread_rwlock_t *hash_shards;
    uint32_t num_hash_shards;
    wal_manager_t *wal_mgr;
    pthread_t flusher_thread;
    bool shutdown_flag;
    pthread_mutex_t flusher_mutex;
    pthread_cond_t flusher_cond;
} buffer_pool_manager_t;

typedef struct {
    page_id_t root_page_id;
    buffer_pool_manager_t *bpm;
    pthread_rwlock_t tree_lock;
} btree_index_t;

#endif /* INCLUDE_TYPES_H */