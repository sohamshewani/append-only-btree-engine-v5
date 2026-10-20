#include "btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>

#define PAGE_SIZE 4096
#define MAX_KEY_SIZE 512
#define MAX_VAL_SIZE 1024

typedef uint32_t page_id_t;
typedef uint64_t lsn_t;

typedef enum {
    NODE_TYPE_INTERNAL = 0x01,
    NODE_TYPE_LEAF = 0x02
} node_type_t;

typedef struct __attribute__((__packed__)) {
    uint8_t type;
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

typedef struct {
    int fd;
    pthread_rwlock_t rwlock;
    uint32_t num_pages;
} disk_manager_t;

typedef struct {
    page_id_t page_id;
    int pin_count;
    bool is_dirty;
    uint8_t ref_bit;
    pthread_rwlock_t rwlock;
    pthread_mutex_t latch;
    char data[PAGE_SIZE];
} frame_t;

typedef struct {
    frame_t *frames;
    size_t pool_size;
    disk_manager_t *disk_mgr;
    size_t clock_hand;
    pthread_mutex_t mutex;
} buffer_pool_manager_t;

typedef struct {
    buffer_pool_manager_t *bpm;
    page_id_t root_page_id;
    pthread_rwlock_t tree_lock;
} btree_t;

static disk_manager_t* disk_manager_init(const char *path) {
    int fd = open(path, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        return NULL;
    }
    disk_manager_t *dm = (disk_manager_t*)malloc(sizeof(disk_manager_t));
    if (!dm) {
        close(fd);
        return NULL;
    }
    dm->fd = fd;
    pthread_rwlock_init(&dm->rwlock, NULL);
    off_t size = lseek(fd, 0, SEEK_END);
    dm->num_pages = (uint32_t)(size / PAGE_SIZE);
    return dm;
}

static void disk_manager_close(disk_manager_t *dm) {
    if (!dm) return;
    pthread_rwlock_destroy(&dm->rwlock);
    close(dm->fd);
    free(dm);
}

static page_id_t disk_manager_allocate_page(disk_manager_t *dm) {
    pthread_rwlock_wrlock(&dm->rwlock);
    page_id_t new_page_id = dm->num_pages++;
    char zero_page[PAGE_SIZE];
    memset(zero_page, 0, PAGE_SIZE);
    pwrite(dm->fd, zero_page, PAGE_SIZE, (off_t)new_page_id * PAGE_SIZE);
    pthread_rwlock_unlock(&dm->rwlock);
    return new_page_id;
}

static void disk_manager_read_page(disk_manager_t *dm, page_id_t page_id, char *page_data) {
    pthread_rwlock_rdlock(&dm->rwlock);
    pread(dm->fd, page_data, PAGE_SIZE, (off_t)page_id * PAGE_SIZE);
    pthread_rwlock_unlock(&dm->rwlock);
}

static void disk_manager_write_page(disk_manager_t *dm, page_id_t page_id, const char *page_data) {
    pthread_rwlock_wrlock(&dm->rwlock);
    pwrite(dm->fd, page_data, PAGE_SIZE, (off_t)page_id * PAGE_SIZE);
    pthread_rwlock_unlock(&dm->rwlock);
}

buffer_pool_manager_t* buffer_pool_manager_init(disk_manager_t *dm, size_t pool_size) {
    buffer_pool_manager_t *bpm = (buffer_pool_manager_t*)malloc(sizeof(buffer_pool_manager_t));
    if (!bpm) return NULL;
    bpm->disk_mgr = dm;
    bpm->pool_size = pool_size;
    bpm->clock_hand = 0;
    pthread_mutex_init(&bpm->mutex, NULL);
    bpm->frames = (frame_t*)malloc(sizeof(frame_t) * pool_size);
    if (!bpm->frames) {
        free(bpm);
        return NULL;
    }
    for (size_t i = 0; i < pool_size; ++i) {
        bpm->frames[i].page_id = UINT32_MAX;
        bpm->frames[i].pin_count = 0;
        bpm->frames[i].is_dirty = false;
        bpm->frames[i].ref_bit = 0;
        pthread_rwlock_init(&bpm->frames[i].rwlock, NULL);
        pthread_mutex_init(&bpm->frames[i].latch, NULL);
        memset(bpm->frames[i].data, 0, PAGE_SIZE);
    }
    return bpm;
}

void buffer_pool_manager_close(buffer_pool_manager_t *bpm) {
    if (!bpm) return;
    pthread_mutex_lock(&bpm->mutex);
    for (size_t i = 0; i < bpm->pool_size; ++i) {
        if (bpm->frames[i].page_id != UINT32_MAX && bpm->frames[i].is_dirty) {
            disk_manager_write_page(bpm->disk_mgr, bpm->frames[i].page_id, bpm->frames[i].data);
        }
        pthread_rwlock_destroy(&bpm->frames[i].rwlock);
        pthread_mutex_destroy(&bpm->frames[i].latch);
    }
    free(bpm->frames);
    pthread_mutex_unlock(&bpm->mutex);
    pthread_mutex_destroy(&bpm->mutex);
    free(bpm);
}

static int buffer_pool_victim(buffer_pool_manager_t *bpm) {
    size_t checked = 0;
    while (checked < bpm->pool_size * 2) {
        size_t idx = bpm->clock_hand;
        frame_t *frame = &bpm->frames[idx];
        if (frame->pin_count == 0) {
            if (frame->ref_bit == 0) {
                bpm->clock_hand = (bpm->clock_hand + 1) % bpm->pool_size;
                return (int)idx;
            } else {
                frame->ref_bit = 0;
            }
        }
        bpm->clock_hand = (bpm->clock_hand + 1) % bpm->pool_size;
        checked++;
    }
    for (size_t i = 0; i < bpm->pool_size; ++i) {
        if (bpm->frames[i].pin_count == 0) {
            return (int)i;
        }
    }
    return -1;
}

frame_t* buffer_pool_manager_fetch_page(buffer_pool_manager_t *bpm, page_id_t page_id) {
    pthread_mutex_lock(&bpm->mutex);
    for (size_t i = 0; i < bpm->pool_size; ++i) {
        if (bpm->frames[i].page_id == page_id) {
            bpm->frames[i].pin_count++;
            bpm->frames[i].ref_bit = 1;
            pthread_mutex_unlock(&bpm->mutex);
            return &bpm->frames[i];
        }
    }
    int victim_idx = buffer_pool_victim(bpm);
    if (victim_idx == -1) {
        pthread_mutex_unlock(&bpm->mutex);
        return NULL;
    }
    frame_t *frame = &bpm->frames[victim_idx];
    if (frame->page_id != UINT32_MAX) {
        if (frame->is_dirty) {
            disk_manager_write_page(bpm->disk_mgr, frame->page_id, frame->data);
            frame->is_dirty = false;
        }
    }
    frame->page_id = page_id;
    frame->pin_count = 1;
    frame->ref_bit = 1;
    frame->is_dirty = false;
    disk_manager_read_page(bpm->disk_mgr, page_id, frame->data);
    pthread_mutex_unlock(&bpm->mutex);
    return frame;
}

void buffer_pool_manager_unpin_page(buffer_pool_manager_t *bpm, page_id_t page_id, bool is_dirty) {
    pthread_mutex_lock(&bpm->mutex);
    for (size_t i = 0; i < bpm->pool_size; ++i) {
        if (bpm->frames[i].page_id == page_id) {
            if (is_dirty) {
                bpm->frames[i].is_dirty = true;
            }
            if (bpm->frames[i].pin_count > 0) {
                bpm->frames[i].pin_count--;
            }
            pthread_mutex_unlock(&bpm->mutex);
            return;
        }
    }
    pthread_mutex_unlock(&bpm->mutex);
}

frame_t* buffer_pool_manager_new_page(buffer_pool_manager_t *bpm, page_id_t *page_id) {
    pthread_mutex_lock(&bpm->mutex);
    page_id_t new_pid = disk_manager_allocate_page(bpm->disk_mgr);
    int victim_idx = buffer_pool_victim(bpm);
    if (victim_idx == -1) {
        pthread_mutex_unlock(&bpm->mutex);
        return NULL;
    }
    frame_t *frame = &bpm->frames[victim_idx];
    if (frame->page_id != UINT32_MAX) {
        if (frame->is_dirty) {
            disk_manager_write_page(bpm->disk_mgr, frame->page_id, frame->data);
            frame->is_dirty = false;
        }
    }
    frame->page_id = new_pid;
    frame->pin_count = 1;
    frame->ref_bit = 1;
    frame->is_dirty = true;
    memset(frame->data, 0, PAGE_SIZE);
    *page_id = new_pid;
    pthread_mutex_unlock(&bpm->mutex);
    return frame;
}

btree_t* btree_init(buffer_pool_manager_t *bpm) {
    btree_t *tree = (btree_t*)malloc(sizeof(btree_t));
    if (!tree) return NULL;
    tree->bpm = bpm;
    pthread_rwlock_init(&tree->tree_lock, NULL);

    page_id_t root_pid;
    frame_t *root_frame = buffer_pool_manager_new_page(bpm, &root_pid);
    if (!root_frame) {
        free(tree);
        return NULL;
    }
    tree->root_page_id = root_pid;

    btree_node_header_t *header = (btree_node_header_t*)root_frame->data;
    header->type = NODE_TYPE_LEAF;
    header->key_count = 0;
    header->lower_offset = sizeof(btree_node_header_t);
    header->upper_offset = PAGE_SIZE;
    header->next_page_id = UINT32_MAX;
    header->prev_page_id = UINT32_MAX;
    header->page_lsn = 0;

    buffer_pool_manager_unpin_page(bpm, root_pid, true);
    return tree;
}

void btree_close(btree_t *tree) {
    if (!tree) return;
    pthread_rwlock_destroy(&tree->tree_lock);
    free(tree);
}

static bool btree_node_insert(frame_t *frame, const char *key, uint16_t key_len, const char *val, uint16_t val_len) {
    btree_node_header_t *header = (btree_node_header_t*)frame->data;
    uint32_t needed_space = sizeof(slot_t) + key_len + val_len;
    uint32_t free_space = header->upper_offset - header->lower_offset;
    if (free_space < needed_space) {
        return false;
    }

    uint16_t new_upper = header->upper_offset - (key_len + val_len);
    memcpy(frame->data + new_upper, key, key_len);
    memcpy(frame->data + new_upper + key_len, val, val_len);

    slot_t *slots = (slot_t*)(frame->data + header->lower_offset);
    uint32_t insert_idx = header->key_count;

    for (uint32_t i = 0; i < header->key_count; ++i) {
        char *k_ptr = frame->data + slots[i].offset;
        uint16_t k_len = slots[i].length;
        if (k_len == key_len && memcmp(k_ptr, key, key_len) == 0) {
            header->upper_offset = new_upper;
            slots[i].offset = new_upper;
            slots[i].length = key_len + val_len;
            return true;
        }
    }

    slots[insert_idx].offset = new_upper;
    slots[insert_idx].length = key_len + val_len;

    header->lower_offset += sizeof(slot_t);
    header->upper_offset = new_upper;
    header->key_count++;
    return true;
}

bool btree_insert(btree_t *tree, const char *key, uint16_t key_len, const char *val, uint16_t val_len) {
    if (key_len > MAX_KEY_SIZE || val_len > MAX_VAL_SIZE) return false;
    pthread_rwlock_wrlock(&tree->tree_lock);

    frame_t *frame = buffer_pool_manager_fetch_page(tree->bpm, tree->root_page_id);
    if (!frame) {
        pthread_rwlock_unlock(&tree->tree_lock);
        return false;
    }

    bool success = btree_node_insert(frame, key, key_len, val, val_len);
    if (!success) {
        buffer_pool_manager_unpin_page(tree->bpm, tree->root_page_id, false);
        pthread_rwlock_unlock(&tree->tree_lock);
        return false;
    }

    buffer_pool_manager_unpin_page(tree->bpm, tree->root_page_id, true);
    pthread_rwlock_unlock(&tree->tree_lock);
    return true;
}

bool btree_search(btree_t *tree, const char *key, uint16_t key_len, char *val_out, uint16_t *val_len_out) {
    pthread_rwlock_rdlock(&tree->tree_lock);

    frame_t *frame = buffer_pool_manager_fetch_page(tree->bpm, tree->root_page_id);
    if (!frame) {
        pthread_rwlock_unlock(&tree->tree_lock);
        return false;
    }

    btree_node_header_t *header = (btree_node_header_t*)frame->data;
    slot_t *