#include "buffer_pool.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/uio.h>

/* Hash function for PageId to sharded buffer pool hash table */
static inline uint32_t hash_page_id(page_id_t page_id) {
    uint32_t hash = (uint32_t)page_id;
    hash = ((hash >> 16) ^ hash) * 0x45d9f3b;
    hash = ((hash >> 16) ^ hash) * 0x45d9f3b;
    hash = (hash >> 16) ^ hash;
    return hash;
}

int buffer_pool_init(BufferPool *bp, const char *db_file_path, size_t pool_size) {
    if (!bp || pool_size == 0 || !db_file_path) {
        return -EINVAL;
    }

    memset(bp, 0, sizeof(BufferPool));
    bp->pool_size = pool_size;

    bp->fd = open(db_file_path, O_RDWR | O_CREAT, 0644);
    if (bp->fd < 0) {
        return -errno;
    }

    bp->frames = (Frame *)malloc(pool_size * sizeof(Frame));
    if (!bp->frames) {
        close(bp->fd);
        return -ENOMEM;
    }

    for (size_t i = 0; i < pool_size; ++i) {
        Frame *frame = &bp->frames[i];
        frame.frame_id = (frame_id_t)i;
        frame.page_id = INVALID_PAGE_ID;
        frame.pin_count = 0;
        frame.is_dirty = 0;
        frame.reference_bit = 0;
        frame.access_count = 0;
        
        int rc = posix_memalign((void **)&frame.page_data, PAGE_SIZE, PAGE_SIZE);
        if (rc != 0) {
            for (size_t j = 0; j < i; ++j) {
                free(bp->frames[j].page_data);
            }
            free(bp->frames);
            close(bp->fd);
            return -ENOMEM;
        }
        memset(frame.page_data, 0, PAGE_SIZE);

        if (pthread_mutex_init(&frame.frame_mutex, NULL) != 0) {
            free(frame.page_data);
            for (size_t j = 0; j < i; ++j) {
                pthread_mutex_destroy(&bp->frames[j].frame_mutex);
                free(bp->frames[j].page_data);
            }
            free(bp->frames);
            close(bp->fd);
            return -EAGAIN;
        }
    }

    bp->hash_table = (HashBucket *)calloc(HASH_TABLE_BUCKETS, sizeof(HashBucket));
    if (!bp->hash_table) {
        for (size_t i = 0; i < pool_size; ++i) {
            pthread_mutex_destroy(&bp->frames[i].frame_mutex);
            free(bp->frames[i].page_data);
        }
        free(bp->frames);
        close(bp->fd);
        return -ENOMEM;
    }

    for (int i = 0; i < HASH_TABLE_BUCKETS; ++i) {
        if (pthread_rwlock_init(&bp->hash_table[i].lock, NULL) != 0) {
            for (int j = 0; j < i; ++j) {
                pthread_rwlock_destroy(&bp->hash_table[j].lock);
            }
            free(bp->hash_table);
            for (size_t k = 0; k < pool_size; ++k) {
                pthread_mutex_destroy(&bp->frames[k].frame_mutex);
                free(bp->frames[k].page_data);
            }
            free(bp->frames);
            close(bp->fd);
            return -EAGAIN;
        }
        bp->hash_table[i].head = NULL;
    }

    if (pthread_mutex_init(&bp->replacer_mutex, NULL) != 0) {
        for (int i = 0; i < HASH_TABLE_BUCKETS; ++i) {
            pthread_rwlock_destroy(&bp->hash_table[i].lock);
        }
        free(bp->hash_table);
        for (size_t k = 0; k < pool_size; ++k) {
            pthread_mutex_destroy(&bp->frames[k].frame_mutex);
            free(bp->frames[k].page_data);
        }
        free(bp->frames);
        close(bp->fd);
        return -EAGAIN;
    }

    bp->clock_hand = 0;
    bp->free_list = (frame_id_t *)malloc(pool_size * sizeof(frame_id_t));
    if (!bp->free_list) {
        pthread_mutex_destroy(&bp->replacer_mutex);
        for (int i = 0; i < HASH_TABLE_BUCKETS; ++i) {
            pthread_rwlock_destroy(&bp->hash_table[i].lock);
        }
        free(bp->hash_table);
        for (size_t k = 0; k < pool_size; ++k) {
            pthread_mutex_destroy(&bp->frames[k].frame_mutex);
            free(bp->frames[k].page_data);
        }
        free(bp->frames);
        close(bp->fd);
        return -ENOMEM;
    }

    bp->free_list_size = pool_size;
    for (size_t i = 0; i < pool_size; ++i) {
        bp->free_list[i] = (frame_id_t)i;
    }

    bp->shutdown_flag = 0;
    if (pthread_mutex_init(&bp->flusher_mutex, NULL) != 0) {
        free(bp->free_list);
        pthread_mutex_destroy(&bp->replacer_mutex);
        for (int i = 0; i < HASH_TABLE_BUCKETS; ++i) {
            pthread_rwlock_destroy(&bp->hash_table[i].lock);
        }
        free(bp->hash_table);
        for (size_t k = 0; k < pool_size; ++k) {
            pthread_mutex_destroy(&bp->frames[k].frame_mutex);
            free(bp->frames[k].page_data);
        }
        free(bp->frames);
        close(bp->fd);
        return -EAGAIN;
    }

    if (pthread_cond_init(&bp->flusher_cond, NULL) != 0) {
        pthread_mutex_destroy(&bp->flusher_mutex);
        free(bp->free_list);
        pthread_mutex_destroy(&bp->replacer_mutex);
        for (int i = 0; i < HASH_TABLE_BUCKETS; ++i) {
            pthread_rwlock_destroy(&bp->hash_table[i].lock);
        }
        free(bp->hash_table);
        for (size_t k = 0; k < pool_size; ++k) {
            pthread_mutex_destroy(&bp->frames[k].frame_mutex);
            free(bp->frames[k].page_data);
        }
        free(bp->frames);
        close(bp->fd);
        return -EAGAIN;
    }

    if (pthread_create(&bp->flusher_thread, NULL, background_flusher_thread, bp) != 0) {
        pthread_cond_destroy(&bp->flusher_cond);
        pthread_mutex_destroy(&bp->flusher_mutex);
        free(bp->free_list);
        pthread_mutex_destroy(&bp->replacer_mutex);
        for (int i = 0; i < HASH_TABLE_BUCKETS; ++i) {
            pthread_rwlock_destroy(&bp->hash_table[i].lock);
        }
        free(bp->hash_table);
        for (size_t k = 0; k < pool_size; ++k) {
            pthread_mutex_destroy(&bp->frames[k].frame_mutex);
            free(bp->frames[k].page_data);
        }
        free(bp->frames);
        close(bp->fd);
        return -EAGAIN;
    }

    return 0;
}

int buffer_pool_shutdown(BufferPool *bp) {
    if (!bp) {
        return -EINVAL;
    }

    pthread_mutex_lock(&bp->flusher_mutex);
    bp->shutdown_flag = 1;
    pthread_cond_signal(&bp->flusher_cond);
    pthread_mutex_unlock(&bp->flusher_mutex);

    pthread_join(bp->flusher_thread, NULL);

    buffer_pool_flush_all(bp);

    pthread_cond_destroy(&bp->flusher_cond);
    pthread_mutex_destroy(&bp->flusher_mutex);
    pthread_mutex_destroy(&bp->replacer_mutex);

    for (int i = 0; i < HASH_TABLE_BUCKETS; ++i) {
        pthread_rwlock_wrlock(&bp->hash_table[i].lock);
        HashNode *curr = bp->hash_table[i].head;
        while (curr) {
            HashNode *next = curr->next;
            free(curr);
            curr = next;
        }
        pthread_rwlock_unlock(&bp->hash_table[i].lock);
        pthread_rwlock_destroy(&bp->hash_table[i].lock);
    }
    free(bp->hash_table);

    for (size_t i = 0; i < bp->pool_size; ++i) {
        pthread_mutex_destroy(&bp->frames[i].frame_mutex);
        free(bp->frames[i].page_data);
    }
    free(bp->frames);
    free(bp->free_list);

    if (bp->fd >= 0) {
        close(bp->fd);
        bp->fd = -1;
    }

    return 0;
}

static HashNode *hash_table_find(BufferPool *bp, uint32_t bucket_idx, page_id_t page_id) {
    HashNode *curr = bp->hash_table[bucket_idx].head;
    while (curr) {
        if (curr->page_id == page_id) {
            return curr;
        }
        curr = curr->next;
    }
    return NULL;
}

static int hash_table_insert(BufferPool *bp, uint32_t bucket_idx, page_id_t page_id, frame_id_t frame_id) {
    HashNode *node = (HashNode *)malloc(sizeof(HashNode));
    if (!node) {
        return -ENOMEM;
    }
    node->page_id = page_id;
    node->frame_id = frame_id;
    node->next = bp->hash_table[bucket_idx].head;
    bp->hash_table[bucket_idx].head = node;
    return 0;
}

static void hash_table_remove(BufferPool *bp, uint32_t bucket_idx, page_id_t page_id) {
    HashNode *curr = bp->hash_table[bucket_idx].head;
    HashNode *prev = NULL;
    while (curr) {
        if (curr->page_id == page_id) {
            if (prev) {
                prev->next = curr->next;
            } else {
                bp->hash_table[bucket_idx].head = curr->next;
            }
            free(curr);
            return;
        }
        prev = curr;
        curr = curr->next;
    }
}

static frame_id_t clock_replacer_victim(BufferPool *bp) {
    pthread_mutex_lock(&bp->replacer_mutex);
    size_t scanned = 0;
    size_t total_frames = bp->pool_size;

    while (scanned < total_frames * 2) {
        Frame *frame = &bp->frames[bp->clock_hand];
        
        pthread_mutex_lock(&frame->frame_mutex);
        if (frame->pin_count == 0) {
            if (frame->reference_bit == 0) {
                frame_id_t victim = bp->clock_hand;
                bp->clock_hand = (bp->clock_hand + 1) % total_frames;
                pthread_mutex_unlock(&frame->frame_mutex);
                pthread_mutex_unlock(&bp->replacer_mutex);
                return victim;
            } else {
                frame->reference_bit = 0;
            }
        }
        pthread_mutex_unlock(&frame->frame_mutex);

        bp->clock_hand = (bp->clock_hand + 1) % total_frames;
        scanned++;
    }

    pthread_mutex_unlock(&bp->replacer_mutex);
    return INVALID_FRAME_ID;
}

Page *buffer_pool_fetch_page(BufferPool *bp, page_id_t page_id) {
    if (!bp || page_id == INVALID_PAGE_ID) {
        return NULL;
    }

    uint32_t bucket_idx = hash_page_id(page_id) % HASH_TABLE_BUCKETS;

    pthread_rwlock_wrlock(&bp->hash_table[bucket_idx].lock);
    HashNode *node = hash_table_find(bp, bucket_idx, page_id);
    if (node) {
        Frame *frame = &bp->frames[node->frame_id];
        pthread_mutex_lock(&frame->frame_mutex);
        pthread_rwlock_unlock(&bp->hash_table[bucket_idx].lock);

        frame->pin_count++;
        frame->reference_bit = 1;
        frame->access_count++;

        Page *page = (Page *)frame->page_data;
        pthread_mutex_unlock(&frame->frame_mutex);
        return page;
    }

    frame_id_t target_frame_id = INVALID_FRAME_ID;

    pthread_mutex_lock(&bp->replacer_mutex);
    if (bp->free_list_size > 0) {
        target_frame_id = bp->free_list[--bp->free_list_size];
        pthread_mutex_unlock(&bp->replacer_mutex);
    } else {
        pthread_mutex_unlock(&bp->replacer_mutex);
        target_frame_id = clock_replacer_victim(bp);
        if (target_frame_id == INVALID_FRAME_ID) {
            pthread_rwlock_unlock(&bp->hash_table[bucket_idx].lock);
            return NULL;
        }
    }

    Frame *target_frame = &bp->frames[target_frame_id];
    pthread_mutex_lock(&target_frame->frame_mutex);

    if (target_frame->page_id != INVALID_PAGE_ID) {
        page_id_t old_page_id = target_frame->page_id;
        uint32_t old_bucket_idx = hash_page_id(old_page_id) % HASH_TABLE_BUCKETS;

        if (old_bucket_idx == bucket_idx) {
            if (target_frame->is_dirty) {
                off_t offset = (off_t)old_page_id * PAGE_SIZE;
                pwrite(bp->fd, target_frame->page_data, PAGE_SIZE, offset);
                target_frame->is_dirty = 0;
            }
            hash_table_remove(bp, bucket_idx, old_page_id);
        } else {
            pthread_rwlock_unlock(&bp->hash_table[bucket_idx].lock);
            pthread_rwlock_wrlock(&bp->hash_table[old_bucket_idx].lock);

            if (target_frame->is_dirty) {
                off_t offset = (off_t)old_page_id * PAGE_SIZE;
                pwrite(