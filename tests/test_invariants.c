#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

/* ========================================================================= *
 * APPEND-ONLY B-TREE ENGINE (Embedded Single-Header Implementation)           *
 * ========================================================================= */

#define BTREE_PAGE_SIZE    4096
#define BTREE_MAX_KEYS     63   /* Must fit within a single page with children */
#define BTREE_MAGIC        0x4254524559303031ULL /* "BTREY001" */

typedef uint64_t btree_lsn_t;
typedef uint64_t btree_key_t;
typedef uint64_t btree_addr_t;

typedef struct {
    uint64_t magic;
    uint32_t version;
    uint32_t flags;
    btree_addr_t root_addr;
    btree_addr_t freelist_addr;
    uint64_t next_lsn;
    uint32_t page_size;
    uint8_t reserved[4016];
} __attribute__((packed)) btree_superblock_t;

typedef struct {
    uint32_t is_leaf;
    uint32_t num_keys;
    btree_addr_t self_addr;
    btree_addr_t next_leaf; /* For range scans */
    btree_key_t keys[BTREE_MAX_KEYS];
    union {
        btree_addr_t children[BTREE_MAX_KEYS + 1];
        uint64_t values[BTREE_MAX_KEYS];
    } data;
} __attribute__((packed)) btree_node_t;

typedef struct {
    int fd;
    char *path;
    btree_addr_t file_size;
    pthread_rwlock_t rwlock;
    btree_superblock_t sb;
    bool dirty_sb;
} btree_db_t;

typedef struct {
    bool found;
    uint64_t value;
    btree_addr_t leaf_addr;
    int key_index;
} btree_get_result_t;

/* Forward declarations */
static btree_addr_t btree_allocate_page(btree_db_t *db);
static int btree_write_page(btree_db_t *db, btree_addr_t addr, void *page_data);
static int btree_read_page(btree_db_t *db, btree_addr_t addr, void *page_data);

static btree_addr_t btree_allocate_page(btree_db_t *db) {
    btree_addr_t addr = db->file_size;
    db->file_size += BTREE_PAGE_SIZE;
    if (ftruncate(db->fd, db->file_size) < 0) {
        perror("ftruncate");
        exit(1);
    }
    return addr;
}

static int btree_write_page(btree_db_t *db, btree_addr_t addr, void *page_data) {
    if (pwrite(db->fd, page_data, BTREE_PAGE_SIZE, addr) != BTREE_PAGE_SIZE) {
        return -1;
    }
    return 0;
}

static int btree_read_page(btree_db_t *db, btree_addr_t addr, void *page_data) {
    if (pread(db->fd, page_data, BTREE_PAGE_SIZE, addr) != BTREE_PAGE_SIZE) {
        return -1;
    }
    return 0;
}

btree_db_t *btree_open(const char *path) {
    btree_db_t *db = calloc(1, sizeof(btree_db_t));
    db->path = strdup(path);
    pthread_rwlock_init(&db->rwlock, NULL);

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open");
        free(db->path);
        free(db);
        return NULL;
    }
    db->fd = fd;

    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("fstat");
        close(fd);
        free(db->path);
        free(db);
        return NULL;
    }

    if (st.st_size == 0) {
        /* Initialize new database */
        db->file_size = BTREE_PAGE_SIZE * 2; /* SB + Root */
        if (ftruncate(fd, db->file_size) < 0) {
            perror("ftruncate init");
            close(fd);
            free(db->path);
            free(db);
            return NULL;
        }

        db->sb.magic = BTREE_MAGIC;
        db->sb.version = 1;
        db->sb.page_size = BTREE_PAGE_SIZE;
        db->sb.root_addr = BTREE_PAGE_SIZE;
        db->sb.next_lsn = 1;

        /* Create initial root leaf node */
        btree_node_t root;
        memset(&root, 0, sizeof(root));
        root.is_leaf = 1;
        root.num_keys = 0;
        root.self_addr = BTREE_PAGE_SIZE;
        root.next_leaf = 0;

        btree_write_page(db, db->sb.root_addr, &root);
        btree_write_page(db, 0, &db->sb);
    } else {
        db->file_size = st.st_size;
        if (btree_read_page(db, 0, &db->sb) < 0 || db->sb.magic != BTREE_MAGIC) {
            fprintf(stderr, "Invalid database or corrupted superblock\n");
            close(fd);
            free(db->path);
            free(db);
            return NULL;
        }
    }
    return db;
}

void btree_close(btree_db_t *db) {
    if (!db) return;
    pthread_rwlock_wrlock(&db->rwlock);
    if (db->dirty_sb) {
        btree_write_page(db, 0, &db->sb);
    }
    close(db->fd);
    pthread_rwlock_unlock(&db->rwlock);
    pthread_rwlock_destroy(&db->rwlock);
    free(db->path);
    free(db);
}

static int btree_binary_search(const btree_node_t *node, btree_key_t key) {
    int low = 0, high = (int)node->num_keys - 1;
    while (low <= high) {
        int mid = low + (high - low) / 2;
        if (node->keys[mid] == key) {
            return mid;
        } else if (node->keys[mid] < key) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    return -low - 1; /* Insertion point */
}

btree_get_result_t btree_get(btree_db_t *db, btree_key_t key) {
    btree_get_result_t res = {0};
    pthread_rwlock_rdlock(&db->rwlock);

    btree_addr_t curr_addr = db->sb.root_addr;
    btree_node_t node;

    while (1) {
        if (btree_read_page(db, curr_addr, &node) < 0) {
            pthread_rwlock_unlock(&db->rwlock);
            return res;
        }

        int idx = btree_binary_search(&node, key);
        if (node.is_leaf) {
            if (idx >= 0) {
                res.found = true;
                res.value = node.data.values[idx];
                res.leaf_addr = curr_addr;
                res.key_index = idx;
            }
            break;
        } else {
            if (idx >= 0) {
                /* Exact match in internal node, internal nodes store keys and right-children.
                 * Standard convention: if found at idx, go to child idx + 1 */
                curr_addr = node.data.children[idx + 1];
            } else {
                int insertion_point = -idx - 1;
                curr_addr = node.data.children[insertion_point];
            }
        }
    }

    pthread_rwlock_unlock(&db->rwlock);
    return res;
}

typedef struct {
    bool split;
    btree_key_t promoted_key;
    btree_addr_t new_child_addr;
} btree_insert_result_t;

static btree_insert_result_t btree_insert_recursive(btree_db_t *db, btree_addr_t addr, btree_key_t key, uint64_t value) {
    btree_node_t node;
    btree_read_page(db, addr, &node);

    btree_insert_result_t res = {0};

    int idx = btree_binary_search(&node, key);
    if (node.is_leaf) {
        if (idx >= 0) {
            /* Update existing key */
            node.data.values[idx] = value;
            btree_write_page(db, addr, &node);
            return res; /* No split */
        }

        int insert_idx = -idx - 1;
        if (node.num_keys < BTREE_MAX_KEYS) {
            /* Shift keys and values */
            for (int i = (int)node.num_keys; i > insert_idx; i--) {
                node.keys[i] = node.keys[i - 1];
                node.data.values[i] = node.data.values[i - 1];
            }
            node.keys[insert_idx] = key;
            node.data.values[insert_idx] = value;
            node.num_keys++;
            btree_write_page(db, addr, &node);
            return res;
        } else {
            /* Split leaf node */
            btree_node_t new_node;
            memset(&new_node, 0, sizeof(new_node));
            new_node.is_leaf = 1;
            new_node.self_addr = btree_allocate_page(db);
            new_node.next_leaf = node.next_leaf;
            node.next_leaf = new_node.self_addr;

            int mid = (BTREE_MAX_KEYS + 1) / 2;
            
            /* Combine current node keys, new key into a temporary buffer logically */
            /* To keep it robust, let's assemble all keys into a temp array */
            btree_key_t temp_keys[BTREE_MAX_KEYS + 1];
            uint64_t temp_vals[BTREE_MAX_KEYS + 1];
            
            int c = 0;
            for (int i = 0; i < node.num_keys; i++) {
                if (c == insert_idx) {
                    temp_keys[c] = key;
                    temp_vals[c] = value;
                    c++;
                }
                temp_keys[c] = node.keys[i];
                temp_vals[c] = node.data.values[i];
                c++;
            }
            if (c == insert_idx) {
                temp_keys[c] = key;
                temp_vals[c] = value;
                c++;
            }

            node.num_keys = mid;
            new_node.num_keys = (BTREE_MAX_KEYS + 1) - mid;

            for (int i = 0; i < node.num_keys; i++)