#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>

#define CACHE_LINE 64
#define MAX_KEY 1000000000ULL
#define WARMUP_OPS 50000
#define BENCH_OPS  200000

// Simple append-only B+ Tree node representation for microbenchmarking
typedef struct BTreeNode {
    uint8_t is_leaf;
    uint16_t num_keys;
    uint64_t keys[63];
    struct BTreeNode* children[64];
} BTreeNode;

typedef struct {
    BTreeNode* root;
} AppendOnlyBTree;

static inline AppendOnlyBTree* btree_create() {
    AppendOnlyBTree* tree = (AppendOnlyBTree*)malloc(sizeof(AppendOnlyBTree));
    tree->root = (BTreeNode*)calloc(1, sizeof(BTreeNode));
    tree->root->is_leaf = 1;
    return tree;
}

static inline int btree_append(AppendOnlyBTree* tree, uint64_t key) {
    BTreeNode* node = tree->root;
    if (node->num_keys < 63) {
        node->keys[node->num_keys++] = key;
        return 0;
    }
    // Simple split for append-only benchmark simulation
    BTreeNode* new_root = (BTreeNode*)calloc(1, sizeof(BTreeNode));
    new_root->is_leaf = 0;
    new_root->children[0] = node;
    
    BTreeNode* new_leaf = (BTreeNode*)calloc(1, sizeof(BTreeNode));
    new_leaf->is_leaf = 1;
    new_leaf->keys[0] = key;
    new_leaf->num_keys = 1;
    
    new_root->keys[0] = key;
    new_root->children[1] = new_leaf;
    tree->root = new_root;
    return 0;
}

static inline uint64_t get_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static int cmp_uint64(const void* a, const void* b) {
    uint64_t fa = *(const uint64_t*)a;
    uint64_t fb = *(const uint64_t*)b;
    return (fa > fb) - (fa < fb);
}

int main() {
    AppendOnlyBTree* tree = btree_create();
    uint64_t* latencies = (uint64_t*)malloc(BENCH_OPS * sizeof(uint64_t));
    
    // Warmup
    for (uint64_t i = 0; i < WARMUP_OPS; i++) {
        btree_append(tree, i);
    }
    
    // Benchmark Run
    uint64_t start_total = get_ns();
    for (uint64_t i = 0; i < BENCH_OPS; i++) {
        uint64_t start_op = get_ns();
        btree_append(tree, WARMUP_OPS + i);
        uint64_t end_op = get_ns();
        latencies[i] = end_op - start_op;
    }
    uint64_t end_total = get_ns();
    
    qsort(latencies, BENCH_OPS, sizeof(uint64_t), cmp_uint64);
    
    uint64_t total_ns = end_total - start_total;
    double seconds = (double)total_ns / 1e9;
    double ops_sec = (double)BENCH_OPS / seconds;
    
    uint64_t p50 = latencies[(size_t)(BENCH_OPS * 0.50)];
    uint64_t p95 = latencies[(size_t)(BENCH_OPS * 0.95)];
    uint64_t p99 = latencies[(size_t)(BENCH_OPS * 0.99)];
    uint64_t p999 = latencies[(size_t)(BENCH_OPS * 0.999)];
    
    printf("+------------------------------------------------------------+\n");
    printf("|          Append-Only B-Tree Engine Microbenchmark            |\n");
    printf("+----------------------------------+-------------------------+\n");
    printf("| Metric                           | Value                   |\n");
    printf("+----------------------------------+-------------------------+\n");
    printf("| Total Operations                 | %-23lu |\n", (unsigned long)BENCH_OPS);
    printf("| Total Time (seconds)             | %-23.4f |\n", seconds);
    printf("| Throughput (ops/sec)             | %-23.2f |\n", ops_sec);
    printf("| Latency P50 (ns)                 | %-23lu |\n", (unsigned long)p50);
    printf("| Latency P95 (ns)                 | %-23lu |\n", (unsigned long)p95);
    printf("| Latency P99 (ns)                 | %-23lu |\n", (unsigned long)p99);
    printf("| Latency P99.9 (ns)               | %-23lu |\n", (unsigned long)p999);
    printf("+----------------------------------+-------------------------+\n");
    
    free(latencies);
    return 0;
}