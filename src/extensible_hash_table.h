#ifndef EXT_HASH_TABLE_H
#define EXT_HASH_TABLE_H
#include <stdlib.h>

// calculation for the bucket size - we want it to fit in a page
/* 4 * n + 8 * n + 16 */
/* 12n + 16 = 4096 */
#define MAX_BUCKET_SIZE 340
#define NUM_BUCKET_INIT 64

// a bucket holds MAX_BUCKET_SIZE entries. It only grows past that when every
// entry has the same key, as splitting cannot separate duplicates
typedef struct ExtHashBucket {
    size_t hb_size;
    size_t hb_capacity;
    size_t local_depth;
    int* hb_keys;
    size_t* hb_values;
} ExtHashBucket;


typedef struct ExtHashTable {
    ExtHashBucket** hash_buckets;
    size_t global_depth;
    size_t num_exb;
    size_t max_exbs;
} ExtHashTable;

typedef struct HashResults {
    size_t num_found;
    size_t hr_capacity;
    size_t* hb_results;
} HashResults;


// creation functions
ExtHashTable* create_ext_hash_table(void);
void free_ext_hash_table(ExtHashTable* ext_ht);

// setters
void ext_hash_table_put(ExtHashTable* ext_ht, int key, size_t value);

// result function
HashResults* ext_hash_func_get(ExtHashTable* ext_ht, int key);
void free_hash_result(HashResults* hres);

#endif
