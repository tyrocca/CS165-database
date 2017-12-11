#include <stdlib.h>
#define MAX_BUCKET_SIZE 2
#define NUM_BUCKET_INIT 64

typedef struct ExtHashBucket {
    size_t hb_size;
    size_t local_depth;
    int hb_keys[MAX_BUCKET_SIZE];
    size_t hb_values[MAX_BUCKET_SIZE];
} ExtHashBucket;


typedef struct ExtHashTable {
    ExtHashBucket* curr_hb;
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
ExtHashTable* create_ext_hash_table();
void free_ext_hash_table(ExtHashTable* ext_ht);
void ext_hash_table_put(ExtHashTable* ext_ht, int key, size_t value);

// result function
HashResults* ext_hash_func_get(ExtHashTable* ext_ht, int key);
