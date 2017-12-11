#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "extendible_hash_table.h"
#include <stdio.h>
#include <time.h>


/// ***************************************************************************
/// Hash result functions
/// ***************************************************************************

/**
 * @brief Function to create hash result object
 *
 * @return result
 */
HashResults* create_hash_result() {
    HashResults* hres = malloc(sizeof(HashResults));
    hres->num_found = 0;
    hres->hr_capacity = MAX_BUCKET_SIZE;
    hres->hb_results = malloc(hres->hr_capacity * sizeof(size_t));
    return hres;
}

/**
 * @brief Function to free a hash result
 *
 * @param hres
 */
void free_hash_result(HashResults* hres) {
    free(hres->hb_results);
    free(hres);
}

/**
 * @brief Function for adding to the results of a hash table
 *
 * @param hres
 * @param result
 */
void add_to_hb_results(HashResults* hres, size_t result) {
    if (hres->num_found == hres->hr_capacity) {
        hres->hr_capacity *= 2;  // double the size
        hres->hb_results = realloc(hres->hb_results,
                                   hres->hr_capacity * sizeof(size_t));
    }
    hres->hb_results[hres->num_found++] = result;
}

/// ***************************************************************************
/// Extensible Hash bucket functions
/// ***************************************************************************

/**
 * @brief Function to initialize a hash bucket
 *
 * @return ExtHashBucket* - pointer to ExtHashBucket
 */
ExtHashBucket* create_hash_bucket() {
    ExtHashBucket* hb = malloc(sizeof(ExtHashBucket));
    hb->hb_size = hb->local_depth = 0;
    return hb;
}

/**
 * @brief Function that returns whether we have a full bucket
 *
 * @param hb - hash table to check
 *
 * @return bool if the values are equal
 */
bool is_full_bucket(ExtHashBucket* hb) {
    return hb->hb_size == MAX_BUCKET_SIZE;
}

/**
 * @brief Function that inserts a key value pair into the table
 *
 * @param hb - hash table to insert into
 * @param key - key to insert
 * @param value - value to insert
 */
void hb_put(ExtHashBucket* hb, int key, size_t value) {
    hb->hb_keys[hb->hb_size] = key;
    hb->hb_values[hb->hb_size++] = value;
}


/**
 * @brief Function to get all of the values for a key
 *
 * @param hb - takes in a hash bucket to query
 * @param key - takes in a key to lookup
 *
 * @return HashResult pointer that contains a list of the data
 */
HashResults* hb_get(ExtHashBucket* hb, int key) {
    HashResults* hres = create_hash_result();
    for (size_t i = 0; i < hb->hb_size; i++) {
        if (hb->hb_keys[i] == key) {
            add_to_hb_results(hres, hb->hb_values[i]);
        }
    }
    return hres;
}


/// ***************************************************************************
/// Extensible Hash Table Functions
/// ***************************************************************************

/**
 * @brief This creates an extensible hash table with one bucket
 *
 * @return New ExtHashTable pointer
 */
ExtHashTable* create_ext_hash_table() {
    ExtHashTable* ext_ht = malloc(sizeof(ExtHashTable));
    ext_ht->max_exbs = NUM_BUCKET_INIT;
    ext_ht->global_depth = 0;
    // init the array for buckets
    ext_ht->hash_buckets = malloc(sizeof(ExtHashTable*) * ext_ht->max_exbs);
    // create a bucket
    ext_ht->hash_buckets[0] = create_hash_bucket();
    ext_ht->num_exb = 1;
    return ext_ht;
}

/**
 * @brief This function frees a extensible hash table and all of its buckets
 *
 * @param ext_ht
 */
void free_ext_hash_table(ExtHashTable* ext_ht) {
    while (ext_ht->num_exb-- > 0) {
        // get the bucket
        ExtHashBucket* bucket = ext_ht->hash_buckets[ext_ht->num_exb];
        if (bucket == NULL) {
            continue;
        }
        // set all other buckets that match to be null
        for (size_t i = 0; i < ext_ht->num_exb; i++) {
            if (ext_ht->hash_buckets[i] == bucket) {
                ext_ht->hash_buckets[i] = NULL;
            }
        }
        // then free the bucket
        free(bucket);
        ext_ht->hash_buckets[ext_ht->num_exb] = NULL;
    }
    free(ext_ht->hash_buckets);
    free(ext_ht);
}

/**
 * @brief Hash function that I found online
 *
 * @param x
 *
 * @return hashed value
 */
unsigned int ext_hash_func(unsigned int x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

/**
 * @brief Function that returns the index of the hash table
 *
 * @param ext_ht
 * @param key
 *
 * @return
 */
unsigned int get_hash_bucket_idx(ExtHashTable* ext_ht, int key) {
    unsigned int h = ext_hash_func((unsigned int) key);
    return h & (( 1 << ext_ht->global_depth) - 1);
}

/**
 * @brief Function that gets the hash bucket for a given key
 *
 * @param ext_ht - the hash table
 * @param key - the key to lookup
 *
 * @return ExtHashBucket* - bucket that is related to the function
 */
ExtHashBucket* get_ext_hash_bucket(ExtHashTable* ext_ht, int key) {
    unsigned int idx = get_hash_bucket_idx(ext_ht, key);
    return ext_ht->hash_buckets[idx];
    /* return ext_ht->hash_buckets[get_hash_bucket_idx(ext_ht, key)]; */
}


/**
 * @brief Function that adds the item to the table
 *
 * @param ext_ht
 * @param key
 * @param value
 */
size_t recurse_limit = 0;
void ext_hash_table_put(ExtHashTable* ext_ht, int key, size_t value) {
    unsigned int hash_idx = get_hash_bucket_idx(ext_ht, key);
    ExtHashBucket* ext_hb = ext_ht->hash_buckets[hash_idx];

    // if we have a local bucket and it equals the local depth, increase
    // the size of the table
    if (is_full_bucket(ext_hb) == false) {
        hb_put(ext_hb, key, value);
        recurse_limit = 0;
        return;
    } else if (ext_hb->local_depth == ext_ht->global_depth) {
        // if the table is full and the local depth equals the global
        // depth, increase the size and try to add
        ext_ht->global_depth++;
        // increase the array for buckets if necessary
        if (ext_ht->num_exb == ext_ht->max_exbs) {
            ext_ht->max_exbs *= 2;
            // increase the the array for buckets
            ext_ht->hash_buckets = realloc(
                    ext_ht->hash_buckets,
                    ext_ht->max_exbs * sizeof(ExtHashBucket*));
        }
        // copy over the buckets so they point correctly
        memcpy(&ext_ht->hash_buckets[ext_ht->num_exb],
               &ext_ht->hash_buckets[0],
               ext_ht->num_exb * sizeof(ExtHashBucket*));
        // now double the size
        ext_ht->num_exb *= 2;
    }

    // we have now resized our shit
    if (ext_hb->local_depth < ext_ht->global_depth) {
        // make a new bucket
        ExtHashBucket* new_bucket = create_hash_bucket();
        // number of items to add
        size_t num_items = ext_hb->hb_size;
        // reset the bucket size for the current bucket
        ext_hb->hb_size = 0;
        // redistribute the values between the two buckets
        for (size_t i = 0; i < num_items; i++) {
            int k = ext_hb->hb_keys[i];
            size_t v = ext_hb->hb_values[i];
            // re hash the balue
            unsigned int hash_v = ext_hash_func((unsigned int) k);
            hash_v = hash_v & ((1 << ext_ht->global_depth) - 1);
            // place in the correct new bucket
            if (((hash_v >> ext_hb->local_depth) & 1) == 1) {
                hb_put(new_bucket, k, v);
            } else {
                hb_put(ext_hb, k, v);
            }
        }
        ext_hb->local_depth++;
        new_bucket->local_depth = ext_hb->local_depth;
        ext_ht->hash_buckets[hash_idx + (ext_ht->num_exb / 2)] = new_bucket;
        // after redistributing, call the function again
        if (recurse_limit++ == 10) {
            // this means there was a terrible error and we tried to
            // rebalance the nodes 10 times
            exit(-1);
        }
        ext_hash_table_put(ext_ht, key, value);
        return;
    } else {
        // there was an error
        exit(-1);
    }
}

/**
 * @brief Function that gets the values for a given key (from the table)
 *
 * @param ext_ht
 * @param key
 *
 * @return
 */
HashResults* ext_hash_func_get(ExtHashTable* ext_ht, int key) {
    return hb_get(get_ext_hash_bucket(ext_ht, key), key);
}


/// ***************************************************************************
/// Testing Functions
/// ***************************************************************************

#if 0
void print_bucket(ExtHashBucket* bucket) {
    for (size_t i = 0; i < bucket->hb_size; i++) {
        printf("\t(key: %d, value: %zu)\n",
                bucket->hb_keys[i],
                bucket->hb_values[i]);
    }
}
void print_ext_hash_table(ExtHashTable* ht) {
    size_t num_buckets = 0;
    ExtHashBucket* buckets[ht->num_exb];
    for (size_t i = 0; i < ht->num_exb; i++) {
        bool found = false;
        printf("Bucket %zu\n", i);
        ExtHashBucket* bucket = ht->hash_buckets[i];
        for (size_t j = 0; j < num_buckets; j++) {
            if (buckets[j] == bucket) {
                found = true;
                printf("Resused bucket\n");
                break;
            }
        }
        if (found == false) {
            buckets[num_buckets++] = bucket;
        }
        print_bucket(bucket);
    }
}

void print_hash_results(ExtHashTable* ht, int key) {
    printf("Results for key %d:\n", key);
    HashResults* results = ext_hash_func_get(ht, key);
    for (size_t i = 0; i < results->num_found; i++) {
        printf("\t%zu\n", results->hb_results[i]);
    }
    free_hash_result(results);
}

int main(void) {
    ExtHashTable* ht = create_ext_hash_table();
    srand(time(NULL));
    for (int value = 0; value < 10; value++) {
        /* int key = rand(); */
        int key = value - 1;
        printf("\n Inserting %d, at %d\n", key, value);
        ext_hash_table_put(ht, key, value);
    }
    ext_hash_table_put(ht, 3, 6);
    print_ext_hash_table(ht);
    print_hash_results(ht, 3);
    free_ext_hash_table(ht);
    return 0;
}
#endif
