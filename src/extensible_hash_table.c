#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "extensible_hash_table.h"
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
HashResults* create_hash_result(void) {
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
ExtHashBucket* create_hash_bucket(void) {
    ExtHashBucket* hb = malloc(sizeof(ExtHashBucket));
    hb->hb_size = hb->local_depth = 0;
    hb->hb_capacity = MAX_BUCKET_SIZE;
    hb->hb_keys = malloc(hb->hb_capacity * sizeof(int));
    hb->hb_values = malloc(hb->hb_capacity * sizeof(size_t));
    return hb;
}

/**
 * @brief Function to free a hash bucket and its arrays
 *
 * @param hb
 */
void free_hash_bucket(ExtHashBucket* hb) {
    free(hb->hb_keys);
    free(hb->hb_values);
    free(hb);
}

/**
 * @brief Function that returns whether every entry in a bucket has the same
 *  key - such a bucket cannot be split, it has to grow
 *
 * @param hb
 *
 * @return bool
 */
bool is_single_key_bucket(ExtHashBucket* hb) {
    for (size_t i = 1; i < hb->hb_size; i++) {
        if (hb->hb_keys[i] != hb->hb_keys[0]) {
            return false;
        }
    }
    return true;
}

/**
 * @brief Function that doubles the capacity of a bucket
 *
 * @param hb
 */
void grow_hash_bucket(ExtHashBucket* hb) {
    hb->hb_capacity *= 2;
    hb->hb_keys = realloc(hb->hb_keys, hb->hb_capacity * sizeof(int));
    hb->hb_values = realloc(hb->hb_values, hb->hb_capacity * sizeof(size_t));
}

/**
 * @brief Function that returns whether we have a full bucket
 *
 * @param hb - hash table to check
 *
 * @return bool if the values are equal
 */
bool is_full_bucket(ExtHashBucket* hb) {
    return hb->hb_size == hb->hb_capacity;
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
ExtHashTable* create_ext_hash_table(void) {
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
        free_hash_bucket(bucket);
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
void ext_hash_table_put(ExtHashTable* ext_ht, int key, size_t value) {
    ExtHashBucket* ext_hb = get_ext_hash_bucket(ext_ht, key);

    // if a full bucket only holds this key, splitting will never separate
    // the entries, so grow the bucket instead
    if (is_full_bucket(ext_hb) && ext_hb->hb_keys[0] == key &&
            is_single_key_bucket(ext_hb)) {
        grow_hash_bucket(ext_hb);
    }

    if (is_full_bucket(ext_hb) == false) {
        hb_put(ext_hb, key, value);
        return;
    }

    // the bucket is full: if it is already as deep as the directory, double
    // the directory (the new upper half mirrors the lower half)
    if (ext_hb->local_depth == ext_ht->global_depth) {
        if (ext_ht->num_exb * 2 > ext_ht->max_exbs) {
            ext_ht->max_exbs *= 2;
            ext_ht->hash_buckets = realloc(
                    ext_ht->hash_buckets,
                    ext_ht->max_exbs * sizeof(ExtHashBucket*));
        }
        memcpy(&ext_ht->hash_buckets[ext_ht->num_exb],
               &ext_ht->hash_buckets[0],
               ext_ht->num_exb * sizeof(ExtHashBucket*));
        ext_ht->num_exb *= 2;
        ext_ht->global_depth++;
    }

    // split the bucket on the next hash bit. The new bucket gets the same
    // capacity so a grown bucket's entries always fit
    size_t split_bit = ext_hb->local_depth;
    ExtHashBucket* new_bucket = create_hash_bucket();
    while (new_bucket->hb_capacity < ext_hb->hb_capacity) {
        grow_hash_bucket(new_bucket);
    }
    size_t num_items = ext_hb->hb_size;
    ext_hb->hb_size = 0;
    for (size_t i = 0; i < num_items; i++) {
        int k = ext_hb->hb_keys[i];
        size_t v = ext_hb->hb_values[i];
        unsigned int hash_v = ext_hash_func((unsigned int) k);
        if ((hash_v >> split_bit) & 1) {
            hb_put(new_bucket, k, v);
        } else {
            hb_put(ext_hb, k, v);
        }
    }
    ext_hb->local_depth++;
    new_bucket->local_depth = ext_hb->local_depth;

    // every directory slot that pointed at the old bucket and has the split
    // bit set now points at the new bucket
    for (size_t i = 0; i < ext_ht->num_exb; i++) {
        if (ext_ht->hash_buckets[i] == ext_hb && ((i >> split_bit) & 1)) {
            ext_ht->hash_buckets[i] = new_bucket;
        }
    }

    // after redistributing, try again (this may split again)
    ext_hash_table_put(ext_ht, key, value);
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
