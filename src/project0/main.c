#include <stdlib.h>
#include <stdio.h>

#include "hash_table.h"

// This is where you can implement your own tests for the hash table
// implementation.
int main(void) {
    hashtable* ht = NULL;
    int size = 10;
    allocate(&ht, size);

    int key = 1;
    int value = -1;

    put(ht, key, value);
    put(ht, 1, 3);

    int num_values = 1;

    valType* values = malloc(num_values * sizeof(valType));

    int* num_results = malloc(sizeof(int));

    get(ht, key, values, num_values, num_results);
    if ((*num_results) > num_values) {
        values = realloc(values, (*num_results) * sizeof(valType));
        get(ht, key, values, *num_results, num_results);
    }

    for (int i = 0; i < (*num_results); i++) {
        printf("value %d is %d \n", i, values[i]);
    }
    free(values);

    erase(ht, key);

    deallocate(ht);
    return 0;
}
