// added by me
#include <stdlib.h>
#include <stdio.h>
#include "hash_table.h"

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
    // The next line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    if (size < 1) {
        return -1;
    }
    // create the hashtable
    hashtable newHashTable;
    newHashTable.size = size;
    newHashTable.current_size = 0;
    // create the array of data node pointers
    newHashTable.tableNodes = malloc(sizeof(dataNode *) * size);
    if (newHashTable.tableNodes == NULL) {
        return -1;
    }
    // set each node to be null
    for (int i = 0; i < size; i++) {
        newHashTable.tableNodes[i] = NULL;
    }
    // update the hash table
    *ht = &newHashTable;

    return 0;
}

int hash_function(hashtable* ht, keyType key) {
    (void) ht;  // TODO: better hash function
    return key % 10;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise
// (e.g., if malloc is called and fails).
int put(hashtable* ht, keyType key, valType value) {
    // get the node at the hash position
    int idx = hash_function(ht, key);
    printf("ASDFASDFASDF  %d \n", idx);
    dataNode *nodePtr = ht->tableNodes[hash_function(ht, key)];
    // go down the linked list
    while (nodePtr != NULL) {
        printf("INSIDE \n");
        nodePtr = nodePtr->next;
    }
    nodePtr = malloc(sizeof(dataNode));
    nodePtr->key = key;
    nodePtr->value = value;
    nodePtr->next = NULL;
    printf("HERE \n");
    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
    (void) ht;
    (void) key;
    (void) values;
    (void) num_values;
    (void) num_results;
    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
    (void) ht;
    (void) key;
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    (void) ht;
    return 0;
}
