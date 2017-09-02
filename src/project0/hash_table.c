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
    // create the hash table
    hashtable* newHashTable = malloc(sizeof(hashtable));
    if (newHashTable == NULL) {
        return -1;
    }
    newHashTable->size = size;
    newHashTable->current_size = 0;
    // create the nodes for the table
    newHashTable->tableNodes = malloc(sizeof(dataNode *) * size);
    if (newHashTable->tableNodes == NULL) {
        return -1;
    }

    // set each node to be null
    for (int i = 0; i < size; i++) {
        newHashTable->tableNodes[i] = NULL;
    }
    // update the hash table
    *ht = newHashTable;

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
    // create the new node
    dataNode* new_node = malloc(sizeof(dataNode));
    if (new_node == NULL) {
        return -1;
    }
    new_node->key = key;
    new_node->value = value;

    // insert into the list
    int idx = hash_function(ht, key);
    dataNode* list_ptr = ht ->tableNodes[idx];
    ht->current_size++;
    // if we have no collisions - return
    if (list_ptr == NULL) {
        new_node->next = list_ptr;
        ht->tableNodes[idx] = new_node;
        return 0;
    }
    // when there is a collision we have to append - this seems slower
    // TODO: do we need to append? it would be faster to add to the front
    // my current implementation keeps them in order
    while (list_ptr->next) {
        list_ptr = list_ptr->next;
    }
    new_node->next = list_ptr->next;
    list_ptr->next = new_node;
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
    // go to the bucket
    dataNode* list_ptr = ht ->tableNodes[hash_function(ht, key)];
    if (!list_ptr|| num_values == 0) {
        return 0;
    }
    // load values
    *num_results = 0;
    while(list_ptr) {
        if (list_ptr->key == key) {
            if (num_values > 0) {
                values[*num_results] = list_ptr->value;
                num_values--;
            }
            *num_results = *num_results + 1;
        }
        list_ptr = list_ptr->next;
    }
    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
    int idx = hash_function(ht, key);
    dataNode* list_ptr = ht->tableNodes[idx];
    dataNode* head_ptr = NULL;
    if (!list_ptr) {
        return 0;
    }
    while(list_ptr) {
        if (list_ptr->key != key) {
            if (!list_ptr) {
                head_ptr = list_ptr;
            }
            list_ptr = list_ptr->next;
        } else {
            dataNode* temp = list_ptr->next;
            free(list_ptr);
            ht->current_size--;
            list_ptr = temp;
        }
    }
    // update the head
    ht->tableNodes[idx] = head_ptr;
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    for (int i = 0; i < ht->size; i++) {
        if (ht->tableNodes[i]) {
            dataNode* node_ptr = ht->tableNodes[i];
            while(node_ptr) {
                dataNode* temp = node_ptr;
                node_ptr = temp->next;
                free(temp);
            }
        }
    }
    free(ht);
    return 0;
}
