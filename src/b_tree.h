#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include <stdlib.h>

/* #include "cs165_api.h" */

// This file contains all the b-tree stuff...
// MAX is 340, half is 170
#define MIN_DEGREE 2  // Min number of pointers in a level
#define MIN_KEYS (MIN_DEGREE - 1)  // Minimum number of keys in a node
#define MAX_KEYS ((2 * MIN_DEGREE) - 1)  // Max num of keys in a node
/* #define MAX_KEYS 2 */
#define LEFT_SPLIT (MAX_KEYS / 2)
#define RIGHT_SPLIT (LEFT_SPLIT - MAX_KEYS)
#define MAX_DEGREE (2 * MIN_DEGREE)  // Max num pointers from a node

#define RESULT_SCALING 16

typedef enum BPTREE_OP {
    INSERT_VAL,
    DELETE_VAL,
    FIND_VAL
} BPTREE_OP;

// Define the "BPTNode"
struct BPTNode;

/**
 * @brief This is the struct for the leaves of the bpt
 *      - Array of column positions
 *      - Pointer to the next leaf
 *      - Pointer to the previous leaf
 */
typedef struct BPTLeaf {
    // a child node
    // the 'fence' contains the degree plus 1 for the number of fences
    size_t col_pos[MAX_KEYS];
    struct BPTNode* next_leaf;
    struct BPTNode* prev_leaf;
} BPTLeaf;

/**
 * @brief This is a struct for the pointers to other leaves. We can
 * have n+1 of them
 */
typedef struct BPTPointers {
    // the positions contain the node degree number of positions if we have
    // a child node
    struct BPTNode* children[MAX_DEGREE];
    bool is_head;
    unsigned int level;
} BPTPointers;

/**
 * @brief This union is either a pointer to an array of nodes (if
 * we have a node), or is a pointer to an array of column positions
 * (if we have struct)
 */
typedef union BPTMeta {
    BPTLeaf bpt_leaf;
    BPTPointers bpt_ptrs;
} BPTMeta;


/**
 * @brief This is the structure for the nodes. A node
 */
typedef struct BPTNode {
    BPTMeta bpt_meta;            // This contains the node details
    size_t num_elements;         // this is the currently used size of the array
    int node_vals[MAX_KEYS];  // these are the datapoints
    bool is_leaf;                // this tells us the type
} BPTNode;

// A structure to represent a stack
typedef struct BPTNodeStack {
    int top;
    unsigned capacity;
    BPTNode** array;
} BPTNodeStack;



#endif
