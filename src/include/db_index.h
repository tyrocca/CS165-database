#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H
// Libraries
#include <stdlib.h>
#include <stdbool.h>
#include "cs165_api.h"

#define TESTING 1

/// ***************************************************************************
/// B Tree Initialization variables
/// ***************************************************************************

// MAX is 340, half is 170
/* #define MIN_DEGREE 2  // Min number of pointers in a level */
/* #define MIN_KEYS (MIN_DEGREE - 1)  // Minimum number of keys in a node */
/* #define MAX_KEYS ((2 * MIN_DEGREE) - 1)  // Max num of keys in a node */
/* #define MAX_DEGREE (2 * MIN_DEGREE)  // Max num pointers from a node */

/* #if TESTING */
/* #define MAX_DEGREE 4 */
/* #else */
#define MAX_DEGREE 340
/* #endif */

#define MAX_KEYS (MAX_DEGREE - 1)
#define MIN_KEYS (MAX_KEYS / 2)
#define MIN_DEGREE (MIN_KEYS + 1)


/// ***************************************************************************
/// Database Indexing Types
/// ***************************************************************************

// should be 1 page worth of values
#define SORTED_NODE_SIZE 1024
typedef struct SortedIndex {
    int* keys;              // this is a pointer to an array of keys
    size_t* col_positions;     // this is a pointer to an array of positions
    size_t num_items;       // the number of items
    size_t allocated_space; // this is the amount of allocated space for a col
    bool has_positions;     // this bool tells us if we have positions
} SortedIndex;

// Define the "BPTNode"
struct BPTNode;

/**
 * @brief This is the struct for the leaves of the bpt
 *      - Array of column positions
 *      - Pointer to the next leaf
 *      - Pointer to the previous leaf
 */
typedef struct BPTLeaf {
    size_t col_pos[MAX_KEYS];  // these are the positions
    struct BPTNode* next_leaf; // this is the next pointer (next leaf)
    struct BPTNode* prev_leaf; // this is the previous pointer (previous leaf)
} BPTLeaf;

/**
 * @brief This is a struct for the pointers to other leaves. We can
 * have n+1 of them
 */
typedef struct BPTPointers {
    struct BPTNode* children[MAX_DEGREE];  // Array of pointers to childern
    /* bool is_root;                          // Bool to indicate if is root */
    unsigned int level;                    // Level of the tree
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
    int node_vals[MAX_KEYS];     // these are the datapoints
    bool is_leaf;                // this tells us the type
} BPTNode;

// A structure to represent a stack
typedef struct BPTNodeStack {
    int top;
    unsigned capacity;
    BPTNode** array;
} BPTNodeStack;

// IDK WHAT to do with this
typedef struct BPTNodeVal {
    BPTNode* left_branch;
    BPTNode* right_branch;
    BPTNode* parent;
    size_t value_position;
    int point_value;
} BPTNodeVal;

/**
 * @brief This structure is used to hold the result of splitting a leaf node
 */
typedef struct SplitNode {
    BPTNode* left_leaf;    // This is the left node resulting from the split
    BPTNode* right_leaf;   // this is the right node (results from split)
    int middle_val;        // this is the middle value (results from split)
} SplitNode;


// ****************************************************************************
// Helper Functions - used for debugging / general things
// ****************************************************************************


void free_tree(BPTNode* node);
void print_tree(BPTNode* node);
void print_sorted_index(SortedIndex* sorted_index);


/// ***************************************************************************
/// Sorted Functions
/// ***************************************************************************

// Creation functions
SortedIndex* create_unclustered_sorted_index();
SortedIndex* create_clustered_sorted_index(int* data);

// Search function
size_t get_sorted_idx(SortedIndex* sorted_index, int value);
Result* get_range_sorted(SortedIndex* sorted_index, int low, int high);

// Insertion (for unclustered)
void insert_into_sorted(SortedIndex* sorted_index, int value, size_t position);


/// **************************************************************************
/// B Plus Tree Functions
/// **************************************************************************

size_t btree_find_insert_position(BPTNode* root, int value);
Result* find_values_unclustered(BPTNode* root, int gte_val, int lt_val);
Result* find_values_clustered(BPTNode* root, int gte_val, int lt_val);

// the overall insertion function for b_tree
BPTNode* btree_insert_value(
    BPTNode* bt_node,
    int value,
    size_t position,
    bool update_positions
);

#endif
