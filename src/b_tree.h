#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H
// Libraries
#include <stdlib.h>
#include <stdbool.h>

/* #include "cs165_api.h" */
#define TESTING 1

/// ***************************************************************************
/// B Tree Initialization variables
/// ***************************************************************************

// MAX is 340, half is 170
/* #define MIN_DEGREE 2  // Min number of pointers in a level */
/* #define MIN_KEYS (MIN_DEGREE - 1)  // Minimum number of keys in a node */
/* #define MAX_KEYS ((2 * MIN_DEGREE) - 1)  // Max num of keys in a node */
/* #define MAX_DEGREE (2 * MIN_DEGREE)  // Max num pointers from a node */

#define MAX_DEGREE 3
#define MAX_KEYS (MAX_DEGREE - 1)
#define MIN_KEYS (MAX_KEYS / 2)
#define MIN_DEGREE (MIN_KEYS + 1)


#define RESULT_SCALING 16

/// ***************************************************************************
/// B Plus Tree Types
/// ***************************************************************************

// TEMP - until integrated
typedef struct Result {
    size_t num_tuples;
    size_t capacity;
    /* DataType data_type; */
    void *payload;
    /* bool free_after_use; */
} Result;

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


/* void value_to_node(BPTNodeVal* return_obj, BPTNode* bt_node, size_t idx) { */
/*     return_obj->parent = bt_node; */
/*     return_obj->point_value = return_obj->parent->node_vals[idx]; */
/*     if (return_obj->parent->is_leaf) { */
/*         return_obj->left_branch = return_obj->parent->bpt_meta.bpt_ptrs.children[idx]; */
/*         return_obj->right_branch = return_obj->parent->bpt_meta.bpt_ptrs.children[idx + 1]; */
/*     } else { */
/*         return_obj->left_branch = return_obj->right_branch = NULL; */
/*         return_obj->point_value = return_obj->parent->bpt_meta.bpt_leaf.col_pos[idx]; */
/*     } */
/* } */

// ****************************************************************************
// NODE Functions
// ****************************************************************************

// allocation functions
BPTNode* allocate_node(bool leaf);
BPTNode* create_node();
BPTNode* create_leaf();


// ****************************************************************************
// Helper Functions - used for debugging / general things
// ****************************************************************************
int binary_search(int* arr, int l_pos, int r_pos, int x);
bool is_child(BPTNode* parent, BPTNode* child);
#if TESTING
void print_node(BPTNode* node);
void print_leaf(BPTNode* node);
void print_tree(BPTNode* node);
void testing_print();
#endif


/// ***************************************************************************
/// Searching Functions
/// ***************************************************************************
void increase_result_array(Result *result);
void add_to_results(Result* result, size_t value);
size_t find_value(BPTNode* bt_node, int value, Result* result);


/// **************************************************************************
/// B Plus Tree Insertion Functions
/// **************************************************************************

void insert_into_leaf(BPTNode* bt_node, int value, size_t position);
#if TESTING
void test_leaf_insert();
#endif

void split_leaf(BPTNode* bt_node, int value, size_t pos, SplitNode* split_node);
#if TESTING
void test_split_leaf();
#endif

// FIXME;
SplitNode* add_to_leaf(BPTNode* bt_node, int value, size_t position);


BPTNodeStack* find_leaf(BPTNode* bt_node, int value);
void insert_into_tree_body(BPTNode* bt_node, SplitNode* split_node);

// TODO:
BPTNode* insert_value(BPTNode* bt_node, int value, size_t position);

// TODO:
void add_to_pointer_node(SplitNode);

#endif