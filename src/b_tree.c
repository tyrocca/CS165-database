#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
/* #include "cs165_api.h" */

// This file contains all the b-tree stuff...
// MAX is 340, half is 170
#define MIN_DEGREE 2  // Min number of pointers in a level
#define MIN_KEYS (MIN_DEGREE - 1)  // Minimum number of keys in a node
#define MAX_KEYS ((2 * MIN_DEGREE) - 1)  // Max num of keys in a node
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

// IDK WHAT to do wit this
typedef struct BPTNodeVal {
    BPTNode* left_branch;
    BPTNode* right_branch;
    BPTNode* parent;
    size_t value_position;
    int point_value;
} BPTNodeVal;


void value_to_node(BPTNodeVal* return_obj, BPTNode* bt_node, size_t idx) {
    return_obj->parent = bt_node;
    return_obj->point_value = return_obj->parent->node_vals[idx];
    if (return_obj->parent->is_leaf) {
        return_obj->left_branch = return_obj->parent->bpt_meta.bpt_ptrs.children[idx];
        return_obj->right_branch = return_obj->parent->bpt_meta.bpt_ptrs.children[idx + 1];
    } else {
        return_obj->left_branch = return_obj->right_branch = NULL;
        return_obj->point_value = return_obj->parent->bpt_meta.bpt_leaf.col_pos[idx];
    }
}


/**
 * @brief Function that creates a node
 *
 * @param leaf - whether we want a node or a leaf
 *
 * @return allocated node
 */
BPTNode* allocate_node(bool leaf) {
    /* BPTNode* new_node = malloc(sizeof(BPTNode)); */
    BPTNode* new_node = calloc(1, sizeof(BPTNode));
    new_node->num_elements = 0;
    new_node->is_leaf = leaf;
    if (new_node->is_leaf) {
        new_node->bpt_meta.bpt_leaf.next_leaf = NULL;
        new_node->bpt_meta.bpt_leaf.prev_leaf = NULL;
    }
    return new_node;
}

/// Make a node
BPTNode* create_node() {
    return allocate_node(false);
}

/// Make a leaf
BPTNode* create_leaf() {
    return allocate_node(true);
}

void print_node(BPTNode* node) {
    printf("[ ");
    for (size_t i = 0; i < node->num_elements; i++) {
        printf("%d ", node->node_vals[i]);
    }
    printf("]\n");
}

/* #define GET_NODE_POSITION (node, idx) (node->bpt_meta. */
void print_leaf(BPTNode* node) {
    printf("[ ");
    for (size_t i = 0; i < node->num_elements; i++) {
        printf(
            "(%d, %zu) ",
            node->node_vals[i],
            node->bpt_meta.bpt_leaf.col_pos[i]
        );
    }
    printf("]\n");
}

bool is_child(BPTNode* parent, BPTNode* child) {
    for (size_t i = 0; i < parent->num_elements; i++) {
        if (parent->bpt_meta.bpt_ptrs.children[i] == child) {
            return true;
        }
    }
    return false;
}

void print_tree(BPTNode* node) {
    BPTNode** all_nodes = malloc(sizeof(BPTNode*) * 2);
    size_t num_nodes = 1;
    size_t node_idx = 0;
    all_nodes[node_idx] = node;
    /* BPTNode* parent = node; */

    while (node_idx < num_nodes) {
        // if there are bpt_ptrs.children nodes
        // copy the node into a current node
        BPTNode* current_node = all_nodes[node_idx];
        // if we have have a node with bpt_ptrs.children
        if (current_node->is_leaf == false) {
            // these are the number of nodes that we will be adding
            size_t to_add = current_node->num_elements + 1;
            // increase our queue of nodes, it will now be equal to the length
            // currently plus
            all_nodes = realloc(all_nodes, sizeof(BPTNode*) * (num_nodes + to_add));
            // copy to the end of the array
            memcpy(
                (void*) &all_nodes[num_nodes],
                (void*) current_node->bpt_meta.bpt_ptrs.children,
                sizeof(BPTNode*) * to_add
            );
            // increase number of nodes
            num_nodes += to_add;
        }
        print_node(current_node);
        node_idx++;
    }
    free(all_nodes);
    printf("\n");
}


/**
 * @brief Binary Search function for searching a leaf
 *
 * @param arr - array of values
 * @param l_pos - left position
 * @param r_pos - right position
 * @param x - comparison value
 *
 * @return index of the item
 */
int binary_search(int* arr, int l_pos, int r_pos, int x) {
   if (r_pos >= l_pos) {
        int mid = l_pos + (r_pos - l_pos) / 2;

        // If the element is present at the middle itself
        if (arr[mid] == x) {
            return mid;
        }

        // If element is smaller than mid, then it can only be present
        // in left subarray
        if (arr[mid] > x) {
            return binary_search(arr, l_pos, mid - 1, x);
        }

        // Else the element can only be present in right subarray
        return binary_search(arr, mid + 1, r_pos, x);
   }
   // We reach here when element is not present in array
   return -1;
}



typedef struct Result {
    size_t num_tuples;
    size_t capacity;
    /* DataType data_type; */
    void *payload;
    /* bool free_after_use; */
} Result;

/**
 * @brief This finds the values in a BPT
 *
 * @param bt_node
 * @param value
 *
 * @return
 */


void increase_result_array(Result *result) {
    result->capacity *= 2;
    // FIXME - change to correct type
    result->payload = realloc(result->payload, sizeof(size_t) * result->capacity);
}

void add_to_results(Result* result, size_t value) {
    if (result->num_tuples == result->capacity) {
        increase_result_array(result);
    }
    // add the value to the array
    ((size_t*)result->payload)[result->num_tuples++] = value;
}

/* size_t find_value(BPTNode* bt_node, int value, Result* result) { */
/*     if (bt_node->is_leaf) { */
/*         for (size_t i = 0; i < bt_node->num_elements; i++) { */
/*             if (bt_node->node_vals[i] == value) { */
/*                 add_to_results(result, */

/*             } */

/*         for (size */
/*     } */
/*     /1* // iterate over data *1/ */

/*     /1* for (size_t i = 0; i < bt_node->num_elements; i++) { *1/ */
/*     /1*     if value is a node value *1/ */
/*     /1*         check the lef *1/ */

/*     /1* } *1/ */
/*     return 0; */
/* } */

void insert_into_leaf(BPTNode* bt_node, int value, size_t position) {
    // start at the first value
    size_t i = 0;
    while (i < bt_node->num_elements) {
        // if the key is larger or if they are equal and the position is
        // larger, shift right
        if (bt_node->node_vals[i] > value || (
                bt_node->node_vals[i] == value
                && bt_node->bpt_meta.bpt_leaf.col_pos[i] > position)
        ) {
            // shift positions right
            memmove((void*) &bt_node->node_vals[i + 1],
                    (void*) &bt_node->node_vals[i],
                    (bt_node->num_elements - i) * sizeof(int));
            memmove((void*) &bt_node->bpt_meta.bpt_leaf.col_pos[i + 1],
                    (void*) &bt_node->bpt_meta.bpt_leaf.col_pos[i],
                    (bt_node->num_elements - i) * sizeof(size_t));
            break;
        }
        i++;
    }
    bt_node->node_vals[i] = value;
    bt_node->bpt_meta.bpt_leaf.col_pos[i] = position;
    bt_node->num_elements++;
}

void test_leaf_insert() {

    printf("Single Insert\n");
    BPTNode* node = create_node();
    insert_into_leaf(node, 10, 1);
    printf("== Expected ==\n");
    printf("[ (10, 1) ]\n");
    print_leaf(node);
    printf("== Result ==\n");
    free(node);

    printf("Multiple Insert\n");
    BPTNode* node2 = create_leaf();
    insert_into_leaf(node2, 10, 3);
    insert_into_leaf(node2, 5, 4);
    insert_into_leaf(node2, 10, 1);
    /* insert_into_leaf(node2, 11, 0); */
    /* insert_into_leaf(node2, 0, 20); */
    printf("== Expected ==\n");
    printf("[ (5, 4) (10, 1) (10, 3) ]\n");
    print_leaf(node2);
    printf("== Result ==\n");
    free(node2);


}


/* void add_to_leaf(BPTNode* bt_node, int value, size_t position) { */
/*     assert(bt_node->is_leaf == true); */
/*     if (bt_node->num_elements < MAX_KEYS) { */
/*         for (size_t i = 1; i < bt_node->num_elements; i++) { */
/*             // loop through the nodes */
/*             if (* */
/*         } */
/*     } */


/* } */


void insert_value(BPTNode* bt_node, int value, size_t position) {
    /* if (bt_node->is_leaf) { */
    /*     if (bt_node */
    /*     // */
    /* } */
}

void rebalance();
void delete();



void testing_print() {
    BPTNode* root = create_node();
    root->num_elements = 2;
    root->node_vals[0] = 0;
    root->node_vals[1] = 10;

    BPTNode* left = create_node();
    root->bpt_meta.bpt_ptrs.children[0] = left;
    left->num_elements = 1;
    left->node_vals[0] = -1;

    BPTNode* asdf = create_leaf();
    left->bpt_meta.bpt_ptrs.children[0] = asdf;
    asdf->num_elements = 1;
    asdf->node_vals[0] = -15;
    asdf->bpt_meta.bpt_leaf.col_pos[0] = 225;

    BPTNode* qwer = create_leaf();
    left->bpt_meta.bpt_ptrs.children[1] = qwer;
    qwer->num_elements = 1;
    qwer->node_vals[0] = -30;
    qwer->bpt_meta.bpt_leaf.col_pos[0] = 900;


    BPTNode* middle = create_leaf();
    root->bpt_meta.bpt_ptrs.children[1] = middle;
    middle->num_elements = 1;
    middle->node_vals[0] = 5;
    middle->bpt_meta.bpt_leaf.col_pos[0] = 25;

    BPTNode* right = create_leaf();
    root->bpt_meta.bpt_ptrs.children[2] = right;
    right->num_elements = 1;
    right->node_vals[0] = 200;
    middle->bpt_meta.bpt_leaf.col_pos[0] = 40000;

    print_tree(root);

    free(right);
    free(middle);
    free(qwer);
    free(asdf);
    free(left);
    free(root);
}

int main(void) {
    /* testing_print(); */
    /* printf("The page size for this system is %ld bytes.\n", */
    /*        sysconf(_SC_PAGESIZE)); /1* _SC_PAGE_SIZE is OK too. *1/ */

    printf("Testing leaf insert\n");
    test_leaf_insert();
    return 0;
}
