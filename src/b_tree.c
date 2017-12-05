#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include "b_tree.h"

// function to create a stack of given capacity. It initializes size of
// stack as 0
BPTNodeStack* createStack(unsigned capacity) {
    BPTNodeStack* stack = malloc(sizeof(BPTNodeStack));
    stack->capacity = capacity;
    stack->top = -1;
    stack->array = malloc(stack->capacity * sizeof(BPTNode*));
    return stack;
}

// BPTNodeStack is full when top is equal to the last index
int isFull(BPTNodeStack* stack) {
    return ((unsigned) stack->top) == stack->capacity - 1;
}

// BPTNodeStack is empty when top is equal to -1
int isEmpty(BPTNodeStack* stack) {
    return stack->top == -1;
}

// Function to add an item to stack.  It increases top by 1
void push(BPTNodeStack* stack, BPTNode* item) {
    if (isFull(stack)) {
        return;
    }
    stack->array[++stack->top] = item;
}

// Function to remove an item from stack.  It decreases top by 1
BPTNode* pop(BPTNodeStack* stack) {
    if (isEmpty(stack)) {
        return NULL;
    }
    return stack->array[stack->top--];
}


// IDK WHAT to do with this
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

/**
 * @brief Simple function that makes sure that inseting into the
 *  leaves works
 */
void test_leaf_insert() {
    // single insert test
    printf("Single Insert\n");
    BPTNode* node = create_node();
    insert_into_leaf(node, 10, 1);
    printf("== Expected ==\n");
    printf("[ (10, 1) ]\n");
    print_leaf(node);
    printf("== Result ==\n");
    free(node);

    // multiple insert test
    printf("Multiple Insert\n");
    BPTNode* node2 = create_leaf();
    insert_into_leaf(node2, 10, 3);
    insert_into_leaf(node2, 5, 4);
    insert_into_leaf(node2, 10, 1);
    printf("== Expected ==\n");
    printf("[ (5, 4) (10, 1) (10, 3) ]\n");
    print_leaf(node2);
    printf("== Result ==\n");
    free(node2);
}

void find_leaf(BPTNode* bt_node, int value, BPTNodeStack* access_list) {
    /* for ( */

}

typedef struct SplitNode {
    BPTNode* left_leaf;
    BPTNode* right_leaf;
    int middle_val;
} SplitNode;

/**
 * @brief Function that takes in a full leaf and a next value and
 *  returns a struct that contains the new left and right pointers
 *  as well as the median values that should be kicked up the tree
 *
 * @param bt_node - node to split
 * @param value - new value being added
 * @param pos - the new position of the new value
 * @param result_node - the output node
 */
void split_leaf(BPTNode* bt_node, int value, size_t pos, SplitNode* split_node) {
    // simple checks
    assert(bt_node->is_leaf == true);
    assert(bt_node->num_elements == MAX_KEYS);

    // this is the temp array
    size_t temp_buf_len = MAX_KEYS + 1;
    int values[temp_buf_len];
    size_t positions[temp_buf_len];

    // set these to be equal
    memcpy((void*) values,
            (void*)bt_node->node_vals,
            MAX_KEYS * sizeof(int));
    memcpy((void*) positions,
            (void*)bt_node->bpt_meta.bpt_leaf.col_pos,
            MAX_KEYS * sizeof(size_t));

    // this is the loop that handles the insertion
    size_t i = 0;
    while (i < MAX_KEYS) {
        if (value < values[i] || (value == values[i] && pos > positions[i])) {
            // shift positions right
            memmove((void*) &values[i + 1],
                    (void*) &values[i],
                    (MAX_KEYS - i) * sizeof(int));
            memmove((void*) &positions[i + 1],
                    (void*) &positions[i],
                    (MAX_KEYS - i) * sizeof(size_t));
            break;
        }
        i++;
    }
    values[i] = value;
    positions[i] = pos;

    // now that we have a buffer full of the results we want to
    // place the values in the correct nodes
    size_t middle = temp_buf_len / 2;
    size_t num_right = temp_buf_len - middle;

    // set the left node (it will have fewer values
    split_node->left_leaf = bt_node;
    split_node->left_leaf->num_elements = middle;
    memcpy((void*) split_node->left_leaf->node_vals,
            (void*) values,
            middle * sizeof(int));
    memcpy((void*) split_node->left_leaf->bpt_meta.bpt_leaf.col_pos,
            (void*) positions,
            middle * sizeof(size_t));

    // set the right leaf to be all the current values at the n/2 + 1 location
    split_node->right_leaf = create_leaf();
    split_node->right_leaf->num_elements = num_right;
    memcpy((void*) split_node->right_leaf->node_vals,
            (void*) &values[middle],
            num_right * sizeof(int));
    memcpy((void*) split_node->right_leaf->bpt_meta.bpt_leaf.col_pos,
            (void*) &positions[middle],
            num_right * sizeof(size_t));
    // set the median value
    split_node->middle_val = values[middle];
}

// function that tests splitting the leaf
void test_split_leaf() {
    SplitNode split_node;
    printf("Splitting Node\n");
    BPTNode* node = create_leaf();
    insert_into_leaf(node, 10, 3);
    insert_into_leaf(node, 5, 4);
    insert_into_leaf(node, 8, 4);
    print_leaf(node);

    split_leaf(node, 12, 3, &split_node);
    print_leaf(split_node.left_leaf);
    print_leaf(split_node.right_leaf);
    printf("Middle %d\n", split_node.middle_val);
    free(node);
}

void add_to_leaf(BPTNode* bt_node, int value, size_t position) {
    assert(bt_node->is_leaf == true);
    // we can either do a naive insert or we
    // need to insert into a full node
    if (bt_node->num_elements < MAX_KEYS) {
        insert_into_leaf(bt_node, value, position);
    } else {
        SplitNode split_node;
        split_leaf(bt_node, value, position, &split_node);
        // TODO: set the pointers so the go to eachother
        add_to_pointer_node(split_node);
    }
}


void insert_value(BPTNode* bt_node, int value, size_t position) {
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
    printf("Testing leaf insert\n");
    test_split_leaf();


    return 0;
}
