#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include "b_tree.h"
#if TESTING
#include <time.h>
#endif

// function to create a stack of given capacity. It initializes size of
// stack as 0
#define DEFAULT_DEPTH 32
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
        stack->capacity *= 2;
        stack->array = realloc(stack->array,
                               stack->capacity * sizeof(BPTNode*));
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

void free_stack(BPTNodeStack* stack) {
    free(stack->array);
    free(stack);
}

/// **************************************************************************
/// Creation functions
/// **************************************************************************

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
    BPTNode* node = allocate_node(false);
    node->bpt_meta.bpt_ptrs.level = (unsigned) -1;
    return node;
}

/// Make a leaf
BPTNode* create_leaf() {
    return allocate_node(true);
}


/// **************************************************************************
/// Helper functions
/// **************************************************************************

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

/* size_t node_search(int* values, size_t* data, size_t l_pos, size_t r_pos, int x, size_t pos) { */
/*    if (r_pos >= l_pos) { */
/*         size_t mid = l_pos + (r_pos - l_pos) / 2; */

/*         // If the element is present at the middle */
/*         if (values[mid] == x) { */
/*             if (data[mid] > pos) { */
/*                 while(mid > l_pos && data[mid] > pos && values[mid] == x) { */
/*                     mid--; */
/*                 } */
/*             } else { */
/*                 while(mid < r_pos && data[mid] < pos && values[mid] == x) { */
/*                     mid++; */
/*                 } */
/*             } */
/*             return mid; */
/*         } */

/*         // If element is smaller than mid, then it can only be present */
/*         // in left subarray */
/*         if (arr[mid] > x) { */
/*             return binary_search(arr, l_pos, mid - 1, x); */
/*         } */

/*         // Else the element can only be present in right subarray */
/*         return binary_search(arr, mid + 1, r_pos, x); */
/*    } */
/*    // We reach here when element is not present in array */
/*    return 0; */
/* } */


/**
 * @brief Function that tells you whether a node is the parent of another node
 *
 * @param parent - a node at a higher level
 * @param child - a node at a lower level
 *
 * @return bool whether they are ancestors
 */
bool is_child(BPTNode* parent, BPTNode* child) {
    for (size_t i = 0; i < parent->num_elements; i++) {
        if (parent->bpt_meta.bpt_ptrs.children[i] == child) {
            return true;
        }
    }
    return false;
}

#if TESTING
/**
 * @brief Function that prints out the values of a PointerNode
 *  print results like [ val, val, val ]
 *
 * @param node - node to print
 */
size_t leafcount = 0;
unsigned current_level = 0;

void print_node(BPTNode* node) {
    printf("[ ");
    if (node->is_leaf == false){
        /* node->bpt_meta.bpt_ptrs.level */
        /* if ( */
        printf("(LEVEL %u) ",node->bpt_meta.bpt_ptrs.level);
    } else {
        printf("(LEAF %zu) ", leafcount++);
    }

    for (size_t i = 0; i < node->num_elements; i++) {
        printf("%d ", node->node_vals[i]);
    }
    printf("]");
    if (leafcount == 0) {
        printf("\n");
    } else {
        printf("\t");
    }
}

/**
 * @brief Function that prints out a leaf node.
 *  prints [ (key, data), (key, data) ]
 *
 * @param node - node to print
 */
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

/**
 * @brief Function that prints a tree from a given node
 *      there should be a new line for each level (does
 *      printing in a BFS manner
 *
 * @param node - node to print
 */
void bfs_traverse_tree(BPTNode* node, bool print) {
    BPTNode** all_nodes = malloc(sizeof(BPTNode*) * 2);
    size_t num_nodes = 1;
    size_t node_idx = 0;
    all_nodes[node_idx] = node;

    if (print) {
        leafcount = 0;
    }

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
        if (print) {
            print_node(current_node);
        } else {
            free(current_node);
        }
        node_idx++;
    }
    free(all_nodes);
    if (print) {
        printf("\n");
    }
}
void print_tree(BPTNode* node) {
    bfs_traverse_tree(node, true);
}
void free_tree(BPTNode* node) {
    bfs_traverse_tree(node, false);
}


/**
 * @brief Function that tests our print
 */
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
#endif


/// ***************************************************************************
/// Searching Functions
/// ***************************************************************************

/**
 * @brief This function increases the size of a result array
 *      TODO: make it so the function can increase using an actual
 *      result from the database
 *
 * @param result
 */
void increase_result_array(Result *result) {
    result->capacity *= 2;
    result->payload = realloc(result->payload, sizeof(size_t) * result->capacity);
}

void add_to_results(Result* result, size_t value) {
    if (result->num_tuples == result->capacity) {
        increase_result_array(result);
    }
    // add the value to the array
    ((size_t*)result->payload)[result->num_tuples++] = value;
}
/* size_t find_value(BPTNode* bt_node, int value, Result* result); */


/// **************************************************************************
/// Insertion Functions
/// **************************************************************************

/**
 * @brief Function for adding (key,value) to a position (once we know that
 *      the leaf has space
 *
 * @param bt_node - node to add to - cannot be full
 * @param value - value to add
 * @param position - position to add
 */
void insert_into_leaf(BPTNode* bt_node, int value, size_t position) {
    // start at the first value
    assert(bt_node->num_elements < MAX_KEYS);
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

#if TESTING
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
    #if MAX_KEYS >= 3
    insert_into_leaf(node2, 10, 1);
    #endif
    printf("== Expected ==\n");
    printf("[ (5, 4) (10, 1) (10, 3) ]\n");
    print_leaf(node2);
    printf("== Result ==\n");
    free(node2);
}
#endif


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

#if TESTING
// function that tests splitting the leaf
void test_split_leaf() {
    SplitNode split_node;
    printf("Splitting Node\n");
    BPTNode* node = create_leaf();
    insert_into_leaf(node, 10, 3);
    insert_into_leaf(node, 5, 4);
    #if MAX_KEYS >= 3
    insert_into_leaf(node, 8, 4);
    #endif
    print_leaf(node);


    split_leaf(node, 12, 3, &split_node);
    print_leaf(split_node.left_leaf);
    print_leaf(split_node.right_leaf);
    printf("Middle %d\n", split_node.middle_val);
    free(node);
}
#endif

/**
 * @brief Function for adding a key, value pair to a leaf
 *
 * @param bt_node - node to add to
 * @param value - value to add to the node
 * @param position - position
 */
SplitNode* add_to_leaf(BPTNode* bt_node, int value, size_t position) {
    assert(bt_node->is_leaf == true);
    // we can either do a naive insert or we
    // need to insert into a full node
    if (bt_node->num_elements < MAX_KEYS) {
        insert_into_leaf(bt_node, value, position);
        return NULL;
    } else {
        SplitNode* split_node = malloc(sizeof(SplitNode));
        split_leaf(bt_node, value, position, split_node);
        // TODO: set the pointers so the go to eachother
        return split_node;
    }
}

/**
 * @brief Function that takes in a node and a value and returns the
 *  node that should eventually contain the leaf
 *      [leaf, parent, grandparent, ...root]
 *
 * @param bt_node - this is the node we start our quest at
 * @param value - this is the value that we are adding
 *
 * @return BPTNodeStack - this is a stack that contains the access list
 */
BPTNodeStack* find_leaf(BPTNode* bt_node, int value) {
    BPTNodeStack* access_stack = createStack(32);
    while (bt_node->is_leaf == false) {
        size_t i = 0;
        while (i < bt_node->num_elements) {
            if (value < bt_node->node_vals[i]) {
                break;
            } else {
                i++;
            }
        }
        // once we have found a child, push to the access stack
        // and move to checkout the child
        push(access_stack, bt_node);
        bt_node = bt_node->bpt_meta.bpt_ptrs.children[i];
    }
    // the last item in the stack will be the leaf
    push(access_stack, bt_node);
    return access_stack;
}

void insert_into_tree_body(BPTNode* bt_node, SplitNode* split_node) {
    assert(bt_node != NULL);
    assert(split_node != NULL);

    assert(bt_node->num_elements < MAX_KEYS);

    // when we have a brand new node we need to also set the left fence value
    // In all other cases we will just set the right fence
    if (bt_node->num_elements == 0) {
        bt_node->bpt_meta.bpt_ptrs.children[bt_node->num_elements] =
                split_node->left_leaf;
    }
    // TODO: Determine if this is the correct place to make them point to
    // eachother - should this happen in the creation of the split??
    split_node->left_leaf->bpt_meta.bpt_leaf.next_leaf = split_node->right_leaf;
    split_node->right_leaf->bpt_meta.bpt_leaf.prev_leaf = split_node->left_leaf;

    // set the middle and then increment
    bt_node->node_vals[bt_node->num_elements++] = split_node->middle_val;

    // At the end we will set the right leaf
    bt_node->bpt_meta.bpt_ptrs.children[bt_node->num_elements] =
            split_node->right_leaf;
}

/**
 * @brief Function for inserting into a tree
 *
 * @param bt_node
 * @param value
 * @param position
 *
 * @return new head of the btree
 */
BPTNode* insert_value(BPTNode* bt_node, int value, size_t position) {
    // if we don't have a first value, make a first value
    if (bt_node == NULL) {
        bt_node = create_leaf();
        add_to_leaf(bt_node, value, position);
        return bt_node;
    }

    // OTHERWISE - we need to handle normal insertion
    // find the leaf that we will add to
    BPTNodeStack* access_stack = find_leaf(bt_node, value);
    BPTNode* leaf = pop(access_stack);
    SplitNode* split_node = add_to_leaf(leaf, value, position);

    // if we don't have to split, we just return the same head
    if (split_node == NULL) {
        free_stack(access_stack);
        return bt_node;
    }

    // Handle rebalancing - this is the case when we have 1 empty value
    if (isEmpty(access_stack)) {
        // base case, this is our first split
        bt_node = create_node();
        bt_node->bpt_meta.bpt_ptrs.level = 0;
        insert_into_tree_body(bt_node, split_node);
    } else {
        insert_into_tree_body(bt_node, split_node);
    }

    // cleanup memory
    free(split_node);
    free_stack(access_stack);
    return bt_node;
}

void testing_kick_up() {
    BPTNode* root = insert_value(NULL, 12, 4);
    /* print_tree(root); */
    printf("Now adding values\n");
    root = insert_value(root, 8, 4);
    root = insert_value(root, 0, 2);
    /* print_tree(root); */

    /* root = insert_value(root, 3, 4); */
    root = insert_value(root, 33, 4);
    /* print_tree(root); */
    bfs_traverse_tree(root, true);
    /* root = insert_value(root, 5, 2); */
    /* root = insert_value(root, 99, 4); */
    /* root = insert_value(root, 2, 2); */
    /* free(root); */

}


/// ***************************************************************************
/// B Plus Tree Body Insertions
/// ***************************************************************************

void add_to_pointer_node(SplitNode split_node){
    (void) split_node;
}




#if TESTING
int main(void) {
    printf("Testing leaf insert\n");
    testing_kick_up();
    return 0;
}
#endif
