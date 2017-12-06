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
        printf("(LEVEL %u) ",node->bpt_meta.bpt_ptrs.level);
    } else {
        printf("\n(LEAF %zu) ", leafcount++);
    }

    for (size_t i = 0; i < node->num_elements; i++) {
        if (node->is_leaf) {
            printf(
                "\n\t{%d, %zu}",
                node->node_vals[i],
                node->bpt_meta.bpt_leaf.col_pos[i]
            );
        } else {
            printf("%d ", node->node_vals[i]);
        }
    }
    printf("]");
    if (leafcount == 0) {
        printf("\n");
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

// TODO unused
void add_to_results(Result* result, size_t value) {
    if (result->num_tuples == result->capacity) {
        increase_result_array(result);
    }
    // add the value to the array
    ((size_t*)result->payload)[result->num_tuples++] = value;
}

/**
 * @brief This function inserts into our result array using memcopy
 *
 * @param result - object to add to
 * @param data - the data
 * @param num_items - number of items
 */
void insert_into_results(Result* result, size_t* data, size_t num_items) {
    // we want to copy into the result column
    if (result->num_tuples + num_items > result->capacity) {
        result->capacity = result->num_tuples + num_items;
        result->capacity *= 2;
        result->payload = realloc(result->payload, sizeof(size_t) * result->capacity);
    }
    // where to start the copying
    void* start = (size_t*) result->payload + result->num_tuples;
    memcpy(start, data, num_items * sizeof(size_t));
    result->num_tuples += num_items;
}

/**
 * @brief This function finds the leaf that contains a value
 *
 * @param bt_node - the node to search (most likely the root)
 * @param value - the value to search for
 *
 * @return the right most node that contains the value
 */
BPTNode* search_for_leaf(BPTNode* bt_node, int value) {
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
        bt_node = bt_node->bpt_meta.bpt_ptrs.children[i];
    }
    // the last item in the stack will be the leaf
    assert(bt_node->is_leaf == true);
    return bt_node;
}


/**
 * @brief Takes in a range [low, high) and returns an result
 * that will hold all of the indices that correlate with the range
 *
 * @param root - the bplus tree root to search from
 * @param gte_val - the min value
 * @param lt_val - the max value
 *
 * @return  result - the result (allocated in function)
 */
Result* find_values_unclustered(BPTNode* root, int gte_val, int lt_val) {
    Result* result = malloc(sizeof(Result));
    result->capacity = MAX_KEYS;
    result->num_tuples = 0;
    result->payload = malloc(sizeof(size_t) * result->capacity);

    // if the high bound is the rightmost leaf and nothing satisfies it
    // then we know that there is no match
    BPTNode* high_bound = search_for_leaf(root, lt_val);
    if (high_bound->node_vals[0] < lt_val &&
            high_bound->bpt_meta.bpt_leaf.prev_leaf == NULL) {
        return result;
    }

    // if the low bound is the rightmost leaf and it doesn't fit
    // in the range then we know that we have no data the satisfies
    BPTNode* low_bound = search_for_leaf(root, gte_val);
    if (low_bound->node_vals[low_bound->num_elements - 1] < lt_val &&
            low_bound->bpt_meta.bpt_leaf.next_leaf == NULL) {
        return result;
    }

    // Next we need to adjust the low bound. We want the low bound to be
    // the lowest leaf that does contain a value that satisfies the
    // predicate - we know know to look backwards if the current "low_bound"
    // has a value that meets this condition (if the low_bound has
    // a leaf that is greater than or equal)
    while (low_bound->node_vals[0] >= gte_val) {
        // get the previous leaf
        BPTNode* prev = low_bound->bpt_meta.bpt_leaf.prev_leaf;
        // check backwards to see if that leaf could contain this value
        // (by checking the max ie: [0, 3, 3] <--> [3, 4, 5]
        if (prev && prev->node_vals[prev->num_elements - 1] >= gte_val) {
            // the previous node is gte the current, so we need to shift back
            low_bound = prev;
        } else {
            break;
        }
    }

    // we should never need to push this forward
    // if the high bound starts will a value that should not be included we
    // need to shift back a node!
    while (high_bound->node_vals[0] >= lt_val) {
        BPTNode* prev = high_bound->bpt_meta.bpt_leaf.prev_leaf;
        if (prev) {
            high_bound = prev;
        } else {
            break;
        }
    }

    // find the first value that meets the condition
    // if nothing fits, then we will have an index that equals the
    // numbe of elements in the list
    size_t low_idx = 0;
    while (low_idx < low_bound->num_elements &&
            low_bound->node_vals[low_idx] < gte_val) {
        low_idx++;
    }

    // if the low bound doesn't equal the high bound then we will just
    // jump to t
    while (low_bound != high_bound) {
        // if we have a situation where multiple leaves fit the bill
        // copy from the matching value over
        if (low_idx != low_bound->num_elements) {
            insert_into_results(result,
                                &low_bound->bpt_meta.bpt_leaf.col_pos[low_idx],
                                low_bound->num_elements - low_idx);
        }
        // the next will have to be contained if it doesn't equal
        // the high bound
        low_bound = low_bound->bpt_meta.bpt_leaf.next_leaf;
        low_idx = 0;
    }

    // once they are equal we need to copy the correct swath -
    // we only want up to the top bound (exclusive)
    size_t high_idx = 0;
    while (high_idx < high_bound->num_elements &&
            high_bound->node_vals[high_idx] < lt_val) {
        high_idx++;
    }
    // now insert!
    if (high_idx - low_idx > 0) {
        insert_into_results(result,
                            &high_bound->bpt_meta.bpt_leaf.col_pos[low_idx],
                            high_idx - low_idx);
        if (result->capacity != result->num_tuples) {
            result->payload = realloc(result->payload,
                                      sizeof(size_t) * result->num_tuples);
        }
    }
    return result;
}

/**
 * @brief this is the function for selecting from a clustered index
 *
 * @param root
 * @param gte_val
 * @param lt_val
 *
 * @return
 */
Result* find_values_clustered(BPTNode* root, int gte_val, int lt_val) {
    Result* result = malloc(sizeof(Result));
    result->num_tuples = 0;
    result->payload = NULL;

    // get right bound and check that it works
    BPTNode* high_bound = search_for_leaf(root, lt_val);
    if (high_bound->node_vals[0] < lt_val &&
            high_bound->bpt_meta.bpt_leaf.prev_leaf == NULL) {
        return result;
    }

    // get left bound and check that it also works
    BPTNode* low_bound = search_for_leaf(root, gte_val);
    if (low_bound->node_vals[low_bound->num_elements - 1] < lt_val &&
            low_bound->bpt_meta.bpt_leaf.next_leaf == NULL) {
        return result;
    }

    // next find the lowest low bound that satisfies the predicate
    // (we need to do this check to make sure we don't miss out on the
    //  far left values)
    while (low_bound->node_vals[0] >= gte_val) {
        // get the previous leaf
        BPTNode* prev = low_bound->bpt_meta.bpt_leaf.prev_leaf;
        // check backwards to see if that leaf could contain this value
        // (by checking the max ie: [0, 3, 3] <--> [3, 4, 5]
        if (prev && prev->node_vals[prev->num_elements - 1] >= gte_val) {
            // the previous node is gte the current, so we need to shift back
            low_bound = prev;
        } else {
            break;
        }
    }

    // reign in the upper bound so we only go as far as the right most
    while (high_bound->node_vals[0] >= lt_val) {
        BPTNode* prev = high_bound->bpt_meta.bpt_leaf.prev_leaf;
        if (prev) {
            high_bound = prev;
        } else {
            break;
        }
    }

    // find the first value that meets the condition -
    // we then want to store this index as the start of the
    // range of indices that we will be getting
    size_t low_idx = 0;
    while (low_idx < low_bound->num_elements &&
            low_bound->node_vals[low_idx] < gte_val) {
        low_idx++;
        if (low_idx == low_bound->num_elements) {
            // TODO: Warning there could be problems from doing this
            low_bound = low_bound->bpt_meta.bpt_leaf.next_leaf;
            low_idx = 0;

        }
    }
    // if the left bound doesn't have anything that matches then
    // means that the low bound should equal the high bound
    // we shouldn't have this case
    assert(low_idx != low_bound->num_elements);

    low_idx = low_bound->bpt_meta.bpt_leaf.col_pos[low_idx];

    // This is the high index. This is the last value that satisfies
    // this condition
    size_t high_idx = 0;
    size_t plus_one = 0; // whether we need to include the upper bound
    while (high_idx + 1 < high_bound->num_elements &&
            high_bound->node_vals[high_idx] < lt_val) {
        high_idx++;
    }

    // this is the high bound - we will add one more if the top of the bound
    // should include this extra value
    if (high_idx + 1 == high_bound->num_elements &&
            high_bound->node_vals[high_idx] < lt_val) {
        plus_one = 1;
    }

    // the new upper index
    high_idx = high_bound->bpt_meta.bpt_leaf.col_pos[high_idx] + plus_one;
    result->capacity = high_idx - low_idx;
    result->num_tuples = result->capacity;
    result->payload = malloc(sizeof(size_t) * result->capacity);
    int i = 0;
    while (low_idx < high_idx) {
        ((size_t*)result->payload)[i++] = low_idx++;
    }
    return result;
}


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
void split_leaf(BPTNode* bt_node, int value, size_t pos, SplitNode* split_leaf) {
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
        if (value < values[i] || (value == values[i] && pos < positions[i])) {
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
    split_leaf->left_leaf = bt_node;
    split_leaf->left_leaf->num_elements = middle;
    memcpy((void*) split_leaf->left_leaf->node_vals,
            (void*) values,
            middle * sizeof(int));
    memcpy((void*) split_leaf->left_leaf->bpt_meta.bpt_leaf.col_pos,
            (void*) positions,
            middle * sizeof(size_t));

    // set the right leaf to be all the current values at the n/2 + 1 location
    split_leaf->right_leaf = create_leaf();
    split_leaf->right_leaf->num_elements = num_right;
    memcpy((void*) split_leaf->right_leaf->node_vals,
            (void*) &values[middle],
            num_right * sizeof(int));
    memcpy((void*) split_leaf->right_leaf->bpt_meta.bpt_leaf.col_pos,
            (void*) &positions[middle],
            num_right * sizeof(size_t));

    // set the median value
    split_leaf->middle_val = values[middle];

    // set interleaf pointers
    // the right leaf should point back to the left leaf
    split_leaf->right_leaf->bpt_meta.bpt_leaf.prev_leaf =
            split_leaf->left_leaf;

    // the right leaf should now point to where the left leaf pointed
    BPTNode* old_next = split_leaf->left_leaf->bpt_meta.bpt_leaf.next_leaf;
    split_leaf->right_leaf->bpt_meta.bpt_leaf.next_leaf = old_next;
    // if it pointed to anything, we need to update that as well so
    // it will now point to the new right leaf
    if (old_next) {
        old_next->bpt_meta.bpt_leaf.prev_leaf = split_leaf->right_leaf;
    }
    // last the left leaf's next should be the right leaf
    split_leaf->left_leaf->bpt_meta.bpt_leaf.next_leaf =
            split_leaf->right_leaf;

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


/// ***************************************************************************
/// B Plus Tree Body Insertions
/// ***************************************************************************

/**
 * @brief This function will take a kicked up split node and will
 *      insert it into the tree
 *
 * @param bt_node - this is the node that we will be inserting into
 * @param split_node - this is the node that we will be add
 */
void insert_into_tree_body(BPTNode* bt_node, SplitNode* split_node) {
    assert(bt_node != NULL);
    assert(split_node != NULL);
    assert(bt_node->num_elements < MAX_KEYS);

    // when we have a brand new node we need to also set the left fence value
    // In all other cases we will just set the right fence
    if (bt_node->num_elements == 0) {
        // set left fence
        bt_node->bpt_meta.bpt_ptrs.children[bt_node->num_elements] =
                split_node->left_leaf;
    }

    // inserting in all other cases - get the
    size_t i = 0;
    while (i < bt_node->num_elements) {
        if (split_node->middle_val < bt_node->node_vals[i]) {
            // shift positions right
            memmove((void*) &bt_node->node_vals[i + 1],
                    (void*) &bt_node->node_vals[i],
                    (bt_node->num_elements - i) * sizeof(int));
            // shift pointers right
            memmove((void*) &bt_node->bpt_meta.bpt_ptrs.children[i + 2],
                    (void*) &bt_node->bpt_meta.bpt_ptrs.children[i + 1],
                    (bt_node->num_elements - i) * sizeof(BPTNode*));
            break;
        }
        i++;
    }

    // place the node in the correct spot
    bt_node->node_vals[i] = split_node->middle_val;
    bt_node->bpt_meta.bpt_ptrs.children[i + 1] = split_node->right_leaf;

    // increase the number of elements
    bt_node->num_elements++;
}

/**
 * @brief this function takes a current parent node and will
 *  add the kicked up child node. It will then split itself
 *  into a left and right half
 *
 * @param bt_node - parent node
 * @param insert_node - split node to add
 * @param result_node - the node we want to attach the result to
 */
void split_body_node(BPTNode* bt_node, SplitNode* insert_node, SplitNode* result_node) {
    assert(bt_node->is_leaf == false);
    assert(bt_node->num_elements == MAX_KEYS);

    // this is the temp array
    size_t temp_buf_len = MAX_KEYS + 1;
    /* size_t temp_buf_len = MAX_KEYS + 1; */
    int values[temp_buf_len];
    BPTNode* pointers[MAX_DEGREE + 1];

    // set these to be equal
    memcpy((void*) values,
            (void*)bt_node->node_vals,
            MAX_KEYS * sizeof(int));
    memcpy((void*) pointers,
            (void*)bt_node->bpt_meta.bpt_ptrs.children,
            MAX_DEGREE * sizeof(BPTNode*));

    // this is the loop that handles the insertion
    size_t i = 0;
    while (i < MAX_KEYS) {
        if (insert_node->middle_val < bt_node->node_vals[i]) {
            // shift positions right
            memmove((void*) &values[i + 1],
                    (void*) &values[i],
                    (MAX_KEYS - i) * sizeof(int));
            // shift pointers right
            memmove((void*) &pointers[i + 2],
                    (void*) &pointers[i + 1],
                    (MAX_KEYS - i) * sizeof(BPTNode*));
            break;
        }
        i++;
    }
    values[i] = insert_node->middle_val;
    pointers[i+1] = insert_node->right_leaf;

    // now that we have a buffer full of the results we want to
    // place the values in the correct nodes
    size_t middle = temp_buf_len / 2;

    // set the left node (it will have equal or more)
    result_node->left_leaf = bt_node;
    result_node->left_leaf->num_elements = middle;
    memcpy((void*) result_node->left_leaf->node_vals,
            (void*) values,
            middle * sizeof(int));
    memcpy((void*) result_node->left_leaf->bpt_meta.bpt_leaf.col_pos,
            (void*) pointers,
            (middle + 1) * sizeof(size_t));

    // set the right leaf to be all the current values at the n/2 + 1 location
    // we will take everything not including the middle
    size_t num_right = temp_buf_len - middle - 1;
    // we need to create a new node now
    result_node->right_leaf = create_node();
    result_node->right_leaf->bpt_meta.bpt_ptrs.level =
            result_node->left_leaf->bpt_meta.bpt_ptrs.level;

    result_node->right_leaf->num_elements = num_right;
    memcpy((void*) result_node->right_leaf->node_vals,
            (void*) &values[middle + 1],
            num_right * sizeof(int));
    memcpy((void*) result_node->right_leaf->bpt_meta.bpt_leaf.col_pos,
            (void*) &pointers[middle + 1],
            (num_right + 1) * sizeof(size_t));

    // set the median value
    result_node->middle_val = values[middle];

    return;
}

/**
 * @brief This function takes a node and will insert into it recursively.
 *      - if there is no node to insert into we will create a new node and
 *  add to it.
 *      - if there is a nonfull node then we will add that split node to it
 *      - if the parent is full we will add to it and split it into two
 *          parts. we will then recursively add to its parents until we
 *          either hit case 1 or 2
 *
 * @param bt_node - the parent node
 * @param split_node - the kicked up node that should be added
 * @param access_stack - this is the list of accesses (a stack)
 *
 * @return Head of the tree
 */
BPTNode* rebalanced_insert(
    BPTNode* bt_node,
    SplitNode* split_node,
    BPTNodeStack* access_stack
) {
    if (bt_node == NULL) {
        bt_node = create_node();
        bt_node->bpt_meta.bpt_ptrs.level = 0;
        // nodee to update
        insert_into_tree_body(bt_node, split_node);
        return bt_node;
    } else if (bt_node->num_elements < MAX_KEYS) {
        insert_into_tree_body(bt_node, split_node);
        if (isEmpty(access_stack)) {
            return bt_node;
        }
        return access_stack->array[0];
    } else {
        SplitNode new_split;
        bt_node->bpt_meta.bpt_ptrs.level++;
        split_body_node(bt_node, split_node, &new_split);
        return rebalanced_insert(pop(access_stack),
                          &new_split,
                          access_stack);

    }
    return NULL;
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


    /* BPTNode* parent = pop(access_stack); */
    bt_node = rebalanced_insert(
        pop(access_stack),
        split_node,
        access_stack
    );

    // cleanup memory
    free(split_node);
    free_stack(access_stack);
    return bt_node;
}

#if TESTING
void testing_kick_up() {
    srand(time(NULL));

    BPTNode* root = NULL;
    for (int i = 1; i < 100; i++) {
        int val = rand() % 200;
        size_t pos = (size_t) rand() % 2000;
        /* printf("\nInserting %dth: value %d, position %zu\n", i, val, pos); */
        root = insert_value(root, val, pos);
    }
    print_tree(root);
    free_tree(root);

}

void testing_search() {
    srand(time(NULL));
    BPTNode* root = NULL;
    size_t pos = 0;
    root = insert_value(root, 3, pos++);
    root = insert_value(root, 3, pos++);
    root = insert_value(root, 3, pos++);
    root = insert_value(root, 3, pos++);
    for (int i = 0; i < 25; i++) {
        for (int j = 0; j < 5; j++) {
            int val = 10 + i;
            root = insert_value(root, val, pos++);
        }
    }
    /* root = insert_value(root, 10, pos++); */
    /* root = insert_value(root, 10, pos++); */
    /* root = insert_value(root, 10, pos++); */
    /* root = insert_value(root, 10, pos++); */
    /*     } */
    /* } */
    print_tree(root);

    /* Result* result = find_values_unclustered(root, 0, 100); */
    Result* result = find_values_clustered(root, 4, 5);
    /* Result* result = find_values_unclustered(root, 4, 6); */
    /* Result* result = find_values_unclustered(root, 15, 18); */
    for (size_t i = 0; i < result->num_tuples;) {
        printf("%zu\n", ((size_t*)result->payload)[i++]);
    }
    printf("\n TOTAL: %zu\n", result->num_tuples);
    free(result->payload);
    free(result);
    free_tree(root);
}
#endif



#if TESTING
int main(void) {
    printf("Testing leaf insert\n");
    testing_search();
    printf("SIZE is %zu\n", sizeof(BPTNode));
    return 0;
}
#endif
