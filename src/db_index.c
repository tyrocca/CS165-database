#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include "db_index.h"

/// ***************************************************************************
/// Sorted Index Functions
/// ***************************************************************************

/**
 * @brief Function that creates a sorted index
 *
 * @return sorted index pointer
 */
SortedIndex* create_sorted_index() {
    SortedIndex* sorted_index = malloc(sizeof(SortedIndex));
    // initialize the index
    sorted_index->col_positions = NULL;
    sorted_index->num_items = 0;
    return sorted_index;
}

/**
 * @brief Function that creates and returns the unclustered sorted index
 *
 * @return Sorted index pointer
 */
SortedIndex* create_unclustered_sorted_index(size_t alloc_space) {
    if (alloc_space == 0) {
        alloc_space = DEFAULT_COLUMN_SIZE;
    }
    SortedIndex* sorted_index = create_sorted_index();
    sorted_index->has_positions = true;
    sorted_index->allocated_space = alloc_space;
    sorted_index->keys = malloc(sizeof(int) * sorted_index->allocated_space);
    sorted_index->col_positions = malloc(
            sizeof(size_t) * sorted_index->allocated_space
    );
    return sorted_index;
}

SortedIndex* create_clustered_sorted_index(int* data) {
    SortedIndex* sorted_index = create_sorted_index();
    sorted_index->has_positions = false;
    sorted_index->keys = data;
    return sorted_index;
}

/**
 * @brief Function that increases the size of the sorted index
 *
 * @param sorted_index - the index to increase
 */
void increase_sorted_index(SortedIndex* sorted_index) {
    if (sorted_index->num_items >= sorted_index->allocated_space) {
        sorted_index->allocated_space *= 2;
        sorted_index->keys = realloc(
            sorted_index->keys,
            sizeof(int) * sorted_index->allocated_space
        );
        // realloc positions if necessary
        if (sorted_index->has_positions) {
            sorted_index->col_positions = realloc(
                sorted_index->col_positions,
                sizeof(size_t) * sorted_index->allocated_space
            );
        }
    }
}

/// **************************************************************************
/// Sorted index searching functions
/// **************************************************************************

/**
 * @brief This function takes in a value and returns the closest page
 *
 * @param value
 *
 * @return rounded down page
 */
size_t nearest_value(size_t value) {
    return (value / SORTED_NODE_SIZE) * SORTED_NODE_SIZE;
}

/**
 * @brief This function searches within a "sorted index node"
 *
 * @param arr - the data array
 * @param l_pos - the left position of the data
 * @param r_pos - the right positions of the data
 * @param value - the value that is being inserted
 * @param found - indicator is we found a result
 *
 * @return the lowest possible value that meets the condition
 */
size_t sorted_node_binary_search(
    int* arr,
    size_t l_pos,
    size_t r_pos,
    int value,
    bool* found
) {
   if (r_pos >= l_pos) {
        size_t mid = l_pos + (r_pos - l_pos) / 2;

        // If the element is present at the middle itself
        // should never hit 0
        if (arr[mid] == value) {
            *found = true;
            return mid;
        } else if (arr[mid] > value) {
            // cases when the middle value is larger:
            // if we are at the first value or if the next value is larger
            // we have found our index
            if (mid == 0 || arr[mid - 1] < value) {
                *found = true;
                return mid;
            }
            // otherwise search the remaining node
            return sorted_node_binary_search(arr, l_pos, mid - 1, value, found);
        }
        // Else the element can only be present in right subarray
        return sorted_node_binary_search(arr, mid + 1, r_pos, value, found);
   }
   *found = false;
   // We reach here when element is not present in array
   return 0;
}

// helper min and max functions
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

/**
 * @brief Cache concious binary search - it looks only at the values that would
 *  fit within a page
 *
 * @param arr
 * @param l_pos
 * @param r_pos
 * @param insert_val
 * @param found
 *
 * @return
 */
size_t search_sorted(int* arr, size_t l_pos, size_t r_pos, int insert_val, bool* found) {
    if (r_pos - l_pos < SORTED_NODE_SIZE) {
        return sorted_node_binary_search(arr, l_pos, r_pos, insert_val, found);
    } else if (r_pos >= l_pos) {
        size_t mid = l_pos + (r_pos - l_pos) / 2;
        // take the low bound
        size_t sub_l = nearest_value(mid);
        // take the high bound
        size_t sub_r = MIN(r_pos, sub_l + SORTED_NODE_SIZE - 1);

        size_t result = sorted_node_binary_search(arr, sub_l, sub_r,
                                                  insert_val, found);
        // if we find the result, move on
        if (*found) {
            return result;
        }

        // If element is smaller than mid, then it can only be present
        // in left subarray
        if (arr[sub_l] > insert_val) {
            return search_sorted(arr, l_pos, sub_l - 1, insert_val, found);
        }
        // Else the element can only be present in right subarray
        return search_sorted(arr, sub_r + 1, r_pos, insert_val, found);
    }
    // We reach here when element is not present in array
    *found = false;
    return 0;
}

/**
 * @brief This function returns the index where the value should be inserted
 *
 * @param sorted_index
 * @param value
 *
 * @return
 */
size_t get_sorted_idx(SortedIndex* sorted_index, int value) {
    size_t insert_idx;
    if (sorted_index->num_items == 0 || sorted_index->keys[0] >= value) {
        // then we don't want to change tne insert index
        insert_idx = 0;
    } else if (sorted_index->keys[sorted_index->num_items - 1] <= value) {
        insert_idx = sorted_index->num_items;
    } else {
        // otherwise search the index
        bool found_match = false;
        insert_idx = search_sorted(sorted_index->keys,
                                   0,
                                   sorted_index->num_items - 1,
                                   value,
                                   &found_match);
    }
    return insert_idx;
}

/**
 * @brief Add ability to sort?
 *
 * @param sorted_index
 * @param low
 * @param high
 * @param result
 *
 * @return values
 */
void get_range_sorted(SortedIndex* sorted_index, int low, int high, Result* result) {
    size_t low_bound = get_sorted_idx(sorted_index, low);
    size_t high_bound = get_sorted_idx(sorted_index, high);

    // make sure the low bound is the lowest qualifying value
    while (low_bound > 0 && sorted_index->keys[low_bound - 1] >= low) {
        low_bound--;
    }

    // make sure the high bound doesn't include any unqualified values
    while (high_bound >= low_bound && sorted_index->keys[high_bound - 1] >= high) {
        high_bound--;
    }

    // start setting the results
    result->data_type = INDEX;
    result->num_tuples = result->capacity = high_bound - low_bound;
    if (low_bound == high_bound) {
        result->num_tuples = 0;
        result->capacity = 0;
        return;
    }

    // allocate space for the result
    // then we can just memcopy into the array!
    result->payload = malloc(sizeof(size_t) * result->capacity);
    if (sorted_index->has_positions) {
        memcpy(result->payload,
               (void*) &sorted_index->col_positions[low_bound],
               (high_bound - low_bound) * sizeof(size_t));
    } else {
        size_t i = 0;
        while (low_bound < high_bound) {
            ((size_t*) result->payload)[i++] = low_bound++;
        }
    }
    return;
}

/// ***************************************************************************
/// Insertion functions
/// ***************************************************************************

/**
 * @brief This function does the insertion for an unclustered sorted index
 *
 * @param sorted_index
 * @param value
 * @param position
 */
void insert_into_sorted(SortedIndex* sorted_index, int value, size_t position) {
    size_t idx = get_sorted_idx(sorted_index, value);
    sorted_index->num_items++;

    // if our column is unclustered then we don't want to increase the index
    // we need to make space - we don't need to worry about this is
    // we have a clustered column
    increase_sorted_index(sorted_index);

    // we need to update the positions (we have a position that is
    // new)
    if (position + 1 < sorted_index->num_items) {
        for (size_t i = 0; i < sorted_index->num_items - 1; i++) {
            if (sorted_index->col_positions[i] >= position) {
                sorted_index->col_positions[i]++;
            }
        }
    }
    // if we need to move the data
    if (idx + 1 < sorted_index->num_items) {
        memmove((void*) &sorted_index->keys[idx + 1],
                (void*) &sorted_index->keys[idx],
                (sorted_index->num_items - idx - 1) * sizeof(int));
        memmove((void*) &sorted_index->col_positions[idx + 1],
                (void*) &sorted_index->col_positions[idx],
                (sorted_index->num_items - idx - 1) * sizeof(size_t));
    }
    sorted_index->col_positions[idx] = position;
    sorted_index->keys[idx] = value;
}

/**
 * @brief Helpful function that will print the sorted index
 *
 * @param sorted_index
 */
void print_sorted_index(SortedIndex* sorted_index) {
    for (size_t i = 0; i < sorted_index->num_items; i++) {
        printf("%zu value: ", i);
        if (sorted_index->has_positions) {
            printf(
                "%d, %zu\n",
                sorted_index->keys[i],
                sorted_index->col_positions[i]
            );
        } else {
            printf("%d\n", sorted_index->keys[i]);
        }
    }
}

void free_sorted_index(SortedIndex* sorted_index) {
    if (sorted_index->has_positions) {
        free(sorted_index->keys);
        free(sorted_index->col_positions);
    }
    free(sorted_index);
}


/// ***************************************************************************
/// Stack functions
/// ***************************************************************************

// function to create a stack of given capacity. It initializes size of
// stack as 0
#define DEFAULT_DEPTH 32
BPTNodeStack* create_stack(unsigned capacity) {
    BPTNodeStack* stack = malloc(sizeof(BPTNodeStack));
    stack->capacity = capacity;
    stack->top = -1;
    stack->array = malloc(stack->capacity * sizeof(BPTNode*));
    return stack;
}

// BPTNodeStack is full when top is equal to the last index
int stack_is_full(BPTNodeStack* stack) {
    return ((unsigned) stack->top) == stack->capacity - 1;
}

// BPTNodeStack is empty when top is equal to -1
int stack_is_empty(BPTNodeStack* stack) {
    return stack->top == -1;
}

// Function to add an item to stack.  It increases top by 1
void stack_push(BPTNodeStack* stack, BPTNode* item) {
    if (stack_is_full(stack)) {
        stack->capacity *= 2;
        stack->array = realloc(stack->array,
                               stack->capacity * sizeof(BPTNode*));
        return;
    }
    stack->array[++stack->top] = item;
}

// Function to remove an item from stack.  It decreases top by 1
BPTNode* stack_pop(BPTNodeStack* stack) {
    if (stack_is_empty(stack)) {
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
    BPTNode* new_node = calloc(1, sizeof(BPTNode));
    new_node->num_elements = 0;
    new_node->is_leaf = leaf;
    if (new_node->is_leaf) {
        new_node->bpt_meta.bpt_leaf.next_leaf = NULL;
        new_node->bpt_meta.bpt_leaf.prev_leaf = NULL;
    }
    return new_node;
}

// Make a node
BPTNode* create_node() {
    BPTNode* node = allocate_node(false);
    node->bpt_meta.bpt_ptrs.level = (unsigned) -1;
    return node;
}

// Make a leaf
BPTNode* create_leaf() {
    return allocate_node(true);
}


/// **************************************************************************
/// Helper functions
/// **************************************************************************

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

/* #if TESTING */
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
        printf("(LEVEL %u) ", node->bpt_meta.bpt_ptrs.level);
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

typedef enum BTreeTraversalOp {
    FREE_NODE,
    PRINT_NODE,
    DUMP_NODE,
} BTreeTraversalOp;

/**
 * @brief Function that prints a tree from a given node
 *      there should be a new line for each level (does
 *      printing in a BFS manner
 *
 * @param node - node to print
 * @param operation - tree operation to perform
 */

void bfs_traverse_tree(BPTNode* node, BTreeTraversalOp tree_op, FILE* dumpfile) {
    BPTNode** all_nodes = malloc(sizeof(BPTNode*) * 2);
    size_t num_nodes = 1;
    size_t node_idx = 0;
    all_nodes[node_idx] = node;

    if (tree_op == PRINT_NODE) {
        leafcount = 0;
    }

    while (node_idx < num_nodes) {
        // if there are bpt_ptrs.children nodes
        // copy the node into a current node
        BPTNode* current_node = all_nodes[node_idx];
        // if we have have a node with bpt_ptrs.children
        if (current_node->is_leaf == false) {
            // these are the number of nodes that we will be adding - we have
            // one extra because there are n + 1 children (as there are fences)
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
        switch (tree_op) {
            case PRINT_NODE:
                print_node(current_node);
                break;
            case FREE_NODE:
                free(current_node);
                break;
            case DUMP_NODE:
                fwrite(current_node, sizeof(BPTNode), 1, dumpfile);
                break;
        }
        node_idx++;
    }
    free(all_nodes);
    if (tree_op == PRINT_NODE) {
        printf("\n");
    }
}
void print_tree(BPTNode* node) {
    bfs_traverse_tree(node, PRINT_NODE, NULL);
}
void free_tree(BPTNode* node) {
    bfs_traverse_tree(node, FREE_NODE, NULL);
}
void dump_tree(BPTNode* node, char* fname) {
    FILE* index_file = fopen(fname, "wb");
    bfs_traverse_tree(node, DUMP_NODE, index_file);
    fclose(index_file);
}


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
 * @brief This function finds the clustered column value's insertion point
 *
 * @param root - root of tree
 * @param value - value to add to tree
 *
 * @return - this is the place where we will insert
 */
size_t btree_find_insert_position(BPTNode* root, int value) {
    BPTNode* containing_leaf = search_for_leaf(root, value);
    // we get a leaf
    //  3 cases - the leaf is too big - this
    // check backwards to see if that leaf could contain this value
    while (containing_leaf->node_vals[0] > value) {
        BPTNode* prev = containing_leaf->bpt_meta.bpt_leaf.prev_leaf;
        assert(prev != NULL);  // we should never go to far back
        if (prev->node_vals[prev->num_elements - 1] >= value) {
            // the previous node is gte the current, so we need to shift back
            containing_leaf = prev;
        } else {
            break;
        }
    }
    size_t insert_idx = 0;
    // while the value at this index
    while (containing_leaf->node_vals[insert_idx] > value) {
        insert_idx++;
    }
    assert(insert_idx < containing_leaf->num_elements);
    return containing_leaf->bpt_meta.bpt_leaf.col_pos[insert_idx];
}

/**
 * @brief Takes in a range [low, high) and returns an result
 * that will hold all of the indices that correlate with the range
 *
 * @param root - the bplus tree root to search from
 * @param gte_val - the min value
 * @param lt_val - the max value
 * @param result - the result column to add to
 *
 * @return  result - the result (allocated in function)
 */
void find_values_unclustered(BPTNode* root, int gte_val, int lt_val, Result* result) {
    result->data_type = INDEX;
    result->capacity = MAX_KEYS;
    result->num_tuples = 0;
    result->payload = malloc(sizeof(size_t) * result->capacity);

    // if the high bound is the rightmost leaf and nothing satisfies it
    // then we know that there is no match
    BPTNode* high_bound = search_for_leaf(root, lt_val);
    if (high_bound->node_vals[0] < gte_val &&
            high_bound->bpt_meta.bpt_leaf.prev_leaf == NULL) {
        return;
    }

    // if the low bound is the rightmost leaf and it doesn't fit
    // in the range then we know that we have no data the satisfies
    BPTNode* low_bound = search_for_leaf(root, gte_val);
    if (low_bound->node_vals[low_bound->num_elements - 1] < lt_val &&
            low_bound->bpt_meta.bpt_leaf.next_leaf == NULL) {
        return;
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
    return;
}

/**
 * @brief this is the function for selecting from a clustered index
 *
 * @param root
 * @param gte_val
 * @param lt_val
 * @param result
 *
 * @return
 */
void find_values_clustered(BPTNode* root, int gte_val, int lt_val, Result* result) {
    result->data_type = INDEX;
    result->num_tuples = 0;
    result->payload = NULL;

    // get right bound and check that it works
    BPTNode* high_bound = search_for_leaf(root, lt_val);
    if (high_bound->node_vals[0] < gte_val &&
            high_bound->bpt_meta.bpt_leaf.prev_leaf == NULL) {
        return;
    }

    // get left bound and check that it also works
    BPTNode* low_bound = search_for_leaf(root, gte_val);
    if (low_bound->node_vals[low_bound->num_elements - 1] < lt_val &&
            low_bound->bpt_meta.bpt_leaf.next_leaf == NULL) {
        return;
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
    return;
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
    BPTNodeStack* access_stack = create_stack(32);
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
        stack_push(access_stack, bt_node);
        bt_node = bt_node->bpt_meta.bpt_ptrs.children[i];
    }
    // the last item in the stack will be the leaf
    stack_push(access_stack, bt_node);
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
        if (stack_is_empty(access_stack)) {
            return bt_node;
        }
        return access_stack->array[0];
    } else {
        SplitNode new_split;
        bt_node->bpt_meta.bpt_ptrs.level++;
        split_body_node(bt_node, split_node, &new_split);
        return rebalanced_insert(stack_pop(access_stack),
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
 * @param update_positions
 *
 * @return new head of the btree
 */
BPTNode* btree_insert_value(
    BPTNode* bt_node,
    int value,
    size_t position,
    bool update_positions
) {
    // if we don't have a first value, make a first value
    if (bt_node == NULL) {
        bt_node = create_leaf();
        add_to_leaf(bt_node, value, position);
        return bt_node;
    }

    // OTHERWISE - we need to handle normal insertion
    // find the leaf that we will add to
    BPTNodeStack* access_stack = find_leaf(bt_node, value);
    BPTNode* leaf = stack_pop(access_stack);
    SplitNode* split_node = add_to_leaf(leaf, value, position);

    // this means that we want to update the positions
    // TODO: make it smarter for primary (only shift right)
    if (update_positions == true) {
        // go left and update all those positions
        BPTNode* left_update = leaf->bpt_meta.bpt_leaf.prev_leaf;
        while (left_update != NULL) {
            size_t* pos_array = left_update->bpt_meta.bpt_leaf.col_pos;
            for (size_t i = 0; i < left_update->num_elements; i++) {
                // we need to shift all the values to the right in each leaf
                if (pos_array[i] >= position) {
                    pos_array[i]++;
                }
            }
            left_update = left_update->bpt_meta.bpt_leaf.prev_leaf;
        }

        // go right and update all of those positions
        BPTNode* right_update = leaf;
        bool set_one = false; // if we alreay have a value that fites the case
        while (right_update != NULL) {
            size_t* pos_array = right_update->bpt_meta.bpt_leaf.col_pos;
            for (size_t i = 0; i < right_update->num_elements; i++) {
                // we need to shift all the values to the right in each leaf
                if (pos_array[i] > position) {
                    pos_array[i]++;
                } else if (pos_array[i] == position) {
                    if (set_one == false && right_update->node_vals[i] == value) {
                        set_one = true;
                    } else {
                        pos_array[i]++;
                    }
                }
            }
            right_update = right_update->bpt_meta.bpt_leaf.next_leaf;
        }
    }

    // if we don't have to split, we just return the same head
    if (split_node == NULL) {
        free_stack(access_stack);
        return bt_node;
    }

    // Handle rebalancing - this is the case when we have 1 empty value
    bt_node = rebalanced_insert(
        stack_pop(access_stack),
        split_node,
        access_stack
    );

    // cleanup memory
    free(split_node);
    free_stack(access_stack);
    return bt_node;
}
