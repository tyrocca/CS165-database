#include <assert.h>
#include <stdio.h>
#include "b_tree.h"

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


int main(void) {
    printf("Testing leaf insert\n");
    return 0;
}
