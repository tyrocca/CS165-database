#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define MAX_KEYS 339
#define MAX_DEGREE 340

struct BPTNode;
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
    bool is_root;                          // Bool to indicate if is root
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
    char padding[3];
} BPTNode;


void bfs_traverse_tree(BPTNode* node, BTreeTraversalOp tree_op, FILE* dumpfile) {
    BPTNode** all_nodes = malloc(sizeof(BPTNode*) * 2);
    if (node->is_leaf) {
        i
    }
    size_t num_nodes = node->num_elements
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

















































/* BPTNode node; */
/* BPTNode read_node; */
/* int main() { */
/*     /1* node.num_elements = 1; *1/ */
/*     /1* node.node_vals[0] = 22; *1/ */
/*     /1* node.is_leaf = true; *1/ */
/*     /1* node.bpt_meta.bpt_leaf.col_pos[0] = 69; *1/ */

/*     /1* FILE* file = fopen("testing.bin", "wb"); *1/ */
/*     /1* fwrite(&node, sizeof(BPTNode), 1, file); *1/ */
/*     /1* fclose(file); *1/ */

/*     FILE* file2 = fopen("./database/awesomebase.grades.project.index.bin", "rb"); */
/*     fread(&read_node, sizeof(BPTNode), 1, file2); */
/*     fclose(file2); */

/*     printf("Done with this %zu now %zu\n", read_node.num_elements, read_node.num_elements); */


/*     return 0; */
/* } */
