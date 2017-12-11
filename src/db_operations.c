#include <limits.h>
#include <assert.h>
#include <string.h>
#include "db_operations.h"
#include "client_context.h"
#include "db_index.h"
#include "cs165_api.h"
#include "extendible_hash_table.h"
#include <stdio.h>


// Min and Max helper functions
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

/// ***************************************************************************
/// Helper Functions
/// ***************************************************************************

/**
 * @brief This function takes in a data_type and returns the size of the
 * data type (useful for malloc)
 *
 * @param data_type
 *
 * @return
 */
size_t type_to_size(DataType data_type) {
    switch (data_type) {
        case INT:
            return sizeof(int);
        case DOUBLE:
            return sizeof(double);
        case LONG:
            return sizeof(long int);
        case INDEX:
            return sizeof(double);
        default:
            return 0;
    }
}

/**
 * @brief This function will return a col val at an index
 * and returns it as a double
 *
 * @param data_ptr
 * @param idx
 *
 * @return double
 */
double col_val_as_double(void* data_ptr, DataType data_type, size_t idx) {
    switch (data_type) {
        case INT:
            return (double) ((int*) data_ptr)[idx];
        case DOUBLE:
            return ((double*) data_ptr)[idx];
        case LONG:
            return (double) ((int*) data_ptr)[idx];
        default:
            return 0;
    }
}

/**
 * @brief This function will return a col va at an index
 * and returns it as a long
 *
 * @param data_ptr
 * @param idx
 *
 * @return long
 */
long col_val_as_long(void* data_ptr, DataType data_type, size_t idx) {
    switch (data_type) {
        case INT:
            return (long) ((int*) data_ptr)[idx];
        case DOUBLE:
            return (long) ((double*) data_ptr)[idx];
        case LONG:
            return ((long*) data_ptr)[idx];
        default:
            return 0;
    }
}


/// ***************************************************************************
/// Insertion Functions
/// ***************************************************************************

/**
 * @brief Function that inserts a value into the table
 *
 * @param table
 * @param values
 * @param status
 */
void insert_into_table(Table* table, int* values, Status* status) {
    // so we now need to have several cases
    // a) if the table has no primary index
    // b) the table has a primary index
    //
    // Column insertion
    //   - The column has a clustered index
    //   - The column has an unclustered index
    //
    // if the column we are inserting into has
    //
    if (table->primary_index == NULL) {
        size_t row_idx = next_table_idx(table, status);
        if (status->code != OK) {
            return;
        }
        // set the values
        // TODO: performace improvement make it so we
        // do the insertion in threads!
        for (size_t idx = 0; idx < table->col_count; idx++) {
            Column* col = &table->columns[idx];
            // if we have an unclusted index on an unclustered column
            // we are pretty happy because that means we don't need to
            // shift the values
            if (col->index_type == BTREE) {
                BPTNode* bt_root = ((BPTNode*) col->index);
                col->index = (void*) btree_insert_value(bt_root,
                                                        values[idx],
                                                        row_idx,
                                                        false);
            } else if (col->index_type == SORTED) {
                insert_into_sorted((SortedIndex*) col->index,
                                   values[idx],
                                   row_idx);
            }
            // insert into the base data
            table->columns[idx].data[row_idx] = values[idx];
        }
    } else {
        // let's imagine that this works - it finds the index where the value
        // should be inserted - we should actually call this as this is the
        // new max index
        Column* index_col = table->primary_index;
        int insert_val = values[table->primary_col_pos];

        // we should increase the size of the table - this function will do that
        // we use the next index as a place holder
        size_t row_idx = next_table_idx(table, status);
        if (status->code != OK) {
            return;
        }
        // whether we need to shift the values
        bool shift_values = false;
        // if we check the max and it is less, we can just append (2 because
        // we increased the size
        if (index_col->index_type == BTREE) {
            if (index_col->index == NULL ||
                index_col->data[table->table_size - 2] <= insert_val
            ) {
                shift_values = false;
            } else if (index_col->data[0] > insert_val) {
                shift_values = true;
                row_idx = 0;
            } else {
                row_idx = btree_find_insert_position((BPTNode*) index_col->index,
                                                     insert_val);
                shift_values = row_idx + 1 < table->table_size;
            }
        } else {
            // We are just inserting into the new column
            SortedIndex* sorted_index = (SortedIndex*) index_col->index;
            // make sure the column is correct
            if (sorted_index->keys != index_col->data) {
                sorted_index->keys = index_col->data;
            }
            row_idx = get_sorted_idx(sorted_index, insert_val);
            sorted_index->num_items = table->table_size;
        }

        // TODO: performace improvement make it so we
        // do the insertion in threads!
        for (size_t idx = 0; idx < table->col_count; idx++) {
            Column* col = &table->columns[idx];
            if (col->index_type == BTREE) {
                // if we have a b_tree we need to insert into the column
                BPTNode* bt_root = ((BPTNode*) col->index);
                col->index = (void*) btree_insert_value(
                    bt_root,
                    values[idx],
                    row_idx,
                    shift_values
                );
            } else if (col->index_type == SORTED && col != index_col) {
                // if we are inserting into an unclustered column then
                // we need to pass the new position and the new index
                insert_into_sorted((SortedIndex*) col->index,
                                    values[idx],
                                    row_idx);
            }
            // if we are inserting make sure the memory move is necessary
            // if it is we want to shift the base values down one position
            // starting with the current location
            if (row_idx + 1 < table->table_size) {
                memmove(
                    (void*) &table->columns[idx].data[row_idx + 1],
                    (void*) &table->columns[idx].data[row_idx],
                    (table->table_size - row_idx - 1) * sizeof(int)
                );
            }
            // this is the operation to set the value
            table->columns[idx].data[row_idx] = values[idx];
        }
    }
}

/**
 * @brief This function does the insert operation on the table
 *
 * @param insert_op InsertOperator
 * @param status Status - used for maintaining status
 *
 * @return string
 */
char* process_insert(InsertOperator insert_op, Status* status) {
    insert_into_table(insert_op.table, insert_op.values, status);
    if (status->code != OK) {
        return NULL;
    }
    status->msg_type = OK_DONE;
    free(insert_op.values);
    return "Success! Values inserted.";
}

/// ***************************************************************************
/// Opening Functions
/// ***************************************************************************

/**
 * @brief This function opens a file and loads it into the system
 *
 * @param open_op - OpenOperator
 * @param status - Status
 *
 * @return
 */
char* process_open(OpenOperator open_op, Status* status) {
    // TODO: implement process open - this is an extra, all it
    // does is implement multiple databases
    (void) open_op;
    (void) status;
    return NULL;
}

/// ***************************************************************************
/// Selection Functions
/// ***************************************************************************

/**
 * @brief Function that returns a result column given an array
 * of selections. The column contains an array of indices
 *  TODO: could break if empty
 *
 * @param comp
 * @param result_col
 */
void select_from_col(Comparator* comp, Result* result_col) {
    // TODO: Make it so this only does 1 comparison at a time
    Column* col = comp->gen_col->column_pointer.column;

    if (col->index_type == NONE || col->index == NULL) {
        size_t* positions = malloc(sizeof(size_t) * (*col->size_ptr));
        result_col->num_tuples = 0;
        for (size_t idx = 0; idx < *col->size_ptr; idx++) {
            positions[result_col->num_tuples] = idx;
            // TODO: what if we are at the top bound for high? will we not get max?
            result_col->num_tuples += (col->data[idx] >= comp->p_low &&
                                       col->data[idx] < comp->p_high);
        }
        // if no matches return
        if (result_col->num_tuples == 0) {
            free(positions);
            result_col->payload = NULL;
            return;
        }
        // reallocate to the exact size of the column
        result_col->payload = (void*) realloc(
            positions,
            sizeof(size_t) * result_col->num_tuples
        );
    } else if (col->index_type == SORTED) {
        get_range_sorted(col->index, comp->p_low, comp->p_high, result_col);
    } else if (col->index_type == BTREE && col->clustered == true) {
        find_values_clustered(col->index, comp->p_low, comp->p_high, result_col);
    } else {
        find_values_unclustered(col->index, comp->p_low, comp->p_high, result_col);
    }
    return;
}

/**
 * @brief Shared column selector
 *
 * @param comp
 * @param result_col
 */
void shared_col_select(
    Comparator* comps[],
    size_t num_queries,
    Result* result_cols[],
    int maxval,
    int minval
) {
    // todo: make it so this only does 1 comparison at a time
    Column* col = comps[0]->gen_col->column_pointer.column;

    // create a result column for each position
    size_t* all_positions[num_queries];
    for (size_t i = 0; i < num_queries; i++) {
        all_positions[i] = malloc(sizeof(size_t) * (*col->size_ptr));
        result_cols[i]->num_tuples = 0;
    }

    // Go through the columns and create the new indices
    for (size_t idx = 0; idx < *col->size_ptr; idx++) {
        // skip val if it's not in the range
        int val = col->data[idx];
        if (val < minval || val > maxval) {
            continue;
        }
        for (size_t q_num = 0; q_num < num_queries; q_num++) {
            // METHOD 1 - conditional
            if ((val >= comps[q_num]->p_low) && (val < comps[q_num]->p_high)) {
                all_positions[q_num][result_cols[q_num]->num_tuples++] = idx;
            }

            // METHOD 2 - always inc - this is slower
            /* all_positions[q_num][result_cols[q_num]->num_tuples] = idx; */
            /* // todo: what if we are at the top bound for high? will we not get max? */
            /* result_cols[q_num]->num_tuples += ( */
            /*         (col->data[idx] >= comps[q_num]->p_low) && */
            /*         (col->data[idx] < comps[q_num]->p_high) */
            /* ); */
        }
    }

    // for each query reallocate the column size and set it to a result
    // column, if no results free the column
    for (size_t i = 0; i < num_queries; i++) {
        if (result_cols[i]->num_tuples == 0) {
            free(all_positions[i]);
            result_cols[i]->payload = NULL;
        } else {
            result_cols[i]->payload = (void*) realloc(
                all_positions[i],
                sizeof(size_t) * result_cols[i]->num_tuples
            );
        }
    }
}


void process_shared_scans(SharedScanOperator* ss_op, ClientContext* context, Status* status) {
    // make it so we
    Comparator* comps[ss_op->num_scans];
    Result* results[ss_op->num_scans];

    int maxval = INT_MIN;
    int minval = INT_MAX;
    for (size_t i = 0; i < ss_op->num_scans; ++i) {
        results[i] = malloc(sizeof(Result));
        comps[i] = &ss_op->db_scans[i]->operator_fields.select_operator.comparator;
        // DELETE - set the ranges
        maxval = MAX(maxval, comps[i]->p_high);
        minval = MIN(minval, comps[i]->p_low);

        GeneralizedColumnHandle* gcol_handle = add_result_column(
            context,
            comps[i]->handle
        );
        gcol_handle->generalized_column.column_type = RESULT;
        gcol_handle->generalized_column.column_pointer.result = results[i];
        results[i]->data_type = INDEX;
    }
    shared_col_select(
        comps,
        ss_op->num_scans,
        results,
        maxval,
        minval
    );
    free(ss_op->db_scans);
    status->msg_type = OK_DONE;

}

/**
 * @brief This function takes a selection of indices from a selected column
 *      TODO: HOW TO HANDLE TYPES - not needed currently
 *
 * @param comp
 * @param idx_col
 * @param result_col
 */
void select_from_selection(Comparator*comp, Result* idx_col, Result* result_col) {
    Result* queryed_col = comp->gen_col->column_pointer.result;
    // this length should be correct (otherwise we have an issue)
    assert(queryed_col->num_tuples == idx_col->num_tuples);
    // make space for the results which we will add to the result col later
    size_t* positions = malloc(sizeof(size_t) * queryed_col->num_tuples);
    // init the count to 0;
    result_col->num_tuples = 0;
    for (size_t idx = 0; idx < queryed_col->num_tuples; idx++) {
        // cast the index column as a int
        positions[result_col->num_tuples] = ((size_t*) idx_col->payload)[idx];
        // TODO: what if we are at the top bound for high? will we not get max?
        // TODO - switch to bitwise and
        // TODO: this needs to work for longs...
        result_col->num_tuples += (
                ((int*)queryed_col->payload)[idx] >= comp->p_low &&
                ((int*)queryed_col->payload)[idx] < comp->p_high
        );
    }
    // if no matches return
    if (result_col->num_tuples == 0) {
        free(positions);
        result_col->payload = NULL;
        return;
    }
    // reallocate to the exact size of the column
    result_col->payload = (void*) realloc(
        positions,
        sizeof(size_t) * result_col->num_tuples
    );
}

/**
 * @brief This function processes a select command
 *
 * @param select_op
 * @param context
 * @param status
 */
void process_select(SelectOperator* select_op, ClientContext* context, Status* status) {
    // this makes the column handle
    GeneralizedColumnHandle* gcol_handle = add_result_column(
        context,
        select_op->comparator.handle
    );
    // this is the result column
    Result* result_col = malloc(sizeof(Result));
    result_col->data_type = INDEX;
    if (select_op->pos_col) {
        assert(select_op->comparator.gen_col->column_type == RESULT);
        select_from_selection(&select_op->comparator, select_op->pos_col, result_col);
    } else {
        // assert that the column will be a result column
        assert(select_op->comparator.gen_col->column_type == COLUMN);
        select_from_col(&select_op->comparator, result_col);
    }
    // set the resulting column
    gcol_handle->generalized_column.column_pointer.result = result_col;
    gcol_handle->generalized_column.column_type = RESULT;
    status->msg_type = OK_DONE;
}


/// ***************************************************************************
/// Fetching Functions
/// ***************************************************************************

/**
 * @brief Function that given a fetch command returns the values. This
 * function takes in a database column and returns a column of ints
 *
 * @param fetch_op
 * @param context
 * @param status
 */
void process_fetch(FetchOperator* fetch_op, ClientContext*context, Status* status) {
    GeneralizedColumnHandle* gcol_handle = add_result_column(context, fetch_op->handle);
    Result* result_col = malloc(sizeof(Result));
    result_col->data_type = INT;
    result_col->num_tuples = fetch_op->idx_col->num_tuples;
    int* values = malloc(sizeof(int) * result_col->num_tuples);
    for (size_t i = 0; i < result_col->num_tuples; i++) {
        values[i] = fetch_op->from_col->data[
            ((size_t*) fetch_op->idx_col->payload)[i]
        ];
    }
    result_col->payload = values;
    gcol_handle->generalized_column.column_pointer.result = result_col;
    gcol_handle->generalized_column.column_type = RESULT;
    status->msg_type = OK_DONE;
}


/// ***************************************************************************
/// Aggregate & Statistic Functions
/// ***************************************************************************

/**
 * @brief Function that sums up a column
 *
 * @param data_type
 * @param data
 * @param data_size
 *
 * @return long
 */
long int calculate_sum(DataType data_type, void* data, size_t data_size) {
    long int sum = 0;
    DataPtr data_ptr = { .void_array = data };
    switch (data_type) {
        case INT:
            for (size_t i = 0; i < data_size; i++) {
                sum += data_ptr.int_array[i];
            }
            break;
        case DOUBLE:
            for (size_t i = 0; i < data_size; i++) {
                sum += data_ptr.double_array[i];
            }
            break;
        case LONG:
            for (size_t i = 0; i < data_size; i++) {
                sum += data_ptr.long_array[i];
            }
        default:
            break;
    }
    return sum;
}

/**
 * @brief This function sums and averages a column
 *
 * @param math_op
 * @param op_type
 * @param context
 * @param status
 */
void process_sum_avg(MathOperator* math_op, OperatorType op_type, ClientContext* context, Status* status) {
    long int* sum = malloc(sizeof(long int));
    size_t num_results = 0;
    if (math_op->gcol1.column_type == RESULT) {
        num_results = math_op->gcol1.column_pointer.result->num_tuples;
        *sum = calculate_sum(
            math_op->gcol1.column_pointer.result->data_type,
            math_op->gcol1.column_pointer.result->payload,
            num_results
        );
    } else {
        num_results = *math_op->gcol1.column_pointer.column->size_ptr;
        *sum = calculate_sum(
            INT,
            (void*) math_op->gcol1.column_pointer.column->data,
            num_results
        );
    }
    // allocate the result column
    Result* result_col = malloc(sizeof(Result));
    GeneralizedColumnHandle* gcol_handle = add_result_column(context,
                                                             math_op->handle1);
    if (num_results > 0) {
        result_col->num_tuples = 1;
        if (op_type == SUM) {
            result_col->data_type = LONG;
            result_col->payload = (void*) sum;
        } else {
            result_col->data_type = DOUBLE;
            double* avg = malloc(sizeof(double));
            *avg = (double)*sum / (double) num_results;
            result_col->payload = (void*) avg;
            free(sum);
        }
    } else {
        // this shouldn't matter
        result_col->data_type = INT;
        result_col->num_tuples = 0;
        result_col->payload = NULL;
        free(sum);
    }

    gcol_handle->generalized_column.column_pointer.result = result_col;
    gcol_handle->generalized_column.column_type = RESULT;
    status->msg_type = OK_DONE;
}


/**
 * @brief This function combines column (by added or subtracting values)
 *
 * @param op_type
 * @param num_values
 * @param col1
 * @param col1_type
 * @param col2
 * @param col2_type
 *
 * @return
 */
void combine_columns(
    OperatorType op_type,
    Result* result_col,
    void* col1,
    DataType col1_type,
    void* col2,
    DataType col2_type
) {
    DataPtr result_data;
    size_t num_values = result_col->num_tuples;
    if (col1_type == DOUBLE || col2_type == DOUBLE) {
        result_col->data_type = DOUBLE;
        // TODO: should check if this exceeds size_t max
        result_data.void_array = malloc(num_values * type_to_size(DOUBLE));
        if (op_type == ADD) {
            for (size_t i = 0; i < num_values; i++) {
                result_data.double_array[i] = (
                    col_val_as_double(col1, col1_type, i) +
                    col_val_as_double(col2, col2_type, i)
                );
            }
        } else {
            for (size_t i = 0; i < num_values; i++) {
                result_data.double_array[i] = (
                    col_val_as_double(col1, col1_type, i) -
                    col_val_as_double(col2, col2_type, i)
                );
            }
        }
    } else {
        result_col->data_type = LONG;
        result_data.void_array = malloc(num_values * type_to_size(LONG));
        if (op_type == ADD) {
            for (size_t i = 0; i < num_values; i++) {
                result_data.long_array[i] = (
                    col_val_as_long(col1, col1_type, i) +
                    col_val_as_long(col2, col2_type, i)
                );
            }
        } else {
            for (size_t i = 0; i < num_values; i++) {
                result_data.long_array[i] = (
                    col_val_as_long(col1, col1_type, i) -
                    col_val_as_long(col2, col2_type, i)
                );
            }
        }
    }
    result_col->payload = result_data.void_array;
    return;
}

/**
 * @brief This function takes in two columns and adds or subtracts them into
 * the correct column type
 *
 * @param math_op
 * @param op_type
 * @param context
 * @param status
 */
void process_col_op(
    MathOperator* math_op,
    OperatorType op_type,
    ClientContext* context,
    Status* status
) {
    // check that we were given two columns (we can just check the
    // values for result as both are pointers)
    if (math_op->gcol1.column_pointer.result == NULL ||
            math_op->gcol2.column_pointer.result == NULL) {
        status->msg_type = QUERY_UNSUPPORTED;
        status->msg = "Two vectors are needed";
        status->code = ERROR;
        return;
    }

    Result* result_col = malloc(sizeof(Result));
    // Columns of the same length can be added
    if (math_op->gcol1.column_type == RESULT) {
        result_col->num_tuples = math_op->gcol1.column_pointer.result->num_tuples;
        if (math_op->gcol2.column_pointer.result->num_tuples != result_col->num_tuples) {
            status->msg_type = QUERY_UNSUPPORTED;
            status->msg = "Different Column lengths are not allowed";
            status->code = ERROR;
            free(result_col);
            return;
        }
        combine_columns(
            op_type,
            result_col,
            math_op->gcol1.column_pointer.result->payload,
            math_op->gcol1.column_pointer.result->data_type,
            math_op->gcol2.column_pointer.result->payload,
            math_op->gcol2.column_pointer.result->data_type
        );
    } else {
        result_col->num_tuples = *math_op->gcol1.column_pointer.column->size_ptr;
        if (*math_op->gcol2.column_pointer.column->size_ptr != result_col->num_tuples) {
            status->msg_type = QUERY_UNSUPPORTED;
            status->msg = "Different Column lengths are not allowed";
            status->code = ERROR;
            return;
        }
        combine_columns(
            op_type,
            result_col,
            math_op->gcol1.column_pointer.column->data,
            INT,
            math_op->gcol2.column_pointer.column->data,
            INT
        );
    }

    // set the values of the database operations
    GeneralizedColumnHandle* gcol_handle = add_result_column(context, math_op->handle1);
    gcol_handle->generalized_column.column_pointer.result = result_col;
    gcol_handle->generalized_column.column_type = RESULT;
    status->msg_type = OK_DONE;
}

/// ***************************************************************************
/// Min & Max Functions
/// ***************************************************************************

/**
 * @brief function that calculates the max
 *
 * @param data_type
 * @param data
 * @param data_size
 *
 * @return
 */
void* single_col_bound(
    OperatorType op_type,
    DataType data_type,
    void* data,
    size_t data_size
) {
    DataPtr data_ptr = { .void_array = data };
    DataPtr result;
    result.void_array = malloc(1 * type_to_size(data_type));
    switch (data_type) {
        case INT:
            result.int_array[0] = data_ptr.int_array[0];
            if (op_type == MAX) {
                for (size_t i = 1; i < data_size; i++) {
                    result.int_array[0] = MAX(result.int_array[0],
                                              data_ptr.int_array[i]);
                }
            } else {
                for (size_t i = 1; i < data_size; i++) {
                    result.int_array[0] = MIN(result.int_array[0],
                                              data_ptr.int_array[i]);
                }
            }
            break;
        case DOUBLE:
            result.double_array[0] = data_ptr.double_array[0];
            if (op_type == MAX) {
                for (size_t i = 1; i < data_size; i++) {
                    result.double_array[0] = MAX(result.double_array[0],
                                              data_ptr.double_array[i]);
                }
            } else {
                for (size_t i = 1; i < data_size; i++) {
                    result.double_array[0] = MIN(result.double_array[0],
                                              data_ptr.double_array[i]);
                }
            }
            break;
        case LONG:
            result.long_array[0] = data_ptr.long_array[0];
            if (op_type == MAX) {
                for (size_t i = 1; i < data_size; i++) {
                    result.long_array[0] = MAX(result.long_array[0],
                                              data_ptr.long_array[i]);
                }
            } else {
                for (size_t i = 1; i < data_size; i++) {
                    result.long_array[0] = MIN(result.long_array[0],
                                               data_ptr.long_array[i]);
                }
            }
            break;
        default:
            break;
    }
    return result.void_array;
}

/**
 * @brief This function processes the min and max for a single column
 *
 * @param math_op
 * @param op_type
 * @param context
 * @param status
 */
void get_range_value(
    MathOperator* math_op,
    OperatorType op_type,
    ClientContext* context,
    Status* status
) {
    // sum the column
    Result* result_col = malloc(sizeof(Result));
    result_col->num_tuples = 1;
    if (math_op->gcol1.column_type == RESULT) {
        result_col->data_type = math_op->gcol1.column_pointer.result->data_type;
        result_col->payload = single_col_bound(
            op_type,
            result_col->data_type,
            math_op->gcol1.column_pointer.result->payload,
            math_op->gcol1.column_pointer.result->num_tuples
        );
    } else {
        result_col->data_type = INT;
        result_col->payload = single_col_bound(
            op_type,
            result_col->data_type,
            (void*) math_op->gcol1.column_pointer.column->data,
            *math_op->gcol1.column_pointer.column->size_ptr
        );
    }
    // set the values of the database operations
    GeneralizedColumnHandle* gcol_handle = add_result_column(context, math_op->handle1);
    gcol_handle->generalized_column.column_pointer.result = result_col;
    gcol_handle->generalized_column.column_type = RESULT;
    status->msg_type = OK_DONE;
}

/**
 * @brief This function takes in the operations, the column and its indices
 *  and return the columns values
 *
 * @param op_type
 * @param data_type
 * @param data
 * @param indices
 * @param data_size
 * @param rcol_vals
 * @param rcol_idx
 *
 * @return
 */
void col_bound_and_index(
    OperatorType op_type,
    DataType data_type,
    void* data,
    void* indices,
    size_t data_size,
    Result* rcol_vals,
    Result* rcol_idx
) {
    DataPtr data_ptr = { .void_array = data };
    DataPtr index_ptr = { .void_array = indices };
    DataPtr result, result_indices;
    result.void_array = malloc(data_size * type_to_size(data_type));
    result_indices.index_array = malloc(data_size * type_to_size(INDEX));
    size_t num_results = 1;
    /*
     * For each type we have an array of data
     */
    switch (data_type) {
        case INT:
            result.int_array[0] = data_ptr.int_array[0];
            if (op_type == MAX) {
                for (size_t i = 1; i < data_size; i++) {
                    if (data_ptr.int_array[i] >= result.int_array[0]) {
                        // if we have a new max, then reset
                        if (data_ptr.int_array[i] > result.int_array[0]) {
                            num_results = 0;
                        }
                        // add to the array and update the index array
                        result.int_array[num_results] = data_ptr.int_array[i];
                        result_indices.index_array[num_results++] = (
                            index_ptr.index_array ? index_ptr.index_array[i] : i
                        );
                    }
                }
            } else {
                for (size_t i = 1; i < data_size; i++) {
                    if (data_ptr.int_array[i] <= result.int_array[0]) {
                        // if we have a new max, then reset
                        if (data_ptr.int_array[i] < result.int_array[0]) {
                            num_results = 0;
                        }
                        // add to the array and update the index array
                        result.int_array[num_results] = data_ptr.int_array[i];
                        result_indices.index_array[num_results++] = (
                            index_ptr.index_array ? index_ptr.index_array[i] : i
                        );
                    }
                }
            }
            break;
        case DOUBLE:
            result.double_array[0] = data_ptr.double_array[0];
            if (op_type == MAX) {
                for (size_t i = 1; i < data_size; i++) {
                    if (data_ptr.double_array[i] >= result.double_array[0]) {
                        // if we have a new max, then reset
                        if (data_ptr.double_array[i] > result.double_array[0]) {
                            num_results = 0;
                        }
                        // add to the array and update the index array
                        result.double_array[num_results] = data_ptr.double_array[i];
                        result_indices.index_array[num_results++] = (
                            index_ptr.index_array ? index_ptr.index_array[i] : i
                        );
                    }
                }
            } else {
                for (size_t i = 1; i < data_size; i++) {
                    if (data_ptr.double_array[i] <= result.double_array[0]) {
                        // if we have a new max, then reset
                        if (data_ptr.double_array[i] < result.double_array[0]) {
                            num_results = 0;
                        }
                        // add to the array and update the index array
                        result.double_array[num_results] = data_ptr.double_array[i];
                        result_indices.index_array[num_results++] = (
                            index_ptr.index_array ? index_ptr.index_array[i] : i
                        );
                    }
                }
            }
            break;
        case LONG:
            result.long_array[0] = data_ptr.long_array[0];
            if (op_type == MAX) {
                for (size_t i = 1; i < data_size; i++) {
                    if (data_ptr.long_array[i] >= result.long_array[0]) {
                        // if we have a new max, then reset
                        if (data_ptr.long_array[i] > result.long_array[0]) {
                            num_results = 0;
                        }
                        // add to the array and update the index array
                        result.long_array[num_results] = data_ptr.long_array[i];
                        result_indices.index_array[num_results++] = (
                            index_ptr.index_array ? index_ptr.index_array[i] : i
                        );
                    }
                }
            } else {
                for (size_t i = 1; i < data_size; i++) {
                    if (data_ptr.long_array[i] <= result.long_array[0]) {
                        // if we have a new max, then reset
                        if (data_ptr.long_array[i] < result.long_array[0]) {
                            num_results = 0;
                        }
                        // add to the array and update the index array
                        result.long_array[num_results] = data_ptr.long_array[i];
                        result_indices.index_array[num_results++] = (
                            index_ptr.index_array ? index_ptr.index_array[i] : i
                        );
                    }
                }
            }
            break;
        default:
            break;
    }

    // set the result values
    rcol_vals->num_tuples = rcol_idx->num_tuples = num_results;
    rcol_vals->payload = (void*) realloc(result.void_array,
                                         num_results * type_to_size(data_type));
    rcol_idx->payload = (void*) realloc(result_indices.void_array,
                                         num_results * type_to_size(INDEX));
    return;
}

/**
 * @brief This function returns the min / max and returns the column
 *
 * @param math_op
 * @param op_type
 * @param context
 * @param status
 */
void get_index_and_range(
    MathOperator* math_op,
    OperatorType op_type,
    ClientContext* context,
    Status* status
) {
    // sum the column
    Result* result_col = malloc(sizeof(Result));
    Result* result_indices = malloc(sizeof(Result));
    result_indices->data_type = INDEX;


    // we just swap the pointers, as we know that this is ok
    if (math_op->gcol1.column_type == RESULT) {
        // neither of these should be null
        assert(math_op->gcol1.column_pointer.result != NULL);
        assert(math_op->gcol2.column_pointer.result != NULL);
        assert(math_op->gcol2.column_pointer.result != NULL);
        col_bound_and_index(
            op_type,
            math_op->gcol2.column_pointer.result->data_type,
            math_op->gcol2.column_pointer.result->payload,
            math_op->gcol1.column_pointer.result->payload,
            math_op->gcol2.column_pointer.result->num_tuples,
            result_col,
            result_indices
        );
    } else {
        col_bound_and_index(
            op_type,
            INT,
            (void*) math_op->gcol1.column_pointer.column->data,
            NULL,
            *math_op->gcol1.column_pointer.column->size_ptr,
            result_col,
            result_indices
        );
    }
    // set the index handle
    GeneralizedColumnHandle* index_handle = add_result_column(context, math_op->handle1);
    index_handle->generalized_column.column_pointer.result = result_indices;
    index_handle->generalized_column.column_type = RESULT;
    // set the column handle
    GeneralizedColumnHandle* gcol_handle = add_result_column(context, math_op->handle2);
    gcol_handle->generalized_column.column_pointer.result = result_col;
    gcol_handle->generalized_column.column_type = RESULT;
    status->msg_type = OK_DONE;
}

/// ***************************************************************************
/// Join Functions
/// ***************************************************************************

/* #define JOIN_SIZE 256 */
#define NUM_PARTITIONS 256
#define PARTITION_BASE_NUM 4096
typedef struct JoinPartion {
    size_t l_sz;  // number of left values
    size_t l_alloc;  // number of left values
    size_t r_sz;  // number of right values
    size_t r_alloc;  // number of right values
    int* l_join_keys;
    size_t* l_join_vals;
    int* r_join_keys;
    size_t* r_join_vals;
} JoinPartion;


/**
 * @brief Function to initialize a partition
 *
 * @param partitions[]
 */
void init_partitions(JoinPartion* partitions) {
    for (size_t i = 0; i < NUM_PARTITIONS; i++) {
        partitions[i].l_sz = partitions[i].r_sz = 0;
        partitions[i].l_alloc = partitions[i].r_alloc = PARTITION_BASE_NUM;
        partitions[i].l_join_keys = malloc(PARTITION_BASE_NUM * sizeof(int));
        partitions[i].l_join_vals = malloc(PARTITION_BASE_NUM * sizeof(size_t));
        partitions[i].r_join_keys = malloc(PARTITION_BASE_NUM * sizeof(int));
        partitions[i].r_join_vals = malloc(PARTITION_BASE_NUM * sizeof(size_t));
    }
}


/**
 * @brief Function that partitions the data into two sets
 *
 * @param partitions[]
 * @param left_vals
 * @param left_pos
 * @param num_left
 * @param right_vals
 * @param right_pos
 * @param num_right
 */
void partition_data(
    JoinPartion* partitions,
    int* left_vals,
    size_t* left_pos,
    size_t num_left,
    int* right_vals,
    size_t* right_pos,
    size_t num_right
) {
    // to partition data we will just
    // make join partitions - these will be used to join the data
    for (size_t i = 0; i < num_left; i++) {
        unsigned partition_loc = ((unsigned int) left_vals[i]) % NUM_PARTITIONS;
        JoinPartion* partition = &partitions[partition_loc];
        if (partition->l_sz == partition->l_alloc) {
            partition->l_alloc *= 2;
            partition->l_join_keys = realloc(partition->l_join_keys,
                                             partition->l_alloc * sizeof(int));
            partition->l_join_vals = realloc(partition->l_join_vals,
                                             partition->l_alloc * sizeof(size_t));
        }
        partition->l_join_keys[partition->l_sz] = left_vals[i];
        partition->l_join_vals[partition->l_sz++] = left_pos[i];

    }
    for (size_t i = 0; i < num_right; i++) {
        unsigned partition_loc = ((unsigned int) right_vals[i]) % NUM_PARTITIONS;
        JoinPartion* partition = &partitions[partition_loc];
        if (partition->r_sz == partition->r_alloc) {
            partition->r_alloc *= 2;
            partition->r_join_keys = realloc(partition->r_join_keys,
                                             partition->r_alloc * sizeof(int));
            partition->r_join_vals = realloc(partition->r_join_vals,
                                             partition->r_alloc * sizeof(size_t));
        }
        partition->r_join_keys[partition->r_sz] = right_vals[i];
        partition->r_join_vals[partition->r_sz++] = right_pos[i];
    }
}

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
 * @brief Function that partitions the dataset
 *
 * @param partition
 */

void process_partition(JoinPartion* partition, Result* l_res, Result* r_res) {
    // don't do anything if nothing is in the partition
    if (partition->l_sz == 0 || partition->r_sz == 0) {
        return;
    }
    ExtHashTable* ht = create_ext_hash_table();
    size_t small_size;
    int* big_keys = NULL;
    size_t* big_pos = NULL;
    Result* big_res = NULL;
    size_t big_size;
    int* small_keys = NULL;
    size_t* small_pos = NULL;
    Result* small_res = NULL;

    // set the side
    if (partition->l_sz <= partition->r_sz) {
        // when the left side is smaller
        small_size = partition->l_sz;
        small_keys = partition->l_join_keys;
        small_pos = partition->l_join_vals;
        small_res = l_res;
        big_size = partition->r_sz;
        big_keys = partition->r_join_keys;
        big_pos = partition->r_join_vals;
        big_res = r_res;
    } else {
        // when the right_side is smaller side is bigger
        small_size = partition->r_sz;
        small_keys = partition->r_join_keys;
        small_pos = partition->r_join_vals;
        small_res = r_res;
        big_size = partition->l_sz;
        big_keys = partition->l_join_keys;
        big_pos = partition->l_join_vals;
        big_res = l_res;
    }

    // load smaller side into the hashtable
    for (size_t i = 0; i < small_size; i++) {
        ext_hash_table_put(ht, small_keys[i], small_pos[i]);
    }
    // now query it
    for (size_t i = 0; i < small_size; i++) {
        HashResults* hres = ext_hash_func_get(ht, big_keys[i]);
        for (size_t res_idx = 0; res_idx < hres->num_found; res_idx++) {
            add_to_results(big_res, big_pos[i]);
            add_to_results(small_res, hres->hb_results[res_idx]);
        }
        free_hash_result(hres);
    }
    free_ext_hash_table(ht);
}

/**
 * @brief This function performs a hash join of two columns
 *
 * @param join_op - the struct containing the join stuff
 * @param context - the client context (for returning)
 * @param status - the status;
 */
void process_hash_join(
    JoinOperator* join_op,
    ClientContext* context,
    Status* status
) {

    int* left_values = (int*) join_op->col1_values->payload;
    size_t* left_pos = (size_t*) join_op->col1_positions->payload;
    assert(join_op->col1_values->num_tuples ==
            join_op->col1_positions->num_tuples);
    size_t num_left = join_op->col1_values->num_tuples;


    // this should also hold true
    int* right_values = (int*) join_op->col2_values->payload;
    size_t* right_pos = (size_t*) join_op->col2_positions->payload;
    assert(join_op->col2_values->num_tuples ==
            join_op->col2_positions->num_tuples);
    size_t num_right = join_op->col2_values->num_tuples;

    JoinPartion* partitions = malloc(NUM_PARTITIONS * sizeof(JoinPartion));
    init_partitions(partitions);
    // partition the data
    partition_data(
        partitions,
        left_values,
        left_pos,
        num_left,
        right_values,
        right_pos,
        num_right
    );

    // create the results
    Result* left_result_column = malloc(sizeof(Result));
    left_result_column->data_type = INDEX;
    left_result_column->num_tuples = 0;
    left_result_column->capacity = DEFAULT_COLUMN_SIZE;
    left_result_column->payload =
            malloc(left_result_column->capacity * sizeof(size_t));
    // create the column
    GeneralizedColumnHandle* left_gcol = add_result_column(context,
                                                           join_op->handle1);
    left_gcol->generalized_column.column_pointer.result = left_result_column;
    left_gcol->generalized_column.column_type = RESULT;

    // create the right result struct
    Result* right_result_column = malloc(sizeof(Result));
    right_result_column->data_type = INDEX;
    right_result_column->num_tuples = 0;
    right_result_column->capacity = DEFAULT_COLUMN_SIZE;
    right_result_column->payload =
            malloc(right_result_column->capacity * sizeof(size_t));
    GeneralizedColumnHandle* right_gcol = add_result_column(context,
                                                            join_op->handle2);
    // create the right result column
    right_gcol->generalized_column.column_pointer.result = right_result_column;
    right_gcol->generalized_column.column_type = RESULT;

    // this is the goal
    for (size_t i = 0; i < NUM_PARTITIONS; i++) {
        // PARTITION and join!!
        process_partition(
            &partitions[i],
            left_result_column,
            right_result_column
        );
        free(partitions[i].l_join_keys);
        free(partitions[i].l_join_vals);
        free(partitions[i].r_join_keys);
        free(partitions[i].r_join_vals);
    }
    free(partitions);
    // partition data
    status->msg_type = OK_DONE;
    return;
}

/**
 * @brief This function performs a simple loop join of two columns
 *
 * @param join_op - the struct containing the join stuff
 * @param context - the client context (for returning)
 * @param status - the status;
 */
void process_nested_loop_join(
    JoinOperator* join_op,
    ClientContext* context,
    Status* status
) {
    // this should be true
    int* left_values = (int*) join_op->col1_values->payload;
    size_t* left_pos = (size_t*) join_op->col1_positions->payload;
    assert(join_op->col1_values->num_tuples == join_op->col1_positions->num_tuples);
    size_t num_left = join_op->col1_values->num_tuples;


    // this should also hold true
    int* right_values = (int*) join_op->col2_values->payload;
    size_t* right_pos = (size_t*) join_op->col2_positions->payload;
    assert(join_op->col2_values->num_tuples == join_op->col2_positions->num_tuples);
    size_t num_right = join_op->col2_values->num_tuples;

    /* rp=RightEntriesThatFitInOnePage */
    /* lp=LeftEntriesThatFitInOnePage */
    // TODO: we should calculate how much space we can actually fit
    size_t rp = PAGE_SZ / sizeof(int);
    size_t lp = PAGE_SZ / sizeof(int);

    // result counter
    size_t num_results = 0;
    size_t capacity = PAGE_SZ;  // we just picked this as our scaling var
    size_t* result_left = malloc(capacity * sizeof(size_t));
    size_t* result_right = malloc(capacity * sizeof(size_t));

    // loop through page sized chunks of left values
    for (size_t outer_l = 0; outer_l < num_left; outer_l += lp) {
        // loop through page sized chunks of right values
        for (size_t outer_r = 0; outer_r < num_right; outer_r += rp) {
            // loop through left values within a page
            for (size_t l_idx = outer_l; l_idx < outer_l + lp; l_idx++) {
                // don't exceed bound
                if (l_idx >= num_left) {
                    break;
                }
                // loop through right values witin a page
                for (size_t r_idx = outer_r; r_idx < outer_r + rp; r_idx++) {
                    // don't exceed bound
                    if (r_idx >= num_right) {
                        break;
                    } else if (left_values[l_idx] == right_values[r_idx]) {
                        if (num_results == capacity) {
                            capacity *= 2;
                            result_left = realloc(result_left,
                                                  capacity * sizeof(size_t));
                            result_right = realloc(result_right,
                                                   capacity * sizeof(size_t));
                        }
                        result_left[num_results] = left_pos[l_idx];
                        result_right[num_results++] = right_pos[r_idx];
                    }

                }
            }
        }
    }

    // tighten the bound for the results

    Result* left_result_column = malloc(sizeof(Result));
    left_result_column->num_tuples = num_results;
    left_result_column->capacity = num_results;
    if (num_results == 0) {
        free(result_left);
        left_result_column->payload = NULL;
    } else {
        left_result_column->payload = realloc(result_left,
                                              num_results * sizeof(size_t));
    }
    left_result_column->data_type = INDEX;
    GeneralizedColumnHandle* left_gcol = add_result_column(context, join_op->handle1);
    left_gcol->generalized_column.column_pointer.result = left_result_column;
    left_gcol->generalized_column.column_type = RESULT;


    Result* right_result_column = malloc(sizeof(Result));
    right_result_column->num_tuples = num_results;
    right_result_column->capacity = num_results;
    if (num_results == 0) {
        free(result_right);
        right_result_column->payload = NULL;
    } else {
        right_result_column->payload = realloc(result_right,
                                               num_results * sizeof(size_t));
    }
    right_result_column->data_type = INDEX;
    GeneralizedColumnHandle* right_gcol = add_result_column(context, join_op->handle2);
    right_gcol->generalized_column.column_pointer.result = right_result_column;
    right_gcol->generalized_column.column_type = RESULT;

    status->msg_type = OK_DONE;
    return;
}

/// ***************************************************************************
/// Main Execution Function
/// ***************************************************************************

/**
 * execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation
 * possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
PrintOperator* execute_DbOperator(DbOperator* query, Status* status) {
    // return if no query
    // TODO: determine what to do about create
    if (!query) {
        free(query);
        return NULL;
    }
    switch (query->type) {
        case SHUTDOWN:
            status->msg = "Shutdown database";
            break;
        case CREATE:
            status->msg = "Created";
            break;
        case SELECT:
            if (query->context->shared_scan == NULL) {
                process_select(
                    &query->operator_fields.select_operator,
                    query->context,
                    status
                );
            } else {
                SharedScanOperator* ss_op = &query->context->shared_scan->operator_fields.shared_operator;
                if (ss_op->num_scans == ss_op->allocated_scans) {
                    // double the number of scans
                    ss_op->allocated_scans *= 2;
                    ss_op->db_scans = realloc(
                        ss_op->db_scans,
                        ss_op->allocated_scans * sizeof(DbOperator*)
                    );
                }
                ss_op->db_scans[ss_op->num_scans++] = query;
                status->msg_type = OK_DONE;
                return NULL;
            }
            break;
        case HASH_JOIN:
            process_hash_join(
                &query->operator_fields.join_operator,
                query->context,
                status
            );
            break;
        case NESTED_LOOP_JOIN:
            process_nested_loop_join(
                &query->operator_fields.join_operator,
                query->context,
                status
            );
            break;
        case SHARED_SCAN:
            process_shared_scans(
                &query->operator_fields.shared_operator,
                query->context,
                status
            );
            break;
        case SUM:
        case AVERAGE:
            process_sum_avg(
                &query->operator_fields.math_operator,
                query->type,
                query->context,
                status
            );
            break;
        case ADD:
        case SUBTRACT:
            process_col_op(
                &query->operator_fields.math_operator,
                query->type,
                query->context,
                status
            );
            break;
        case MIN:
        case MAX:
            // if one col
            if (query->operator_fields.math_operator.handle2[0] == '\0') {
                get_range_value(
                    &query->operator_fields.math_operator,
                    query->type,
                    query->context,
                    status
                );
            } else {
                get_index_and_range(
                    &query->operator_fields.math_operator,
                    query->type,
                    query->context,
                    status
                );
            }
            break;
        case FETCH:
            process_fetch(
                &query->operator_fields.fetch_operator,
                query->context,
                status
            );
            break;
        case PRINT:
            return &query->operator_fields.print_operator;
        case OPEN:
            status->msg = process_open(
                query->operator_fields.open_operator,
                status
            );
            break;
        case INSERT:
            // on insert do this
            process_insert(query->operator_fields.insert_operator, status);
            break;
        default:
            status->msg = "Undefined Operation";
            status->msg_type = QUERY_UNSUPPORTED;
            break;
    }
    // free query
    free(query);
    return NULL;
}
