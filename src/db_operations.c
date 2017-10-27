#include "db_operations.h"
#include "client_context.h"
#include "assert.h"

/**
 * @brief This function does the insert operation on the table
 *
 * @param insert_op InsertOperator
 * @param status Status - used for maintaining status
 *
 * @return string
 */
char* process_insert(InsertOperator insert_op, Status* status) {
    size_t row_idx = next_table_idx(insert_op.table, status);
    if (status->code != OK) {
        return NULL;
    }
    // set the values
    for(size_t idx = 0; idx < insert_op.table->col_count; idx++) {
        insert_op.table->columns[idx].data[row_idx] = insert_op.values[idx];
    }
    status->msg_type = OK_DONE;
    free(insert_op.values);
    return "Success! Values inserted.";
}

/**
 * @brief This function opens a file and loads it into the system
 *
 * @param open_op - OpenOperator
 * @param status - Status
 *
 * @return
 */
char* process_open(OpenOperator open_op, Status* status) {
    // TODO: implement process open
    (void) open_op;
    (void) status;
    return NULL;
}

void select_from_col(Comparator* comp, Result* result_col) {
    // TODO: Make it so this only does 1 comparison at a time
    Column* col = comp->gen_col->column_pointer.column;
    int* positions = malloc(sizeof(int) * (*col->size_ptr));
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
        sizeof(int) * result_col->num_tuples
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
    Result* result_col = malloc(sizeof(Result));
    if (select_op->pos_col) {
        // do stuff with positions
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
            process_select(
                &query->operator_fields.select_operator,
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
