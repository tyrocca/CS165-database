#include "db_operations.h"
#include "client_context.h"

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
        return status->msg;
    }
    // set the values
    for(size_t idx = 0; idx < insert_op.table->col_count; idx++) {
        insert_op.table->columns[idx].data[row_idx] = insert_op.values[idx];
    }
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

Result* col_to_result(Column* col) {
    Result* result = malloc(sizeof(Result));
    result->free_after_use = true;
    result->num_tuples = *col->size_ptr;
    result->data_type = INT;

}

Result* process_print(PrintOperator print_op, Status* status) {
    assert(print_op.num_columns > 0);
    // TODO: make it so we can print really long things
    // Return null when there is nothing we can do
    Result* result = NULL;
    // this should alway be true
    // get the column type and print size
    GeneralizedColumnType type = print_op.print_objects[0].column_type;
    size_t print_sz = type == (
            RESULT ? print_op.print_objects[0].column_pointer.result->num_tuples
            : *print_op.print_objects[0].column_pointer.column->size_ptr
    );
    // TODO: should the coersion to result happen in parse?
    if (print_op.num_columns == 1 && type == RESULT) {
        result = print_op.print_objects[0].column_pointer.result;
    } else if (print_op.num_columns == 1 && type == COLUMN) {
    }
    // if we only have 1 column return it
    if (print_objects.num_columns == 1) {
        // if we have a result column, just return
        if (type == RESULT) {
            result = print_op->print_objects[0].column_pointer.result
        } else
    }

    else if (print_op->num_columns == 1) {
        if (print_op->print_objects[0].column_type == RESULT) {
            return print_ob

        }


    }


}


/**
 * execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
Result* execute_DbOperator(DbOperator* query, Status* status) {
    // return if no query
    // TODO: determine what to do about create
    Result* result = NULL;
    if (!query) {
        free(query);
        return NULL;
    }
    switch (query->type) {
        case SHUTDOWN:
            status->msg = "Shutdown database";
            shutdown_server();
            break;
        case CREATE:
            status->msg = "Created";
            break;
        case PRINT:
            result = process_print(
                query->operator_fields.print_operator,
                status
            );
            break;
        case OPEN:
            status->msg = process_open(
                query->operator_fields.open_operator,
                status
            );
            break;
        case INSERT:
            status->msg = process_insert(
                query->operator_fields.insert_operator,
                status
            );
            break;
        default:
            status->msg = "Undefined Operation";
            break;
    }
    // free query
    free(query);
    return result;
}
