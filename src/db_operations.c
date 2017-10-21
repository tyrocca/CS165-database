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
        return status->error_message;
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

/**
 * execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
char* execute_DbOperator(DbOperator* query, Status* status) {
    // return if no query
    // TODO: determine what to do about create
    if (!query) {
        free(query);
        return "NO QUERY";
    }
    char* result = NULL;
    switch (query->type) {
        case SHUTDOWN:
            result = "Shutdown database";
            shutdown_server();
            break;
        case CREATE:
            result = "Created";
            break;
        case OPEN:
            result = process_open(query->operator_fields.open_operator, status);
            break;
        case INSERT:
            result = process_insert(query->operator_fields.insert_operator, status);
            break;
        default:
            result = "Undefined Operation";
            break;
    }
    // free query
    free(query);
    return result;
}
