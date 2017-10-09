#include "db_operations.h"
#include "client_context.h"

char* process_open(OpenOperator open_op, Status* status) {
    (void) open_op;
    (void) status;
    return "";
}

char* process_insert(InsertOperator insert_op, Status* status) {
    size_t row_idx = next_table_idx(insert_op.table, status);
    if (status->code != OK) {
        return status->error_message;
    }
    // set the values
    for(size_t idx = 0; idx < insert_op.table->col_count; idx++) {
        insert_op.table->columns[idx].data[row_idx] = insert_op.values[idx];
    }
    return "Success - values inserted";
}

/**
 * execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
char* execute_DbOperator(DbOperator* query) {
    // return if no query
    // TODO: determine what to do about create
    Status db_op_status = { .code= OK };
    if (!query) {
        free(query);
        return "NO QUERY";
    }
    char* result = NULL;
    switch (query->type) {
        case CREATE:
            result = "Created";
            break;
        case OPEN:
            result = process_open(query->operator_fields.open_operator, &db_op_status);
            break;
        case INSERT:
            result = process_insert(query->operator_fields.insert_operator, &db_op_status);
            break;
        default:
            result = "Undefined Operation";
            break;
    }
    // free query
    free(query);
    return result;
}
