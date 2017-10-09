#include "cs165_api.h"

/**
 * execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
void process_open(DbOperator* query) {
    (void) query;
}

void process_insert(DbOperator* query) {
    (void) query;
    //this function processes the query
    //
}
char* execute_DbOperator(DbOperator* query) {
    // return if no query
    // TODO: determine what to do about create
    if (!query) {
        free(query);
        return "NO QUERY";
    }
    switch (query->type) {
        case CREATE:
            break;
        case INSERT:
            process_insert(query);
            break;
        case OPEN:
            process_open(query);
            break;
        default:
            break;
    }
    // free query
    free(query);
    return "165";
}
