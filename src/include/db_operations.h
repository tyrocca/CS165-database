#ifndef DB_OPERATIONS_H
#define DB_OPERATIONS_H

#include "cs165_api.h"

// define the various db operations
char* process_open(OpenOperator open_op, Status* status);

char* process_insert(InsertOperator insert_op, Status* status);

PrintOperator* execute_DbOperator(DbOperator* query, Status* status);

#endif
