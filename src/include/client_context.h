#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Result* get_result(
    ClientContext* context,
    const char* result_name,
    Status* status
);

GeneralizedColumnHandle* add_result_column(
    ClientContext* context,
    const char* handle
);
size_t next_table_idx(Table* table, Status* ret_status);

Db* get_valid_db(const char* db_name, Status* status);

Table* get_table(Db* db, const char* table_name, Status* status);

Table* get_table_from_db(const char* db_name, const char* table_name, Status* status);

Column* get_column(Table* table, const char* col_name, Status* status);

Column* get_column_from_db(
    const char* db_name,
    const char* table_name,
    const char* col_name,
    Status* status
);

#endif
