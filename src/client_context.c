#include <string.h>
#include <stdlib.h>
#include "client_context.h"
#define HANDLE_INIT_SIZE 8


/**
 * @brief This function gets a result column from the client
 *
 * @param context
 * @param result_name
 * @param status
 *
 * @return
 */
Result* get_result(ClientContext* context, const char* result_name, Status* status) {
    // this try to find matching column (speed improvements here)
    for (int i = 0; i < context->chandles_in_use; i++) {
        if(strcmp(context->chandle_table[i].name, result_name) == 0) {
            status->code = OK;
            status->msg = "Result Found";
            return context->chandle_table[i].generalized_column.column_pointer.result;
        }
    }
    // if it didn't return we know that we had an error
    status->code = ERROR;
    status->msg = "No Result found";
    status->msg_type = OBJECT_NOT_FOUND;
    return NULL;
}

/**
 * @brief This function takes the client context and a string for the handle
 *  and returns the address of the allocated GeneralizedColumnHandle
 *
 * @param context
 * @param handle
 *
 * @return
 */
GeneralizedColumnHandle* add_result_column(ClientContext* context, const char* handle) {
    // MAKE this function take the handle and then make it so that it
    // TODO: switch this to a hash table
    for (int i = 0; i < context->chandles_in_use; i++) {
        if(strcmp(context->chandle_table[i].name, handle) == 0) {
            // free the column
            free(context->chandle_table[i].generalized_column.column_pointer.result);
            context->chandle_table[i].generalized_column.column_pointer.result = NULL;
            return &context->chandle_table[i];
        }
    }

    // increase the number of client handles if necessary
    if (context->chandles_in_use == context->chandle_slots) {
        context->chandle_slots *= 2;
        context->chandle_table = realloc(
            context->chandle_table,
            context->chandle_slots * sizeof(GeneralizedColumnHandle)
        );
    }
    // copy the name
    strcpy(context->chandle_table[context->chandles_in_use].name, handle);
    return &context->chandle_table[context->chandles_in_use++];
}

/**
 * @brief This function is used for getting the next index in a column. It
 *  takes a the table and status and will return an index that tells you
 *  where to add the column values
 *
 * @param table - Table* pointer to the
 * @param ret_status - Status* pointer to the status object (tells success)
 *
 * @return size_t - where to add the column
 */
size_t next_table_idx(Table* table, Status* ret_status) {
    // get the index of the column - realloc if needed
    if (table->table_size == table->table_length) {
        // double size of table
        table->table_length *= 2;
        // reallocate each column
        size_t idx = 0;
        while (idx < table->col_count && ret_status->code != ERROR) {
            // realloc the table
            int* tmp = realloc(
                table->columns[idx].data,
                table->table_length * sizeof(int)
            );
            // check for error in realloc
            if (!tmp) {
                ret_status->code = ERROR;
                ret_status->msg_type = MEM_ALLOC_FAILED;
                ret_status->msg = "Could not reallocate new data";
            }
            table->columns[idx].data = tmp;
            idx++;
        }
    }
    // increase size and return previous value
    return table->table_size++;
}

/**
 * @brief get_valid_db checks to see if we have a valid database name
 *      and updates the message status. This also returns the db
 *
 * @param db_name - name of the database
 * @param message_status - message status enum ptr
 *
 * @returns a pointer to the database
 */
Db* get_valid_db(const char* db_name, Status* status) {
    // check that the database argument is the current active Database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        status->code = ERROR;
        status->msg_type = OBJECT_NOT_FOUND;
        status->msg = "Database name is not valid";
        return NULL;
    }
    return current_db;
}

/**
 * @brief This function takes a database and table name and returns
 *   the name of the table
 *
 * @param db db object
 * @param table_name the table's name
 * @param status pointer to the status of the operation
 *
 * @return
 */
Table* get_table(Db* db, const char* table_name, Status* status) {
    // HACK: made it so you can pass null
    if (!db) {
        db = current_db;
    }
    // TODO: make this a hash table or something faster!
    for (size_t i = 0; i < db->tables_size; i++) {
        if(strcmp(db->tables[i].name, table_name) == 0) {
            status->code = OK;
            status->msg = "Table Found";
            return db->tables + i;
        }
    }
    // if it didn't return we know that we had an error
    status->code = ERROR;
    status->msg = "No Table found";
    status->msg_type = OBJECT_NOT_FOUND;
    return NULL;
}

/**
 * @brief This function takes in a database name and table name and
 *  returns the value (checking that both exist)
 *
 * @param db_name
 * @param table_name
 * @param status
 *
 * @return
 */
Table* get_table_from_db(const char* db_name, const char* table_name, Status* status) {
    // if no database return null
    Db* database = get_valid_db(db_name, status);
    if (!database) {
        return NULL;
    }
    return get_table(database, table_name, status);
}

/**
 * @brief this function will return the column on a get request
 * TODO: lookup improvements
 *
 * @param db_name
 * @param table_name
 * @param col_name
 * @param status
 *
 * @return
 */
Column* get_column(Table* table, const char* col_name, Status* status) {
    // this try to find matching column (speed improvements here)
    for (size_t i = 0; i < table->col_count; i++) {
        if(strcmp(table->columns[i].name, col_name) == 0) {
            status->code = OK;
            status->msg = "Column Found";
            return table->columns + i;
        }
    }
    // if it didn't return we know that we had an error
    status->code = ERROR;
    status->msg = "No Table found";
    status->msg_type = OBJECT_NOT_FOUND;
    return NULL;
}

/**
 * @brief This is a wrapper for getting a column from only strings
 *
 * @param db_name - string of db name
 * @param table_name - string of table name
 * @param col_name - string of column name
 * @param status - status struct
 *
 * @return - column or null if none found
 */
Column* get_column_from_db(
    const char* db_name,
    const char* table_name,
    const char* col_name,
    Status* status
) {
    // Check for database
    Table* table = get_table_from_db(db_name, table_name, status);
    if (!table) {
        return NULL;
    }
    return get_column(table, col_name, status);
}

