#include <string.h>
#include <stdlib.h>
#include "cs165_api.h"
#define DEFAULT_TABLE_SIZE 10

// In this class, there will always be only one active database at a time
Db* current_db;

/**
 * @brief This function creates a new column in the database
 *
 * @param name - column name
 * @param table - table name
 * @param sorted - whether it is sorted
 * @param ret_status - status struct
 *
 * @return
 */
Column* create_column(char *name, Table *table, bool sorted, Status *ret_status) {
    // TODO: add in sorting ability
    (void) sorted;
    ret_status->code = ERROR;
    Column* new_col = NULL;

    // determine where we can add the column (check if there is an open col)
    size_t idx = 0;
    while (idx < table->col_count) {
        // set column if we can find a free one
        if(table->columns[idx].name[0] == '\0') {
            ret_status->code = OK;
            ret_status->error_type = 0;
            // set the new column
            new_col = table->columns + idx;
            strcpy(new_col->name, name);
            new_col->data = NULL;
            // TODO: add indexes
            new_col->index = NULL;
            return new_col;
        }
        idx++;
    }
    ret_status->error_type = EXECUTION_ERROR;
    ret_status->error_message = "Column could not be created";
    return new_col;
}

/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
    // add table to the array of tables in the db (realloc if needed)
    if (db->tables_size == db->tables_capacity) {
        // TODO: make sure int overflow doesn't happen
        db->tables_capacity *= 2;
        Table* tmp = realloc(db->tables, db->tables_capacity * sizeof(Table));
        if (!tmp) {
            ret_status->code = ERROR;
            ret_status->error_type = MEM_ALLOC_FAILED;
            ret_status->error_message = "Could not reallocate new tables";
            return NULL;
        }
    }

    // set the table at the newest space
    Table* new_table = db->tables + db->tables_size;
    strcpy(new_table->name, name);
    new_table->table_length = 0;
    new_table->col_count = num_columns;

    // allocate new columns
    /* new_table->columns = malloc(new_table->col_count * sizeof(Column)); */
    new_table->columns = calloc(new_table->col_count, sizeof(Column));
    if (!new_table->columns) {
        ret_status->code = ERROR;
        ret_status->error_type = MEM_ALLOC_FAILED;
        ret_status->error_message = "Couldn't allocate new columns";
        return NULL;
    }

    // increase the database table count
    db->tables_size++;
    ret_status->code = OK;
    return new_table;
}

/*
 * Similarly, this method is meant to create a database.
 * As an implementation choice, one can use the same method
 * for creating a new database and for loading a database
 * from disk, or one can divide the two into two different
 * methods.
 */
Status add_db(const char* db_name, bool is_new) {
    // TODO: use is_new flag to determine whether we need to create
    struct Status ret_status = { .code = OK };

    // allocate space for db
    current_db = malloc(sizeof(Db));
    if (!current_db) {
        ret_status.code = ERROR;
        ret_status.error_type = MEM_ALLOC_FAILED;
        ret_status.error_message = "Could not allocate space for DB";
        return ret_status;
    }
    // initialize DB and allocate space for the tables
    strcpy(current_db->name, db_name);
    current_db->tables_size = 0;
    current_db->tables_capacity = DEFAULT_TABLE_SIZE;
    // allocate table
    current_db->tables = malloc(current_db->tables_capacity * sizeof(Table));
    if (!current_db->tables) {
        ret_status.code = ERROR;
        ret_status.error_type = MEM_ALLOC_FAILED;
        ret_status.error_message = "Could not allocate space for tables";
    }
    return ret_status;
}
