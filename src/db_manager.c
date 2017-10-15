#include "cs165_api.h"
#include <string.h>
#include <stdlib.h>
#define DEFAULT_TABLE_SIZE 8
#define DEFAULT_COLUMN_SIZE 256

// In this class, there will always be only one active database at a time
Db* current_db = NULL;
// use this to keep track of all databases
Db* db_head = NULL;

/**
 * @brief This function creates a new column in the database
 *
 * @param name - column name
 * @param table - table name
 * @param sorted - whether it is sorted
 * @param ret_status - status struct
 *
 * @return The created column
 */
Column* create_column(char *name, Table* table, bool sorted, Status *ret_status) {
    // TODO: add in sorting ability
    (void) sorted;
    ret_status->code = ERROR;
    Column* new_col = NULL;

    // determine where we can add the column (check if there is an open col)
    size_t idx = 0;
    while (idx < table->col_count) {
        // set column if we can find a free one
        if (table->columns[idx].name[0] == '\0') {
            ret_status->code = OK;
            ret_status->error_type = 0;
            // set the new column
            new_col = table->columns + idx;
            strcpy(new_col->name, name);
            // TODO: add indexes
            new_col->index = NULL;
            new_col->data = malloc(table->table_length * sizeof(int));
            if (!new_col->data) {
                ret_status->code = ERROR;
                ret_status->error_type = MEM_ALLOC_FAILED;
                ret_status->error_message = "Couldn't allocate new data";
                return NULL;
            }
            return new_col;
        }
        idx++;
    }
    ret_status->error_type = EXECUTION_ERROR;
    ret_status->error_message = "Column could not be created";
    return new_col;
}

/**
 * @brief This function creates a table object. It will return the new table
 *   and will update the return status
 *
 * @param db - Db* - database
 * @param name - string of the name
 * @param num_columns - size of the columns
 * @param ret_status - status
 *
 * @return Table* - the new table
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
        db->tables = tmp;
    }

    // set the table at the newest space
    Table* new_table = db->tables + db->tables_size;
    strcpy(new_table->name, name);
    new_table->table_length = DEFAULT_COLUMN_SIZE;
    new_table->table_size = 0;
    new_table->col_count = num_columns;

    // allocate new columns
    // TODO: is calloc necessary here
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
Status add_db(const char* db_name, bool from_load, size_t capacity) {
    // TODO: use is_new flag to determine whether we need to create
    // also determine if we need to rewrite the files
    struct Status ret_status = { .code = OK };
    Db* db_ptr = db_head;

    // see if db exists and return (if not from load)
    if (from_load == false) {
        while (db_ptr && db_ptr->next_db) {
            if (strcmp(db_ptr->name, db_name) == 0) {
                current_db = db_ptr;
                return ret_status;
            }
            db_ptr = db_ptr->next_db;
        }
    }

    // If we get here we have a new database so we should
    // enter into the next cycle
    db_ptr = malloc(sizeof(Db));
    if (!db_ptr) {
        ret_status.code = ERROR;
        ret_status.error_type = MEM_ALLOC_FAILED;
        ret_status.error_message = "Could not allocate space for DB";
        return ret_status;
    }

    // initialize DB and allocate space for the tables
    strcpy(db_ptr->name, db_name);
    db_ptr->next_db = NULL;
    db_ptr->tables_size = 0;
    db_ptr->tables_capacity = capacity ? capacity : DEFAULT_TABLE_SIZE;

    // allocate table
    db_ptr->tables = malloc(db_ptr->tables_capacity * sizeof(Table));
    if (db_ptr->tables == NULL) {
        ret_status.code = ERROR;
        ret_status.error_type = MEM_ALLOC_FAILED;
        ret_status.error_message = "Could not allocate space for tables";
    }

    // set the head of the list - or update the chain of dbs
    if (!db_head) {
        db_head = db_ptr;
        db_ptr->previous_db = NULL;
    } else {
        current_db->next_db = db_ptr;
        db_ptr->previous_db = current_db;
    }
    current_db = db_ptr;
    return ret_status;
}
