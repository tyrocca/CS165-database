#include <string.h>
#include <stdlib.h>
#include "cs165_api.h"
#define DEFAULT_TABLE_SIZE 10

// In this class, there will always be only one active database at a time
Db* current_db;

/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
    // add table to the array of tables in the db (realloc if needed)
    ret_status->code = OK;
    if (db->tables_size == db->tables_capacity) {
        // TODO: make sure int overflow doesn't happen
        db->tables_capacity *= 2;
        Table* tmp = realloc(db->tables, db->tables_capacity * sizeof(Table));
        if (!tmp) {
            ret_status->code = ERROR;
            return NULL;
        }
        db->tables_capacity++;
    }

    // ensure that we are not duplicating functionality
    // TODO: Make this faster (hashmap?)
    Table* new_table = db->tables;
    while(new_table) {
        // we can't have two tables in the same db that share the same name
        if(strcmp(new_table->name, name) == 0) {
            ret_status->code = ERROR;
            return NULL
        }
        new_table++
    }
    // set the table at the newest space
    Table* new_table = db->tables + db->tables_size;
    strcpy(new_table->name, name);
    new_table->table_length = 0;
    // allocate new columns
    new_table->col_count = num_columns;
    new_table->columns = malloc(new_table->col_count * sizeof(Column));
    if (!new_table->columns) {
        ret_status->code = ERROR;
        return NULL;
    }
    // increase the database table count
    db->tables_size++;
    return NULL;
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

    current_db = malloc(sizeof(Db));
    if (!current_db) {
        ret_status.code = ERROR;
        // TODO: determine if we should use trhe
        // ret_status.code
        return ret_status;
    }

    strcpy(current_db->name, db_name);
    current_db->tables_size = 0;
    current_db->tables_capacity = DEFAULT_TABLE_SIZE;
    // allocate table
    current_db->tables = malloc(current_db->tables_capacity * sizeof(Table));
    if (!current_db->tables) {
        ret_status.code = ERROR;
    }
    return ret_status;
}
