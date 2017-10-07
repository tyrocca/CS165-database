#include <string.h>
#include <stdlib.h>
#include "cs165_api.h"
#define DEFAULT_TABLE_SIZE 10

// In this class, there will always be only one active database at a time
Db* current_db;

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
    status->code = OK;
    if (strcmp(current_db->name, db_name) != 0) {
        status->code = ERROR;
        status->error_type = OBJECT_NOT_FOUND;
        status->error_message = "Database name is not valid";
        return NULL;
    }
    return current_db;
}

/**
 * @brief This function takes a database and table name and returns
 *   the name of the table
 *
 * @param db - db object
 * @param table_name - the table's name
 * @param status - pointer to the status of the operation
 *
 * @return
 */
Table* get_table(Db* db, const char* table_name, Status* status) {
    // HACK: made it so you can pass null
    if (!db) {
        db = current_db;
    }
    // TODO: make this a hash table or something faster!
    int max_size = (int) db->tables_size;
    for (int i = 0; i < max_size; i++) {
        if(strcmp(db->tables[i].name, table_name) == 0) {
            status->code = OK;
            status->error_type = OBJECT_ALREADY_EXISTS;
            status->error_message = "Table Found";
            return db->tables + i;
        }
    }
    // if it didn't return we know that we had an error
    status->code = ERROR;
    status->error_message = "No Table found";
    status->error_type = OBJECT_NOT_FOUND;
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
    if (!db) {
        db = current_db;
    }
    if (!table) {
        table = get_table(db, table_name,
    }
    Db* database = get_valid_db(db_name, status);
    if (database) {
        return NULL;
    }
    return get_table(database, table_name, status);
}

/**
 * @brief this function will return the column on a get request
 * TODO: speed up - super slow right now
 *
 * @param db_name
 * @param table_name
 * @param col_name
 * @param status
 *
 * @return
 */
Column* get_column(Table* table, const char* col_name, Status* status) {
    int max_size = (int) tbl->col_count;
    for (int i = 0; i < max_size; i++) {
        if(strcmp(tbl->columns[i].name, col_name) == 0) {
            status->code = OK;
            status->error_type = OBJECT_ALREADY_EXISTS;
            status->error_message = "Table Found";
            return tbl->columns + i;
        }
    }
    // if it didn't return we know that we had an error
    status->code = ERROR;
    status->error_message = "No Table found";
    status->error_type = OBJECT_NOT_FOUND;
    return NULL;
}

/**
 * @brief This is a wrapper for getting a column from only strings
 *
 * @param db_name
 * @param table_name
 * @param col_name
 * @param status
 *
 * @return
 */
Column* get_column_from_db(
    const char* db_name,
    const char* table_name,
    const char* col_name,
    Status* status
) {
    // Check for database
    Db* database = get_valid_db(db_name, status);
    if (database) {
        return NULL;
    }
    return get_column(table_name, col_name, status);
}

/**
 * Creation Functions
 */

Column* create_column(char *name, Table *table, bool sorted, Status *ret_status) {
    // ensure that we are not duplicating tables
    if (get_column(table, name, ret_status)) {
        ret_status->code = ERROR;
        ret_status->error_type = OBJECT_ALREADY_EXISTS;
        ret_status->error_message = "Column already exists";
        return NULL;
    }
    ret_status->code = OK;

    // set the table at the newest space
    Column* new_table = table->tables + table->col_count;
    strcpy(new_table->name, name);
    new_table->table_length = 0;
    new_table->col_count = num_columns;

    // increase the database table count
    table->col_count++;
    return new_table;


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
        db->tables_capacity++;
    }

    // ensure that we are not duplicating tables
    if (get_table(db, name, ret_status)) {
        ret_status->code = ERROR;
        ret_status->error_type = OBJECT_ALREADY_EXISTS;
        ret_status->error_message = "Table already exists";
        return NULL;
    }
    ret_status->code = OK;

    // set the table at the newest space
    Table* new_table = db->tables + db->tables_size;
    strcpy(new_table->name, name);
    new_table->table_length = 0;
    new_table->col_count = num_columns;

    // allocate new columns
    new_table->columns = malloc(new_table->col_count * sizeof(Column));
    if (!new_table->columns) {
        ret_status->code = ERROR;
        ret_status->error_type = MEM_ALLOC_FAILED;
        ret_status->error_message = "Couldn't allocate new columns";
        return NULL;
    }

    // increase the database table count
    db->tables_size++;
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
