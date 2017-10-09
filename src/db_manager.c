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
    for (size_t i = 0; i < db->tables_size; i++) {
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
            status->error_type = OBJECT_ALREADY_EXISTS;
            status->error_message = "Column Found";
            return table->columns + i;
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

/**
 * Creation Functions
 */

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
    // ensure that we are not duplicating tables
    if (get_column(table, name, ret_status)) {
        ret_status->error_type = OBJECT_ALREADY_EXISTS;
        ret_status->error_message = "Column already exists";
        return new_col;
    }
    // determine where we can add the column
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
