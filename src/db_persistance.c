#include <unistd.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include "cs165_api.h"
#define MAX_LINE_LEN 2048

// This is used for the storage
typedef enum StorageType {
    STORED_DB = 1,
    STORED_TABLE = 2,
} StorageType;

// this is used for the storage group
typedef struct StorageGroup {
    char name[MAX_SIZE_NAME];
    size_t count_1;
    size_t count_2;
    size_t count_3;
    StorageType type;
} StorageGroup;

// this is used for storing columns
typedef struct StoredColumn {
    char name[MAX_SIZE_NAME];
    bool clustered;
} StoredColumn;

/**
 * @brief This function generates the table file name
 *
 * @param db_name - Char*
 * @param table_name - Char* table name
 * @param fileoutname - Char* this is the file
 *
 * @return
 */
int make_table_fname(char* db_name, char* table_name, char* fileoutname) {
    return sprintf(fileoutname, "./database/%s.%s.bin", db_name, table_name);
}

/**
 * @brief This function makes the binary file name for the column
 *
 * @param db_name - this is the db name (char*)
 * @param table_name - this is the table name (char*)
 * @param col_name - this is the col name
 * @param fileoutname - this is where it all gets returned
 *
 * @return
 */
int make_column_fname(char* db_name, char* table_name, char* col_name, char* fileoutname) {
    return sprintf(fileoutname, "./database/%s.%s.%s.bin", db_name, table_name, col_name);
}

///////////////////////
// LOADING FUNCTIONS //
///////////////////////

/**
 * @brief This function takes in a loaded storage group and updates the status
 *
 * @param sg_ptr
 * @param status
 */
void load_table(StorageGroup* sg_ptr, Status* status) {
    Table* tbl_ptr = create_table(
        current_db,
        sg_ptr->name,
        sg_ptr->count_1,
        status
    );
    if (status->code == ERROR) {
        return;
    }
    tbl_ptr->table_size = sg_ptr->count_2;
    tbl_ptr->table_length = sg_ptr->count_3;

    // load the column file
    StoredColumn* scolumns = malloc(sizeof(StoredColumn) * tbl_ptr->col_count);
    char table_fname[MAX_SIZE_NAME * 2 + 8];
    make_table_fname(current_db->name, tbl_ptr->name, table_fname);
    FILE* table_file = fopen(table_fname, "rb");
    if (table_file == NULL || scolumns == NULL) {
        status->code = ERROR;
        return;
    }
    fread(scolumns, sizeof(StoredColumn), tbl_ptr->col_count, table_file);

    // load in the columns
    for (size_t i = 0; status->code != ERROR && i < tbl_ptr->col_count; i++) {
        char col_fname[MAX_SIZE_NAME * 3 + 8];
        Column* col = tbl_ptr->columns + i;
        strcpy(col->name, scolumns[i].name);
        // load if data was allocated
        col->data = malloc(tbl_ptr->table_length * sizeof(int));
        if (col->data == NULL) {
            status->code = ERROR;
            status->error_type = MEM_ALLOC_FAILED;
            fclose(table_file);
            return;
        }
        // set up the data
        make_column_fname(current_db->name, tbl_ptr->name, col->name, col_fname);
        FILE* col_file = fopen(col_fname, "rb");
        if (col_file == NULL) {
            status->code = ERROR;
            status->error_type = MEM_ALLOC_FAILED;
            return;
        }
        fread(col->data, sizeof(int), tbl_ptr->table_size, col_file);
    }
    free(scolumns);
    fclose(table_file);
}

/**
 * @brief This function tries to load a database if it exists - if it doesn't
 *  then we create a new one
 *
 * @return status of the startup process
 */
Status db_startup() {
    Status startup_status = { .code = OK };
    // open file
    FILE* db_fp = fopen("./database/database.bin", "r");
    if (db_fp == NULL) {
        startup_status.code = ERROR;
        startup_status.error_type = FILE_NOT_FOUND;
        fclose(db_fp);
        return startup_status;
    }

    // create a new storage group object
    StorageGroup stored_db;
    while (startup_status.code != ERROR && fread(&stored_db, sizeof(StorageGroup), 1, db_fp)) {
        // load the database
        startup_status = add_db(stored_db.name, true, stored_db.count_2);
        // make space and read the tables
        StorageGroup* sgrouping = malloc(sizeof(StorageGroup) * stored_db.count_1);
        if (sgrouping == NULL) {
            startup_status.code = ERROR;
            startup_status.error_type = MEM_ALLOC_FAILED;
            return startup_status;
        }
        fread(sgrouping, sizeof(StorageGroup), stored_db.count_1, db_fp);
        // process the tables
        while (startup_status.code != ERROR && current_db->tables_size != stored_db.count_1) {
            load_table(sgrouping + current_db->tables_size, &startup_status);
        }
        // free at finish
        free(sgrouping);
    }

    // clean up open file pointer
    fclose(db_fp);
    return startup_status;
}

///////////////////////
// STORAGE FUNCTIONS //
///////////////////////

/**
 * @brief This function takes a column and reformats it to be stored
 *
 * @param column - Column*
 * @return StoredColumn
 */
StoredColumn store_column(Column* column) {
    StoredColumn sc;
    strcpy(sc.name, column->name);
    return sc;
}

/**
 * @brief This function takes a table and reformats it to be stored
 *
 * @param table - table*
 * @param sg - StorageGroup*
 */
void store_table(Table* table, StorageGroup* sg) {
    // store the table
    sg->type = STORED_TABLE;
    sg->count_1 = table->col_count;
    sg->count_2 = table->table_size;
    sg->count_3 = table->table_length;
    strcpy(sg->name, table->name);
}

/**
 * @brief This function takes in a database and formats it into a storage obj
 *
 * @param db_ptr - pointer to database
 * @return StorageGroup object
 */
StorageGroup store_db(Db* db_ptr) {
    StorageGroup sg;
    sg.type = STORED_DB;
    sg.count_1 = db_ptr->tables_size;
    sg.count_2 = db_ptr->tables_capacity;
    strcpy(sg.name, db_ptr->name);
    return sg;
}

/**
 * @brief This function takes a column and a file name and dumps the column
 *
 * @param fname
 * @param col
 * @param data_len length of the data
 * @param status
 *
 */
void dump_column(const char* fname, Column* col, size_t data_len, Status* status) {
    FILE* col_file = fopen(fname, "wb");
    if (col_file == NULL) {
        status->code = ERROR;
        status->error_type = FILE_NOT_FOUND;
        return;
    }
    fwrite(col->data, sizeof(int), data_len, col_file);
    fclose(col_file);
}

/**
 * @brief This function takes a table and dumps it to a file
 *
 * @param fname - file name
 * @param table - the pointer to the Table
 *
 * @return status
 */
Status dump_db_table(const char* fname, Db* db, Table* table) {
    Status status = { .code = OK };
    FILE* table_file = fopen(fname, "wb");
    if (table_file == NULL) {
        status.code = ERROR;
        status.error_type = FILE_NOT_FOUND;
        return status;
    }
    for (size_t i = 0; status.code != ERROR && i < table->col_count; i++) {
        char col_fname[MAX_SIZE_NAME * 3 + 8];
        Column* col = table->columns + i;
        StoredColumn sc = store_column(col);
        fwrite(&sc, sizeof(StorageGroup), 1, table_file);
        // dump the column
        make_column_fname(db->name, table->name, col->name, col_fname);
        dump_column(col_fname, col, table->table_size, &status);
    }
    fclose(table_file);
    return status;
}


/**
 * @brief This function will update the database.bin file with all of the
 *   databases and tables in the system
 *
 * @return status of the update
 */
Status dump_databases() {
    Db* db_ptr = db_head;
    Status status = { .code = OK };

    // open the file
    FILE* db_fp = fopen("./database/database.bin", "wb");
    if (db_fp == NULL) {
        status.code = ERROR;
        status.error_type = FILE_NOT_FOUND;
        fclose(db_fp);
        return status;
    }

    // see if db exists and return (if not from load)
    while (db_ptr) {
        // write out the database object
        StorageGroup db_store_obj = store_db(db_ptr);
        fwrite(&db_store_obj, sizeof(StorageGroup), 1, db_fp);

        // TODO: Make sure that this values is not going to cause an overflow
        // load the table into an object
        StorageGroup* sgrouping = malloc(sizeof(StorageGroup) * db_ptr->tables_size);
        if (sgrouping == NULL) {
            status.code = ERROR;
            status.error_type = MEM_ALLOC_FAILED;
            return status;
        }
        for (size_t i = 0; i < db_ptr->tables_size; i++) {
            store_table(db_ptr->tables + i, sgrouping + i);
        }

        // TODO: Add check for fwrite
        fwrite(sgrouping, sizeof(StorageGroup), db_ptr->tables_size, db_fp);

        // clean up and move to next database
        free(sgrouping);
        db_ptr = db_ptr->next_db;
    }
    fclose(db_fp);
    return status;
}

/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/
Status sync_db(Db* db) {
    // first store the databases and their tables
    Status status = dump_databases();
    if (status.code == ERROR) {
        status.error_message = "Error updating the database file";
        return status;
    }
    // store the database
    for (size_t i = 0; status.code != ERROR && i < db->tables_size; i++) {
        char table_fname[MAX_SIZE_NAME * 2 + 8];
        Table* table = db->tables + i;
        make_table_fname(db->name, table->name, table_fname);
        status = dump_db_table(table_fname, db, table);
    }
    return status;
}

////////////////////////
// Clean up functions //
////////////////////////

/**
 * @brief this frees a column and all of its dependancies
 *
 * @param column - Column* - free column parts
 */
void free_column(Column* column) {
    free(column->data);
}

/**
 * @brief Function frees all of the subcomponents of a table
 *
 * @param table
 */
void free_table(Table* table) {
    // cleanup all nested components
    for (size_t i = 0; i < table->col_count; i++) {
        free_column(table->columns + i);
    }
    free(table->columns);
}

/**
 * @brief This function is for shutting down the database
 *
 * @param db
 *
 * @return
 */
void shutdown_database(Db* db) {
    for (size_t i = 0; i < db->tables_size; i++) {
        free_table(db->tables + i);
    }
    free(db->tables);
    // update the database's location
    if (db->previous_db) {
        // if we have a previous, set its next to the current next
        db->previous_db->next_db = db->next_db;
    } else {
        db_head = db->next_db;
    }
    // if we have a next, set its next's previous to the current prev
    if (db->next_db) {
        db->next_db->previous_db = db->previous_db;
    }
    free(db);
}

// TODO: load
void load_file() {}


