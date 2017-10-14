#include <unistd.h>
#include <stdbool.h>
#include <stdio.h>
#include "cs165_api.h"
#define MAX_LINE_LEN 2048

/**
 * @brief This function reads in the a line and creates the appropriate tables
 *
 * @param db_fp - file pointer to db
 * @param db - database object
 * @param t_size - tables size
 * @param status - status of the operation
 *
 * @return
 */
FILE* read_tables(FILE* db_fp, Db* db, size_t t_size, Status* status) {
    char buffer[MAX_LINE_LEN];
    char table_name[MAX_SIZE_NAME];
    size_t num_columns = 0;
    // read in lines
    size_t table_num = 0;
    while (status->code == OK && table_num < t_size) {
        // process line
        fgets(buffer, MAX_LINE_LEN, db_fp);
        sscanf(buffer, "%s %zu", table_name, &num_columns);
        // returns table
        Table* new_table = create_table(db, table_name, num_columns, status);
        (void) new_table;
        // create_columns(table, num_columns, status)
        table_num++;
    }
    return db_fp;
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
    FILE* db_fp = fopen("./database/database.txt", "r");
    if (db_fp == NULL) {
        startup_status.code = ERROR;
        startup_status.error_type = FILE_NOT_FOUND;
        fclose(db_fp);
        return startup_status;
    }
    char buffer[MAX_LINE_LEN];
    while (startup_status.code == OK && fgets(buffer, MAX_LINE_LEN, db_fp)) {
        char db_name[MAX_SIZE_NAME];
        size_t tables_size;
        sscanf(buffer, "%s %zu", db_name, &tables_size);
        startup_status = add_db(db_name, true);
        if (startup_status.code == OK) {
            db_fp = read_tables(db_fp, current_db, tables_size, &startup_status);
        }
    }
    fclose(db_fp);
    return startup_status;
}

/**
 * @brief This function will update the database.txt file with all of the
 *   databases in the system
 *
 * @return status of the update
 */
Status update_db_file() {
    Db* db_ptr = db_head;
    Status status = { .code = OK };
    // open the file
    FILE* db_fp = fopen("./database/database.txt", "w");
    if (db_fp == NULL) {
        status.code = ERROR;
        status.error_type = FILE_NOT_FOUND;
        fclose(db_fp);
        return status;
    }
    // see if db exists and return (if not from load)
    while (db_ptr) {
        fprintf(db_fp, "%s %zu\n", db_ptr->name, db_ptr->tables_size);
        for (size_t i = 0; i < db_ptr->tables_size; i++) {
            fprintf(db_fp, "%s %zu\n", db_ptr->tables[i].name, db_ptr->tables[i].col_count);
        }
        db_ptr = db_ptr->next_db;
    }
    fclose(db_fp);
    return status;
}

int file_name(Db* db, char* table_name, char** fileoutname) {
    return sprintf(*fileoutname, "%s.%s.txt", db->name,
}
/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/
Status sync_db(Db* db) {
    Status status = update_db_file();
    if (status.code == ERROR) {
        status.error_message = "Error updating the database file";
        return status;
    }
    char table_fname[MAX_SIZE_NAME * 2 + 8];
    for (size_t i = 0; i < db->tables_size; i++) {
        file_name(db, table_name
    }



}

