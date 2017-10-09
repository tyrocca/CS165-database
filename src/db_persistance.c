#include <unistd.h>
#include <stdbool.h>
#include <stdio.h>
#include "cs165_api.h"
#define MAX_LINE_LEN 2048

/**
 * @brief This function reads in the a line and creates the appropriate tables
 *
 * @param db_fp
 * @param db
 * @param t_size
 * @param status
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
        sscanf(buffer, "%s %zd", table_name, &num_columns);
        // returns table
        create_table(db, table_name, num_columns, status);
        // create_columns(table, num_columns, status)
        table_num++;
    }
    return db_fp;
}

/**
 * @brief This function tries to load a database if it exists - if it doesn't
 *  then we create a new one
 *
 * @return
 */
Status db_startup() {
    Status startup_status;
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
        sscanf(buffer, "%s %zd", db_name, &tables_size);
        startup_status = add_db(db_name, true);
        if (startup_status.code == OK) {
            db_fp = read_tables(db_fp, current_db, tables_size, &startup_status);
        }
    }
    fclose(db_fp);
    return startup_status;
}

/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/
/* Status sync_db(Db* db); */

/**
 * HELPERS IN DBOPS files
 */
/* Status add_db(const char* db_name, bool is_new); */

