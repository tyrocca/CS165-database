#include <unistd.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include "cs165_api.h"
#define MAX_LINE_LEN 2048

typedef enum StorageType {
    STORED_DB = 1,
    STORED_TABLE = 2,
    STORED_COLUMN = 3,
} StorageType;

typedef struct StorageGroup {
    char name[MAX_SIZE_NAME];
    size_t count_1;
    size_t count_2;
    size_t count_3;
    StorageType type;
} StorageGroup;

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
    size_t table_length = 0;
    size_t table_size = 0;
    // read in lines
    size_t table_num = 0;
    while (status->code == OK && table_num < t_size) {
        // process line
        fgets(buffer, MAX_LINE_LEN, db_fp);
        sscanf(buffer, "%s %zu %zu %zu", table_name, &num_columns,
               &table_size, &table_length);
        // set the correct values for the table
        Table* new_table = create_table(db, table_name, num_columns, status);
        if (status->code != OK) {
            return db_fp;
        }
        new_table->table_size = table_size;
        new_table->table_length = table_length;
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
    FILE* db_fp = fopen("./database/database.bin", "r");
    if (db_fp == NULL) {
        startup_status.code = ERROR;
        startup_status.error_type = FILE_NOT_FOUND;
        fclose(db_fp);
        return startup_status;
    }
    // create a new storage group object
    StorageGroup stored_db;
    while (fread(&stored_db, sizeof(StorageGroup), 1, db_fp)) {
        // load the database
        startup_status = add_db(stored_db.name, true, stored_db.count_2);
        // make space and load the tables
        StorageGroup* sgrouping = malloc(sizeof(StorageGroup) * stored_db.count_1);
        fread(sgrouping, sizeof(StorageGroup), stored_db.count_1, db_fp);
        // save the tables
        while (current_db->tables_size != stored_db.count_1) {
            // set the tables
            StorageGroup* sg_ptr = sgrouping + current_db->tables_size;
            Table* tbl_ptr = create_table(
                current_db,
                sg_ptr->name,
                sg_ptr->count_1,
                &startup_status
            );
            tbl_ptr->table_size = sg_ptr->count_2;
            tbl_ptr->table_length = sg_ptr->count_3;
        }
        // free the read
        free(sgrouping);
    }
    // clean up open file pointer
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
        StorageGroup db_store_obj;
        db_store_obj.count_1 = db_ptr->tables_size;
        db_store_obj.type = STORED_DB;
        db_store_obj.count_2 = db_ptr->tables_capacity;
        strcpy(db_store_obj.name, db_ptr->name);
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
            Table* tbl_ptr = db_ptr->tables + i;
            // store the table
            sgrouping[i].type = STORED_TABLE;
            sgrouping[i].count_1 = tbl_ptr->col_count;
            sgrouping[i].count_2 = tbl_ptr->table_size;
            sgrouping[i].count_3 = tbl_ptr->table_length;
            strcpy(sgrouping[i].name, tbl_ptr->name);
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

int make_table_fname(char* db_name, char* table_name, char* fileoutname) {
    return sprintf(fileoutname, "./database/%s.%s.txt", db_name, table_name);
}

int make_column_fname(char* db_name, char* table_name, char* col_name, char* fileoutname) {
    return sprintf(fileoutname, "./database/%s.%s.%s.txt", db_name, table_name, col_name);
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
    /* char table_fname[MAX_SIZE_NAME * 2 + 8]; */
    /* FILE* table_file = NULL; */
    /* for (size_t i = 0; i < db->tables_size; i++) { */
    /*     Table* tbl = db->tables[i]; */
    /*     make_table_fname(db->name, tbl.name, table_fname); */
    /*     table_file = fopen(table_fname, "w"); */
    /*     fprintf(table_file, "OMG BECKYYYY -- %s\n", db->tables[i].name); */
    /*     fclose(table_file); */
    /* } */
    return status;
}

