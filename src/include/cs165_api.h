/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
// TODO - needed
/* #include <stdio.h> */
#include "message.h"
/* #include "b_tree.h" */

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define DEFAULT_READ_SIZE 4096

// MACROS
// We are using an array
#define BIT_SZ 32
#define SetBit(A,k)     ( A[(k/BIT_SZ)] |= (1 << (k%BIT_SZ)) )
#define ClearBit(A,k)   ( A[(k/BIT_SZ)] &= ~(1 << (k%BIT_SZ)) )
#define TestBit(A,k)    ( A[(k/BIT_SZ)] & (1 << (k%BIT_SZ)) )

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
    INT,
    LONG,
    DOUBLE,
    INDEX
} DataType;

typedef enum IndexType {
    NONE,
    BTREE,
    SORTED
} IndexType;

typedef union DataPtr {
    int* int_array;
    long* long_array;
    double* double_array;
    size_t* index_array;
    void* void_array;
    char* char_array;
} DataPtr;

/* typedef union DataValue { */
/*     int int_val; */
/*     long long_val; */
/*     double double_val; */
/*     size_t index_val; */
/*     char char_val; */
/* } DataType; */

struct Comparator;
//struct ColumnIndex;

typedef struct Column {
    char name[MAX_SIZE_NAME];
    size_t* size_ptr;           // The size pointer
    int* data;                  // TODO - make this datatype
    /* Table* table;               // The column's table */
    void* index;                // Pointer to the index
    IndexType index_type;       // The type of index
    bool clustered;             // Bool to indicate if the column is clustered
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to
 * (i.e., include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_size, the size of the table (how large columns are) (current size)
 * - table_length, the size of the columns in the table (how much space in cols)
 *      also could be called capacity
 **/

typedef struct Table {
    char name[MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t table_size;
    size_t table_length;
    size_t primary_col_pos;
    Column* primary_index;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the
 *   currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME];
    Table* tables;
    size_t tables_size;
    size_t tables_capacity;
    struct Db* next_db;
    struct Db* previous_db;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    message_status msg_type;
    char* msg;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column,
 * which includes the number of tuples in the result,
 * the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    size_t capacity;
    void *payload;
    DataType data_type;
    bool free_after_use;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;

/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
struct DbOperator;
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    struct DbOperator* shared_scan;  // this is a list of database operators
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column.
 **/
typedef struct Comparator {
    char handle[HANDLE_MAX_SIZE];
    GeneralizedColumn* gen_col;
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares.
    ComparatorType type1;
    ComparatorType type2;
} Comparator;

/*
 * tells the database what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    SELECT,
    SHARED_SCAN,
    SUM,
    AVERAGE,
    MIN,
    MAX,
    ADD,
    SUBTRACT,
    FETCH,
    PRINT,
    OPEN,
    SHUTDOWN
} OperatorType;
/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;
/*
 * necessary fields for Opening
 */
typedef struct OpenOperator {
    char* db_name;
} OpenOperator;

typedef struct PrintOperator {
    GeneralizedColumn* print_objects;
    size_t num_columns;
} PrintOperator;

typedef struct SelectOperator {
    // this is the column that we filter from
    Result* pos_col;
    // this is the column that has the comparisons
    Comparator comparator;
} SelectOperator;

typedef struct FetchOperator {
    Result* idx_col;
    Column* from_col;
    char handle[HANDLE_MAX_SIZE];
} FetchOperator;

// TODO: use this
typedef struct CreateOperator {
    char* db_name[MAX_SIZE_NAME];
    char* table_name[MAX_SIZE_NAME];
    char* column_name[MAX_SIZE_NAME];
} CreateOperator;

/**
 * @brief This operator is used for the math functions.
 * we will have either 1 or 2 handles and either 1 or two
 * result columns
 */
typedef struct MathOperator {
    char handle1[HANDLE_MAX_SIZE];  // handle for the result
    char handle2[HANDLE_MAX_SIZE];  // handle for the result
    GeneralizedColumn gcol1; //
    GeneralizedColumn gcol2; //
} MathOperator;

typedef struct SharedScanOperator {
    GeneralizedColumnHandle* gcol;
    struct DbOperator** db_scans;
    size_t num_scans;
    size_t allocated_scans;
    bool process_scans;
} SharedScanOperator;

/**
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    InsertOperator insert_operator;
    OpenOperator open_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    MathOperator math_operator;
    SharedScanOperator shared_operator;
} OperatorFields;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the
 *          local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

extern Db* current_db;
extern Db* db_head;

Status db_startup();

// this will update the file with the databases
Status update_db_file();

/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/
Status sync_db(Db* db);

/**
 * HELPERS IN DBOPS files
 */
Status add_db(const char* db_name, bool from_load, size_t capacity);

Table* create_table(
    Db* db,
    const char* name,
    size_t num_columns,
    Status *status
);

Column* create_column(
    char *name,
    Table* table,
    IndexType index_type,
    bool clustered,
    Status *ret_status
);

// functions around shutdown
Status shutdown_server();
// was status...
void shutdown_database(Db* db);

char** execute_db_operator(DbOperator* query);
void db_operator_free(DbOperator* query);


#endif /* CS165_H */

