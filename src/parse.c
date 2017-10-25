/*
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <assert.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#define DEFAULT_COL_ALLOC 8

/*
 * TODOs:
 * - make it so only parsing happens in this file
 * - Goal:
 *   "parse command" - this takes in a string and returns a query
 *   "create" - make a create
 *
 */


/**
 * @brief Takes a pointer to a string.  This method returns the original
 * string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after
 * that comma.  This method destroys its input.
 *
 * @param tokenizer
 * @param status
 * @param split
 *
 * @return
 */
char* get_next_token(char** tokenizer, message_status* status, char* split) {
    char* token = strsep(tokenizer, split);
    if (token == NULL) {
        *status = INCORRECT_FORMAT;
    }
    return token;
}

char* next_token(char** tokenizer, message_status* status) {
    return get_next_token(tokenizer, status, ",");
}

/**
 * @brief function that splits the field and returns the next . separated field
 *
 * @param tokenizer
 * @param status
 *
 * @return char* - string of next field
 */
char* next_db_field(char** tokenizer, message_status* status) {
    return get_next_token(tokenizer, status, ".");
}

// TODO: see if this is helpful and switch to using this for parsing
typedef struct NameLookup {
    char db_name[MAX_SIZE_NAME];
    char table_name[MAX_SIZE_NAME];
    char column_name[MAX_SIZE_NAME];
} NameLookup;

typedef enum LookupType {
    DB_LOOKUP,
    TABLE_LOOKUP,
    COLUMN_LOOKUP,
    HANDLE_LOOKUP
} LookupType;

/**
 * @brief This function takes in a string and updates the lookup
 *   TODO: make this capable of looking up a string in a hashtable
 *
 *
 * @param string
 * @param lookup_type
 * @param struct_type
 */
void* process_lookup(const char* string, LookupType struct_type, Status* status) {
    NameLookup lookup = { "", "", "" };
    sscanf(string, "%[^.,\n].%[^.,\n].%[^.,\n]",
           lookup.db_name, lookup.table_name, lookup.column_name);
    switch(struct_type) {
        case HANDLE_LOOKUP:
            return NULL;
            /* return (void*) get_handle(lookup.db_name, status); */
        case DB_LOOKUP:
            return (void*) get_valid_db(lookup.db_name, status);
        case TABLE_LOOKUP:
            return (void*) get_table_from_db(
                lookup.db_name,
                lookup.table_name,
                status
            );
        case COLUMN_LOOKUP:
            return (void*) get_column_from_db(
                lookup.db_name,
                lookup.table_name,
                lookup.column_name,
                status
            );
        default:
            return NULL;
    }
}

/**
 * @brief Function that looks up a database struct and returns it if found
 * TODO: use this
 *
 * @param lookup
 * @param struct_type
 *
 * @return
 */
void* lookup_struct(NameLookup* lookup, LookupType struct_type) {
    Status foo;
    switch (struct_type) {
        case DB_LOOKUP:
            return (void *) get_valid_db(lookup->db_name, &foo);
        case TABLE_LOOKUP:
            return (void *) get_table_from_db(
                lookup->db_name,
                lookup->table_name,
                &foo
            );
        case COLUMN_LOOKUP:
            return (void *) get_column_from_db(
                lookup->db_name,
                lookup->table_name,
                lookup->column_name,
                &foo
            );
        default:
            return NULL;
    }
}


/**
 * @brief this function takes in the argument string for the creation
 * of columns and will create that new column. it will return a status
 * create(col,"project",awesomebase.grades)
 *
 * TODO: Make it so that this parses sorted status
 *
 * @param create_arguments - this is a string that looks like
 *  this "project", awesomebase.grades)
 *
 * @return message status
 */
void parse_create_col(char* create_arguments, Status* status) {
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &status->msg_type);
    // TODO - use the lookup thing here
    char* db_name = next_db_field(create_arguments_index, &status->msg_type);
    char* table_name = next_token(create_arguments_index, &status->msg_type);
    // TODO - enable sorting
    bool sorted = false;

    // not enough arguments
    if (status->msg_type == INCORRECT_FORMAT) {
        status->code = ERROR;
        status->msg = "Wrong # of args for create col";
        return;
    }

    // trim quotes and check for finishing parenthesis.
    column_name = trim_quotes(column_name);
    int last_char = strlen(table_name) - 1;
    if (last_char <= 0 || table_name[last_char] != ')') {
        status->msg_type = INCORRECT_FORMAT;
        status->code = ERROR;
        status->msg = "Create Col does doesn't end with )";
        return;
    }
    // replace final ')' with null-termination character.
    table_name[last_char] = '\0';
    Table* tbl = get_table_from_db(db_name, table_name, status);
    if (tbl) {
        create_column(column_name, tbl, sorted, status);
    }
}

/**
 * @brief This method takes in a string representing the arguments to create a
 * table. It parses those arguments, checks that they are valid, and
 * creates a table.
 * TODO: determine what to do about status here
 *
 * @param create_arguments
 * @param status
 *
 */
void parse_create_tbl(char* create_arguments, Status* status) {
    // process args
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status->msg_type);
    char* db_name = next_token(create_arguments_index, &status->msg_type);
    char* col_cnt = next_token(create_arguments_index, &status->msg_type);
    // not enough arguments
    if (status->msg_type == INCORRECT_FORMAT) {
        status->code = ERROR;
        return;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        status->code = ERROR;
        status->msg_type = INCORRECT_FORMAT;
        return;
    }
    // replace the ')' with a null terminating character.
    col_cnt[last_char] = '\0';
    // turn the string column count into an integer, and
    // check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        status->code = ERROR;
        status->msg_type = INCORRECT_FORMAT;
        return;
    }
    // now make the table
    // check that the database argument is the current active database
    Db* database = get_valid_db(db_name, status);
    if (!database) {
        cs165_log(stdout, "query unsupported. Bad db name\n");
        return;
    } else if (get_table(database, table_name, status)) {
        // ensure that we are not duplicating tables
        status->code = ERROR;
        status->msg_type = OBJECT_ALREADY_EXISTS;
        status->msg = "Table already exists";
        return;
    }
    status->code = OK;
    status->msg_type = OK_WAIT_FOR_RESPONSE;
    // if everything works then return
    create_table(database, table_name, column_cnt, status);
}

/**
 * @brief This method takes in a string representing the arguments to create
 * a database. It parses those arguments, checks that they are valid,
 * and creates a database.
 *
 * @param create_arguments - char*
 *
 * @return message_status
 */
message_status parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return INCORRECT_FORMAT;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return INCORRECT_FORMAT;
        }
        if (add_db(db_name, false, 0).code == OK) {
            return OK_DONE;
        } else {
            return EXECUTION_ERROR;
        }
    }
}


/**
 * @brief parse_create parses a create statement and then passes the necessary
 * arguments off to the next function
 *
 * @param create_arguments
 * @param status
 *
 * @return message_status
 */
DbOperator* parse_create(char* create_arguments, Status* status) {
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input.
    // could we also use strdup?
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create.
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to
        // just past first ","
        token = next_token(&tokenizer_copy, &status->msg_type);
        // reject if it messed up
        if (status->msg_type == INCORRECT_FORMAT) {
            status->code = ERROR;
            return NULL;
        }

        // pass off to next parse function.
        if (strcmp(token, "db") == 0) {
            status->msg_type = parse_create_db(tokenizer_copy);
            // TODO: clean up this code
            status->code = status->msg_type != OK_DONE ? ERROR : OK;
        } else if (strcmp(token, "tbl") == 0) {
            parse_create_tbl(tokenizer_copy, status);
        } else if (strcmp(token, "col") == 0) {
            parse_create_col(tokenizer_copy, status);
        } else {
            status->msg_type = UNKNOWN_COMMAND;
        }
    } else {
        status->msg_type = UNKNOWN_COMMAND;
    }
    free(to_free);
    // TODO - make these statuses do the correct thing
    if (status->code == OK) {
        status->msg_type = OK_DONE;
    }
    return NULL;
}


/**
 * @brief parse_insert reads in the arguments for a create statement and
 * then passes these arguments to a database function to insert a row.
 *
 * @param query_command
 * @param status
 *
 * @return
 */
DbOperator* parse_insert(char* query_command, Status* status) {
    unsigned int columns_inserted = 0;
    char* token = NULL;

    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* db_name = next_db_field(command_index, &status->msg_type);
        char* table_name = next_token(command_index, &status->msg_type);
        if (status->msg_type == INCORRECT_FORMAT) {
            status->code = ERROR;
            status->msg = "Wrong format for table name";
            return NULL;
        }
        // lookup the table and make sure it exists.
        Table* insert_table = get_table_from_db(db_name, table_name, status);
        if (insert_table == NULL) {
            return NULL;
        }
        // make insert operator.
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(
            sizeof(int) * insert_table->col_count
        );
        // parse inputs until we reach the end. Turn each given string into an integer.
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            status->msg_type = INCORRECT_FORMAT;
            status->code = ERROR;
            status->msg = "Wrong number of values for row";
            free (dbo);
            return NULL;
        }
        return dbo;
    } else {
        status->msg_type = UNKNOWN_COMMAND;
        status->code = ERROR;
        return NULL;
    }
}

/**
 * @brief This function loads in a table
 *
 * @param query_command
 * @param internal_status
 */
void parse_load(char* query_command, Status* status) {
    if (strncmp(query_command, "(", 1) != 0) {
        status->msg_type = UNKNOWN_COMMAND;
        status->code = ERROR;
        return;
    }
    // clean string
    char file_name[DEFAULT_READ_SIZE];
    if(!sscanf(query_command, "(\"%[^\"]", file_name)) {
        status->msg_type = INCORRECT_FORMAT;
        status->msg = "File name cannot be read";
        return;
    }

    // TODO: make it read in a file
    FILE* load_file = fopen(file_name, "r");
    if (load_file == NULL) {
        status->code = ERROR;
        status->msg_type = FILE_NOT_FOUND;
        status->msg = "Error, loaded file was not found.";
        return;
    }

    // See if we can find the column then we can get the table
    char csv_line[DEFAULT_READ_SIZE];
    // TODO: edge case - table columns are not in order...
    fgets(csv_line, DEFAULT_READ_SIZE, load_file);
    Table* table = (Table*) process_lookup(csv_line, TABLE_LOOKUP, status);
    if (!table) {
        fclose(load_file);
        return;
    }

    // TODO: this is the assumption I am making - no massive number of columns
    assert(table->col_count * MAX_SIZE_NAME < DEFAULT_READ_SIZE);

    // TODO: edge case - the file is super wide
    // We know the max width is col_count
    while (fgets(csv_line, DEFAULT_READ_SIZE, load_file)) {
        char* token = NULL;
        char* read_ptr = csv_line;
        size_t data_idx = next_table_idx(table, status);
        size_t col_idx = 0;
        while ((token = strsep(&read_ptr, ",")) != NULL &&
                col_idx < table->col_count) {
            table->columns[col_idx++].data[data_idx] = atoi(token);
        }
    }
    if (status->code == OK) {
        status->msg_type = OK_DONE;
    }
    fclose(load_file);
    return;
}

/**
 * @brief parse_print takes in a string and will parse out the objects that
 *  user wants to print
 *
 * @param query_command - string for printing
 * @param status - status for succss
 *
 * @return DbOperator - the operation to be done
 */
DbOperator* parse_print(char* query_command, Status* status) {
    // make sure we have the correct thing
    // TODO: group these checks for the brackets
    if (strncmp(query_command, "(", 1) != 0) {
        status->code = ERROR;
        status->msg_type = INCORRECT_FORMAT;
        return NULL;
    }
    // remove parens
    query_command = trim_parenthesis(query_command);
    DbOperator* dbo = malloc(sizeof(DbOperator));
    size_t num_alloced = DEFAULT_COL_ALLOC;
    size_t ncols = 0;

    // allocate an array of GeneralizedColumns
    GeneralizedColumn* print_objects = malloc(
            sizeof(GeneralizedColumn) * num_alloced);

    // set the type (only one allowed)
    GeneralizedColumnType col_type = RESULT;
    LookupType look_type = HANDLE_LOOKUP;
    if (strchr(query_command, '.')) {
        col_type = COLUMN;
        look_type = COLUMN_LOOKUP;
    }

    char* token = NULL;
    while (status->code == OK &&
            (token = strsep(&query_command, ",")) != NULL) {
        // reallocate twice as many if needed
        if (ncols == num_alloced) {
            num_alloced *= 2;
            print_objects = realloc(
                print_objects,
                sizeof(GeneralizedColumn) * num_alloced
            );
        }
        // determine if we have a result or if we have a col
        print_objects[ncols].column_type = col_type;

        // break if nothing is found
        void* db_obj = NULL;
        if ((db_obj = process_lookup(token, look_type, status))) {
            // TODO: create result column here?
            if (col_type == COLUMN) {
                print_objects[ncols++].column_pointer.column = (Column*) db_obj;
            } else {
                print_objects[ncols++].column_pointer.result = (Result*) db_obj;
            }
        }
    }

    // if any of the columns aren't found we will hit this and we can reject
    if (status->code != OK || ncols == 0) {
        free(print_objects);
        free(dbo);
        return NULL;
    }
    // return the dbo obj
    dbo->operator_fields.print_operator.print_objects = print_objects;
    dbo->operator_fields.print_operator.num_columns = ncols;
    dbo->type = PRINT;
    return dbo;
}

/**
 * parse_command takes as input:
 * - the send_message from the client and then parses it into the
 *   appropriate query.
 * - Stores into send_message the status to send back.
 *
 * Returns a db_operator.
 **/
DbOperator* parse_command(
    char* query_command,
    Status* internal_status,
    int client_socket,
    ClientContext* context
) {
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    // if we have a comment eliminate that
    query_command = trim_comments(query_command);
    if (!query_command || query_command[0] == '\0') {
        internal_status->msg_type = OK_DONE;
        internal_status->code = OK;
        // The -- signifies a comment line, no operator needed.
        return NULL;
    }

    // this is when we have a handle
    char* equals_pointer = strchr(query_command, '=');
    char* handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here.
        *equals_pointer = '\0';
        handle = trim_whitespace(handle);
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    // display the query in the log
    cs165_log(stdout, "QUERY: %s\n", query_command);
    query_command = trim_whitespace(query_command);

    // check what command is given.
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        // TODO: make the creation happen in the database operators
        parse_create(query_command, internal_status);
        /* dbo = malloc(sizeof(DbOperator)); */
        /* dbo->type = CREATE; */
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, internal_status);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, internal_status);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        parse_load(query_command, internal_status);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        // TODO: cleanup shutdown
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
        internal_status->msg_type = SHUTDOWN_SERVER;
    } else {
        internal_status->code = ERROR;
        internal_status->msg_type = UNKNOWN_COMMAND;
        internal_status->msg = "Error: Unknown Command";
    }

    // return null is nothing was returned
    if (dbo == NULL) {
        return dbo;
    }

    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
