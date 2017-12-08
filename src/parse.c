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
#include <limits.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#define DEFAULT_COL_ALLOC 8
#define DEFAULT_SHARED_ALLOC 16


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
 * @brief Wrapper for the column lookup
 *
 * @param string
 * @param status
 *
 * @return
 */
Column* get_col_from_string(const char* string, Status* status) {
    return (Column*) process_lookup(string, COLUMN_LOOKUP, status);
}

void set_generalized_col(
    GeneralizedColumn* gcol,
    const char* string,
    ClientContext* context,
    Status* status
) {
    if (strchr(string, '.')) {
        gcol->column_type = COLUMN;
        gcol->column_pointer.column = get_col_from_string(string, status);
    } else {
        gcol->column_type = RESULT;
        gcol->column_pointer.result = get_result(context, string, status);
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
 * CREATION FUNCTIONS
 *
 * Below are a list of the parse functions for creating columns
 */

// create(idx,<col_name>,[btree, sorted], [clustered, unclustered])
void parse_create_index(char* create_arguments, Status* status) {
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &status->msg_type);
    Column* column = (Column*) get_col_from_string(column_name, status);

    // now setup the index stuff
    char* index_string = next_token(create_arguments_index, &status->msg_type);
    column->index_type = strncmp(index_string, "btree", 5) == 0 ? BTREE : SORTED;
    assert(create_arguments_index != NULL);

    // now handle the clustering things
    char* cluster_param = next_token(create_arguments_index, &status->msg_type);

    // not enough arguments
    if (status->msg_type == INCORRECT_FORMAT) {
        status->code = ERROR;
        status->msg = "Wrong # of args for create_index";
        return;
    }

    // otherwise set the clustering
    if (strncmp(cluster_param, "clustered", 9) == 0) {
        column->clustered = true;
        column->table->primary_index = column;
        // set primary columns index
        for (size_t i = 0; i < column->table->col_count; i++) {
            if (&column->table->columns[i] == column) {
                column->table->primary_col_pos = i;
                break;
            }
        }
    }

}

/**
 * @brief this function takes in the argument string for the creation
 * of columns and will create that new column. it will return a status
 * create(col,"project",awesomebase.grades)
// create(col,"<colname>", full_table_name, [btree, sorted], [clustered, unclustered])
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

    bool clustered = false;
    IndexType index_type = NONE;
    // this means there is more to come
    if (create_arguments != NULL) {
        char* index_string = next_token(create_arguments_index, &status->msg_type);
        index_type = strncmp(index_string, "btree", 5) == 0 ? BTREE : SORTED;
        assert(create_arguments_index != NULL);
        char* cluster_param = next_token(create_arguments_index, &status->msg_type);
        clustered = strncmp(cluster_param, "clustered", 9) == 0;
    }

    // not enough arguments
    if (status->msg_type == INCORRECT_FORMAT) {
        status->code = ERROR;
        status->msg = "Wrong # of args for create col";
        return;
    }

    // trim quotes and check for finishing parenthesis.
    column_name = trim_quotes(column_name);
    if (index_type == NONE) {
        int last_char = strlen(table_name) - 1;
        if (last_char <= 0 || table_name[last_char] != ')') {
            status->msg_type = INCORRECT_FORMAT;
            status->code = ERROR;
            status->msg = "Create Col does doesn't end with )";
            return;
        }
        // replace final ')' with null-termination character.
        table_name[last_char] = '\0';
    }

    Table* tbl = get_table_from_db(db_name, table_name, status);
    if (tbl) {
        create_column(column_name, tbl, index_type, clustered, status);
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
        } else if (strcmp(token, "idx") == 0) {
            parse_create_index(tokenizer_copy, status);
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
 * INSERT Functions:
 * Below are a list of the functions for parsing insertion
 */

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
            free(dbo);
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
DbOperator* parse_print(char* query_command, ClientContext* context, Status* status) {
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
    GeneralizedColumnType col_type = strchr(query_command, '.') ? COLUMN : RESULT;

    // we make a token to fill
    char* token = NULL;
    size_t col_len = 0;
    while (status->code == OK && (token = strsep(&query_command, ",")) != NULL) {
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
        if (col_type == COLUMN) {
            Column* col = NULL;
            if ((col = get_col_from_string(token, status)) == NULL) {
                break;
            }
            print_objects[ncols++].column_pointer.column = col;
            // validate the the column lengths are all the same
            if (col_len == 0) {
                col_len = *col->size_ptr;
            } else if (col_len != *col->size_ptr) {
                status->code = ERROR;
                status->msg_type = QUERY_UNSUPPORTED;
                status->msg = "Cannot print different length items";
            }
        } else {
            Result* rcol = NULL;
            if ((rcol = get_result(context, token, status)) == NULL) {
                break;
            }
            print_objects[ncols++].column_pointer.result = rcol;
            // reject if length is not clear
            if (col_len == 0) {
                col_len = rcol->num_tuples;
            } else if (col_len != rcol->num_tuples) {
                status->code = ERROR;
                status->msg_type = QUERY_UNSUPPORTED;
                status->msg = "Cannot print different length items";
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
 * @brief This function takes a query string, and updates the comparator
 * so that it will run the correct comparison
 *
 * @param range_str - string with the "null, 100"
 * @param comp - the comparison object
 * @param status - the status that will be updated
 */
void set_bounds(char* range_str, Comparator* comp, Status* status) {
    char* token = next_token(&range_str, &status->msg_type);
    comp->p_low = strncmp(token, "null", 4) == 0 ? INT_MIN : atoi(token);
    comp->type1 = GREATER_THAN_OR_EQUAL;
    // it will be < higher
    comp->p_high = strncmp(range_str, "null", 4) == 0 ? INT_MAX : atoi(range_str);
    comp->type2 = LESS_THAN;
    if (status->msg_type == INCORRECT_FORMAT) {
        status->code = ERROR;
    }

    // make the comparison smarter
    if (comp->p_low == comp->p_high) {
        comp->type1 = comp->type2 = EQUAL;
    } else if (comp->p_low == INT_MIN) {
        comp->type1 = NO_COMPARISON;
    } else if (comp->p_high == INT_MAX) {
        comp->type2 = NO_COMPARISON;
    }
}

/**
 * @brief This function takes the string for a selection and parses it
 *
 * @param query_command
 * @param context
 * @param status
 *
 * @return
 */
DbOperator* parse_select(char* query_command, ClientContext* context, Status* status) {
    if (strncmp(query_command, "(", 1) != 0) {
        status->code = ERROR;
        status->msg_type = INCORRECT_FORMAT;
        return NULL;
    }
    // cut off the parens
    query_command = trim_parenthesis(query_command);
    // move the token
    char* token = next_token(&query_command, &status->msg_type);
    if (status->msg_type == INCORRECT_FORMAT) {
        status->code = ERROR;
        return NULL;
    }
    // create space for the operator
    DbOperator* db_query = malloc(sizeof(DbOperator));
    db_query->type = SELECT;
    // make space for the generalized column
    // TODO: do we need to use a generalized column?
    GeneralizedColumn* gcol = malloc(sizeof(GeneralizedColumn));
    // generalized column
    if (strchr(token, '.')) {
        gcol->column_type = COLUMN;
        db_query->operator_fields.select_operator.pos_col = NULL;
        // get the column
        gcol->column_pointer.column = get_col_from_string(token, status);
    } else {
        // get the position column
        gcol->column_type = RESULT;
        db_query->operator_fields.select_operator.pos_col = get_result(
            context,
            token,
            status
        );
        token = next_token(&query_command, &status->msg_type);
        // get the result column
        gcol->column_pointer.result = get_result(context, token, status);
        // make sure the two columns have the same length
        if(db_query->operator_fields.select_operator.pos_col &&
                gcol->column_pointer.result &&
                (db_query->operator_fields.select_operator.pos_col->num_tuples !=
                 gcol->column_pointer.result->num_tuples)){
            // mark this as an eror
            status->code = ERROR;
            status->msg_type = QUERY_UNSUPPORTED;
            status->msg = "Selects cannot have different lengths";
        }
    }
    if (status->code != OK) {
        free(gcol);
        free(db_query);
        return NULL;
    }

    // set the bounds on the query
    set_bounds(
        query_command,
        &db_query->operator_fields.select_operator.comparator,
        status
    );
    db_query->operator_fields.select_operator.comparator.gen_col = gcol;
    return db_query;
}

/**
 * @brief Function that processes the fetch command
 *
 * @param query_command
 * @param context
 * @param status
 *
 * @return
 */
DbOperator* parse_fetch(char* query_command, ClientContext* context, Status* status) {
    if (strncmp(query_command, "(", 1) != 0) {
        status->code = ERROR;
        status->msg_type = INCORRECT_FORMAT;
        return NULL;
    }
    // cut off the parens
    query_command = trim_parenthesis(query_command);
    // move the token
    char* token = next_token(&query_command, &status->msg_type);
    if (status->msg_type == INCORRECT_FORMAT) {
        status->code = ERROR;
        return NULL;
    }
    // create space for the operator
    DbOperator* db_query = malloc(sizeof(DbOperator));
    db_query->type = FETCH;
    // set the fetch
    db_query->operator_fields.fetch_operator.from_col = get_col_from_string(token, status);
    db_query->operator_fields.fetch_operator.idx_col = get_result(context, query_command, status);

    // make space for the generalized column
    if (status->code != OK) {
        free(db_query);
        return NULL;
    }
    return db_query;
}

// TODO: does this need to support the a,b = min thing?
DbOperator* parse_math(char* query_command, ClientContext* context, Status* status) {
    if (strncmp(query_command, "(", 1) != 0) {
        status->code = ERROR;
        status->msg_type = INCORRECT_FORMAT;
        return NULL;
    }
    // cut off the parens
    query_command = trim_parenthesis(query_command);

    // check the number of args
    DbOperator* db_query = calloc(1, sizeof(DbOperator));

    // move the token
    if (strncmp(query_command, "null,", 5) == 0) {
        query_command += 5;
    } else if (strchr(query_command, ',')) {
        char* token = next_token(&query_command, &status->msg_type);
        set_generalized_col(
            &db_query->operator_fields.math_operator.gcol2,
            query_command,
            context,
            status
        );
        query_command = token;
    }
    // here we set the params of the generalized column
    set_generalized_col(
        &db_query->operator_fields.math_operator.gcol1,
        query_command,
        context,
        status
    );
    // make space for the generalized column
    if (status->code != OK) {
        free(db_query);
        return NULL;
    }
    return db_query;
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
    char* handle_2 = NULL;
    if (equals_pointer != NULL) {
        // handle exists, store here.
        *equals_pointer = '\0';
        query_command = ++equals_pointer;

        // check for a second handle
        if ((handle_2 = strchr(handle, ',')) != NULL) {
            *handle_2 = '\0';
            handle_2++;
            handle_2 = trim_whitespace(handle_2);
        }
        // clean handle
        handle = trim_whitespace(handle);
        // we can reuse handles
        internal_status->code = OK;
        internal_status->msg_type = OK_WAIT_FOR_RESPONSE;

        // log the input handle
        cs165_log(stdout, "FILE HANDLE 1: %s\n", handle);
        if (handle_2) {
            cs165_log(stdout, "FILE HANDLE 2: %s\n", handle_2);
        }
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
    } else if (strncmp(query_command, "batch_queries", 13) == 0) {
        assert(NULL == context->shared_scan);
        context->shared_scan = dbo = malloc(sizeof(DbOperator));
        dbo->type = SHARED_SCAN;
        SharedScanOperator* ss_op = &dbo->operator_fields.shared_operator;
        ss_op->process_scans = false;
        ss_op->allocated_scans = DEFAULT_SHARED_ALLOC;
        ss_op->num_scans = 0;
        ss_op->db_scans = malloc(sizeof(DbOperator*) * ss_op->allocated_scans);
        internal_status->msg_type = OK_DONE;
    } else if (strncmp(query_command, "batch_execute", 13) == 0) {
        assert(NULL != context->shared_scan);
        // when we are doing the batched execute, process the
        // set of shared scans
        dbo = context->shared_scan;
        dbo->operator_fields.shared_operator.process_scans = true;
        context->shared_scan = NULL;
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(query_command, context, internal_status);
        if (dbo) {
            strcpy(dbo->operator_fields.select_operator.comparator.handle, handle);
        }
    // Below is some vile copied and pasted code
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, context, internal_status);
        if (dbo) {
            strcpy(dbo->operator_fields.fetch_operator.handle, handle);
        }
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_math(query_command, context, internal_status);
        if (dbo) {
            strcpy(dbo->operator_fields.math_operator.handle1, handle);
            if (handle_2) {
                strcpy(dbo->operator_fields.math_operator.handle2, handle_2);
            }
            dbo->type = SUM;
        }
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_math(query_command, context, internal_status);
        if (dbo) {
            strcpy(dbo->operator_fields.math_operator.handle1, handle);
            if (handle_2) {
                strcpy(dbo->operator_fields.math_operator.handle2, handle_2);
            }
            dbo->type = AVERAGE;
        }
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_math(query_command, context, internal_status);
        if (dbo) {
            strcpy(dbo->operator_fields.math_operator.handle1, handle);
            if (handle_2) {
                strcpy(dbo->operator_fields.math_operator.handle2, handle_2);
            }
            dbo->type = MIN;
        }
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_math(query_command, context, internal_status);
        if (dbo) {
            strcpy(dbo->operator_fields.math_operator.handle1, handle);
            if (handle_2) {
                strcpy(dbo->operator_fields.math_operator.handle2, handle_2);
            }
            dbo->type = MAX;
        }
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_math(query_command, context, internal_status);
        if (dbo) {
            strcpy(dbo->operator_fields.math_operator.handle1, handle);
            if (handle_2) {
                strcpy(dbo->operator_fields.math_operator.handle2, handle_2);
            }
            dbo->type = ADD;
        }
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_math(query_command, context, internal_status);
        if (dbo) {
            strcpy(dbo->operator_fields.math_operator.handle1, handle);
            if (handle_2) {
                strcpy(dbo->operator_fields.math_operator.handle2, handle_2);
            }
            dbo->type = SUBTRACT;
        }
    // end the "handle commands"
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        // TODO: pass client context to print
        dbo = parse_print(query_command, context, internal_status);
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
