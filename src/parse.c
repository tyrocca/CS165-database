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
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated
 * to where its first comma lies.
 * In addition, the original string now points to the
 * first character after that comma.
 * This method destroys its input.
 **/
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

char* next_db_field(char** tokenizer, message_status* status) {
    return get_next_token(tokenizer, status, ".");
}

/**
 * @brief this function takes in the argument string for the creation
 * of columns and will create that new column. it will return a status
 * create(col,"project",awesomebase.grades)
 * TODO: Make it so that this parses sorted status
 *
 * @param create_arguments - this is a string that looks like
 *   this "project", awesomebase.grades)
 *
 * @return message status
 */
void parse_create_col(char* create_arguments, Status* status) {
    char** create_arguments_index = &create_arguments;
    char* column_name = next_token(create_arguments_index, &status->error_type);
    char* db_name = next_db_field(create_arguments_index, &status->error_type);
    char* table_name = next_token(create_arguments_index, &status->error_type);
    // TODO
    bool sorted = false;

    // not enough arguments
    if (status->error_type == INCORRECT_FORMAT) {
        status->error_message = "Wrong # of args for create col";
        return;
    }

    // trim quotes and check for finishing parenthesis.
    column_name = trim_quotes(column_name);
    int last_char = strlen(table_name) - 1;
    if (last_char <= 0 || table_name[last_char] != ')') {
        status->error_type = INCORRECT_FORMAT;
        status->error_message = "Create Col does doesn't end with )";
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
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 * TODO: determine what to do about status here
 **/
message_status parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }

    // replace the ')' with a null terminating character.
    col_cnt[last_char] = '\0';

    // turn the string column count into an integer, and
    // check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    // now make the table
    Status create_status;
    // check that the database argument is the current active database
    Db* database = get_valid_db(db_name, &create_status);
    if (!database) {
        cs165_log(stdout, "query unsupported. Bad db name\n");
        return QUERY_UNSUPPORTED;
    }
    // ensure that we are not duplicating tables
    if (get_table(database, table_name, &create_status)) {
        create_status.code = ERROR;
        create_status.error_type = OBJECT_ALREADY_EXISTS;
        create_status.error_message = "Table already exists";
        return create_status.error_type;
    }
    // if everything works then return
    create_table(database, table_name, column_cnt, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a table failed.\n");
        return EXECUTION_ERROR;
    }
    return status;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

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
 * parse_create parses a create statement and then passes the necessary
 * arguments off to the next function
 **/
message_status parse_create(char* create_arguments, Status* status) {
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
        token = next_token(&tokenizer_copy, &status->error_type);
        if (status->error_type == INCORRECT_FORMAT) {
            return status->error_type;
        } else {
            // pass off to next parse function.
            if (strcmp(token, "db") == 0) {
                status->error_type = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                status->error_type = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                parse_create_col(tokenizer_copy, status);
            } else {
                status->error_type = UNKNOWN_COMMAND;
            }
        }
    } else {
        status->error_type = UNKNOWN_COMMAND;
    }
    free(to_free);
    return status->error_type;
}


/**
 * parse_insert reads in the arguments for a create statement and
 * then passes these arguments to a database function to insert a row.
 **/
DbOperator* parse_insert(char* query_command, Status* status) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* db_name = next_db_field(command_index, &status->error_type);
        char* table_name = next_token(command_index, &status->error_type);
        if (status->error_type == INCORRECT_FORMAT) {
            status->code = ERROR;
            status->error_message = "Wrong format for table name";
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
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer.
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            status->error_type = INCORRECT_FORMAT;
            status->code = ERROR;
            status->error_message = "Wrong number of values for row";
            free (dbo);
            return NULL;
        }
        return dbo;
    } else {
        status->error_type = UNKNOWN_COMMAND;
        status->code = ERROR;
        return NULL;
    }
}

/* DbOperator* parse_load(char* query_command, Status* status) { */
/*     char* token = NULL; */
/*     if (strncmp(query_command, "(", 1) == 0) { */
/*         token = next_token(&tokenizer_copy, &status->error_type); */
/*     } */
/* } */

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
    message* send_message,
    int client_socket,
    ClientContext* context
) {
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator)); // calloc?

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here.
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    // display the query in the log
    cs165_log(stdout, "QUERY: %s\n", query_command);

    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);

    // use this for tracking the status inside
    Status internal_status = {
        .code = OK,
        .error_type = OK_WAIT_FOR_RESPONSE,
        .error_message = ""
    };
    // check what command is given.
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        send_message->status = parse_create(query_command, &internal_status);
        dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, &internal_status);
        send_message->status = internal_status.error_type;
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        /* dbo = parse_load(query_command, &internal_status); */
        send_message->status = internal_status.error_type;
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
        send_message->status = OK_WAIT_FOR_RESPONSE;
    }
    // appropriately log errors
    if (internal_status.code == ERROR) {
        cs165_log(
            stdout,
            "Internal Error [%d]: %s\n",
            internal_status.error_type,
            internal_status.error_message
        );
    }

    // return null is nothing was returned
    if (dbo == NULL) {
        return dbo;
    }
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
