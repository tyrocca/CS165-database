/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "assert.h"
// TODO: cleanup
#include "db_operations.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define MAX_CLIENTS 16
#define DEFAULT_CLIENT_INIT 8

/**
 * @brief Function that returns a bool as to whether a file exists
 *
 * @return Bool - if the database file exists
 */
bool db_exists() {
    struct stat st;
    if (stat("./database", &st) == -1) {
        mkdir("./database", 0777);
    }
    return access("./database/database.bin", F_OK) != -1 ? true : false;
}

/**
 * @brief This function will send a string to a client
 *
 * @param client_socket
 * @param msg
 * @param response
 */
void send_to_client(int client_socket, message* msg, char* response){
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    if (send(client_socket, msg, sizeof(message), 0) == -1) {
        log_err("Failed to send message.\n");
        exit(1);
    }
    // 4. Send response of request
    if (send(client_socket, response, msg->length, 0) == -1) {
        log_err("Failed to send message.\n");
        exit(1);
    }
}

/**
 * @brief This function takes in a column and returns its values as a string
 * that can later be parsed
 *
 * @param response
 * @param char_idx
 * @param data_type
 * @param data_ptr
 * @param row_idx
 *
 * @return
 */
int val_to_str(
    char* response,
    int char_idx,
    DataType data_type,
    void* data_ptr,
    int row_idx
) {
    switch (data_type) {
        case INT:
            return snprintf(
                &response[char_idx],
                DEFAULT_QUERY_BUFFER_SIZE - char_idx,
                "%d",
                ((int*) data_ptr)[row_idx]
            );
        case DOUBLE:
            return snprintf(
                &response[char_idx],
                DEFAULT_QUERY_BUFFER_SIZE - char_idx,
                "%lf",
                ((double*) data_ptr)[row_idx]
            );
        case LONG:
            return snprintf(
                &response[char_idx],
                DEFAULT_QUERY_BUFFER_SIZE - char_idx,
                "%ld",
                ((long*) data_ptr)[row_idx]
            );
        case INDEX:
            return snprintf(
                &response[char_idx],
                DEFAULT_QUERY_BUFFER_SIZE - char_idx,
                "%zu",
                ((size_t*) data_ptr)[row_idx]
            );
        default:
            return -1;
    }
}



/**
 * @brief This function takes in a bunch of print objects and
 *      sends the to the client correctly
 *
 * @param client_socket
 * @param msg
 * @param print_op
 * @param status
 */
void print_to_client(
    int client_socket,
    message* msg,
    PrintOperator* print_op,
    Status* status
) {

    // TODO: what if we have a temp column (like if this is an average)
    // make sure we have colummns
    assert(print_op->num_columns > 0);

    // this isn't great but this is how i will do this for now
    GeneralizedColumnType type = print_op->print_objects[0].column_type;

    // TODO make this handle data types
    size_t print_sz = 0;
    DataType data_type = INT;
    if (type == RESULT) {
        print_sz = print_op->print_objects[0].column_pointer.result->num_tuples;
        data_type = print_op->print_objects[0].column_pointer.result->data_type;
    } else {
        print_sz = *print_op->print_objects[0].column_pointer.column->size_ptr;
    }

    // if nothing to print, just return! - TODO is this okay?
    if (print_sz == 0) {
        char empty_string[] = "";
        status->msg_type = OK_DONE;
        msg->length = 1;
        msg->payload = empty_string;
        msg->status = status->msg_type;
        send_to_client(client_socket, msg, empty_string);
        free(print_op->print_objects);
        return;
    }

    // when formatting the string for dumping, do this
    char response[DEFAULT_QUERY_BUFFER_SIZE];
    size_t row_idx = 0;
    int char_idx = 0;

    // different printing functions as printing is different if we
    // are printing one or mulitple columns
    if (print_op->num_columns == 1) {
        void* data_ptr = (
                type == RESULT
                ? print_op->print_objects[0].column_pointer.result->payload
                : (void*) print_op->print_objects[0].column_pointer.column->data
        );
        // make a string to save in
        // make the data
        while (row_idx < print_sz) {
            int result = val_to_str(
                response,
                char_idx,
                data_type,
                data_ptr,
                row_idx
            );
            assert(result > 0);
            // make it adjust appropriately
            if ((char_idx + result) >= DEFAULT_QUERY_BUFFER_SIZE - 2) {
                // end the string
                response[char_idx] = '\0';
                char_idx = 0;
                msg->length = DEFAULT_QUERY_BUFFER_SIZE;
                msg->payload = response;
                send_to_client(client_socket, msg, response);
            } else {
                char_idx += result;
                response[char_idx++] = '\n';
                row_idx++;
            }
        }
    } else {
        size_t col_idx = 0;
        while (row_idx < print_sz) {
            // inner loop to print each column
            // TODO: validate all lengths are the same
            // TODO - are the data_types all the same?
            void* data_ptr = NULL;
            if (type == RESULT) {
                data_ptr = print_op->print_objects[col_idx].column_pointer.result->payload;
                data_type = print_op->print_objects[col_idx].column_pointer.result->data_type;
            } else {
                data_ptr = print_op->print_objects[col_idx].column_pointer.column->data;
            }
            int result = val_to_str(
                response,
                char_idx,
                data_type,
                data_ptr,
                row_idx
            );
            assert(result > 0);
            // make it adjust appropriately
            if ((char_idx + result) >= DEFAULT_QUERY_BUFFER_SIZE - 2) {
                // end the string
                response[char_idx] = '\0';
                char_idx = 0;
                msg->length = DEFAULT_QUERY_BUFFER_SIZE;
                msg->payload = response;
                send_to_client(client_socket, msg, response);
            } else if ((col_idx + 1) == print_op->num_columns) {
                // if we are at the end of the column add
                // a new line character
                char_idx += result;
                response[char_idx++] = '\n';
                col_idx = 0;
                row_idx++;
            } else {
                char_idx += result;
                response[char_idx++] = ',';
                col_idx++;
            }
        }

    }
    // update the status of the message
    if (status->msg_type == OK_WAIT_FOR_RESPONSE) {
        status->msg_type = OK_DONE;
    }
    if (char_idx > 0) {
        response[char_idx-1] = '\0';
        msg->length = char_idx;
        msg->payload = response;
        msg->status = status->msg_type;
        send_to_client(client_socket, msg, response);
    }
    free(print_op->print_objects);
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/

void handle_client(int client_socket, int* exit_server) {
    int done = 0;
    int length = 0;

    log_info("-- Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = malloc(sizeof(ClientContext));
    client_context->shared_scan = NULL;  // set to null
    client_context->chandle_table = malloc(sizeof(GeneralizedColumnHandle) * DEFAULT_CLIENT_INIT);
    client_context->chandle_slots = DEFAULT_CLIENT_INIT;
    client_context->chandles_in_use = 0;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            // This is the internal_status marker
            Status internal_status = {
                .code = OK,
                .msg_type = OK_WAIT_FOR_RESPONSE,
                .msg = ""
            };
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length, 0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            // TODO: keep this? &send_message,
            DbOperator* query = parse_command(
                recv_message.payload,
                &internal_status,
                client_socket,
                client_context
            );

            // 2. Handle request
            // if we have had a failure - don't continue
            // TODO: OK_WAIT_FOR_RESPONSE - should this be allowed?
            PrintOperator* print_op = NULL;
            switch(internal_status.msg_type) {
                case OK_DONE:
                    break;
                case SHUTDOWN_SERVER:
                    // TODO: See if db op should be used
                    done = 1;
                    *exit_server = 1;
                    break;
                case OK_WAIT_FOR_RESPONSE:
                    print_op = execute_DbOperator(query, &internal_status);
                    break;
                default:
                    log_info("-- Error inside parse, case not found\n");
                    free(query);
            }

            // when there is an error send the error to the client
            // if there wasn't an error, log the message on the
            // server
            if (internal_status.code == ERROR) {
                cs165_log(
                    stdout,
                    "-- Internal Error [%d]: %s\n",
                    internal_status.msg_type,
                    internal_status.msg
                );
            } else {
                cs165_log(stdout, "-- Result: %s\n", internal_status.msg);
                internal_status.msg = "";
            }

            // set the send_message status
            send_message.status = internal_status.msg_type;
            // if there is no result - or error - report to the user
            if (!print_op) {
                // process the internal_status of the message
                send_message.length = strlen(internal_status.msg);
                // TODO: enable printing of messages
                /* if (strncmp(send_message, "--", 2) != 0) { */
                /*     send_message.length += 3; */
                /*     send_message */
                /* } */
                char send_buffer[send_message.length + 1];
                strcpy(send_buffer, internal_status.msg);
                send_message.payload = send_buffer;
                send_to_client(client_socket, &send_message, send_buffer);
            } else {
                // when we have a result column we need to send it in an
                // orderly fashion
                print_to_client(client_socket, &send_message, print_op, &internal_status);
                free(query);
            }
        }
    } while (!done);

    // free all of the generalized_columns and their results
    for (int i = 0; i < client_context->chandles_in_use; i++) {
        GeneralizedColumnHandle* gcolh = &client_context->chandle_table[i];
        if(gcolh->generalized_column.column_pointer.result != NULL) {
            free(gcolh->generalized_column.column_pointer.result->payload);
            free(gcolh->generalized_column.column_pointer.result);
        }
    }
    // free the table of generalized columns
    free(client_context->chandle_table);
    // free the client context
    free(client_context);
    log_info("-- Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("-- Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void) {
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("-- Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    // check if there is a database to load
    if (db_exists()) {
        log_info("-- Database found... loading\n", client_socket);
        db_startup();
    }
    fprintf(stderr, "DONE\n");

    int exit_server = 0;
    while (exit_server != 1) {
        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }
        handle_client(client_socket, &exit_server);
    }
    // TODO: determine correct location for shutdown
    shutdown_server();
    return 0;
}
