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
// TODO: cleanup
#include "db_operations.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

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
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);
    // check if there is a database to load
    if (db_exists()) {
        log_info("Database found... loading\n", client_socket);
        db_startup();
    }

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = NULL;

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
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length, 0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // This is the internal_status marker
            Status internal_status = {
                .code = OK,
                .msg_type = OK_WAIT_FOR_RESPONSE,
                .msg = ""
            };

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
            Result* result = NULL;
            switch(internal_status.msg_type) {
                case OK_DONE:
                    break;
                case SHUTDOWN_SERVER:
                    // TODO: Determine if I want to use a DB operator for
                    // this
                    done = 1;
                    break;
                case OK_WAIT_FOR_RESPONSE:
                    result = execute_DbOperator(query, &internal_status);
                    break;
                default:
                    log_info("Error inside parse \n");
                    free(query);
            }
            if (internal_status.code == ERROR) {
                cs165_log(
                    stdout,
                    "Internal Error [%d]: %s\n",
                    internal_status.msg_type,
                    internal_status.msg
                );
            } else {
                cs165_log(stdout, "INFO: %s\n", internal_status.msg);
                internal_status.msg = "";
            }

            // set the send_message status
            send_message.status = internal_status.msg_type;
            // if there is no result - or there was an error - report to
            // the user
            if (!result || internal_status.code != OK) {
                send_message.length = strlen(internal_status.msg);
                char send_buffer[send_message.length + 1];
                strcpy(send_buffer, internal_status.msg);
                send_message.payload = send_buffer;
            } else {
                // process result
                send_message.length = 29;
                send_message.payload = "This is a result placeholder";
            }

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.\n");
                exit(1);
            }

            // 4. Send response of request
            if (send(client_socket, internal_status.msg, send_message.length, 0) == -1) {
                log_err("Failed to send message.\n");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
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

    log_info("Attempting to setup server...\n");

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

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }
    handle_client(client_socket);
    // TODO: determine correct location for shutdown
    shutdown_server();
    return 0;
}
