#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include "common.h"

// --- Configuration for this SS ---
#define MY_IP "127.0.0.1"
#define MY_CLIENT_PORT 9090 // The port this SS will open for clients

// --- Name Server Configuration ---
#define NM_IP "127.0.0.1"
// NM_PORT is already defined in common.h

/**
 * @brief Handles a single client connection (for read/write/etc.)
 */
void* handle_client_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    printf("   [SS-Thread]: Got a connection! (conn_fd: %d)\n", conn_fd);

    client_request_t req;
    ss_response_t res = {0}; // Zero-initialize response

    // 1. Receive the request from the client
    ssize_t n = recv(conn_fd, &req, sizeof(client_request_t), 0);
    if (n != sizeof(client_request_t)) {
        fprintf(stderr, "   [SS-Thread]: Error receiving client request.\n");
        close(conn_fd);
        return NULL;
    }

    // 2. Process the command
    if (req.command == CMD_CREATE_FILE) {
        printf("   [SS-Thread]: Handling CMD_CREATE_FILE for '%s'\n", req.filename);
        
        // --- This is the core file operation ---
        FILE* fp = fopen(req.filename, "w"); // "w" creates an empty file
        
        if (fp == NULL) {
            // Failed to create file
            perror("   [SS-Thread]: fopen failed");
            res.status = STATUS_ERROR;
            strcpy(res.error_msg, "File creation failed on Storage Server");
        } else {
            // Success
            fclose(fp);
            res.status = STATUS_OK;
            printf("   [SS-Thread]: Successfully created file '%s'\n", req.filename);
        }
    } else {
        // Handle other commands (READ, WRITE) here
        res.status = STATUS_ERROR;
        strcpy(res.error_msg, "Unknown command received by SS");
    }

    // 3. Send response back to client
    if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
        perror("   [SS-Thread]: send response failed");
    }

    close(conn_fd);
    printf("   [SS-Thread]: Closing client connection.\n");
    return NULL;
}


// ------------------------------------------------------------------
// --- Main Function (No changes needed from your version) ---
// ------------------------------------------------------------------
int main() {
    int sock_fd;
    struct sockaddr_in nm_addr;
    ss_registration_t reg_data;

    // ... (PART 1: Register with Name Server - all this code is unchanged) ...
    // 1. Create socket (for NM)
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // 2. Set up the Name Server's address
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0) {
        perror("invalid Name Server IP address");
        exit(EXIT_FAILURE);
    }

    // 3. Connect to the Name Server
    printf("Attempting to connect to Name Server at %s:%d...\n", NM_IP, NM_PORT);
    if (connect(sock_fd, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
        perror("connection to Name Server failed");
        exit(EXIT_FAILURE);
    }
    printf("Connected to Name Server!\n");

    // 4. Prepare and send registration data
    message_type_t msg_type = MSG_SS_REGISTER;
    if (send(sock_fd, &msg_type, sizeof(message_type_t), 0) < 0) {
        perror("send message type failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    strcpy(reg_data.ss_ip, MY_IP);
    reg_data.client_port = MY_CLIENT_PORT;
    if (send(sock_fd, &reg_data, sizeof(reg_data), 0) < 0) {
        perror("send registration data failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    printf("Successfully registered with Name Server.\n");

    // 5. Close the connection to the Name Server
    close(sock_fd);

    // ... (PART 2: Become a Server for Clients - all this code is unchanged) ...
    int listen_fd, conn_fd;
    struct sockaddr_in ss_serv_addr, client_addr;
    socklen_t client_len;

    // 1. Create socket (for Clients)
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("SS server socket creation failed");
        exit(EXIT_FAILURE);
    }
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));

    // 2. Bind socket
    memset(&ss_serv_addr, 0, sizeof(ss_serv_addr));
    ss_serv_addr.sin_family = AF_INET;
    ss_serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    ss_serv_addr.sin_port = htons(MY_CLIENT_PORT);
    if (bind(listen_fd, (struct sockaddr*)&ss_serv_addr, sizeof(ss_serv_addr)) < 0) {
        perror("SS server bind failed");
        exit(EXIT_FAILURE);
    }

    // 3. Listen
    if (listen(listen_fd, 5) < 0) {
        perror("SS server listen failed");
        exit(EXIT_FAILURE);
    }
    printf("\n[SS-Main]: Storage Server now listening for clients on port %d...\n", MY_CLIENT_PORT);

    // 4. Main accept loop
    while (1) {
        client_len = sizeof(client_addr);
        conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            perror("SS server accept failed");
            continue; 
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("[SS-Main]: Accepted new client connection from %s\n", client_ip);

        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        
        if (pthread_create(&tid, NULL, handle_client_connection, p_conn_fd) != 0) {
            perror("pthread_create failed");
            free(p_conn_fd);
        }
        pthread_detach(tid);
    }

    close(listen_fd);
    return 0;
}