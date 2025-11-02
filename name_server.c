#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "common.h"

// --- Storage Server List ---
#define MAX_STORAGE_SERVERS 10
typedef struct {
    char ip[MAX_IP_LEN];
    int client_port;
} ss_info_t;

ss_info_t server_list[MAX_STORAGE_SERVERS];
int server_count = 0;
pthread_mutex_t server_list_mutex;

// --- NEW: File Metadata List ---
#define MAX_FILES 1000
typedef struct {
    char filename[MAX_FILENAME_LEN];
    int ss_index; // Index into the server_list
} file_info_t;

file_info_t file_list[MAX_FILES];
int file_count = 0;
pthread_mutex_t file_list_mutex; // Mutex for the file list


/**
 * @brief Handles a CMD_CREATE_FILE request from a client.
 * Checks for duplicates, selects an SS, and stores metadata.
 */
void handle_create_file(client_request_t req, nm_response_t* res) {
    // --- CRITICAL SECTION: Lock file list ---
    pthread_mutex_lock(&file_list_mutex);

    // 1. Check if file already exists
    for (int i = 0; i < file_count; i++) {
        if (strcmp(file_list[i].filename, req.filename) == 0) {
            res->status = STATUS_ERROR;
            strcpy(res->error_msg, "File already exists");
            pthread_mutex_unlock(&file_list_mutex); // Unlock before returning
            return;
        }
    }

    // 2. Check if file list is full
    if (file_count >= MAX_FILES) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Name Server file capacity reached");
        pthread_mutex_unlock(&file_list_mutex);
        return;
    }

    // --- CRITICAL SECTION: Lock server list to read it ---
    pthread_mutex_lock(&server_list_mutex);
    
    // 3. Check if any SS are available
    if (server_count == 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "No Storage Servers available");
        pthread_mutex_unlock(&server_list_mutex); // Unlock server list
        pthread_mutex_unlock(&file_list_mutex);  // Unlock file list
        return;
    }

    // 4. Select an SS (simple round-robin)
    int ss_idx = file_count % server_count;

    // 5. Store new file metadata
    strcpy(file_list[file_count].filename, req.filename);
    file_list[file_count].ss_index = ss_idx;
    file_count++;

    // 6. Prepare the "OK" response
    res->status = STATUS_OK;
    strcpy(res->ss_ip, server_list[ss_idx].ip);
    res->ss_port = server_list[ss_idx].client_port;

    printf("-> Metadata Added: File '%s' will be on SS#%d (%s:%d)\n", 
           req.filename, ss_idx, res->ss_ip, res->ss_port);

    pthread_mutex_unlock(&server_list_mutex); // Unlock server list
    pthread_mutex_unlock(&file_list_mutex);  // Unlock file list
}

/**
 * @brief Handles a single connection (either SS or Client) in its own thread.
 */
void* handle_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    message_type_t msg_type;
    ssize_t n = recv(conn_fd, &msg_type, sizeof(message_type_t), 0);

    if (n != sizeof(message_type_t)) {
        fprintf(stderr, "Error receiving message type.\n");
        close(conn_fd);
        return NULL;
    }

    if (msg_type == MSG_SS_REGISTER) {
        // --- Storage Server Registration ---
        ss_registration_t reg_data;
        n = recv(conn_fd, &reg_data, sizeof(ss_registration_t), 0);

        if (n == sizeof(ss_registration_t)) {
            pthread_mutex_lock(&server_list_mutex); 
            if (server_count < MAX_STORAGE_SERVERS) {
                strcpy(server_list[server_count].ip, reg_data.ss_ip);
                server_list[server_count].client_port = reg_data.client_port;
                server_count++;

                printf("-> Registered new Storage Server:\n");
                printf("   IP = %s, Client Port = %d, Total SS count: %d\n\n", 
                       reg_data.ss_ip, reg_data.client_port, server_count);
            } else {
                printf("Storage server list is full. Ignoring new server.\n");
            }
            pthread_mutex_unlock(&server_list_mutex);
        } else {
            fprintf(stderr, "Error receiving SS registration data.\n");
        }
        
    } else if (msg_type == MSG_CLIENT_NM_REQUEST) {
        // --- Client Request ---
        printf("-> Received a connection from a Client!\n");

        client_request_t req;
        nm_response_t res = {0}; // Zero-initialize the response

        // 1. Receive the client's command
        n = recv(conn_fd, &req, sizeof(client_request_t), 0);
        if (n != sizeof(client_request_t)) {
            fprintf(stderr, "Error receiving client request.\n");
            close(conn_fd);
            return NULL;
        }

        // 2. Route to the correct handler based on command
        if (req.command == CMD_CREATE_FILE) {
            printf("   Handling CMD_CREATE_FILE for '%s'\n", req.filename);
            handle_create_file(req, &res);
        } else {
            // Handle other commands (DELETE, READ, etc.) here
            res.status = STATUS_ERROR;
            strcpy(res.error_msg, "Unknown command");
        }
        
        // 3. Send the response back to the client
        if (send(conn_fd, &res, sizeof(nm_response_t), 0) < 0) {
            perror("send NM response failed");
        }
        
    } else {
        fprintf(stderr, "Unknown message type received: %d\n", msg_type);
    }

    close(conn_fd);
    return NULL;
}

int main() {
    int listen_fd, conn_fd;
    struct sockaddr_in serv_addr, client_addr;
    socklen_t client_len;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(NM_PORT);

    if (bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(listen_fd, 5) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }
    
    // --- Initialize both mutexes ---
    if (pthread_mutex_init(&server_list_mutex, NULL) != 0) {
        perror("server mutex init failed");
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_init(&file_list_mutex, NULL) != 0) {
        perror("file mutex init failed");
        exit(EXIT_FAILURE);
    }
    
    printf("Name Server listening on port %d...\n", NM_PORT);

    while (1) {
        client_len = sizeof(client_addr);
        conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            perror("accept failed");
            continue;
        }

        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        
        if (pthread_create(&tid, NULL, handle_connection, p_conn_fd) != 0) {
             perror("pthread_create failed");
             free(p_conn_fd);
        }
        pthread_detach(tid);
    } 
    
    close(listen_fd);
    pthread_mutex_destroy(&server_list_mutex);
    pthread_mutex_destroy(&file_list_mutex);
    return 0;
}