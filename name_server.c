#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <stdbool.h> // NEW
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

// --- File Metadata List ---
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char owner[MAX_USERNAME_LEN]; // NEW: Store the owner
    int ss_index; 
} file_info_t;

file_info_t file_list[MAX_FILES];
int file_count = 0;
pthread_mutex_t file_list_mutex;


/**
 * @brief Handles a CMD_CREATE_FILE request from a client.
 */
void handle_create_file(nm_response_t* res, client_request_t req) {
    pthread_mutex_lock(&file_list_mutex);

    // 1. Check if file already exists
    for (int i = 0; i < file_count; i++) {
        if (strcmp(file_list[i].filename, req.filename) == 0) {
            res->status = STATUS_ERROR;
            strcpy(res->error_msg, "File already exists");
            pthread_mutex_unlock(&file_list_mutex); 
            return;
        }
    }

    // 2. Check capacity
    if (file_count >= MAX_FILES) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Name Server file capacity reached");
        pthread_mutex_unlock(&file_list_mutex);
        return;
    }

    pthread_mutex_lock(&server_list_mutex);
    
    // 3. Check for available SS
    if (server_count == 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "No Storage Servers available");
        pthread_mutex_unlock(&server_list_mutex); 
        pthread_mutex_unlock(&file_list_mutex);  
        return;
    }

    // 4. Select an SS
    int ss_idx = file_count % server_count;

    // 5. Store new file metadata
    strcpy(file_list[file_count].filename, req.filename);
    strcpy(file_list[file_count].owner, req.username); // NEW: Store owner
    file_list[file_count].ss_index = ss_idx;
    file_count++;

    // 6. Prepare the "OK" response
    res->status = STATUS_OK;
    strcpy(res->ss_ip, server_list[ss_idx].ip);
    res->ss_port = server_list[ss_idx].client_port;

    printf("-> Metadata Added: File '%s' (Owner: %s) on SS#%d\n", 
           req.filename, req.username, ss_idx);

    pthread_mutex_unlock(&server_list_mutex); 
    pthread_mutex_unlock(&file_list_mutex);  
}

/**
 * @brief Handles a CMD_VIEW_FILES request from a client.
 */
void handle_view_files(int conn_fd, client_request_t req) {
    nm_response_t res_header = {0};
    nm_file_entry_t file_entry;
    
    int files_to_send_count = 0;
    
    // --- CRITICAL SECTION: Lock lists ---
    pthread_mutex_lock(&file_list_mutex);
    pthread_mutex_lock(&server_list_mutex);
    
    // 1. First, count how many files we need to send
    if (req.view_all) { // -a flag
        files_to_send_count = file_count;
    } else { // User-only
        for (int i = 0; i < file_count; i++) {
            if (strcmp(file_list[i].owner, req.username) == 0) {
                files_to_send_count++;
            }
        }
    }
    
    res_header.status = STATUS_OK;
    res_header.file_count = files_to_send_count;

    // 2. Send the response header
    if (send(conn_fd, &res_header, sizeof(nm_response_t), 0) < 0) {
        perror("send VIEW response header failed");
        pthread_mutex_unlock(&server_list_mutex);
        pthread_mutex_unlock(&file_list_mutex);
        return;
    }

    // 3. Send each file entry one by one
    printf("   Sending file list for user '%s' (%d files)...\n", req.username, files_to_send_count);
    for (int i = 0; i < file_count; i++) {
        
        // Skip this file if it's not owned by user (and -a is not set)
        if (!req.view_all && strcmp(file_list[i].owner, req.username) != 0) {
            continue;
        }

        // This is a file we need to send. Populate the entry.
        int ss_idx = file_list[i].ss_index;
        
        // Check for invalid SS index (if SS disconnected, etc.)
        if (ss_idx >= server_count) {
             fprintf(stderr, "   Skipping file '%s' with invalid ss_index %d\n", file_list[i].filename, ss_idx);
             continue;
        }

        strcpy(file_entry.filename, file_list[i].filename);
        strcpy(file_entry.owner, file_list[i].owner);
        strcpy(file_entry.ss_ip, server_list[ss_idx].ip);
        file_entry.ss_port = server_list[ss_idx].client_port;
        
        if (send(conn_fd, &file_entry, sizeof(nm_file_entry_t), 0) < 0) {
            perror("send file entry failed");
            break; 
        }
    }
    
    pthread_mutex_unlock(&server_list_mutex);
    pthread_mutex_unlock(&file_list_mutex);
    printf("   File list sent.\n");
}


/**
 * @brief Handles a single connection (either SS or Client).
 */
void* handle_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    message_type_t msg_type;
    ssize_t n = recv(conn_fd, &msg_type, sizeof(message_type_t), 0);
    // ... (error check n) ...

    if (msg_type == MSG_SS_REGISTER) {
        // --- SS Registration (Unchanged) ---
        ss_registration_t reg_data;
        n = recv(conn_fd, &reg_data, sizeof(ss_registration_t), 0);
        // ... (rest of SS registration logic) ...
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
        nm_response_t res = {0};
        int response_sent_by_handler = 0; 

        n = recv(conn_fd, &req, sizeof(client_request_t), 0);
        // ... (error check n) ...

        if (req.command == CMD_CREATE_FILE) {
            printf("   Handling CMD_CREATE_FILE for '%s' (User: %s)\n", req.filename, req.username);
            handle_create_file(&res, req); 
        
        } else if (req.command == CMD_VIEW_FILES) {
            printf("   Handling CMD_VIEW_FILES (User: %s, all: %d, long: %d)\n", 
                   req.username, req.view_all, req.view_long);
            handle_view_files(conn_fd, req); 
            response_sent_by_handler = 1;

        } else if (req.command == CMD_DELETE_FILE) {
            // TODO: Implement handle_delete_file
            res.status = STATUS_ERROR;
            strcpy(res.error_msg, "DELETE command not yet implemented");

        } else {
            res.status = STATUS_ERROR;
            strcpy(res.error_msg, "Unknown command");
        }
        
        if (!response_sent_by_handler) {
            if (send(conn_fd, &res, sizeof(nm_response_t), 0) < 0) {
                perror("send NM response failed");
            }
        }
        
    } else {
        fprintf(stderr, "Unknown message type received: %d\n", msg_type);
    }

    close(conn_fd);
    return NULL;
}

// ------------------------------------------------------------------
// --- Main Function (Unchanged) ---
// ------------------------------------------------------------------
int main() {
    // ... (All your main() code is unchanged) ...
    // ... (socket, setsockopt, bind, listen) ...
    // ... (mutex inits) ...
    // ... (while(1) accept loop with pthread_create) ...

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