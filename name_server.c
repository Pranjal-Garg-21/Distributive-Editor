#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <stdbool.h>
#include "common.h"

// --- Storage Server List ---
#define MAX_STORAGE_SERVERS 10
typedef struct {
    char ip[MAX_IP_LEN];
    int client_port;
} ss_info_t;

// --- File Metadata List ---
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char owner[MAX_USERNAME_LEN]; 
    int ss_index; 
} file_info_t;

ss_info_t server_list[MAX_STORAGE_SERVERS];
int server_count = 0;
pthread_rwlock_t server_list_rwlock; // CHANGED

file_info_t file_list[MAX_FILES];
int file_count = 0;
pthread_rwlock_t file_list_rwlock; // CHANGED


void handle_create_file(nm_response_t* res, client_request_t req) {
    // --- WRITE LOCK on file_list ---
    pthread_rwlock_wrlock(&file_list_rwlock); // CHANGED
    
    for (int i = 0; i < file_count; i++) {
        if (strcmp(file_list[i].filename, req.filename) == 0) {
            res->status = STATUS_ERROR;
            strcpy(res->error_msg, "File already exists");
            pthread_rwlock_unlock(&file_list_rwlock); // CHANGED
            return;
        }
    }
    if (file_count >= MAX_FILES) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Name Server file capacity reached");
        pthread_rwlock_unlock(&file_list_rwlock); // CHANGED
        return;
    }
    
    // --- READ LOCK on server_list ---
    pthread_rwlock_rdlock(&server_list_rwlock); // CHANGED
    
    if (server_count == 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "No Storage Servers available");
        pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
        pthread_rwlock_unlock(&file_list_rwlock);  // CHANGED
        return;
    }
    
    // --- Continue with write logic for file_list ---
    int ss_idx = file_count % server_count;
    strcpy(file_list[file_count].filename, req.filename);
    strcpy(file_list[file_count].owner, req.username);
    file_list[file_count].ss_index = ss_idx;
    file_count++;
    
    // --- Continue with read logic for server_list ---
    res->status = STATUS_OK;
    strcpy(res->ss_ip, server_list[ss_idx].ip);
    res->ss_port = server_list[ss_idx].client_port;
    printf("-> Metadata Added: File '%s' (Owner: %s) on SS#%d\n", 
           req.filename, req.username, ss_idx);
           
    pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
    pthread_rwlock_unlock(&file_list_rwlock);  // CHANGED
}

void handle_view_files(int conn_fd, client_request_t req) {
    nm_response_t res_header = {0};
    nm_file_entry_t file_entry;
    int files_to_send_count = 0;
    
    // --- READ LOCKS on both lists ---
    pthread_rwlock_rdlock(&file_list_rwlock); // CHANGED
    pthread_rwlock_rdlock(&server_list_rwlock); // CHANGED
    
    if (req.view_all) {
        files_to_send_count = file_count;
    } else { 
        for (int i = 0; i < file_count; i++) {
            if (strcmp(file_list[i].owner, req.username) == 0) {
                files_to_send_count++;
            }
        }
    }
    res_header.status = STATUS_OK;
    res_header.file_count = files_to_send_count;
    
    if (send(conn_fd, &res_header, sizeof(nm_response_t), 0) < 0) {
        perror("send VIEW response header failed");
        pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
        pthread_rwlock_unlock(&file_list_rwlock); // CHANGED
        return;
    }
    
    printf("   Sending file list for user '%s' (%d files)...\n", req.username, files_to_send_count);
    for (int i = 0; i < file_count; i++) {
        if (!req.view_all && strcmp(file_list[i].owner, req.username) != 0) {
            continue;
        }
        int ss_idx = file_list[i].ss_index;
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
    
    pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
    pthread_rwlock_unlock(&file_list_rwlock); // CHANGED
    printf("   File list sent.\n");
}


void handle_read_file(nm_response_t* res, client_request_t req) {
    // --- READ LOCKS on both lists ---
    pthread_rwlock_rdlock(&file_list_rwlock); // CHANGED
    pthread_rwlock_rdlock(&server_list_rwlock); // CHANGED

    int file_idx = -1;
    // 1. Find the file
    for (int i = 0; i < file_count; i++) {
        if (strcmp(file_list[i].filename, req.filename) == 0) {
            file_idx = i;
            break;
        }
    }

    if (file_idx == -1) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File not found");
        pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
        pthread_rwlock_unlock(&file_list_rwlock); // CHANGED
        return;
    }

    // 2. Check permissions (assuming only owner can read)
    if (strcmp(file_list[file_idx].owner, req.username) != 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Permission denied");
        pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
        pthread_rwlock_unlock(&file_list_rwlock); // CHANGED
        return;
    }

    // 3. Get Storage Server location
    int ss_idx = file_list[file_idx].ss_index;
    if (ss_idx >= server_count) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Storage Server for this file is currently offline");
        pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
        pthread_rwlock_unlock(&file_list_rwlock); // CHANGED
        return;
    }

    // 4. Grant permission and send SS info
    res->status = STATUS_OK;
    strcpy(res->ss_ip, server_list[ss_idx].ip);
    res->ss_port = server_list[ss_idx].client_port;
    printf("-> Read Access Granted: File '%s' (User: %s) on SS#%d\n", 
           req.filename, req.username, ss_idx);

    pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
    pthread_rwlock_unlock(&file_list_rwlock);  // CHANGED
}


void* handle_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    message_type_t msg_type;
    ssize_t n = recv(conn_fd, &msg_type, sizeof(message_type_t), 0);
    // ... (error handling) ...

    if (msg_type == MSG_SS_REGISTER) {
        ss_registration_t reg_data;
        n = recv(conn_fd, &reg_data, sizeof(ss_registration_t), 0);
         if (n == sizeof(ss_registration_t)) {
            
            // --- WRITE LOCK on server_list ---
            pthread_rwlock_wrlock(&server_list_rwlock); // CHANGED
            
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
            pthread_rwlock_unlock(&server_list_rwlock); // CHANGED
        } else {
            fprintf(stderr, "Error receiving SS registration data.\n");
        }
        
    } else if (msg_type == MSG_CLIENT_NM_REQUEST) {
        // --- Client Request ---
        printf("-> Received a connection from a Client!\n");

        client_request_t req;
        nm_response_t res = {0};
        
        // --- ADD THIS LINE ---
        int response_sent_by_handler = 0; 
        // --- END ADD ---

        n = recv(conn_fd, &req, sizeof(client_request_t), 0);
        if (n != sizeof(client_request_t)) {
            fprintf(stderr, "Error receiving client request.\n");
            close(conn_fd);
            return NULL;
        }
        if (req.command == CMD_CREATE_FILE) {
            printf("   Handling CMD_CREATE_FILE for '%s' (User: %s)\n", req.filename, req.username);
            handle_create_file(&res, req); 
        
        } else if (req.command == CMD_VIEW_FILES) {
            printf("   Handling CMD_VIEW_FILES (User: %s, all: %d, long: %d)\n", 
                   req.username, req.view_all, req.view_long);
            handle_view_files(conn_fd, req); 
            response_sent_by_handler = 1;

        } else if (req.command == CMD_READ_FILE) {
            printf("   Handling CMD_READ_FILE for '%s' (User: %s)\n", req.filename, req.username);
            handle_read_file(&res, req);

        } 
        
        // ... (rest of the if-else block) ...
    }
    // ...
    close(conn_fd);
    return NULL;
}

// ------------------------------------------------------------------
// --- Main Function (Init/Destroy) ---
// ------------------------------------------------------------------
int main() {
    int listen_fd, conn_fd;
    struct sockaddr_in serv_addr, client_addr;
    socklen_t client_len;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    // ... (error handling) ...
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
    // ... (bind setup) ...
    if (bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        // ... (error handling) ...
    }
    if (listen(listen_fd, 5) < 0) {
        // ... (error handling) ...
    }
    
    // --- CHANGED to use rwlock_init ---
    if (pthread_rwlock_init(&server_list_rwlock, NULL) != 0) {
        perror("server rwlock init failed");
        exit(EXIT_FAILURE);
    }
    if (pthread_rwlock_init(&file_list_rwlock, NULL) != 0) {
        perror("file rwlock init failed");
        exit(EXIT_FAILURE);
    }
    printf("Name Server listening on port %d...\n", NM_PORT);

    while (1) {
        client_len = sizeof(client_addr);
        conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        // ... (accept error handling) ...
        
        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        // ... (pthread_create error handling) ...
        pthread_detach(tid);
    } 
    
    close(listen_fd);
    // --- CHANGED to use rwlock_destroy ---
    pthread_rwlock_destroy(&server_list_rwlock);
    pthread_rwlock_destroy(&file_list_rwlock);
    return 0;
}