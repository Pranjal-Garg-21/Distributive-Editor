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
#include "uthash.h" 


// --- (All structs, globals, and helpers up to handle_delete_file are unchanged) ---
#define MAX_STORAGE_SERVERS 10
typedef struct { char ip[MAX_IP_LEN]; int client_port; } ss_info_t;
typedef enum { NM_ACCESS_READ, NM_ACCESS_WRITE } nm_access_level_t;
typedef struct { char username[MAX_USERNAME_LEN]; nm_access_level_t level; UT_hash_handle hh; } access_entry_t;
typedef struct { char filename[MAX_FILENAME_LEN]; char owner[MAX_USERNAME_LEN]; int ss_index; access_entry_t* access_list; UT_hash_handle hh; } file_info_t;
ss_info_t server_list[MAX_STORAGE_SERVERS];
int server_count = 0;
pthread_rwlock_t server_list_rwlock; 
file_info_t* g_file_map = NULL; 
pthread_rwlock_t file_map_rwlock;

/**
 * @brief Connects to a server at the given IP and port.
 * (Copied from client.c, as NM needs to act as a client to SS)
 */
int connect_to_server(char* ip, int port) {
    int sock_fd;
    struct sockaddr_in serv_addr;
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket creation failed");
        return -1;
    }
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        perror("invalid server IP address");
        close(sock_fd);
        return -1;
    }
    if (connect(sock_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock_fd);
        return -1;
    }
    return sock_fd;
}

bool check_permission(file_info_t* file, const char* username, nm_access_level_t required_level) {
    if (file == NULL) return false;
    access_entry_t* found_access;
    HASH_FIND_STR(file->access_list, username, found_access);
    if (found_access == NULL) { return false; }
    if (required_level == NM_ACCESS_READ) { return (found_access->level == NM_ACCESS_READ || found_access->level == NM_ACCESS_WRITE); }
    if (required_level == NM_ACCESS_WRITE) { return (found_access->level == NM_ACCESS_WRITE); }
    return false;
}

/**
 * @brief Handles the EXEC command.
 * 1. Checks client's READ permission.
 * 2. Connects to the SS and fetches the file content.
 * 3. Saves content to a /tmp file.
 * 4. Executes the temp file with "sh" via popen().
 * 5. Pipes the stdout/stderr of the command back to the client.
 */
void handle_exec_file(int client_conn_fd, client_request_t req) {
    printf("   Handling CMD_EXEC for '%s' by user '%s'\n", req.filename, req.username);

    char ss_ip[MAX_IP_LEN];
    int ss_port;
    nm_response_t res_header = {0}; // This is the header we send to the client

    // --- Step 1: Authorize and find file location ---
    pthread_rwlock_rdlock(&file_map_rwlock);
    pthread_rwlock_rdlock(&server_list_rwlock);

    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);

    if (!found_file) {
        res_header.status = STATUS_ERROR;
        strcpy(res_header.error_msg, "File not found");
    } else if (!check_permission(found_file, req.username, NM_ACCESS_READ)) {
        res_header.status = STATUS_ERROR;
        strcpy(res_header.error_msg, "Permission denied (Read access required)");
    } else if (found_file->ss_index >= server_count) {
        res_header.status = STATUS_ERROR;
        strcpy(res_header.error_msg, "Storage Server for this file is offline");
    } else {
        // All good. Copy location and set status OK.
        res_header.status = STATUS_OK;
        strcpy(ss_ip, server_list[found_file->ss_index].ip);
        ss_port = server_list[found_file->ss_index].client_port;
    }

    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);

    // If any check failed, send error response to client and we're done.
    if (res_header.status == STATUS_ERROR) {
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0);
        return; // NOTE: We do NOT close client_conn_fd here, main handler does.
    }

    // --- Step 2: NM acts as client to fetch file from SS ---
    int ss_sock_fd = connect_to_server(ss_ip, ss_port);
    if (ss_sock_fd < 0) {
        fprintf(stderr, "   [EXEC] NM failed to connect to SS at %s:%d\n", ss_ip, ss_port);
        res_header.status = STATUS_ERROR;
        strcpy(res_header.error_msg, "NM Error: Could not connect to Storage Server");
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0);
        return;
    }

    // Re-use the request struct, but set command to READ
    req.command = CMD_READ_FILE;
    send(ss_sock_fd, &req, sizeof(client_request_t), 0);

    ss_response_t ss_res; // SS's header
    recv(ss_sock_fd, &ss_res, sizeof(ss_response_t), 0);

    if (ss_res.status == STATUS_ERROR) {
        fprintf(stderr, "   [EXEC] SS returned error: %s\n", ss_res.error_msg);
        res_header.status = STATUS_ERROR;
        strncpy(res_header.error_msg, ss_res.error_msg, MAX_ERROR_MSG_LEN -1);
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0);
        close(ss_sock_fd);
        return;
    }

    // --- Step 3: Save file content to a temp file on NM ---
    char temp_filename[] = "/tmp/nm_exec_XXXXXX";
    int temp_fd = mkstemp(temp_filename);
    if (temp_fd < 0) {
        perror("   [EXEC] mkstemp failed");
        res_header.status = STATUS_ERROR;
        strcpy(res_header.error_msg, "NM Error: Could not create temp exec file");
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0);
        close(ss_sock_fd);
        return;
    }

    ss_file_data_chunk_t chunk;
    while (recv(ss_sock_fd, &chunk, sizeof(ss_file_data_chunk_t), 0) == sizeof(ss_file_data_chunk_t) && chunk.data_size > 0) {
        write(temp_fd, chunk.data, chunk.data_size);
    }
    close(temp_fd);
    close(ss_sock_fd);

    // --- Step 4: Send "OK" header to client *before* executing ---
    // This tells the client to switch to "raw output" mode.
    send(client_conn_fd, &res_header, sizeof(nm_response_t), 0);

    // --- Step 5: Execute and pipe output back to client ---
    char exec_command[MAX_FILENAME_LEN + 10];
    // Execute as shell commands [cite: 124]
    sprintf(exec_command, "sh %s 2>&1", temp_filename); // "2>&1" merges stderr into stdout

    printf("   [EXEC] Running: %s\n", exec_command);
    FILE* pipe_fp = popen(exec_command, "r");
    if (pipe_fp == NULL) {
        perror("   [EXEC] popen failed");
        // Client is already waiting for output, just send an error string
        send(client_conn_fd, "NM Error: popen() failed\n", 25, 0);
    } else {
        char pipe_buffer[4096];
        // Read line-by-line from the command's output
        while (fgets(pipe_buffer, sizeof(pipe_buffer), pipe_fp) != NULL) {
            // Send that line directly to the client
            if (send(client_conn_fd, pipe_buffer, strlen(pipe_buffer), 0) < 0) {
                perror("   [EXEC] send to client failed, pipe broken");
                break; // Stop if client disconnects
            }
        }
        pclose(pipe_fp);
    }

    // --- Step 6: Cleanup ---
    unlink(temp_filename); // Delete the temporary file
    printf("   [EXEC] Finished and cleaned up %s\n", temp_filename);
}

void handle_create_file(nm_response_t* res, client_request_t req) {
    pthread_rwlock_wrlock(&file_map_rwlock); 
    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);
    if (found_file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File already exists");
        pthread_rwlock_unlock(&file_map_rwlock); 
        return;
    }
    pthread_rwlock_rdlock(&server_list_rwlock); 
    if (server_count == 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "No Storage Servers available");
        pthread_rwlock_unlock(&server_list_rwlock); 
        pthread_rwlock_unlock(&file_map_rwlock);  
        return;
    }
    file_info_t* new_file = (file_info_t*)malloc(sizeof(file_info_t));
    if (!new_file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Name Server out of memory");
        pthread_rwlock_unlock(&server_list_rwlock); 
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    strcpy(new_file->filename, req.filename);
    strcpy(new_file->owner, req.username);
    new_file->ss_index = HASH_COUNT(g_file_map) % server_count;
    new_file->access_list = NULL; 
    access_entry_t* owner_access = (access_entry_t*)malloc(sizeof(access_entry_t));
    if (!owner_access) {
         res->status = STATUS_ERROR;
         strcpy(res->error_msg, "Name Server out of memory");
         free(new_file);
         pthread_rwlock_unlock(&server_list_rwlock); 
         pthread_rwlock_unlock(&file_map_rwlock);
         return;
    }
    strcpy(owner_access->username, req.username);
    owner_access->level = NM_ACCESS_WRITE; 
    HASH_ADD_STR(new_file->access_list, username, owner_access);
    HASH_ADD_STR(g_file_map, filename, new_file);
    res->status = STATUS_OK;
    strcpy(res->ss_ip, server_list[new_file->ss_index].ip);
    res->ss_port = server_list[new_file->ss_index].client_port;
    printf("-> Metadata Added: File '%s' (Owner: %s) on SS#%d\n", req.filename, req.username, new_file->ss_index);
    pthread_rwlock_unlock(&server_list_rwlock); 
    pthread_rwlock_unlock(&file_map_rwlock);
}
void handle_view_files(int conn_fd, client_request_t req) {
    nm_response_t res_header = {0};
    nm_file_entry_t file_entry;
    int files_to_send_count = 0;
    pthread_rwlock_rdlock(&file_map_rwlock); 
    pthread_rwlock_rdlock(&server_list_rwlock);
    file_info_t *current_file, *tmp;
    HASH_ITER(hh, g_file_map, current_file, tmp) {
        if (req.view_all || check_permission(current_file, req.username, NM_ACCESS_READ)) {
            files_to_send_count++;
        }
    }
    res_header.status = STATUS_OK;
    res_header.file_count = files_to_send_count;
    if (send(conn_fd, &res_header, sizeof(nm_response_t), 0) < 0) {
        perror("send VIEW response header failed");
        pthread_rwlock_unlock(&server_list_rwlock);
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    printf("   Sending file list for user '%s' (%d files)...\n", req.username, files_to_send_count);
    HASH_ITER(hh, g_file_map, current_file, tmp) {
        if (!req.view_all && !check_permission(current_file, req.username, NM_ACCESS_READ)) {
            continue;
        }
        int ss_idx = current_file->ss_index;
        if (ss_idx >= server_count) {
             fprintf(stderr, "   Skipping file '%s' with invalid ss_index %d\n", current_file->filename, ss_idx);
             continue;
        }
        strcpy(file_entry.filename, current_file->filename);
        strcpy(file_entry.owner, current_file->owner);
        strcpy(file_entry.ss_ip, server_list[ss_idx].ip);
        file_entry.ss_port = server_list[ss_idx].client_port;
        if (send(conn_fd, &file_entry, sizeof(nm_file_entry_t), 0) < 0) {
            perror("send file entry failed");
            break; 
        }
    }
    pthread_rwlock_unlock(&server_list_rwlock); 
    pthread_rwlock_unlock(&file_map_rwlock); 
    printf("   File list sent.\n");
}
void handle_read_file(nm_response_t* res, client_request_t req) {
    pthread_rwlock_rdlock(&file_map_rwlock); 
    pthread_rwlock_rdlock(&server_list_rwlock); 
    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);
    if (!found_file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File not found");
        pthread_rwlock_unlock(&server_list_rwlock);
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    if (!check_permission(found_file, req.username, NM_ACCESS_READ)) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Permission denied");
        pthread_rwlock_unlock(&server_list_rwlock); 
        pthread_rwlock_unlock(&file_map_rwlock); 
        return;
    }
    int ss_idx = found_file->ss_index;
    if (ss_idx >= server_count) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Storage Server for this file is currently offline");
        pthread_rwlock_unlock(&server_list_rwlock); 
        pthread_rwlock_unlock(&file_map_rwlock); 
        return;
    }
    res->status = STATUS_OK;
    strcpy(res->ss_ip, server_list[ss_idx].ip);
    res->ss_port = server_list[ss_idx].client_port;
    printf("-> Read Access Granted: File '%s' (User: %s) on SS#%d\n", req.filename, req.username, ss_idx);
    pthread_rwlock_unlock(&server_list_rwlock); 
    pthread_rwlock_unlock(&file_map_rwlock);  
}
void handle_write_file(nm_response_t* res, client_request_t req) {
    pthread_rwlock_rdlock(&file_map_rwlock);
    pthread_rwlock_rdlock(&server_list_rwlock);
    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);
    if (!found_file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File not found");
        pthread_rwlock_unlock(&server_list_rwlock); 
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    if (!check_permission(found_file, req.username, NM_ACCESS_WRITE)) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Permission denied (Write access required)");
        pthread_rwlock_unlock(&server_list_rwlock); 
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    int ss_idx = found_file->ss_index;
    if (ss_idx >= server_count) {
         res->status = STATUS_ERROR;
         strcpy(res->error_msg, "Storage Server for this file is currently offline");
         pthread_rwlock_unlock(&server_list_rwlock); 
         pthread_rwlock_unlock(&file_map_rwlock);
         return;
    }
    res->status = STATUS_OK;
    strcpy(res->ss_ip, server_list[ss_idx].ip);
    res->ss_port = server_list[ss_idx].client_port;
    printf("-> Write Access Granted: File '%s' (User: %s) on SS#%d\n", req.filename, req.username, ss_idx);
    pthread_rwlock_unlock(&server_list_rwlock); 
    pthread_rwlock_unlock(&file_map_rwlock);  
}
void handle_add_access(nm_response_t* res, client_request_t req) {
    pthread_rwlock_wrlock(&file_map_rwlock); 
    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);
    if (!found_file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File not found");
    } else if (strcmp(found_file->owner, req.username) != 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Permission denied (Only the owner can add access)");
    } else {
        access_entry_t* target_access;
        HASH_FIND_STR(found_file->access_list, req.target_username, target_access);
        nm_access_level_t new_level = (req.access_level == ACCESS_WRITE) ? NM_ACCESS_WRITE : NM_ACCESS_READ;
        if (target_access) {
            target_access->level = new_level;
            res->status = STATUS_OK;
            printf("   Access Updated: User '%s' set to %s for file '%s'\n", req.target_username, (new_level == NM_ACCESS_WRITE) ? "WRITE" : "READ", req.filename);
        } else {
            access_entry_t* new_access = (access_entry_t*)malloc(sizeof(access_entry_t));
            if (!new_access) {
                res->status = STATUS_ERROR;
                strcpy(res->error_msg, "Name Server out of memory");
            } else {
                strcpy(new_access->username, req.target_username);
                new_access->level = new_level;
                HASH_ADD_STR(found_file->access_list, username, new_access);
                res->status = STATUS_OK;
                printf("   Access Added: User '%s' given %s for file '%s'\n", req.target_username, (new_level == NM_ACCESS_WRITE) ? "WRITE" : "READ", req.filename);
            }
        }
    }
    pthread_rwlock_unlock(&file_map_rwlock);
}
void handle_rem_access(nm_response_t* res, client_request_t req) {
    pthread_rwlock_wrlock(&file_map_rwlock); 
    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);
    if (!found_file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File not found");
    } else if (strcmp(found_file->owner, req.username) != 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Permission denied (Only the owner can remove access)");
    } else if (strcmp(found_file->owner, req.target_username) == 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Cannot remove the owner's access");
    } else {
        access_entry_t* target_access;
        HASH_FIND_STR(found_file->access_list, req.target_username, target_access);
        if (target_access) {
            HASH_DEL(found_file->access_list, target_access);
            free(target_access);
            res->status = STATUS_OK;
            printf("   Access Removed: User '%s' from file '%s'\n", req.target_username, req.filename);
        } else {
            res->status = STATUS_ERROR;
            strcpy(res->error_msg, "User does not have access to this file");
        }
    }
    pthread_rwlock_unlock(&file_map_rwlock);
}
void handle_delete_file(nm_response_t* res, client_request_t req) {
    pthread_rwlock_wrlock(&file_map_rwlock);   
    pthread_rwlock_rdlock(&server_list_rwlock); 
    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);
    if (!found_file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File not found");
        pthread_rwlock_unlock(&server_list_rwlock);
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    if (strcmp(found_file->owner, req.username) != 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Permission denied (Only the owner can delete)");
        pthread_rwlock_unlock(&server_list_rwlock);
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    int ss_idx = found_file->ss_index;
    if (ss_idx >= server_count) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Storage Server for this file is offline, cannot delete");
        pthread_rwlock_unlock(&server_list_rwlock);
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    res->status = STATUS_OK;
    strcpy(res->ss_ip, server_list[ss_idx].ip);
    res->ss_port = server_list[ss_idx].client_port;
    access_entry_t *current_access, *tmp_access;
    HASH_ITER(hh, found_file->access_list, current_access, tmp_access) {
        HASH_DEL(found_file->access_list, current_access);
        free(current_access);
    }
    HASH_DEL(g_file_map, found_file);
    free(found_file);
    printf("-> Metadata Deleted: File '%s' (User: %s). Notifying SS.\n", req.filename, req.username);
    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);
}

void handle_get_info(int conn_fd, client_request_t req) {
    nm_info_response_t res_header = {0};
    
    pthread_rwlock_rdlock(&file_map_rwlock); 
    pthread_rwlock_rdlock(&server_list_rwlock);

    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);

    if (!found_file) {
        res_header.status = STATUS_ERROR;
        strcpy(res_header.error_msg, "File not found");
    } else if (!check_permission(found_file, req.username, NM_ACCESS_READ)) {
        res_header.status = STATUS_ERROR;
        strcpy(res_header.error_msg, "Permission denied");
    } else {
        int ss_idx = found_file->ss_index;
        if (ss_idx >= server_count) {
            res_header.status = STATUS_ERROR;
            strcpy(res_header.error_msg, "Storage Server for this file is currently offline");
        } else {
            // All checks passed, send header
            res_header.status = STATUS_OK;
            strcpy(res_header.owner, found_file->owner);
            strcpy(res_header.ss_ip, server_list[ss_idx].ip);
            res_header.ss_port = server_list[ss_idx].client_port;
            res_header.acl_count = HASH_COUNT(found_file->access_list);
        }
    }
    
    // Unlock server list, we're done with it
    pthread_rwlock_unlock(&server_list_rwlock);

    // Send the header (either OK or Error)
    if (send(conn_fd, &res_header, sizeof(nm_info_response_t), 0) < 0) {
        perror("send INFO response header failed");
        pthread_rwlock_unlock(&file_map_rwlock); // Unlock file map
        return;
    }

    // If header was not OK, we are done.
    if (res_header.status == STATUS_ERROR) {
        pthread_rwlock_unlock(&file_map_rwlock); // Unlock file map
        return;
    }

    // --- Header was OK, now send the ACL ---
    printf("   Sending ACL for '%s' (%d entries)...\n", req.filename, res_header.acl_count);

    nm_acl_entry_t acl_entry;
    access_entry_t *current_access, *tmp_access;
    HASH_ITER(hh, found_file->access_list, current_access, tmp_access) {
        strcpy(acl_entry.username, current_access->username);
        acl_entry.level = (current_access->level == NM_ACCESS_WRITE) ? ACCESS_WRITE : ACCESS_READ;
        
        if (send(conn_fd, &acl_entry, sizeof(nm_acl_entry_t), 0) < 0) {
            perror("send ACL entry failed");
            break; 
        }
    }
    
    pthread_rwlock_unlock(&file_map_rwlock); 
    printf("   ACL sent.\n");
}


// --- ADD THIS NEW FUNCTION ---

// Helper struct for finding unique users
typedef struct {
    char username[MAX_USERNAME_LEN]; 
    UT_hash_handle hh;
} temp_user_set_t;

void handle_list_users(int conn_fd, client_request_t req) {
    printf("   Handling CMD_LIST_USERS...\n");
    temp_user_set_t* user_set = NULL;
    nm_response_t res_header = {0};

    // We need to find all unique usernames.
    // Usernames are stored as:
    // 1. Owners (in file_info_t)
    // 2. ACL entries (in access_entry_t)
    // We use a temporary hash set to ensure uniqueness.
    
    pthread_rwlock_rdlock(&file_map_rwlock);
    
    file_info_t *current_file, *tmp_file;
    HASH_ITER(hh, g_file_map, current_file, tmp_file) {
        
        // --- 1. Add owner ---
        temp_user_set_t* found;
        HASH_FIND_STR(user_set, current_file->owner, found);
        if (found == NULL) {
            temp_user_set_t* new_user = (temp_user_set_t*)malloc(sizeof(temp_user_set_t));
            if (new_user) {
                strcpy(new_user->username, current_file->owner);
                HASH_ADD_STR(user_set, username, new_user);
            } else {
                perror("malloc failed for temp_user_set");
            }
        }

        // --- 2. Add all users from ACL ---
        access_entry_t *current_access, *tmp_access;
        HASH_ITER(hh, current_file->access_list, current_access, tmp_access) {
            HASH_FIND_STR(user_set, current_access->username, found);
            if (found == NULL) {
                temp_user_set_t* new_user = (temp_user_set_t*)malloc(sizeof(temp_user_set_t));
                if (new_user) {
                    strcpy(new_user->username, current_access->username);
                    HASH_ADD_STR(user_set, username, new_user);
                } else {
                     perror("malloc failed for temp_user_set (acl)");
                }
            }
        }
    }
    
    int user_count = HASH_COUNT(user_set);
    pthread_rwlock_unlock(&file_map_rwlock);

    // --- Send response back to client ---
    res_header.status = STATUS_OK;
    res_header.file_count = user_count; // Re-using file_count field

    if (send(conn_fd, &res_header, sizeof(nm_response_t), 0) < 0) {
        perror("send LIST response header failed");
    } else {
        printf("   Sending user list (%d users)...\n", user_count);
        nm_user_entry_t user_entry;
        temp_user_set_t *current_user, *tmp_user;
        
        // Iterate and send each user
        HASH_ITER(hh, user_set, current_user, tmp_user) {
            strcpy(user_entry.username, current_user->username);
            if (send(conn_fd, &user_entry, sizeof(nm_user_entry_t), 0) < 0) {
                perror("send user entry failed");
                break;
            }
        }
    }

    // --- Cleanup temporary hash set ---
    temp_user_set_t *current_user, *tmp_user;
    HASH_ITER(hh, user_set, current_user, tmp_user) {
        HASH_DEL(user_set, current_user);
        free(current_user);
    }
    printf("   User list sent and cleaned up.\n");
}

// --- Main Connection Handler ---
void* handle_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);
    message_type_t msg_type;
    ssize_t n = recv(conn_fd, &msg_type, sizeof(message_type_t), 0);
    
    if (n <= 0) {
        if (n < 0) { perror("[Thread] recv message type failed"); }
        else { printf("[Thread] Client disconnected before sending message type.\n"); }
        close(conn_fd);
        return NULL;
    }

    if (msg_type == MSG_SS_REGISTER) {
        ss_registration_t reg_data;
        n = recv(conn_fd, &reg_data, sizeof(ss_registration_t), 0);
         if (n == sizeof(ss_registration_t)) {
            pthread_rwlock_wrlock(&server_list_rwlock); 
            if (server_count < MAX_STORAGE_SERVERS) {
                strcpy(server_list[server_count].ip, reg_data.ss_ip);
                server_list[server_count].client_port = reg_data.client_port;
                server_count++;
                printf("-> Registered new Storage Server: %s:%d (Total: %d)\n\n", reg_data.ss_ip, reg_data.client_port, server_count);
            } else {
                printf("[Thread] Storage server list is full. Ignoring new server.\n");
            }
            pthread_rwlock_unlock(&server_list_rwlock); 
        } else {
            fprintf(stderr, "[Thread] Error receiving SS registration data. Expected %zu, got %zd\n", sizeof(ss_registration_t), n);
        }
        
    } else if (msg_type == MSG_CLIENT_NM_REQUEST) {
        printf("-> Received a connection from a Client!\n");
        client_request_t req;
        nm_response_t res = {0}; // Generic response, used by most handlers
        int response_sent_by_handler = 0; 

        n = recv(conn_fd, &req, sizeof(client_request_t), 0);
        
        if (n != sizeof(client_request_t)) {
            fprintf(stderr, "[Thread] Error receiving client request. Expected %zu, got %zd\n", sizeof(client_request_t), n);
            close(conn_fd);
            return NULL;
        }

        if (req.command == CMD_CREATE_FILE) {
            printf("   Handling CMD_CREATE_FILE for '%s'\n", req.filename);
            handle_create_file(&res, req); 
        
        } else if (req.command == CMD_VIEW_FILES) {
            printf("   Handling CMD_VIEW_FILES for user '%s'\n", req.username);
            handle_view_files(conn_fd, req); 
            response_sent_by_handler = 1;

        } else if (req.command == CMD_READ_FILE) {
            printf("   Handling CMD_READ_FILE for '%s'\n", req.filename);
            handle_read_file(&res, req);
        
        } else if (req.command == CMD_STREAM_FILE) {
            printf("   Handling CMD_STREAM_FILE for '%s'\n", req.filename);
            handle_read_file(&res, req); // Re-use read logic
        
        // --- ADDED THIS BLOCK ---
        } else if (req.command == CMD_LIST_USERS) {
            printf("   Handling CMD_LIST_USERS for user '%s'\n", req.username);
            handle_list_users(conn_fd, req); // Sends its own multi-part response
            response_sent_by_handler = 1;
        // --- END OF ADDED BLOCK ---

        } else if (req.command == CMD_GET_INFO) {
            printf("   Handling CMD_GET_INFO for '%s'\n", req.filename);
            handle_get_info(conn_fd, req); // Sends its own multi-part response
            response_sent_by_handler = 1;

        } else if (req.command == CMD_WRITE_FILE) {
            printf("   Handling CMD_WRITE_FILE for '%s'\n", req.filename);
            handle_write_file(&res, req);
        
        } else if (req.command == CMD_ADD_ACCESS) { 
            printf("   Handling CMD_ADD_ACCESS for '%s'\n", req.filename);
            handle_add_access(&res, req);
            
        } else if (req.command == CMD_REM_ACCESS) { 
             printf("   Handling CMD_REM_ACCESS for '%s'\n", req.filename);
             handle_rem_access(&res, req);

        } else if (req.command == CMD_DELETE_FILE) {
            printf("   Handling CMD_DELETE_FILE for '%s'\n", req.filename);
            handle_delete_file(&res, req);
        
        } else if (req.command == CMD_UNDO_FILE) {
            printf("   Handling CMD_UNDO_FILE for '%s'\n", req.filename);
            // UNDO just needs write permission, which handle_write_file checks
            handle_write_file(&res, req);
        
        } else if (req.command == CMD_EXEC) {
            printf("   Handling CMD_EXEC for '%s'\n", req.filename);
            handle_exec_file(conn_fd, req);
            response_sent_by_handler = 1; // IMPORTANT!
        }
        else {
            res.status = STATUS_ERROR;
            strcpy(res.error_msg, "Unknown command");
        }
        
        if (!response_sent_by_handler) {
            if (send(conn_fd, &res, sizeof(nm_response_t), 0) < 0) {
                perror("[Thread] send response to client failed");
            }
        }
    }
    
    close(conn_fd);
    return NULL;
}

// --- (Main function is unchanged) ---
int main() {
    int listen_fd;
    struct sockaddr_in serv_addr;
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("[Main] socket creation failed"); exit(EXIT_FAILURE); }
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serv_addr.sin_port = htons(NM_PORT);
    if (bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("[Main] bind failed"); exit(EXIT_FAILURE);
    }
    if (listen(listen_fd, 10) < 0) {
        perror("[Main] listen failed"); exit(EXIT_FAILURE);
    }
    if (pthread_rwlock_init(&server_list_rwlock, NULL) != 0) {
        perror("[Main] server rwlock init failed"); exit(EXIT_FAILURE);
    }
    if (pthread_rwlock_init(&file_map_rwlock, NULL) != 0) {
        perror("[Main] file map rwlock init failed"); exit(EXIT_FAILURE);
    }
    printf("Name Server listening on port %d...\n", NM_PORT);
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            perror("[Main] accept failed");
            continue; 
        }
        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        if (pthread_create(&tid, NULL, handle_connection, p_conn_fd) != 0) {
             perror("[Main] pthread_create failed");
             free(p_conn_fd); 
             close(conn_fd);  
        } else {
            pthread_detach(tid);
        }
    } 
    close(listen_fd);
    pthread_rwlock_destroy(&server_list_rwlock);
    pthread_rwlock_destroy(&file_map_rwlock);
    file_info_t *current_file, *tmp_file;
    HASH_ITER(hh, g_file_map, current_file, tmp_file) {
        access_entry_t *current_access, *tmp_access;
        HASH_ITER(hh, current_file->access_list, current_access, tmp_access) {
            HASH_DEL(current_file->access_list, current_access);
            free(current_access);
        }
        HASH_DEL(g_file_map, current_file);
        free(current_file);
    }
    return 0;
}