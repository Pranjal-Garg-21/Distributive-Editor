#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <stdbool.h>
#include <signal.h>   // For signal handling
#include <errno.h>    // For errno
#include <time.h>     // For cache
#include "common.h"
#include "uthash.h" 
#include "logger.h"

#define METADATA_FILE "nm_metadata.dat"
#define MAX_CACHE_SIZE 128


// --- (All structs, globals, and helpers up to handle_delete_file are unchanged) ---
#define MAX_STORAGE_SERVERS 10
typedef struct { 
    char ip[MAX_IP_LEN]; 
    int client_port; 
    bool active; // NEW: To mark if SS is currently connected
} ss_info_t;

typedef enum { NM_ACCESS_READ, NM_ACCESS_WRITE } nm_access_level_t;
typedef struct { char username[MAX_USERNAME_LEN]; nm_access_level_t level; UT_hash_handle hh; } access_entry_t;
typedef struct { char filename[MAX_FILENAME_LEN]; char owner[MAX_USERNAME_LEN]; int ss_index; access_entry_t* access_list; UT_hash_handle hh; } file_info_t;
ss_info_t server_list[MAX_STORAGE_SERVERS];
int server_count = 0;
pthread_rwlock_t server_list_rwlock; 
file_info_t* g_file_map = NULL; 
pthread_rwlock_t file_map_rwlock;

// --- Forward Declarations for new functions ---
void save_metadata_to_disk();
void load_metadata_from_disk();
void free_file_map();
// --- NEW: Forward Declarations for cache functions ---
static file_info_t* cache_get(const char* filename);
static void cache_put(file_info_t* file_info);
static void cache_invalidate(const char* filename);


// --- NEW CACHE DEFINITIONS ---

// An entry in our LRU cache
typedef struct cache_entry_s {
    char filename[MAX_FILENAME_LEN]; // The key
    file_info_t* file_info;          // The value (pointer to main map's data)
    
    // Doubly-linked list pointers for LRU
    struct cache_entry_s* prev;
    struct cache_entry_s* next;
    
    // Hash handle for O(1) lookup
    UT_hash_handle hh;
} cache_entry_t;

// Cache globals
static cache_entry_t* g_file_cache = NULL;      // Hash map head
static cache_entry_t* g_cache_head = NULL;      // Most-recently-used
static cache_entry_t* g_cache_tail = NULL;      // Least-recently-used
static int g_cache_size = 0;
static pthread_mutex_t g_cache_mutex;          // Mutex to protect all cache operations

// --- END CACHE DEFINITIONS ---


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

// --- (All handle_... functions from handle_exec_file to handle_list_users are UNCHANGED) ---
void handle_exec_file(int client_conn_fd, client_request_t req) {
    printf("   Handling CMD_EXEC for '%s' by user '%s'\n", req.filename, req.username);

    char ss_ip[MAX_IP_LEN];
    int ss_port;
    nm_response_t res_header = {0}; // This is the header we send to the client

    // --- Step 1: Authorize and find file location ---
    // We must hold the read lock while using the file_info_t pointer
    // to prevent it from being freed by handle_delete_file
    pthread_rwlock_rdlock(&file_map_rwlock);
    
    file_info_t* found_file = cache_get(req.filename); // 1. Check cache
    if (!found_file) {
        // 2. Cache Miss
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        if (found_file) {
            cache_put(found_file); // 3. Populate cache
        }
    }

    pthread_rwlock_rdlock(&server_list_rwlock);

    if (!found_file) {
        res_header.status = STATUS_ERROR;
        res_header.error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res_header.error_msg, "File not found");
    } else if (!check_permission(found_file, req.username, NM_ACCESS_READ)) {
        res_header.status = STATUS_ERROR;
        res_header.error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res_header.error_msg, "Permission denied (Read access required)");
    } else if (found_file->ss_index >= server_count || !server_list[found_file->ss_index].active) { 
        res_header.status = STATUS_ERROR;
        res_header.error_code = NFS_ERR_SS_DOWN; 
        strcpy(res_header.error_msg, "Storage Server for this file is offline");
    } else {
        // All good. Copy location and set status OK.
        res_header.status = STATUS_OK;
        res_header.error_code = NFS_OK; 
        strcpy(ss_ip, server_list[found_file->ss_index].ip);
        ss_port = server_list[found_file->ss_index].client_port;
    }

    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock); // Release map lock

    // If any check failed, send error response to client and we're done.
    if (res_header.status == STATUS_ERROR) {
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0);
        return; 
    }

    // --- Step 2: NM acts as client to fetch file from SS ---
    int ss_sock_fd = connect_to_server(ss_ip, ss_port);
    if (ss_sock_fd < 0) {
        fprintf(stderr, "   [EXEC] NM failed to connect to SS at %s:%d\n", ss_ip, ss_port);
        res_header.status = STATUS_ERROR;
        res_header.error_code = NFS_ERR_SS_DOWN; 
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
        res_header.error_code = ss_res.error_code; 
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
        res_header.error_code = NFS_ERR_NM_INTERNAL; 
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

// REPLACED handle_create_file with a more performant, concurrent version
void handle_create_file(nm_response_t* res, client_request_t req) {
    // --- Step 1: Check if file *already* exists (using cache and read lock)
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* found_file = cache_get(req.filename);
    if (!found_file) {
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        // No cache_put here, we only cache on success
    }
    pthread_rwlock_unlock(&file_map_rwlock);

    if (found_file) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_FILE_ALREADY_EXISTS; 
        strcpy(res->error_msg, "File already exists");
        return;
    }

    // --- Step 2: Find an active SS (using read lock)
    int assigned_ss_index = -1;
    char ss_ip[MAX_IP_LEN];
    int ss_port;

    pthread_rwlock_rdlock(&server_list_rwlock); 
    for (int i = 0; i < server_count; i++) {
        if (server_list[i].active) {
            assigned_ss_index = i;
            strcpy(ss_ip, server_list[assigned_ss_index].ip);
            ss_port = server_list[assigned_ss_index].client_port;
            break;
        }
    }
    pthread_rwlock_unlock(&server_list_rwlock); 

    if (assigned_ss_index == -1) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_SS_DOWN; 
        strcpy(res->error_msg, "No Storage Servers available");
        return;
    }

    // --- Step 3: Contact SS (NO LOCKS HELD)
    int ss_sock_fd = connect_to_server(ss_ip, ss_port);
    if (ss_sock_fd < 0) {
        fprintf(stderr, "   [NM-Create] NM failed to connect to SS at %s:%d\n", ss_ip, ss_port);
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_SS_DOWN;
        strcpy(res->error_msg, "NM Error: Could not connect to Storage Server");
        printf("   [NM] Marking SS#%d as INACTIVE due to connection failure.\n", assigned_ss_index);
        pthread_rwlock_wrlock(&server_list_rwlock);
        if (assigned_ss_index >= 0 && assigned_ss_index < server_count) { // Safety check
             server_list[assigned_ss_index].active = false;
        }
        pthread_rwlock_unlock(&server_list_rwlock);
        return;
    }

    send(ss_sock_fd, &req, sizeof(client_request_t), 0);
    ss_response_t ss_res;
    if (recv(ss_sock_fd, &ss_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("   [NM-Create] recv response from SS failed");
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_SS_INTERNAL;
        strcpy(res->error_msg, "NM Error: No ACK received from Storage Server");
        close(ss_sock_fd);
        return;
    }
    close(ss_sock_fd);

    if (ss_res.status == STATUS_ERROR) {
        res->status = STATUS_ERROR;
        res->error_code = ss_res.error_code;
        strncpy(res->error_msg, ss_res.error_msg, MAX_ERROR_MSG_LEN - 1);
        return;
    }

    // --- Step 4: SS was successful, NOW update NM metadata (using write lock)
    pthread_rwlock_wrlock(&file_map_rwlock);

    // *Must re-check* for race condition (another client created same file)
    HASH_FIND_STR(g_file_map, req.filename, found_file);
    if (found_file) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_FILE_ALREADY_EXISTS; 
        strcpy(res->error_msg, "File created by another user during operation");
        pthread_rwlock_unlock(&file_map_rwlock);
        printf("   [NM] Marking SS#%d as INACTIVE due to connection failure.\n", assigned_ss_index);
        pthread_rwlock_wrlock(&server_list_rwlock);
        if (assigned_ss_index >= 0 && assigned_ss_index < server_count) { // Safety check
            server_list[assigned_ss_index].active = false;
        }
        pthread_rwlock_unlock(&server_list_rwlock);
        return;
    }

    file_info_t* new_file = (file_info_t*)malloc(sizeof(file_info_t));
    if (!new_file) {
         res->status = STATUS_ERROR;
         res->error_code = NFS_ERR_NM_INTERNAL; 
         strcpy(res->error_msg, "Name Server out of memory");
         pthread_rwlock_unlock(&file_map_rwlock);
         return;
    }
    
    strcpy(new_file->filename, req.filename);
    strcpy(new_file->owner, req.username);
    new_file->ss_index = assigned_ss_index; 
    new_file->access_list = NULL; 
    
    access_entry_t* owner_access = (access_entry_t*)malloc(sizeof(access_entry_t));
    if (!owner_access) {
         res->status = STATUS_ERROR;
         res->error_code = NFS_ERR_NM_INTERNAL; 
         strcpy(res->error_msg, "Name Server out of memory");
         free(new_file);
         pthread_rwlock_unlock(&file_map_rwlock);
         return;
    }
    strcpy(owner_access->username, req.username);
    owner_access->level = NM_ACCESS_WRITE; 
    HASH_ADD_STR(new_file->access_list, username, owner_access);
    
    HASH_ADD_STR(g_file_map, filename, new_file);
    cache_put(new_file); // --- ADD TO CACHE ---
    
    res->status = STATUS_OK;
    res->error_code = NFS_OK; 
    printf("-> Metadata Added: File '%s' (Owner: %s) on SS#%d\n", req.filename, req.username, new_file->ss_index);
    pthread_rwlock_unlock(&file_map_rwlock);
}


// REPLACED handle_delete_file with a more performant, concurrent version
void handle_delete_file(nm_response_t* res, client_request_t req) {
    char ss_ip[MAX_IP_LEN];
    int ss_port;
    int ss_idx;

    // --- Step 1: Check permissions and get SS location (using read lock)
    pthread_rwlock_rdlock(&file_map_rwlock);   
    
    file_info_t* found_file = cache_get(req.filename);
    if (!found_file) {
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        if (found_file) {
            cache_put(found_file);
        }
    }
    
    pthread_rwlock_rdlock(&server_list_rwlock); 
    
    if (!found_file) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res->error_msg, "File not found");
    } else if (strcmp(found_file->owner, req.username) != 0) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res->error_msg, "Permission denied (Only the owner can delete)");
    } else {
        ss_idx = found_file->ss_index;
        if (ss_idx >= server_count || !server_list[ss_idx].active) { 
            res->status = STATUS_ERROR;
            res->error_code = NFS_ERR_SS_DOWN; 
            strcpy(res->error_msg, "Storage Server for this file is offline, cannot delete");
        } else {
            // All checks passed
            res->status = STATUS_OK;
            strcpy(ss_ip, server_list[ss_idx].ip);
            ss_port = server_list[ss_idx].client_port;
        }
    }
    
    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);

    if (res->status == STATUS_ERROR) {
        return; // Return with error message already set
    }

    // --- Step 2: Contact SS (NO LOCKS HELD)
    int ss_sock_fd = connect_to_server(ss_ip, ss_port);
    if (ss_sock_fd < 0) {
        fprintf(stderr, "   [NM-Delete] NM failed to connect to SS at %s:%d\n", ss_ip, ss_port);
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_SS_DOWN;
        strcpy(res->error_msg, "NM Error: Could not connect to Storage Server");
        printf("   [NM] Marking SS#%d as INACTIVE due to connection failure.\n", ss_idx);
        pthread_rwlock_wrlock(&server_list_rwlock);
        if (ss_idx >= 0 && ss_idx < server_count) { // Safety check
            server_list[ss_idx].active = false;
        }
        pthread_rwlock_unlock(&server_list_rwlock);
        return;
    }

    send(ss_sock_fd, &req, sizeof(client_request_t), 0);
    ss_response_t ss_res;
    if (recv(ss_sock_fd, &ss_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("   [NM-Delete] recv response from SS failed");
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_SS_INTERNAL;
        strcpy(res->error_msg, "NM Error: No ACK received from Storage Server");
        close(ss_sock_fd);
        return;
    }
    close(ss_sock_fd);

    if (ss_res.status == STATUS_ERROR) {
        res->status = STATUS_ERROR;
        res->error_code = ss_res.error_code;
        strncpy(res->error_msg, ss_res.error_msg, MAX_ERROR_MSG_LEN - 1);
        return;
    }

    // --- Step 3: SS was successful, NOW update NM metadata (using write lock)
    pthread_rwlock_wrlock(&file_map_rwlock);
    
    // Re-find the file (must be atomic with delete)
    HASH_FIND_STR(g_file_map, req.filename, found_file);
    if (found_file) {
        // Free ACL
        access_entry_t *current_access, *tmp_access;
        HASH_ITER(hh, found_file->access_list, current_access, tmp_access) {
            HASH_DEL(found_file->access_list, current_access);
            free(current_access);
        }
        // Free file entry
        HASH_DEL(g_file_map, found_file);
        free(found_file);
        
        cache_invalidate(req.filename); // --- INVALIDATE CACHE ---
        
        res->status = STATUS_OK;
        res->error_code = NFS_OK; 
        printf("-> Metadata Deleted: File '%s' (User: %s). SS notified.\n", req.filename, req.username);
    } else {
        // This should not happen if we're careful, but good to check
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_FILE_NOT_FOUND;
        strcpy(res->error_msg, "File was deleted by another user during operation");
    }
    
    pthread_rwlock_unlock(&file_map_rwlock);
}

void handle_view_files(int conn_fd, client_request_t req) {
    nm_response_t res_header = {0};
    nm_file_entry_t file_entry;
    int files_to_send_count = 0;

    // We must hold read locks while iterating maps
    pthread_rwlock_rdlock(&file_map_rwlock); 
    pthread_rwlock_rdlock(&server_list_rwlock);
    
    file_info_t *current_file, *tmp;
    HASH_ITER(hh, g_file_map, current_file, tmp) {
        if (req.view_all || check_permission(current_file, req.username, NM_ACCESS_READ)) {
            files_to_send_count++;
        }
    }
    res_header.status = STATUS_OK;
    res_header.error_code = NFS_OK; 
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
        
        if (ss_idx >= server_count || !server_list[ss_idx].active) {
             fprintf(stderr, "   Skipping file '%s' with invalid/inactive ss_index %d\n", current_file->filename, ss_idx);
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
    // Hold read lock to ensure pointer from cache/map is valid
    pthread_rwlock_rdlock(&file_map_rwlock); 
    
    file_info_t* found_file = cache_get(req.filename); // 1. Check cache
    if (!found_file) {
        // 2. Cache Miss
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        if (found_file) {
            cache_put(found_file); // 3. Populate cache
        }
    }
    
    pthread_rwlock_rdlock(&server_list_rwlock); 
    
    if (!found_file) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res->error_msg, "File not found");
    } else if (!check_permission(found_file, req.username, NM_ACCESS_READ)) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res->error_msg, "Permission denied");
    } else {
        int ss_idx = found_file->ss_index;
        if (ss_idx >= server_count || !server_list[ss_idx].active) { 
            res->status = STATUS_ERROR;
            res->error_code = NFS_ERR_SS_DOWN; 
            strcpy(res->error_msg, "Storage Server for this file is currently offline");
        } else {
            res->status = STATUS_OK;
            res->error_code = NFS_OK; 
            strcpy(res->ss_ip, server_list[ss_idx].ip);
            res->ss_port = server_list[ss_idx].client_port;
            printf("-> Read Access Granted: File '%s' (User: %s) on SS#%d\n", req.filename, req.username, ss_idx);
        }
    }
    
    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);  
}

void handle_write_file(nm_response_t* res, client_request_t req) {
    // Hold read lock to ensure pointer from cache/map is valid
    pthread_rwlock_rdlock(&file_map_rwlock);
    
    file_info_t* found_file = cache_get(req.filename); // 1. Check cache
    if (!found_file) {
        // 2. Cache Miss
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        if (found_file) {
            cache_put(found_file); // 3. Populate cache
        }
    }

    pthread_rwlock_rdlock(&server_list_rwlock);
    
    if (!found_file) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res->error_msg, "File not found");
    } else if (!check_permission(found_file, req.username, NM_ACCESS_WRITE)) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res->error_msg, "Permission denied (Write access required)");
    } else {
        int ss_idx = found_file->ss_index;
        if (ss_idx >= server_count || !server_list[ss_idx].active) { 
             res->status = STATUS_ERROR;
             res->error_code = NFS_ERR_SS_DOWN; 
             strcpy(res->error_msg, "Storage Server for this file is currently offline");
        } else {
            res->status = STATUS_OK;
            res->error_code = NFS_OK; 
            strcpy(res->ss_ip, server_list[ss_idx].ip);
            res->ss_port = server_list[ss_idx].client_port;
            printf("-> Write Access Granted: File '%s' (User: %s) on SS#%d\n", req.filename, req.username, ss_idx);
        }
    }
    
    pthread_rwlock_unlock(&server_list_rwlock); 
    pthread_rwlock_unlock(&file_map_rwlock);  
}

void handle_add_access(nm_response_t* res, client_request_t req) {
    // Must hold write lock, as we are modifying the access list
    pthread_rwlock_wrlock(&file_map_rwlock); 
    
    file_info_t* found_file = cache_get(req.filename); // 1. Check cache
    if (!found_file) {
        // 2. Cache Miss
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        // No cache_put, we will invalidate
    }
    
    if (!found_file) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res->error_msg, "File not found");
    } else if (strcmp(found_file->owner, req.username) != 0) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res->error_msg, "Permission denied (Only the owner can add access)");
    } else {
        access_entry_t* target_access;
        HASH_FIND_STR(found_file->access_list, req.target_username, target_access);
        
        nm_access_level_t new_level = (req.access_level == ACCESS_WRITE) ? NM_ACCESS_WRITE : NM_ACCESS_READ;

        if (target_access) {
            // --- THIS IS THE FIX ---
            // Only upgrade permissions. Never downgrade.
            if (target_access->level == NM_ACCESS_WRITE && new_level == NM_ACCESS_READ) {
                // User already has WRITE, which includes READ. Do nothing.
                res->status = STATUS_OK;
                res->error_code = NFS_OK;
                printf("   Access Updated: User '%s' already has WRITE access for '%s'. No change needed.\n", req.target_username, req.filename);
            } else {
                // Either upgrading R -> W, or setting W -> W (no change)
                target_access->level = new_level;
                res->status = STATUS_OK;
                res->error_code = NFS_OK;
                printf("   Access Updated: User '%s' set to %s for file '%s'\n", req.target_username, (new_level == NM_ACCESS_WRITE) ? "WRITE" : "READ", req.filename);
                cache_invalidate(req.filename); // --- INVALIDATE CACHE ---
            }
            // --- END FIX ---
        } else {
            // User is not in the list, so create a new entry
            access_entry_t* new_access = (access_entry_t*)malloc(sizeof(access_entry_t));
            if (!new_access) {
                res->status = STATUS_ERROR;
                res->error_code = NFS_ERR_NM_INTERNAL; 
                strcpy(res->error_msg, "Name Server out of memory");
            } else {
                strcpy(new_access->username, req.target_username);
                new_access->level = new_level;
                HASH_ADD_STR(found_file->access_list, username, new_access);
                res->status = STATUS_OK;
                res->error_code = NFS_OK; 
                printf("   Access Added: User '%s' given %s for file '%s'\n", req.target_username, (new_level == NM_ACCESS_WRITE) ? "WRITE" : "READ", req.filename);
                cache_invalidate(req.filename); // --- INVALIDATE CACHE ---
            }
        }
    }
    pthread_rwlock_unlock(&file_map_rwlock);
}

void handle_rem_access(nm_response_t* res, client_request_t req) {
    // Must hold write lock, as we are modifying the access list
    pthread_rwlock_wrlock(&file_map_rwlock); 
    
    file_info_t* found_file = cache_get(req.filename); // 1. Check cache
    if (!found_file) {
        // 2. Cache Miss
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        // No cache_put, we will invalidate
    }

    if (!found_file) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res->error_msg, "File not found");
    } else if (strcmp(found_file->owner, req.username) != 0) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res->error_msg, "Permission denied (Only the owner can remove access)");
    } else if (strcmp(found_file->owner, req.target_username) == 0) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_INVALID_INPUT; 
        strcpy(res->error_msg, "Cannot remove the owner's access");
    } else {
        access_entry_t* target_access;
        HASH_FIND_STR(found_file->access_list, req.target_username, target_access);
        if (target_access) {
            HASH_DEL(found_file->access_list, target_access);
            free(target_access);
            res->status = STATUS_OK;
            res->error_code = NFS_OK; 
            printf("   Access Removed: User '%s' from file '%s'\n", req.target_username, req.filename);
            cache_invalidate(req.filename); // --- INVALIDATE CACHE ---
        } else {
            res->status = STATUS_ERROR;
            res->error_code = NFS_ERR_FILE_NOT_FOUND; 
            strcpy(res->error_msg, "User does not have access to this file");
        }
    }
    pthread_rwlock_unlock(&file_map_rwlock);
}

void handle_get_info(int conn_fd, client_request_t req) {
    nm_info_response_t res_header = {0};
    
    // Hold read lock to ensure pointer from cache/map is valid
    pthread_rwlock_rdlock(&file_map_rwlock); 
    
    file_info_t* found_file = cache_get(req.filename); // 1. Check cache
    if (!found_file) {
        // 2. Cache Miss
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        if (found_file) {
            cache_put(found_file); // 3. Populate cache
        }
    }
    
    pthread_rwlock_rdlock(&server_list_rwlock);

    if (!found_file) {
        res_header.status = STATUS_ERROR;
        res_header.error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res_header.error_msg, "File not found");
    } else if (!check_permission(found_file, req.username, NM_ACCESS_READ)) {
        res_header.status = STATUS_ERROR;
        res_header.error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res_header.error_msg, "Permission denied");
    } else {
        int ss_idx = found_file->ss_index;
        if (ss_idx >= server_count || !server_list[ss_idx].active) { 
            res_header.status = STATUS_ERROR;
            res_header.error_code = NFS_ERR_SS_DOWN; 
            strcpy(res_header.error_msg, "Storage Server for this file is currently offline");
        } else {
            // All checks passed, send header
            res_header.status = STATUS_OK;
            res_header.error_code = NFS_OK; 
            strcpy(res_header.owner, found_file->owner);
            strcpy(res_header.ss_ip, server_list[ss_idx].ip);
            res_header.ss_port = server_list[ss_idx].client_port;
            res_header.acl_count = HASH_COUNT(found_file->access_list);
        }
    }
    
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
    // We are still holding the file_map_rwlock, so this is safe
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
    res_header.error_code = NFS_OK; 
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
void* handle_connection(void* p_arg) {
    // --- NEW: Unpack thread arguments ---
    thread_arg_t* arg = (thread_arg_t*)p_arg;
    int conn_fd = arg->conn_fd;
    char ip_str[INET_ADDRSTRLEN];
    strcpy(ip_str, arg->ip_str); // Copy to stack
    int port = arg->port;
    free(arg); // Free the heap-allocated struct
    // --- END NEW ---

    message_type_t msg_type;
    ssize_t n = recv(conn_fd, &msg_type, sizeof(message_type_t), 0);
    
    if (n <= 0) {
        if (n < 0) { 
            perror("[Thread] recv message type failed");
            server_log(LOG_ERROR, ip_str, port, "N/A", "recv message type failed: %s", strerror(errno));
        } else { 
            printf("[Thread] Client disconnected before sending message type.\n"); 
            server_log(LOG_INFO, ip_str, port, "N/A", "Client disconnected before sending message type.");
        }
        close(conn_fd);
        return NULL;
    }

    if (msg_type == MSG_SS_REGISTER) {
        ss_registration_t reg_data;
        n = recv(conn_fd, &reg_data, sizeof(ss_registration_t), 0);
         if (n == sizeof(ss_registration_t)) {
            pthread_rwlock_wrlock(&server_list_rwlock); 
            
            // Check if this server is already in our list
            int ss_idx = -1;
            for(int i = 0; i < server_count; i++) {
                if (strcmp(server_list[i].ip, reg_data.ss_ip) == 0 && 
                    server_list[i].client_port == reg_data.client_port) {
                    ss_idx = i;
                    break;
                }
            }

            if (ss_idx == -1) { // New server
                if (server_count < MAX_STORAGE_SERVERS) {
                    ss_idx = server_count;
                    strcpy(server_list[ss_idx].ip, reg_data.ss_ip);
                    server_list[ss_idx].client_port = reg_data.client_port;
                    server_count++;
                    printf("-> Registered new Storage Server: %s:%d (Assigned SS#%d)\n", reg_data.ss_ip, reg_data.client_port, ss_idx);
                    server_log(LOG_INFO, ip_str, port, "SS", "Registered new Storage Server: %s:%d (Assigned SS#%d)", reg_data.ss_ip, reg_data.client_port, ss_idx);
                } else {
                     printf("[Thread] Storage server list is full. Ignoring new server.\n");
                     server_log(LOG_WARN, ip_str, port, "SS", "Storage server list is full. Ignoring new server %s:%d", reg_data.ss_ip, reg_data.client_port);
                     pthread_rwlock_unlock(&server_list_rwlock);
                     close(conn_fd);
                     return NULL;
                }
            } else { // Reconnecting server
                printf("-> Re-registered Storage Server: %s:%d (SS#%d)\n", reg_data.ss_ip, reg_data.client_port, ss_idx);
                server_log(LOG_INFO, ip_str, port, "SS", "Re-registered Storage Server: %s:%d (SS#%d)", reg_data.ss_ip, reg_data.client_port, ss_idx);
            }
            
            server_list[ss_idx].active = true;
            pthread_rwlock_unlock(&server_list_rwlock);

            // --- Now, handle the file list from this SS ---
            pthread_rwlock_wrlock(&file_map_rwlock); // Lock file map
            
            message_type_t file_msg_type;
            nm_ss_file_entry_t file_entry;
            while(recv(conn_fd, &file_msg_type, sizeof(message_type_t), 0) == sizeof(message_type_t)) {
                if (file_msg_type == MSG_SS_FILE_LIST_END) {
                    break; // End of list
                }

                if (file_msg_type == MSG_SS_FILE_LIST_ENTRY) {
                    if (recv(conn_fd, &file_entry, sizeof(nm_ss_file_entry_t), 0) == sizeof(nm_ss_file_entry_t)) {
                        
                        file_info_t* found_file;
                        HASH_FIND_STR(g_file_map, file_entry.filename, found_file);
                        
                        if (found_file) {
                            // We know about this file. Let's update its SS index if it's different.
                            // This handles the case where the file was on a different SS before.
                            if (found_file->ss_index != ss_idx) {
                                printf("   [Sync] Re-mapping file '%s' from SS#%d to SS#%d\n", 
                                       file_entry.filename, found_file->ss_index, ss_idx);
                                server_log(LOG_INFO, ip_str, port, "SS", "[Sync] Re-mapping file '%s' from SS#%d to SS#%d", file_entry.filename, found_file->ss_index, ss_idx);
                                found_file->ss_index = ss_idx;
                                cache_invalidate(found_file->filename); // Invalidate stale cache
                            }
                        } else {
                            // This is an "orphan" file. The SS has it but we don't.
                            // We need to create metadata for it.
                            // BUT: Who is the owner? We don't know.
                            // For now, let's just log it. A proper implementation would
                            // require an "admin" user or a way to claim orphans.
                             printf("   [Sync] Found ORPHAN file '%s' on SS#%d. Metadata not created.\n", 
                                    file_entry.filename, ss_idx);
                             server_log(LOG_WARN, ip_str, port, "SS", "[Sync] Found ORPHAN file '%s' on SS#%d. Metadata not created.", file_entry.filename, ss_idx);
                        }
                    }
                }
            }
            pthread_rwlock_unlock(&file_map_rwlock); // Unlock file map

            printf("-> Finished processing file list for SS#%d\n", ss_idx);

        } else {
            fprintf(stderr, "[Thread] Error receiving SS registration data. Expected %zu, got %zd\n", sizeof(ss_registration_t), n);
            server_log(LOG_ERROR, ip_str, port, "SS", "Error receiving SS registration data. Expected %zu, got %zd", sizeof(ss_registration_t), n);
        }
        
    } else if (msg_type == MSG_CLIENT_NM_REQUEST) {
        printf("-> Received a connection from a Client!\n");
        client_request_t req;
        nm_response_t res = {0}; // Generic response, used by most handlers
        int response_sent_by_handler = 0; 

        n = recv(conn_fd, &req, sizeof(client_request_t), 0);
        
        if (n != sizeof(client_request_t)) {
            fprintf(stderr, "[Thread] Error receiving client request. Expected %zu, got %zd\n", sizeof(client_request_t), n);
            server_log(LOG_ERROR, ip_str, port, "N/A", "Error receiving client request. Expected %zu, got %zd", sizeof(client_request_t), n);
            close(conn_fd);
            return NULL;
        }

        // --- NEW: Log the incoming request ---
        server_log(LOG_INFO, ip_str, port, req.username, "REQ: CMD=%d, File='%s', TargetUser='%s'", req.command, req.filename, req.target_username);


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
        
        } else if (req.command == CMD_LIST_USERS) {
            printf("   Handling CMD_LIST_USERS for user '%s'\n", req.username);
            handle_list_users(conn_fd, req); // Sends its own multi-part response
            response_sent_by_handler = 1;

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
            res.error_code = NFS_ERR_INVALID_INPUT; 
            strcpy(res.error_msg, "Unknown command");
        }
        
        if (!response_sent_by_handler) {
            if (send(conn_fd, &res, sizeof(nm_response_t), 0) < 0) {
                perror("[Thread] send response to client failed");
                server_log(LOG_ERROR, ip_str, port, req.username, "Send response failed: %s", strerror(errno));
            } else {
                // --- NEW: Log the response ---
                if (res.status == STATUS_OK) {
                    server_log(LOG_INFO, ip_str, port, req.username, "ACK: CMD=%d, Status=OK", req.command);
                } else {
                    server_log(LOG_WARN, ip_str, port, req.username, "NAK: CMD=%d, Status=ERROR, Msg=%s", req.command, res.error_msg);
                }
                // --- END NEW ---
            }
        }
    }
    
    server_log(LOG_INFO, ip_str, port, "N/A", "Closing connection.");
    close(conn_fd);
    return NULL;
}


// --- NEW: Signal Handler for graceful shutdown ---
void signal_handler(int sig) {
    if (sig == SIGINT) {
        printf("\n[Main] SIGINT received. Shutting down gracefully.\n");
        server_log(LOG_INFO, "N/A", 0, "SYS", "SIGINT received. Shutting down gracefully.");
        
        printf("[Main] Saving metadata to disk...\n");
        save_metadata_to_disk();
        printf("[Main] Metadata saved. Goodbye.\n");
        
        // Cleanup all memory
        free_file_map();
        pthread_rwlock_destroy(&server_list_rwlock);
        pthread_rwlock_destroy(&file_map_rwlock);
        pthread_mutex_destroy(&g_cache_mutex);
        
        log_shutdown(); // <-- ADD THIS
        exit(EXIT_SUCCESS);
    }
}

// --- NEW: Function to save metadata to disk ---
void save_metadata_to_disk() {
    FILE* fp = fopen(METADATA_FILE, "w");
    if (fp == NULL) {
        perror("[Main] Failed to open metadata file for writing");
        return;
    }

    pthread_rwlock_rdlock(&file_map_rwlock);
    
    file_info_t *current_file, *tmp_file;
    HASH_ITER(hh, g_file_map, current_file, tmp_file) {
        // Write FILE entry
        fprintf(fp, "FILE %s %s %d\n", 
                current_file->filename, 
                current_file->owner, 
                current_file->ss_index);
        
        // Write ACL entries for this file
        access_entry_t *current_access, *tmp_access;
        HASH_ITER(hh, current_file->access_list, current_access, tmp_access) {
            fprintf(fp, "ACL %s %s %c\n",
                    current_file->filename,
                    current_access->username,
                    (current_access->level == NM_ACCESS_WRITE) ? 'W' : 'R');
        }
    }
    
    pthread_rwlock_unlock(&file_map_rwlock);
    fclose(fp);
    printf("   ...Metadata save complete.\n");
}

// --- NEW: Function to load metadata from disk ---
void load_metadata_from_disk() {
    FILE* fp = fopen(METADATA_FILE, "r");
    if (fp == NULL) {
        if (errno == ENOENT) {
            printf("[Main] No metadata file found (%s). Starting fresh.\n", METADATA_FILE);
        } else {
            perror("[Main] Failed to open metadata file for reading");
        }
        return;
    }

    printf("[Main] Loading metadata from %s...\n", METADATA_FILE);
    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        line[strcspn(line, "\n")] = 0; // Remove newline
        
        char type[10];
        char filename[MAX_FILENAME_LEN];
        char username[MAX_USERNAME_LEN];
        
        if (sscanf(line, "%s %s", type, filename) < 2) {
            continue; // Skip blank or invalid lines
        }

        if (strcmp(type, "FILE") == 0) {
            char owner[MAX_USERNAME_LEN];
            int ss_index;
            if (sscanf(line, "FILE %s %s %d", filename, owner, &ss_index) == 3) {
                file_info_t* new_file = (file_info_t*)calloc(1, sizeof(file_info_t));
                if (new_file) {
                    strcpy(new_file->filename, filename);
                    strcpy(new_file->owner, owner);
                    new_file->ss_index = ss_index;
                    new_file->access_list = NULL; // ACLs will be added next
                    HASH_ADD_STR(g_file_map, filename, new_file);
                }
            }
        } else if (strcmp(type, "ACL") == 0) {
            char level_char;
            if (sscanf(line, "ACL %s %s %c", filename, username, &level_char) == 3) {
                file_info_t* found_file;
                HASH_FIND_STR(g_file_map, filename, found_file);
                if (found_file) {
                    access_entry_t* new_access = (access_entry_t*)malloc(sizeof(access_entry_t));
                    if (new_access) {
                        strcpy(new_access->username, username);
                        new_access->level = (level_char == 'W') ? NM_ACCESS_WRITE : NM_ACCESS_READ;
                        HASH_ADD_STR(found_file->access_list, username, new_access);
                    }
                } else {
                    fprintf(stderr, "   [Load] Found ACL for unknown file: %s\n", filename);
                }
            }
        }
    }

    fclose(fp);
    printf("   ...Metadata load complete. Loaded %d file entries.\n", HASH_COUNT(g_file_map));
}

// --- NEW: Function to free all allocated metadata ---
void free_cache() {
    cache_entry_t *current_entry, *tmp;
    HASH_ITER(hh, g_file_cache, current_entry, tmp) {
        HASH_DEL(g_file_cache, current_entry);
        free(current_entry);
    }
}

void free_file_map() {
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
    free_cache(); // Free the cache as well
}

// =================================================================
// --- NEW LRU CACHE FUNCTIONS ---
// =================================================================

/**
 * @brief (Internal) Moves a given cache entry to the front (head) of the LRU list.
 * MUST be called with g_cache_mutex held.
 */
static void lru_move_to_front(cache_entry_t* entry) {
    if (entry == g_cache_head) {
        return; // Already at the front
    }

    // 1. Unlink entry from its current position
    if (entry->prev) {
        entry->prev->next = entry->next;
    }
    if (entry->next) {
        entry->next->prev = entry->prev;
    }
    if (entry == g_cache_tail) {
        g_cache_tail = entry->prev; // New tail
    }

    // 2. Add to front
    entry->next = g_cache_head;
    entry->prev = NULL;
    if (g_cache_head) {
        g_cache_head->prev = entry;
    }
    g_cache_head = entry;

    // 3. If list was empty, tail is also head
    if (g_cache_tail == NULL) {
        g_cache_tail = g_cache_head;
    }
}

/**
 * @brief (Internal) Evicts the tail (least recent) entry from the cache.
 * MUST be called with g_cache_mutex held.
 */
static void lru_evict_tail() {
    if (g_cache_tail == NULL) {
        return; // Cache is empty
    }
    
    cache_entry_t* tail_to_evict = g_cache_tail;

    // 1. Update list pointers
    g_cache_tail = tail_to_evict->prev;
    if (g_cache_tail) {
        g_cache_tail->next = NULL;
    } else {
        g_cache_head = NULL; // Cache is now empty
    }

    // 2. Remove from hash map
    HASH_DEL(g_file_cache, tail_to_evict);
    g_cache_size--;

    printf("   [Cache] Evicted '%s'\n", tail_to_evict->filename);

    // 3. Free the cache entry
    free(tail_to_evict);
}

/**
 * @brief Gets a file's info from the LRU cache.
 * If found, marks it as recently used and returns it.
 *
 * @param filename The name of the file to find.
 * @return file_info_t* pointer if found in cache, else NULL.
 */
static file_info_t* cache_get(const char* filename) {
    pthread_mutex_lock(&g_cache_mutex);

    cache_entry_t* found_entry;
    HASH_FIND_STR(g_file_cache, filename, found_entry);

    if (found_entry) {
        // Cache Hit!
        lru_move_to_front(found_entry);
        pthread_mutex_unlock(&g_cache_mutex);
        return found_entry->file_info;
    }

    // Cache Miss
    pthread_mutex_unlock(&g_cache_mutex);
    return NULL;
}

/**
 * @brief Adds a file's info to the LRU cache (or updates it).
 *
 * @param file_info Pointer to the file_info_t struct (from g_file_map).
 */
static void cache_put(file_info_t* file_info) {
    if (file_info == NULL) return;

    pthread_mutex_lock(&g_cache_mutex);

    cache_entry_t* existing_entry;
    HASH_FIND_STR(g_file_cache, file_info->filename, existing_entry);

    if (existing_entry) {
        // Entry already exists, just mark it as recent
        existing_entry->file_info = file_info; // Update pointer just in case
        lru_move_to_front(existing_entry);
    } else {
        // New entry, check for eviction
        if (g_cache_size >= MAX_CACHE_SIZE) {
            lru_evict_tail();
        }

        // Create new entry
        cache_entry_t* new_entry = (cache_entry_t*)calloc(1, sizeof(cache_entry_t));
        if (!new_entry) {
            perror("[Cache] calloc failed for new entry");
            pthread_mutex_unlock(&g_cache_mutex);
            return;
        }

        strcpy(new_entry->filename, file_info->filename);
        new_entry->file_info = file_info;

        // Add to hash map
        HASH_ADD_STR(g_file_cache, filename, new_entry);
        g_cache_size++;

        // Add to front of list
        new_entry->next = g_cache_head;
        if (g_cache_head) {
            g_cache_head->prev = new_entry;
        }
        g_cache_head = new_entry;
        if (g_cache_tail == NULL) {
            g_cache_tail = new_entry;
        }
        
        printf("   [Cache] Added '%s'\n", new_entry->filename);
    }

    pthread_mutex_unlock(&g_cache_mutex);
}

/**
 * @brief Removes a file from the cache (e.g., when it's deleted).
 *
 * @param filename The name of the file to invalidate.
 */
static void cache_invalidate(const char* filename) {
    pthread_mutex_lock(&g_cache_mutex);

    cache_entry_t* found_entry;
    HASH_FIND_STR(g_file_cache, filename, found_entry);

    if (found_entry) {
        // 1. Unlink from list
        if (found_entry->prev) {
            found_entry->prev->next = found_entry->next;
        } else {
            g_cache_head = found_entry->next;
        }
        if (found_entry->next) {
            found_entry->next->prev = found_entry->prev;
        } else {
            g_cache_tail = found_entry->prev;
        }

        // 2. Remove from hash map
        HASH_DEL(g_file_cache, found_entry);
        g_cache_size--;

        printf("   [Cache] Invalidated '%s'\n", found_entry->filename);
        
        // 3. Free the entry
        free(found_entry);
    }

    pthread_mutex_unlock(&g_cache_mutex);
}

// =================================================================
// --- END LRU CACHE FUNCTIONS ---
// =================================================================


// --- Main function ---
int main() {
    int listen_fd;
    struct sockaddr_in serv_addr;

    log_init("nm.log"); // <-- ADD THIS
    server_log(LOG_INFO, "N/A", 0, "SYS", "--- Name Server Starting ---"); // <-- ADD THIS

    // --- NEW: Setup signal handler ---
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT, &sa, NULL) == -1) {
        perror("[Main] sigaction failed");
        server_log(LOG_ERROR, "N/A", 0, "SYS", "sigaction failed: %s", strerror(errno)); // <-- ADD THIS
        log_shutdown(); // <-- ADD THIS
        exit(EXIT_FAILURE);
    }

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { 
        perror("[Main] socket creation failed"); 
        server_log(LOG_ERROR, "N/A", 0, "SYS", "socket creation failed: %s", strerror(errno)); // <-- ADD THIS
        log_shutdown(); // <-- ADD THIS
        exit(EXIT_FAILURE); 
    }
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serv_addr.sin_port = htons(NM_PORT);
    if (bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("[Main] bind failed"); 
        server_log(LOG_ERROR, "N/A", 0, "SYS", "bind failed: %s", strerror(errno)); // <-- ADD THIS
        log_shutdown(); // <-- ADD THIS
        exit(EXIT_FAILURE); 
    }
    if (listen(listen_fd, 10) < 0) {
        perror("[Main] listen failed"); 
        server_log(LOG_ERROR, "N/A", 0, "SYS", "listen failed: %s", strerror(errno)); // <-- ADD THIS
        log_shutdown(); // <-- ADD THIS
        exit(EXIT_FAILURE); 
    }
    if (pthread_rwlock_init(&server_list_rwlock, NULL) != 0) {
        perror("[Main] server rwlock init failed"); 
        server_log(LOG_ERROR, "N/A", 0, "SYS", "server rwlock init failed: %s", strerror(errno)); // <-- ADD THIS
        log_shutdown(); // <-- ADD THIS
        exit(EXIT_FAILURE); 
    }
    if (pthread_rwlock_init(&file_map_rwlock, NULL) != 0) {
        perror("[Main] file map rwlock init failed"); 
        server_log(LOG_ERROR, "N/A", 0, "SYS", "file map rwlock init failed: %s", strerror(errno)); // <-- ADD THIS
        log_shutdown(); // <-- ADD THIS
        exit(EXIT_FAILURE); 
    }
    if (pthread_mutex_init(&g_cache_mutex, NULL) != 0) {
        perror("[Main] cache mutex init failed"); 
        server_log(LOG_ERROR, "N/A", 0, "SYS", "cache mutex init failed: %s", strerror(errno)); // <-- ADD THIS
        log_shutdown(); // <-- ADD THIS
        exit(EXIT_FAILURE); 
    }
    // --- NEW: Load metadata from disk before starting ---
    // We must lock the map while loading it.
    pthread_rwlock_wrlock(&file_map_rwlock);
    load_metadata_from_disk();
    pthread_rwlock_unlock(&file_map_rwlock);
    // --- END NEW ---

    printf("Name Server listening on port %d...\n", NM_PORT);
    server_log(LOG_INFO, "N/A", 0, "SYS", "Name Server listening on port %d", NM_PORT); // <-- ADD THIS

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            if (errno == EINTR) {
                // Interrupted by our signal handler, which is fine
                continue;
            }
            perror("[Main] accept failed");
            server_log(LOG_ERROR, "N/A", 0, "SYS", "Accept failed: %s", strerror(errno)); // <-- ADD THIS
            continue; 
        }

        // --- NEW: Pack args for thread ---
        thread_arg_t* arg = malloc(sizeof(thread_arg_t));
        if (!arg) {
            perror("[Main] malloc for thread arg failed");
            server_log(LOG_ERROR, "N/A", 0, "SYS", "malloc for thread arg failed");
            close(conn_fd);
            continue;
        }
        arg->conn_fd = conn_fd;
        arg->port = ntohs(client_addr.sin_port);
        inet_ntop(AF_INET, &client_addr.sin_addr, arg->ip_str, INET_ADDRSTRLEN);
        
        server_log(LOG_INFO, arg->ip_str, arg->port, "N/A", "Connection accepted."); // <-- ADD THIS

        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_connection, arg) != 0) { // Pass arg
             perror("[Main] pthread_create failed");
             server_log(LOG_ERROR, arg->ip_str, arg->port, "SYS", "Failed to create thread."); // <-- ADD THIS
             free(arg); // Free the arg
             close(conn_fd);  
        } else {
            pthread_detach(tid);
        }
        // --- END NEW ---
    } 
    
    // This part will only be reached if the loop breaks, 
    // but cleanup is handled by the signal handler anyway.
    close(listen_fd);
    save_metadata_to_disk(); // Final save
    free_file_map();
    pthread_rwlock_destroy(&server_list_rwlock);
    pthread_rwlock_destroy(&file_map_rwlock);
    pthread_mutex_destroy(&g_cache_mutex);
    
    log_shutdown(); // <-- ADD THIS
    return 0;
}