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
typedef struct { char filename[MAX_FILENAME_LEN]; char service_name[MAX_FILENAME_LEN];char owner[MAX_USERNAME_LEN]; int ss_index; int ss_index_backup; access_entry_t* access_list; UT_hash_handle hh; bool is_directory; } file_info_t;
// Helper to check if a string starts with a prefix (for VIEW_FOLDER)
bool starts_with(const char *pre, const char *str) {
    size_t lenpre = strlen(pre), lenstr = strlen(str);
    return lenstr < lenpre ? false : memcmp(pre, str, lenpre) == 0;
}
ss_info_t server_list[MAX_STORAGE_SERVERS];
int server_count = 0;
pthread_rwlock_t server_list_rwlock; 
file_info_t* g_file_map = NULL; 
pthread_rwlock_t file_map_rwlock;

typedef struct rep_task {
    char filename[MAX_FILENAME_LEN]; // Logical name
    char service_name[MAX_FILENAME_LEN]; // Physical name
    int source_ss_idx;
    int dest_ss_idx;
    struct rep_task* next;
} rep_task_t;

rep_task_t* g_replication_queue = NULL;
pthread_mutex_t g_rep_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_rep_queue_cond = PTHREAD_COND_INITIALIZER;

// Forward declarations for new threads
void* heartbeat_monitor(void* arg);
void* replication_worker(void* arg);

// --- Forward Declarations for new functions ---
void save_metadata_to_disk();
void load_metadata_from_disk();
void free_file_map();
// --- NEW: Forward Declarations for cache functions ---
static file_info_t* cache_get(const char* filename);
static void cache_put(file_info_t* file_info);
static void cache_invalidate(const char* filename);

bool check_permission(file_info_t* file, const char* username, nm_access_level_t required_level);
// --- NEW CACHE DEFINITIONS ---

int g_last_used_ss_idx = -1; // Keeps track of the last SS assigned
int g_last_used_ss_idx_backup=-1;  
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

typedef struct access_request_s {
    int request_id;
    char filename[MAX_FILENAME_LEN];
    char owner[MAX_USERNAME_LEN];
    char requester[MAX_USERNAME_LEN];
    UT_hash_handle hh; // Make it a hash map for fast lookup by ID
} access_request_t;

static access_request_t* g_access_requests = NULL;
static int g_next_request_id = 1; // Start IDs from 1
static pthread_mutex_t g_request_list_mutex;



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
// In name_server.c
/**
 * @brief Checks if a user has the required permission level for a file.
 * * @param file The file_info_t struct for the file.
 * @param username The user to check.
 * @param required_level The minimum level (NM_ACCESS_READ or NM_ACCESS_WRITE).
 * @return true if permission is granted, false otherwise.
 */
bool check_permission(file_info_t* file, const char* username, nm_access_level_t required_level) {
    if (!file) {
        return false; // File doesn't exist
    }

    // 1. Check if user is the owner (owner always has full access)
    if (strcmp(file->owner, username) == 0) {
        return true;
    }

    // 2. Check the access list for the user
    access_entry_t* found_access;
    HASH_FIND_STR(file->access_list, username, found_access);

    if (!found_access) {
        return false; // User not in list at all
    }

    // 3. Check if their permission level is sufficient
    if (required_level == NM_ACCESS_READ) {
        // Both READ and WRITE permissions are sufficient for a READ request
        return (found_access->level == NM_ACCESS_READ || found_access->level == NM_ACCESS_WRITE);
    }
    
    if (required_level == NM_ACCESS_WRITE) {
        // Only WRITE permission is sufficient for a WRITE request
        return (found_access->level == NM_ACCESS_WRITE);
    }

    return false; // Should not be reached
}
// --- HANDLER: Create Checkpoint ---
void handle_checkpoint(nm_response_t* res, client_request_t req) {
    // 1. Lock & Verify Permissions (READ access is enough to create a copy, 
    //    but usually you want WRITE access to manage the file's versions)
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* file = cache_get(req.filename);
    if (!file) HASH_FIND_STR(g_file_map, req.filename, file);

    if (!file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File not found");
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    // Permission check: Needs READ access to copy the data
    if (!check_permission(file, req.username, NM_ACCESS_READ)) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Permission denied");
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    
    // 2. Forward to SS
    pthread_rwlock_rdlock(&server_list_rwlock);
    int ss_idx = file->ss_index;
    if (ss_idx < 0 || !server_list[ss_idx].active) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Storage Server offline");
        pthread_rwlock_unlock(&server_list_rwlock);
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }

    int ss_sock = connect_to_server(server_list[ss_idx].ip, server_list[ss_idx].client_port);
    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);

    if (ss_sock < 0) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Could not connect to Storage Server");
        return;
    }

    // Send physical name to SS
    strcpy(req.filename, file->service_name); 
    send(ss_sock, &req, sizeof(client_request_t), 0);

    ss_response_t ss_res;
    recv(ss_sock, &ss_res, sizeof(ss_response_t), 0);
    close(ss_sock);

    res->status = ss_res.status;
    strcpy(res->error_msg, ss_res.error_msg);
}

// In name_server.c
// ...

// --- NEW: Handler for CMD_REQUEST_ACCESS ---
void handle_request_access(nm_response_t* res, client_request_t req) {
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* file = cache_get(req.filename);
    if (!file) HASH_FIND_STR(g_file_map, req.filename, file);

    if (!file) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "File not found");
        pthread_rwlock_unlock(&file_map_rwlock); return;
    }
    if (strcmp(file->owner, req.username) == 0) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "You are the owner of this file");
        pthread_rwlock_unlock(&file_map_rwlock); return;
    }
    if (check_permission(file, req.username, NM_ACCESS_READ)) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "You already have access to this file");
        pthread_rwlock_unlock(&file_map_rwlock); return;
    }

    // Capture owner to release file map lock
    char owner_name[MAX_USERNAME_LEN];
    strcpy(owner_name, file->owner);
    pthread_rwlock_unlock(&file_map_rwlock);

    // Now lock the request list
    pthread_mutex_lock(&g_request_list_mutex);
    
    // Check for duplicate request
   access_request_t *current_req, *tmp;
    HASH_ITER(hh, g_access_requests, current_req, tmp) {
        if (strcmp(current_req->filename, req.filename) == 0 && strcmp(current_req->requester, req.username) == 0) {
            res->status = STATUS_ERROR; strcpy(res->error_msg, "You have already requested access for this file");
            pthread_mutex_unlock(&g_request_list_mutex); return;
        }
    }

    access_request_t* new_req = (access_request_t*)malloc(sizeof(access_request_t));
    if (!new_req) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "Server internal error");
        pthread_mutex_unlock(&g_request_list_mutex); return;
    }

    new_req->request_id = g_next_request_id++;
    strcpy(new_req->filename, req.filename);
    strcpy(new_req->owner, owner_name);
    strcpy(new_req->requester, req.username);
    
    HASH_ADD_INT(g_access_requests, request_id, new_req);
    
    pthread_mutex_unlock(&g_request_list_mutex);
    res->status = STATUS_OK;
    printf("   -> New Access Request #%d (%s for %s) logged.\n", new_req->request_id, req.username, req.filename);
}

// --- NEW: Handler for CMD_LIST_REQUESTS ---
void handle_list_requests(int client_fd, client_request_t req) {
    nm_response_t res_header = {0};
    int count = 0;

    pthread_mutex_lock(&g_request_list_mutex);
    
    // 1st pass: Count
    access_request_t *r, *tmp;
    HASH_ITER(hh, g_access_requests, r, tmp) {
        if (strcmp(r->owner, req.username) == 0) {
            count++;
        }
    }

    res_header.status = STATUS_OK;
    res_header.file_count = count; // Re-using file_count
    send(client_fd, &res_header, sizeof(nm_response_t), 0);

    // 2nd pass: Send data
    nm_access_request_entry_t entry;
    HASH_ITER(hh, g_access_requests, r, tmp) {
        if (strcmp(r->owner, req.username) == 0) {
            entry.request_id = r->request_id;
            strcpy(entry.filename, r->filename);
            strcpy(entry.username, r->requester);
            send(client_fd, &entry, sizeof(nm_access_request_entry_t), 0);
        }
    }
    
    pthread_mutex_unlock(&g_request_list_mutex);
}

// --- NEW: Handler for CMD_APPROVE_REQUEST ---
void handle_approve_request(nm_response_t* res, client_request_t req) {
    pthread_mutex_lock(&g_request_list_mutex);
    
    access_request_t* found_req;
    HASH_FIND_INT(g_access_requests, &req.request_id, found_req);

    if (!found_req) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "Request ID not found");
        pthread_mutex_unlock(&g_request_list_mutex); return;
    }
    if (strcmp(found_req->owner, req.username) != 0) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "You are not the owner of this file");
        pthread_mutex_unlock(&g_request_list_mutex); return;
    }

    // Capture details before deleting
    char target_file[MAX_FILENAME_LEN];
    char target_user[MAX_USERNAME_LEN];
    strcpy(target_file, found_req->filename);
    strcpy(target_user, found_req->requester);

    // Delete request
    HASH_DEL(g_access_requests, found_req);
    free(found_req);
    pthread_mutex_unlock(&g_request_list_mutex);

    // Now, add the access (This is the same logic from handle_add_access)
    pthread_rwlock_wrlock(&file_map_rwlock);
    file_info_t* file;
    HASH_FIND_STR(g_file_map, target_file, file);

    if (!file) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "File no longer exists");
        pthread_rwlock_unlock(&file_map_rwlock); return;
    }

    access_entry_t* target_access;
    HASH_FIND_STR(file->access_list, target_user, target_access);
    nm_access_level_t new_level = (req.access_level == ACCESS_WRITE) ? NM_ACCESS_WRITE : NM_ACCESS_READ;

    if (target_access) {
        if (new_level > target_access->level) { // Only upgrade
            target_access->level = new_level;
        }
    } else {
        access_entry_t* new_access = (access_entry_t*)malloc(sizeof(access_entry_t));
        strcpy(new_access->username, target_user);
        new_access->level = new_level;
        HASH_ADD_STR(file->access_list, username, new_access);
    }
    
    cache_invalidate(target_file);
    pthread_rwlock_unlock(&file_map_rwlock);

    res->status = STATUS_OK;
    printf("   -> Request %d approved by %s\n", req.request_id, req.username);
}

// --- NEW: Handler for CMD_DENY_REQUEST ---
void handle_deny_request(nm_response_t* res, client_request_t req) {
    pthread_mutex_lock(&g_request_list_mutex);
    
    access_request_t* found_req;
    HASH_FIND_INT(g_access_requests, &req.request_id, found_req);

    if (!found_req) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "Request ID not found");
        pthread_mutex_unlock(&g_request_list_mutex); return;
    }
    if (strcmp(found_req->owner, req.username) != 0) {
        res->status = STATUS_ERROR; strcpy(res->error_msg, "You are not the owner of this file");
        pthread_mutex_unlock(&g_request_list_mutex); return;
    }

    // Delete request
    HASH_DEL(g_access_requests, found_req);
    free(found_req);
    pthread_mutex_unlock(&g_request_list_mutex);

    res->status = STATUS_OK;
    printf("   -> Request %d denied by %s\n", req.request_id, req.username);
}
// --- ADD: Helper to queue async replication ---
void queue_replication(const char* logical_name, const char* physical_name, int src_idx, int dest_idx) {
    rep_task_t* task = (rep_task_t*)malloc(sizeof(rep_task_t));
    strcpy(task->filename, logical_name);
    strcpy(task->service_name, physical_name);
    task->source_ss_idx = src_idx;
    task->dest_ss_idx = dest_idx;
    task->next = NULL;

    pthread_mutex_lock(&g_rep_queue_mutex);
    task->next = g_replication_queue;
    g_replication_queue = task;
    pthread_cond_signal(&g_rep_queue_cond);
    pthread_mutex_unlock(&g_rep_queue_mutex);
}

// --- ADD: Heartbeat Thread ---
void* heartbeat_monitor(void* arg) {
    while (1) {
        sleep(3); // Check every 3 seconds
        pthread_rwlock_wrlock(&server_list_rwlock);
        for (int i = 0; i < server_count; i++) {
            int sock = connect_to_server(server_list[i].ip, server_list[i].client_port);
            if (sock < 0) {
                if(server_list[i].active==true){
                printf("!! FAILURE DETECTED: SS#%d (%s) is unreachable !!\n", i, server_list[i].ip);}
                server_log(LOG_ERROR, server_list[i].ip, server_list[i].client_port, "NM", "Heartbeat failed. Server marked OFFLINE.");
                server_list[i].active = false;
            } else {
                server_list[i].active = true;
                close(sock);
            }
        }
        pthread_rwlock_unlock(&server_list_rwlock);
    }
    return NULL;
}

// --- ADD: Replication Worker Thread ---
void* replication_worker(void* arg) {
    while (1) {
        pthread_mutex_lock(&g_rep_queue_mutex);
        while (g_replication_queue == NULL) {
            pthread_cond_wait(&g_rep_queue_cond, &g_rep_queue_mutex);
        }
        rep_task_t* task = g_replication_queue;
        g_replication_queue = task->next;
        pthread_mutex_unlock(&g_rep_queue_mutex);

        pthread_rwlock_rdlock(&server_list_rwlock);
        if (!server_list[task->source_ss_idx].active || !server_list[task->dest_ss_idx].active) {
            pthread_rwlock_unlock(&server_list_rwlock);
            free(task); continue;
        }
        char src_ip[MAX_IP_LEN], dest_ip[MAX_IP_LEN];
        int src_port = server_list[task->source_ss_idx].client_port;
        int dest_port = server_list[task->dest_ss_idx].client_port;
        strcpy(src_ip, server_list[task->source_ss_idx].ip);
        strcpy(dest_ip, server_list[task->dest_ss_idx].ip);
        pthread_rwlock_unlock(&server_list_rwlock);

        printf("[RepWorker] Replicating '%s' (SS#%d -> SS#%d)...\n", task->filename, task->source_ss_idx, task->dest_ss_idx);
        server_log(LOG_INFO, "Internal", 0, "REP", "Started replication: '%s' (SS#%d -> SS#%d)", task->filename, task->source_ss_idx, task->dest_ss_idx);
        // Connect to Source to READ
        int src_sock = connect_to_server(src_ip, src_port);
        if (src_sock < 0) { free(task); continue; }
        client_request_t read_req = {0};
        read_req.command = CMD_READ_FILE;
        strcpy(read_req.filename, task->service_name);
        send(src_sock, &read_req, sizeof(client_request_t), 0);
        
        ss_response_t src_res;
        recv(src_sock, &src_res, sizeof(ss_response_t), 0);
        if (src_res.status != STATUS_OK) { close(src_sock); free(task); continue; }
// 1. Connect to Dest SS (WRITE)
int dest_sock = connect_to_server(dest_ip, dest_port);
if (dest_sock < 0) {
    close(src_sock);
    free(task);
    continue;
}

// 2. Prepare Request for Destination
client_request_t write_req = {0};
write_req.command = CMD_REPLICATE; // The new command we added to SS
strcpy(write_req.filename, task->service_name); // Use Physical Name!
send(dest_sock, &write_req, sizeof(client_request_t), 0);

// 3. Wait for Dest SS to say "Ready"
ss_response_t dest_res;
recv(dest_sock, &dest_res, sizeof(ss_response_t), 0);
if (dest_res.status != STATUS_OK) {
    printf("[RepWorker] Dest SS refused replication: %s\n", dest_res.error_msg);
    server_log(LOG_ERROR, "Internal", 0, "REP", "Dest SS refused replication: %s", dest_res.error_msg);
    close(src_sock);
    close(dest_sock);
    free(task);
    continue;
}

// 4. THE DATA PUMP (Source -> Buffer -> Dest)
ss_file_data_chunk_t chunk;
while (1) {
    // Read chunk from Source
    ssize_t n = recv(src_sock, &chunk, sizeof(ss_file_data_chunk_t), 0);
    
    // Check for End of Stream or Error
    if (n <= 0 || chunk.data_size == 0) break;

    // Write raw bytes to Dest
    // Note: We send raw bytes, not the struct, because SS replication handler expects raw stream
    if (send(dest_sock, chunk.data, chunk.data_size, 0) < 0) {
        perror("[RepWorker] Failed to send data to Dest SS");
        server_log(LOG_ERROR, "Internal", 0, "REP", "Failed to send replication data: %s", strerror(errno));
        break;
    }
}

// 5. Cleanup
close(src_sock);
close(dest_sock);
printf("[RepWorker] Replication task for '%s' finished successfully.\n", task->filename);
server_log(LOG_INFO, "Internal", 0, "REP", "Replication finished for '%s'", task->filename);
free(task);
        
    }
    return NULL;
}


void handle_list_checkpoints(int client_fd, client_request_t req) {
    nm_response_t res = {0};
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* f = cache_get(req.filename);
    if (!f) HASH_FIND_STR(g_file_map, req.filename, f);
    if (!f || !check_permission(f, req.username, NM_ACCESS_READ)) {
        res.status=STATUS_ERROR; strcpy(res.error_msg, "Access denied");
        pthread_rwlock_unlock(&file_map_rwlock); send(client_fd, &res, sizeof(res), 0); return;
    }

    pthread_rwlock_rdlock(&server_list_rwlock);
    int target = -1;
    if(f->ss_index!=-1 && server_list[f->ss_index].active) target=f->ss_index;
    else if(f->ss_index_backup!=-1 && server_list[f->ss_index_backup].active) target=f->ss_index_backup;

    if(target==-1) {
        res.status=STATUS_ERROR; strcpy(res.error_msg, "Servers offline");
        pthread_rwlock_unlock(&server_list_rwlock); pthread_rwlock_unlock(&file_map_rwlock);
        send(client_fd, &res, sizeof(res), 0); return;
    }
    
    int sock = connect_to_server(server_list[target].ip, server_list[target].client_port);
    strcpy(req.filename, f->service_name);
    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);

    if(sock<0) {
         res.status=STATUS_ERROR; strcpy(res.error_msg, "Connect failed");
         send(client_fd, &res, sizeof(res), 0); return;
    }

    send(sock, &req, sizeof(client_request_t), 0);
    recv(sock, &res, sizeof(nm_response_t), 0);
    send(client_fd, &res, sizeof(nm_response_t), 0);

    if(res.status==STATUS_OK) {
        char buf[MAX_FILENAME_LEN];
        for(int i=0; i<res.file_count; i++) {
            recv(sock, buf, MAX_FILENAME_LEN, 0);
            send(client_fd, buf, MAX_FILENAME_LEN, 0);
        }
    }
    close(sock);
}
void handle_revert_checkpoint(nm_response_t* res, client_request_t req) {
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* f = cache_get(req.filename);
    if (!f) HASH_FIND_STR(g_file_map, req.filename, f);
    if (!f || !check_permission(f, req.username, NM_ACCESS_WRITE)) {
        res->status=STATUS_ERROR; strcpy(res->error_msg, "Access denied/File not found"); 
        pthread_rwlock_unlock(&file_map_rwlock); return;
    }
    int p=f->ss_index, b=f->ss_index_backup;
    char s_name[MAX_FILENAME_LEN]; strcpy(s_name, f->service_name);
    pthread_rwlock_unlock(&file_map_rwlock);

    int success=0;
    pthread_rwlock_rdlock(&server_list_rwlock);
    if(p!=-1 && server_list[p].active) {
        int sock = connect_to_server(server_list[p].ip, server_list[p].client_port);
        if(sock>=0) {
            strcpy(req.filename, s_name); send(sock, &req, sizeof(client_request_t), 0);
            ss_response_t r; recv(sock, &r, sizeof(r), 0); close(sock);
            if(r.status==STATUS_OK) success++;
        }
    }
    if(b!=-1 && server_list[b].active) {
        int sock = connect_to_server(server_list[b].ip, server_list[b].client_port);
        if(sock>=0) {
            strcpy(req.filename, s_name); send(sock, &req, sizeof(client_request_t), 0);
            ss_response_t r; recv(sock, &r, sizeof(r), 0); close(sock);
            if(r.status==STATUS_OK) success++;
        }
    }
    pthread_rwlock_unlock(&server_list_rwlock);
    
    if(success>0) res->status=STATUS_OK;
    else { res->status=STATUS_ERROR; strcpy(res->error_msg, "Revert failed on all copies"); }
}
void handle_view_checkpoint(nm_response_t* res, client_request_t req) {
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* f = cache_get(req.filename);
    if (!f) HASH_FIND_STR(g_file_map, req.filename, f);
    if (!f || !check_permission(f, req.username, NM_ACCESS_READ)) {
        res->status=STATUS_ERROR; strcpy(res->error_msg, "Access denied");
        pthread_rwlock_unlock(&file_map_rwlock); return;
    }

    pthread_rwlock_rdlock(&server_list_rwlock);
    int target = -1;
    if(f->ss_index!=-1 && server_list[f->ss_index].active) target=f->ss_index;
    else if(f->ss_index_backup!=-1 && server_list[f->ss_index_backup].active) target=f->ss_index_backup;
    
    if(target!=-1) {
        strcpy(res->ss_ip, server_list[target].ip);
        res->ss_port = server_list[target].client_port;
        snprintf(res->storage_filename, MAX_FILENAME_LEN, "%.190s.cp.%.50s", f->service_name, req.checkpoint_tag);
        res->status=STATUS_OK;
    } else {
        res->status=STATUS_ERROR; strcpy(res->error_msg, "All copies offline");
    }
    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);
}

void handle_create_folder(nm_response_t* res, client_request_t req) {
    pthread_rwlock_wrlock(&file_map_rwlock);

    file_info_t* found_file;
    HASH_FIND_STR(g_file_map, req.filename, found_file);

    if (found_file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Folder or file already exists");
        pthread_rwlock_unlock(&file_map_rwlock); // <-- Unlock before returning
        return;
    }

    // --- NEW PARENT DIRECTORY VALIDATION ---
    char parent_path[MAX_FILENAME_LEN] = {0};
    char* last_slash = strrchr(req.filename, '/');

    if (last_slash != NULL) { // This is a sub-folder, e.g., "folder1/folder2"
        size_t parent_len = last_slash - req.filename;
        
        if (parent_len == 0) {
            // Path starts with '/', e.g., "/folder2". Treat root as parent.
            // Root always exists, so no check needed.
        } else {
            // Path is "folder1/folder2". Check for "folder1".
            strncpy(parent_path, req.filename, parent_len);
            parent_path[parent_len] = '\0'; // Manually null-terminate
            
            file_info_t* parent_folder;
            HASH_FIND_STR(g_file_map, parent_path, parent_folder);

            if (!parent_folder || !parent_folder->is_directory) {
                res->status = STATUS_ERROR;
                strcpy(res->error_msg, "Parent directory does not exist or is not a folder");
                pthread_rwlock_unlock(&file_map_rwlock);
                return;
            }
        }
    }
    // --- END PARENT DIRECTORY VALIDATION ---

    // If we're here, the parent exists (or it's a root folder)
    // and the new folder name is unique. Proceed with creation.
    
    file_info_t* new_folder = (file_info_t*)calloc(1, sizeof(file_info_t));
    strcpy(new_folder->filename, req.filename);
    strcpy(new_folder->owner, req.username);
    new_folder->is_directory = true;
    new_folder->ss_index = -1; // Folders don't live on an SS
    
    // Add owner access
    access_entry_t* owner_access = (access_entry_t*)malloc(sizeof(access_entry_t));
    strcpy(owner_access->username, req.username);
    owner_access->level = NM_ACCESS_WRITE;
    HASH_ADD_STR(new_folder->access_list, username, owner_access);

    HASH_ADD_STR(g_file_map, filename, new_folder);
    
    res->status = STATUS_OK;
    printf("-> Folder Created: '%s'\n", req.filename);
    server_log(LOG_INFO, "Client", 0, req.username, "Created folder '%s'", req.filename);
    pthread_rwlock_unlock(&file_map_rwlock);
}

// In name_server.c
// (You'll need this helper struct definition inside the function)

// In name_server.c

// In name_server.c

void handle_move_file(nm_response_t* res, client_request_t req) {
    // Helper struct for the two-pass move
    typedef struct move_node {
        file_info_t* file;
        struct move_node* next;
    } move_node_t;

    pthread_rwlock_wrlock(&file_map_rwlock);

    file_info_t* file;
    HASH_FIND_STR(g_file_map, req.filename, file);

    // 1. Check source
    if (!file) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Source file not found");
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }
    if (!check_permission(file, req.username, NM_ACCESS_WRITE)) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "Permission denied");
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }

    // 2. Check destination folder
    if (strlen(req.dest_path) > 0) {
        file_info_t* folder;
        HASH_FIND_STR(g_file_map, req.dest_path, folder);
        if (!folder || !folder->is_directory) {
            res->status = STATUS_ERROR;
            strcpy(res->error_msg, "Destination folder does not exist");
            pthread_rwlock_unlock(&file_map_rwlock);
            return;
        }
    }

    // 3. Construct new logical path for the item being moved
    char new_full_path[MAX_FILENAME_LEN];
    char* leaf = strrchr(file->filename, '/');
    const char* just_name = leaf ? leaf + 1 : file->filename;
    int written_len;

    if (strlen(req.dest_path) == 0) {
        // Move to root
        written_len = snprintf(new_full_path, sizeof(new_full_path), "%s", just_name);
    } else {
        // Move to folder
        written_len = snprintf(new_full_path, sizeof(new_full_path), "%s/%s", req.dest_path, just_name);
    }

    // Check for truncation (path too long)
    if (written_len < 0 || written_len >= sizeof(new_full_path)) {
        res->status = STATUS_ERROR;
        res->error_code = NFS_ERR_INVALID_FILENAME;
        strcpy(res->error_msg, "Resulting path is too long");
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }

    // 4. Check collision
    file_info_t* collision;
    HASH_FIND_STR(g_file_map, new_full_path, collision);
    if (collision) {
        res->status = STATUS_ERROR;
        strcpy(res->error_msg, "File already exists in destination");
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }

    // 5. Perform the Move
    if (!file->is_directory) {
        
        // --- A) This is a FILE move. ---
        file_info_t* new_entry = (file_info_t*)malloc(sizeof(file_info_t));
        memcpy(new_entry, file, sizeof(file_info_t));
        strcpy(new_entry->filename, new_full_path); // Update key
        
        file->access_list = NULL; // Move access list pointer

        HASH_ADD_STR(g_file_map, filename, new_entry);
        HASH_DEL(g_file_map, file);
        free(file);

        cache_invalidate(req.filename);
        
        res->status = STATUS_OK;
        printf("-> Moved file '%s' to '%s'\n", req.filename, new_full_path);
        server_log(LOG_INFO, "Client", 0, req.username, "Moved file '%s' to '%s'", req.filename, new_full_path);
        
    } else {
        
        // --- B) This is a FOLDER move. ---
        
        // 1. Create prefixes for renaming
        char old_prefix[MAX_FILENAME_LEN + 2]; // e.g., "folder1/"
        snprintf(old_prefix, sizeof(old_prefix), "%s/", req.filename);
        size_t old_prefix_len = strlen(old_prefix);

        char new_prefix[MAX_FILENAME_LEN + 2]; // e.g., "folder2/folder1/"
        snprintf(new_prefix, sizeof(new_prefix), "%s/", new_full_path);
        
        // 2. Pass 1: Build temporary list of items to move
        move_node_t* move_list = NULL;
        file_info_t* current_file, *tmp;
        
        HASH_ITER(hh, g_file_map, current_file, tmp) {
            if (starts_with(old_prefix, current_file->filename)) {
                move_node_t* node = (move_node_t*)malloc(sizeof(move_node_t));
                node->file = current_file;
                node->next = move_list;
                move_list = node;
            }
        }
        
        move_node_t* self_node = (move_node_t*)malloc(sizeof(move_node_t));
        self_node->file = file; // Add the folder itself
        self_node->next = move_list;
        move_list = self_node;

        // 3. Pass 2: Rename and re-hash
        move_node_t* current_node = move_list;
        while (current_node) {
            file_info_t* f = current_node->file;
            char old_name[MAX_FILENAME_LEN];
            strcpy(old_name, f->filename); // Store old name

            // Generate new name
            char new_name[MAX_FILENAME_LEN]; // Final destination buffer
            int child_path_len;
            
            if (strcmp(old_name, req.filename) == 0) {
                // This is the root folder itself
                child_path_len = snprintf(new_name, sizeof(new_name), "%s", new_full_path);
            } else {
                // This is a child. new_name = new_prefix + (old_name - old_prefix_len)
                char* remainder = f->filename + old_prefix_len;
                child_path_len = snprintf(new_name, sizeof(new_name), "%s%s", new_prefix, remainder);
            }

            // Check for path length overflow on children
            if (child_path_len < 0 || child_path_len >= sizeof(new_name)) {
                 printf("   !! WARNING: Skipping move for '%s', new path is too long.\n", old_name);
            } else {
                // Re-hash: Delete old key, update name field, add new key
                HASH_DEL(g_file_map, f);
                strcpy(f->filename, new_name); // Update the key
                HASH_ADD_STR(g_file_map, filename, f);
                
                cache_invalidate(old_name); // Invalidate old name
            }
            
            // Free the temp list node
            move_node_t* next = current_node->next;
            free(current_node);
            current_node = next;
        }

        res->status = STATUS_OK;
        printf("-> Moved folder '%s' and all descendants to '%s'\n", req.filename, new_full_path);
        server_log(LOG_INFO, "Client", 0, req.username, "Moved folder '%s' and descendants to '%s'", req.filename, new_full_path);
    }

    pthread_rwlock_unlock(&file_map_rwlock);
}
void handle_view_folder(int conn_fd, client_request_t req) {
    nm_response_t res_header = {0};
    int file_count = 0;

    // 1. Hold read lock
    pthread_rwlock_rdlock(&file_map_rwlock);
    
    // 2. Count matching files
    file_info_t *current_file, *tmp;
    // FIX: Increase size slightly to hold the trailing '/' without truncation warning
char prefix[MAX_FILENAME_LEN + 5];
    
    // Logic: If req.filename is empty, view root. Otherwise view specific folder.
    if (strlen(req.filename) > 0) {
        snprintf(prefix, sizeof(prefix), "%s/", req.filename); 
    } else {
        prefix[0] = '\0'; // Root view
    }

    HASH_ITER(hh, g_file_map, current_file, tmp) {
        // Check if file is inside the requested folder
        if (strlen(prefix) == 0 || starts_with(prefix, current_file->filename)) {
            file_count++;
        }
    }

    // 3. Send Header
    res_header.status = STATUS_OK;
    res_header.error_code = NFS_OK;
    res_header.file_count = file_count;
    
    if (send(conn_fd, &res_header, sizeof(nm_response_t), 0) < 0) {
        perror("send VIEW_FOLDER header failed");
        pthread_rwlock_unlock(&file_map_rwlock);
        return;
    }

    // 4. Send Entries (This is where the fix is applied)
    nm_file_entry_t entry;
    HASH_ITER(hh, g_file_map, current_file, tmp) {
        if (strlen(prefix) == 0 || starts_with(prefix, current_file->filename)) {
             
             // Copy Basic Info
             strcpy(entry.filename, current_file->filename);
             strcpy(entry.owner, current_file->owner);
             
             // --- FIX START: Populate SS details and Physical Name ---
             // We need to check if the SS index is valid to avoid segfaults
             pthread_rwlock_rdlock(&server_list_rwlock); 
             if (!current_file->is_directory && 
                  current_file->ss_index >= 0 && 
                  current_file->ss_index < server_count && 
                  server_list[current_file->ss_index].active) {
                 
                 strcpy(entry.ss_ip, server_list[current_file->ss_index].ip);
                 entry.ss_port = server_list[current_file->ss_index].client_port;
                 // Map Logical Name -> Physical Name so client can talk to SS
                 strcpy(entry.storage_filename, current_file->service_name); 
                 
             } else {
                 // It's a folder, or the SS is down/invalid
                 strcpy(entry.ss_ip, "N/A");
                 entry.ss_port = 0;
                 entry.storage_filename[0] = '\0'; 
             }
             pthread_rwlock_unlock(&server_list_rwlock);
             // --- FIX END ---

             send(conn_fd, &entry, sizeof(nm_file_entry_t), 0);
        }
    }
    
    pthread_rwlock_unlock(&file_map_rwlock);
}
// --- REPLACE handle_exec_file WITH THIS ---
void handle_exec_file(int client_conn_fd, client_request_t req) {
    printf("   Handling CMD_EXEC for '%s' by user '%s'\n", req.filename, req.username);
    nm_response_t res_header = {0};
    char ss_ip[MAX_IP_LEN]; int ss_port;

    // 1. Lookup & Auth
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* file = cache_get(req.filename);
    if (!file) { HASH_FIND_STR(g_file_map, req.filename, file); if(file) cache_put(file); }
    
    pthread_rwlock_rdlock(&server_list_rwlock);
    if (!file) {
        res_header.status = STATUS_ERROR; res_header.error_code = NFS_ERR_FILE_NOT_FOUND;
        strcpy(res_header.error_msg, "File not found");
    } else if (!check_permission(file, req.username, NM_ACCESS_READ)) {
        res_header.status = STATUS_ERROR; res_header.error_code = NFS_ERR_PERMISSION_DENIED;
        strcpy(res_header.error_msg, "Permission denied");
    } else {
        // Failover Logic
        int target = -1;
        if (server_list[file->ss_index].active) target = file->ss_index;
        else if (file->ss_index_backup != -1 && server_list[file->ss_index_backup].active) {
            printf("   [Failover] Executing on Backup SS#%d\n", file->ss_index_backup);
            target = file->ss_index_backup;
        }

        if (target != -1) {
            res_header.status = STATUS_OK; res_header.error_code = NFS_OK;
            strcpy(ss_ip, server_list[target].ip);
            ss_port = server_list[target].client_port;
            strcpy(req.filename, file->service_name); // Use physical name
        } else {
            res_header.status = STATUS_ERROR; res_header.error_code = NFS_ERR_SS_DOWN;
            strcpy(res_header.error_msg, "All copies offline");
        }
    }
    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);

    if (res_header.status == STATUS_ERROR) {
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0); return;
    }

    // 2. Fetch script from SS
    int ss_sock = connect_to_server(ss_ip, ss_port);
    if (ss_sock < 0) {
        res_header.status = STATUS_ERROR; strcpy(res_header.error_msg, "NM could not connect to SS");
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0); return;
    }
    req.command = CMD_READ_FILE;
    send(ss_sock, &req, sizeof(client_request_t), 0);
    ss_response_t ss_res; recv(ss_sock, &ss_res, sizeof(ss_response_t), 0);

    if (ss_res.status == STATUS_ERROR) {
        res_header.status = STATUS_ERROR; strcpy(res_header.error_msg, ss_res.error_msg);
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0);
        close(ss_sock); return;
    }

    char temp_filename[] = "/tmp/nm_exec_XXXXXX";
    int temp_fd = mkstemp(temp_filename);
    if (temp_fd < 0) {
        res_header.status = STATUS_ERROR; strcpy(res_header.error_msg, "NM internal error (mkstemp)");
        send(client_conn_fd, &res_header, sizeof(nm_response_t), 0);
        close(ss_sock); return;
    }

    ss_file_data_chunk_t chunk;
    while (recv(ss_sock, &chunk, sizeof(ss_file_data_chunk_t), 0) == sizeof(ss_file_data_chunk_t) && chunk.data_size > 0) {
        write(temp_fd, chunk.data, chunk.data_size);
    }
    close(temp_fd); close(ss_sock);

    // 3. Execute & Pipe
    send(client_conn_fd, &res_header, sizeof(nm_response_t), 0); // Send OK header
    
    char cmd[MAX_FILENAME_LEN + 10]; sprintf(cmd, "sh %s 2>&1", temp_filename);
    FILE* pipe = popen(cmd, "r");
    if (pipe) {
        char buf[4096];
        while (fgets(buf, sizeof(buf), pipe)) {
            if (send(client_conn_fd, buf, strlen(buf), 0) < 0) break;
        }
        pclose(pipe);
    } else {
        send(client_conn_fd, "NM Error: popen failed\n", 24, 0);
    }
    unlink(temp_filename);
}

// In name_server.c
void handle_create_file(nm_response_t* res, client_request_t req) {
    // 1. Check Cache/Map
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* found_file = cache_get(req.filename);
    if (!found_file) HASH_FIND_STR(g_file_map, req.filename, found_file);
    pthread_rwlock_unlock(&file_map_rwlock);

    if (found_file) {
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_FILE_ALREADY_EXISTS;
        strcpy(res->error_msg, "File already exists"); return;
    }
    pthread_rwlock_rdlock(&file_map_rwlock);
    // --- VALIDATE PARENT FOLDER ---
    char* last_slash = strrchr(req.filename, '/');
    if (last_slash != NULL) {
        char parent_path[MAX_FILENAME_LEN];
        size_t parent_len = last_slash - req.filename;
        
        if (parent_len > 0) {
            strncpy(parent_path, req.filename, parent_len);
            parent_path[parent_len] = '\0';
            
            file_info_t* parent_folder;
            // Check cache first, then map
            parent_folder = cache_get(parent_path); 
            if (!parent_folder) HASH_FIND_STR(g_file_map, parent_path, parent_folder);

            if (!parent_folder || !parent_folder->is_directory) {
                res->status = STATUS_ERROR;
                res->error_code = NFS_ERR_FILE_NOT_FOUND; 
                strcpy(res->error_msg, "Parent folder does not exist");
                pthread_rwlock_unlock(&file_map_rwlock); // Don't forget to unlock!
                return;
            }
        }
    }
    pthread_rwlock_unlock(&file_map_rwlock);
    // ------------------------------
    // 2. Find TWO Active Servers
    int primary_idx = -1, backup_idx = -1;
    pthread_rwlock_rdlock(&server_list_rwlock);
    if (server_count > 0) {
        int start = (g_last_used_ss_idx + 1) % server_count;
        // Find Primary
        for (int i = 0; i < server_count; i++) {
            int idx = (start + i) % server_count;
            if (server_list[idx].active) {
                primary_idx = idx;
                g_last_used_ss_idx = idx;
                break;
            }
        }
        // Find Backup
        if (primary_idx != -1) {
            for (int i = 1; i < server_count; i++) {
                int idx = (primary_idx + i) % server_count;
                if (server_list[idx].active) {
                    backup_idx = idx;
                    break;
                }
            }
        }
    }
    char p_ip[MAX_IP_LEN], b_ip[MAX_IP_LEN];
    int p_port, b_port;
    if(primary_idx != -1) { strcpy(p_ip, server_list[primary_idx].ip); p_port = server_list[primary_idx].client_port; }
    if(backup_idx != -1) { strcpy(b_ip, server_list[backup_idx].ip); b_port = server_list[backup_idx].client_port; }
    pthread_rwlock_unlock(&server_list_rwlock);

    if (primary_idx == -1) {
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_SS_DOWN;
        strcpy(res->error_msg, "No Storage Servers available"); return;
    }

    // 3. Generate Physical Name (Flattened)
    client_request_t ss_req = req;
    // *** FIX IS HERE ***
    // Changed separator from '_' to '^' to avoid collision
    for (int i = 0; ss_req.filename[i]; i++) if (ss_req.filename[i] == '/') ss_req.filename[i] = '^';

    // 4. Create on Primary
    int ss_sock = connect_to_server(p_ip, p_port);
    if (ss_sock < 0) { 
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_SS_DOWN;
        strcpy(res->error_msg, "Failed to connect to Primary SS"); return; 
    }
    send(ss_sock, &ss_req, sizeof(client_request_t), 0);
    ss_response_t ss_res;
    recv(ss_sock, &ss_res, sizeof(ss_response_t), 0);
    close(ss_sock);
    if (ss_res.status == STATUS_ERROR) {
        res->status = STATUS_ERROR; res->error_code = ss_res.error_code;
        strncpy(res->error_msg, ss_res.error_msg, MAX_ERROR_MSG_LEN - 1); return;
    }

    // 5. Create on Backup (Best Effort)
    if (backup_idx != -1) {
        int bk_sock = connect_to_server(b_ip, b_port);
        if (bk_sock >= 0) {
            send(bk_sock, &ss_req, sizeof(client_request_t), 0);
            ss_response_t bk_res; recv(bk_sock, &bk_res, sizeof(ss_response_t), 0);
            close(bk_sock);
        } else { backup_idx = -1; }
    }

    // 6. Update Metadata
    pthread_rwlock_wrlock(&file_map_rwlock);
    HASH_FIND_STR(g_file_map, req.filename, found_file); // Race check
    if (found_file) {
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_FILE_ALREADY_EXISTS;
        strcpy(res->error_msg, "Race condition: File created"); 
        pthread_rwlock_unlock(&file_map_rwlock); return;
    }

    file_info_t* new_file = (file_info_t*)calloc(1, sizeof(file_info_t));
    strcpy(new_file->filename, req.filename);
    strcpy(new_file->service_name, ss_req.filename); // Store the flattened name
    strcpy(new_file->owner, req.username);
    new_file->ss_index = primary_idx;
    new_file->ss_index_backup = backup_idx;
    new_file->is_directory = false;

    access_entry_t* owner_access = (access_entry_t*)malloc(sizeof(access_entry_t));
    strcpy(owner_access->username, req.username);
    owner_access->level = NM_ACCESS_WRITE;
    HASH_ADD_STR(new_file->access_list, username, owner_access);

    HASH_ADD_STR(g_file_map, filename, new_file);
    cache_put(new_file);
    pthread_rwlock_unlock(&file_map_rwlock);

    res->status = STATUS_OK; res->error_code = NFS_OK;
    printf("-> Created '%s' on SS#%d (Backup: SS#%d)\n", req.filename, primary_idx, backup_idx);
}
// --- REPLACE handle_delete_file WITH THIS ---
void handle_delete_file(nm_response_t* res, client_request_t req) {
    // 1. Check Metadata (Cache/Map)
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* found_file = cache_get(req.filename);
    if (!found_file) {
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        if (found_file) cache_put(found_file);
    }
    
    if (!found_file) {
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res->error_msg, "File not found");
        pthread_rwlock_unlock(&file_map_rwlock); return;
    } 
    if (strcmp(found_file->owner, req.username) != 0) {
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res->error_msg, "Permission denied (Only owner can delete)");
        pthread_rwlock_unlock(&file_map_rwlock); return;
    }

    // Capture info to release lock early (avoids deadlock during net ops)
    int p_idx = found_file->ss_index;
    int b_idx = found_file->ss_index_backup;
    char phys_name[MAX_FILENAME_LEN];
    strcpy(phys_name, found_file->service_name);
    pthread_rwlock_unlock(&file_map_rwlock);

    bool deleted_any = false;
    pthread_rwlock_rdlock(&server_list_rwlock);

    // 2. Delete from Primary
    if (p_idx != -1 && server_list[p_idx].active) {
        int sock = connect_to_server(server_list[p_idx].ip, server_list[p_idx].client_port);
        if (sock >= 0) {
            client_request_t del_req = req;
            strcpy(del_req.filename, phys_name); // Use physical name
            send(sock, &del_req, sizeof(client_request_t), 0);
            ss_response_t ss_res;
            recv(sock, &ss_res, sizeof(ss_response_t), 0);
            close(sock);
            if (ss_res.status == STATUS_OK) deleted_any = true;
        }
    }

    // 3. Delete from Backup (Best Effort)
    if (b_idx != -1 && server_list[b_idx].active) {
        int sock = connect_to_server(server_list[b_idx].ip, server_list[b_idx].client_port);
        if (sock >= 0) {
            client_request_t del_req = req;
            strcpy(del_req.filename, phys_name);
            send(sock, &del_req, sizeof(client_request_t), 0);
            // We don't check response here, primary deletion or metadata removal is key
            ss_response_t ss_res; recv(sock, &ss_res, sizeof(ss_response_t), 0);
            close(sock);
        }
    }
    pthread_rwlock_unlock(&server_list_rwlock);

    // 4. Remove Metadata
    if (deleted_any || (b_idx != -1)) { // Proceed if at least one path worked
        pthread_rwlock_wrlock(&file_map_rwlock);
        HASH_FIND_STR(g_file_map, req.filename, found_file); // Re-find atomically
        if (found_file) {
            access_entry_t *acc, *tmp;
            HASH_ITER(hh, found_file->access_list, acc, tmp) {
                HASH_DEL(found_file->access_list, acc); free(acc);
            }
            HASH_DEL(g_file_map, found_file);
            free(found_file);
            cache_invalidate(req.filename);

            res->status = STATUS_OK; res->error_code = NFS_OK;
            printf("-> Deleted '%s' (User: %s)\n", req.filename, req.username);
        } else {
            res->status = STATUS_ERROR; strcpy(res->error_msg, "File already deleted");
        }
        pthread_rwlock_unlock(&file_map_rwlock);
    } else {
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_SS_DOWN;
        strcpy(res->error_msg, "Could not contact Primary or Backup server");
    }
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
        strcpy(file_entry.storage_filename, current_file->service_name); // FIX
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
        int target_ss = -1;
        // FAULT TOLERANCE: Check Primary, then Backup
        if (server_list[found_file->ss_index].active) {
            target_ss = found_file->ss_index;
        } else if (found_file->ss_index_backup != -1 && server_list[found_file->ss_index_backup].active) {
            printf("   [Failover] Primary SS#%d down. Reading from Backup SS#%d\n", 
                   found_file->ss_index, found_file->ss_index_backup);
            target_ss = found_file->ss_index_backup;
        }

        if (target_ss != -1) {
            res->status = STATUS_OK; res->error_code = NFS_OK;
            strcpy(res->ss_ip, server_list[target_ss].ip);
            res->ss_port = server_list[target_ss].client_port;
            strcpy(res->storage_filename, found_file->service_name);
        } else {
            res->status = STATUS_ERROR; res->error_code = NFS_ERR_SS_DOWN;
            strcpy(res->error_msg, "File unavailable (All copies offline)");
        }
    }
    
    pthread_rwlock_unlock(&server_list_rwlock);
    pthread_rwlock_unlock(&file_map_rwlock);  
}

void handle_write_file(nm_response_t* res, client_request_t req) {
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* found_file = cache_get(req.filename);
    if (!found_file) {
        HASH_FIND_STR(g_file_map, req.filename, found_file);
        if (found_file) cache_put(found_file);
    }
    pthread_rwlock_rdlock(&server_list_rwlock);

    if (!found_file) {
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_FILE_NOT_FOUND;
        strcpy(res->error_msg, "File not found");
    } else if (!check_permission(found_file, req.username, NM_ACCESS_WRITE)) {
        res->status = STATUS_ERROR; res->error_code = NFS_ERR_PERMISSION_DENIED;
        strcpy(res->error_msg, "Permission denied (Write access required)");
    } else {
        int target_ss = -1;
        // Write to Primary if UP
        if (server_list[found_file->ss_index].active) {
            target_ss = found_file->ss_index;
        } 
        // Failover Write to Backup
        else if (found_file->ss_index_backup != -1 && server_list[found_file->ss_index_backup].active) {
             printf("   [Failover] Primary SS#%d down. Writing to Backup SS#%d\n", 
                   found_file->ss_index, found_file->ss_index_backup);
             target_ss = found_file->ss_index_backup;
        }

        if (target_ss != -1) {
            res->status = STATUS_OK; res->error_code = NFS_OK;
            strcpy(res->ss_ip, server_list[target_ss].ip);
            res->ss_port = server_list[target_ss].client_port;
            strcpy(res->storage_filename, found_file->service_name);
        } else {
            res->status = STATUS_ERROR; res->error_code = NFS_ERR_SS_DOWN;
            strcpy(res->error_msg, "Storage Server offline");
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
// --- REPLACE handle_get_info WITH THIS ---
void handle_get_info(int conn_fd, client_request_t req) {
    nm_info_response_t res = {0};
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t* f = cache_get(req.filename);
    if (!f) { HASH_FIND_STR(g_file_map, req.filename, f); if(f) cache_put(f); }
    
    pthread_rwlock_rdlock(&server_list_rwlock);
    if (!f) {
        res.status = STATUS_ERROR; res.error_code = NFS_ERR_FILE_NOT_FOUND; 
        strcpy(res.error_msg, "File not found");
    } else if (!check_permission(f, req.username, NM_ACCESS_READ)) {
        res.status = STATUS_ERROR; res.error_code = NFS_ERR_PERMISSION_DENIED; 
        strcpy(res.error_msg, "Permission denied");
    } else {
        int target = -1;
        if (server_list[f->ss_index].active) target = f->ss_index;
        else if (f->ss_index_backup != -1 && server_list[f->ss_index_backup].active) target = f->ss_index_backup;

        if (target != -1) {
            res.status = STATUS_OK; res.error_code = NFS_OK;
            strcpy(res.owner, f->owner);
            strcpy(res.ss_ip, server_list[target].ip);
            res.ss_port = server_list[target].client_port;
            res.acl_count = HASH_COUNT(f->access_list);
            strcpy(res.storage_filename, f->service_name);
        } else {
            res.status = STATUS_ERROR; res.error_code = NFS_ERR_SS_DOWN;
            strcpy(res.error_msg, "Storage Server offline");
        }
    }
    pthread_rwlock_unlock(&server_list_rwlock);

    send(conn_fd, &res, sizeof(nm_info_response_t), 0);
    if (res.status == STATUS_OK) {
        nm_acl_entry_t acl; access_entry_t *a, *tmp;
        HASH_ITER(hh, f->access_list, a, tmp) {
            strcpy(acl.username, a->username);
            acl.level = (a->level == NM_ACCESS_WRITE) ? ACCESS_WRITE : ACCESS_READ;
            send(conn_fd, &acl, sizeof(nm_acl_entry_t), 0);
        }
    }
    pthread_rwlock_unlock(&file_map_rwlock);
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
                if (!server_list[ss_idx].active) {
                printf("-> SS#%d RECOVERED. Starting Sync...\n", ss_idx);
                server_list[ss_idx].active = true;
                
                // RECOVERY LOGIC: Find files where this SS is Primary or Backup and sync
                pthread_rwlock_rdlock(&file_map_rwlock);
                file_info_t *f, *tmp;
                HASH_ITER(hh, g_file_map, f, tmp) {
                    // If Recovered SS is Primary, fetch from Backup
                    if (f->ss_index == ss_idx) {
                        if (f->ss_index_backup != -1 && server_list[f->ss_index_backup].active) {
                            queue_replication(f->filename, f->service_name, f->ss_index_backup, ss_idx);
                        }
                    } 
                    // If Recovered SS is Backup, fetch from Primary
                    else if (f->ss_index_backup == ss_idx) {
                        if (server_list[f->ss_index].active) {
                            queue_replication(f->filename, f->service_name, f->ss_index, ss_idx);
                        }
                    }
                }
                pthread_rwlock_unlock(&file_map_rwlock);
            }
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
        
    }else if (msg_type == MSG_SS_UPDATE_NOTIFY) {
    ss_notify_arg_t payload;
    recv(conn_fd, &payload, sizeof(payload), 0);
    
    printf("   [NM] Received update notification for '%s'\n", payload.service_name);

    // We need to find which file this is to know the backup server
    pthread_rwlock_rdlock(&file_map_rwlock);
    file_info_t *f, *tmp;
    
    // Note: Our hash map is keyed by Logical Name, but we only have Physical Name.
    // We must iterate to find it. (Acceptable for async tasks)
    HASH_ITER(hh, g_file_map, f, tmp) {
        if (strcmp(f->service_name, payload.service_name) == 0) {
            // Found the file! Check if it needs replication.
            if (f->ss_index_backup != -1) {
                 // Important: Check if backup is actually active before queuing
                 pthread_rwlock_rdlock(&server_list_rwlock);
                 bool backup_active = server_list[f->ss_index_backup].active;
                 pthread_rwlock_unlock(&server_list_rwlock);

                 if (backup_active) {
                     queue_replication(f->filename, f->service_name, 
                                       f->ss_index, f->ss_index_backup);
                     printf("   [NM] Queued replication: SS%d -> SS%d\n", 
                            f->ss_index, f->ss_index_backup);
                 }
            }
            break; 
        }
    }
    pthread_rwlock_unlock(&file_map_rwlock);
} 
    else if (msg_type == MSG_CLIENT_NM_REQUEST) {
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
        } else if (req.command == CMD_CREATE_FOLDER) {
            printf("   Handling CMD_CREATE_FOLDER for '%s'\n", req.filename);
            handle_create_folder(&res, req);
        
        } else if (req.command == CMD_MOVE_FILE) {
            printf("   Handling CMD_MOVE_FILE for '%s' -> '%s'\n", req.filename, req.dest_path);
            handle_move_file(&res, req);

        } else if (req.command == CMD_VIEW_FOLDER) {
            printf("   Handling CMD_VIEW_FOLDER for '%s'\n", req.filename);
            handle_view_folder(conn_fd, req);
            response_sent_by_handler = 1; // Important: Handler sent response directly
        } else if (req.command == CMD_CHECKPOINT) {
            printf("   Handling CMD_CHECKPOINT for '%s'\n", req.filename);
            handle_checkpoint(&res, req);

        } else if (req.command == CMD_REVERT) {
            printf("   Handling CMD_REVERT for '%s'\n", req.filename);
            handle_revert_checkpoint(&res, req);

        } else if (req.command == CMD_VIEW_CHECKPOINT) {
             printf("   Handling CMD_VIEW_CHECKPOINT for '%s'\n", req.filename);
             handle_view_checkpoint(&res, req);
        
        } else if (req.command == CMD_LIST_CHECKPOINTS) {
             printf("   Handling CMD_LIST_CHECKPOINTS for '%s'\n", req.filename);
             handle_list_checkpoints(conn_fd, req);
             response_sent_by_handler = 1;
        }  else if (req.command == CMD_REQUEST_ACCESS) {
            printf("   Handling CMD_REQUEST_ACCESS for '%s'\n", req.filename);
            handle_request_access(&res, req);
        } else if (req.command == CMD_LIST_REQUESTS) {
            printf("   Handling CMD_LIST_REQUESTS for '%s'\n", req.username);
            handle_list_requests(conn_fd, req);
            response_sent_by_handler = 1;
        } else if (req.command == CMD_APPROVE_REQUEST) {
            printf("   Handling CMD_APPROVE_REQUEST for ID %d\n", req.request_id);
            handle_approve_request(&res, req);
        } else if (req.command == CMD_DENY_REQUEST) {
            printf("   Handling CMD_DENY_REQUEST for ID %d\n", req.request_id);
            handle_deny_request(&res, req);
        } else {
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
    pthread_mutex_lock(&g_request_list_mutex);
    access_request_t *r, *tmp_r;
    HASH_ITER(hh, g_access_requests, r, tmp_r) {
        fprintf(fp, "REQ %d %s %s %s\n",
                r->request_id,
                r->filename,
                r->owner,
                r->requester);
    }
    pthread_mutex_unlock(&g_request_list_mutex);
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
    int max_id = 0; // To track highest loaded ID
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
        }  else if (strcmp(type, "REQ") == 0) {
            int id;
            char owner[MAX_USERNAME_LEN], requester[MAX_USERNAME_LEN];
            if (sscanf(line, "REQ %d %s %s %s", &id, filename, owner, requester) == 4) {
                access_request_t* new_req = (access_request_t*)malloc(sizeof(access_request_t));
                if (new_req) {
                    new_req->request_id = id;
                    strcpy(new_req->filename, filename);
                    strcpy(new_req->owner, owner);
                    strcpy(new_req->requester, requester);
                    HASH_ADD_INT(g_access_requests, request_id, new_req);
                    if (id > max_id) max_id = id;
                }
            }
       }
    }
    g_next_request_id = max_id + 1; // Ensure new IDs are unique
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
    access_request_t *r, *tmp_r;
    HASH_ITER(hh, g_access_requests, r, tmp_r) {
        HASH_DEL(g_access_requests, r);
        free(r);
    }
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
    if (pthread_mutex_init(&g_request_list_mutex, NULL) != 0) {
        perror("[Main] request list mutex init failed"); 
        server_log(LOG_ERROR, "N/A", 0, "SYS", "request list mutex init failed: %s", strerror(errno));
        log_shutdown();
        exit(EXIT_FAILURE); 
    }
    // --- NEW: Load metadata from disk before starting ---
    // We must lock the map while loading it.
    pthread_rwlock_wrlock(&file_map_rwlock);
    load_metadata_from_disk();
    pthread_rwlock_unlock(&file_map_rwlock); 
    pthread_mutex_destroy(&g_request_list_mutex);
    // --- END NEW ---

    printf("Name Server listening on port %d...\n", NM_PORT);
    server_log(LOG_INFO, "N/A", 0, "SYS", "Name Server listening on port %d", NM_PORT); // <-- ADD THIS
    printf("------------------------------------------------\n");
    printf("Run 'hostname -I' (Linux/Mac) or 'ipconfig' (Windows)\n");
    printf("to find this machine's IP address to give to Clients/SS.\n");
    printf("------------------------------------------------\n");
    pthread_t hb_thread, rep_thread;
    if (pthread_create(&hb_thread, NULL, heartbeat_monitor, NULL) != 0) {
        perror("[Main] Failed to create heartbeat thread");
    }
    if (pthread_create(&rep_thread, NULL, replication_worker, NULL) != 0) {
        perror("[Main] Failed to create replication thread");
    }
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
    pthread_mutex_destroy(&g_request_list_mutex);
    log_shutdown(); // <-- ADD THIS
    return 0;
}