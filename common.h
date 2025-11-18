#ifndef COMMON_H
#define COMMON_H

#include <unistd.h> // For ssize_t
#include <pthread.h>
#include <stdbool.h> 
#include <time.h>   

// --- Network Configuration ---
#define NM_PORT 8080
#define MAX_IP_LEN 16

// --- File System Limits ---
#define MAX_FILENAME_LEN 256
#define MAX_ERROR_MSG_LEN 256
#define MAX_FILES 1000
#define MAX_USERNAME_LEN 50 
#define FILE_BUFFER_SIZE 4096 
#define MAX_WORDS_PER_SENTENCE 2048

// --- Message Types (Client <-> NM Handshake) ---
typedef enum {
    MSG_SS_REGISTER,
    MSG_SS_FILE_LIST_ENTRY, 
    MSG_SS_FILE_LIST_END,   
    MSG_CLIENT_NM_REQUEST,
    MSG_SS_UPDATE_NOTIFY
} message_type_t;

typedef struct {
    char service_name[MAX_FILENAME_LEN]; // The physical filename on disk
} ss_notify_arg_t;

// --- General Status Codes ---
typedef enum {
    STATUS_OK,
    STATUS_ERROR
} response_status_t;

// --- NEW: Universal Error Codes (as required by spec) ---
typedef enum {
    NFS_OK = 0,                 // Success
    NFS_ERR_FILE_NOT_FOUND,     // File not found
    NFS_ERR_FILE_ALREADY_EXISTS,// File already exists
    NFS_ERR_PERMISSION_DENIED,  // General permission error
    NFS_ERR_NOT_OWNER,          // Specific "not owner" error
    NFS_ERR_FILE_LOCKED,        // Resource contention / sentence locked
    NFS_ERR_INVALID_INPUT,      // Bad arguments from client
    NFS_ERR_INVALID_FILENAME,   // e.g., contains '/' or '..'
    NFS_ERR_INDEX_OUT_OF_BOUNDS,// For WRITE command
    NFS_ERR_SS_DOWN,            // Storage Server is offline
    NFS_ERR_NM_DOWN,            // (Client-side) Name Server is offline
    NFS_ERR_SS_INTERNAL,        // Storage Server had an internal error (malloc, fopen)
    NFS_ERR_NM_INTERNAL,        // Name Server had an internal error (malloc)
    NFS_ERR_NET_SEND_FAILED,    // Network send error
    NFS_ERR_NET_RECV_FAILED,    // Network receive error
    NFS_ERR_UNKNOWN             // Catch-all
} nfs_error_code_t;


// --- Command Enums ---
typedef enum {
    CMD_CREATE_FILE,
    CMD_VIEW_FILES,
    CMD_DELETE_FILE,
    CMD_READ_FILE,
    CMD_GET_STATS,
    CMD_WRITE_FILE,
    CMD_ADD_ACCESS,
    CMD_REM_ACCESS,
    CMD_STREAM_FILE,
    CMD_UNDO_FILE,
    CMD_GET_INFO,
    CMD_LIST_USERS,    
    CMD_EXEC,
    CMD_VIEW_FOLDER,
    CMD_CREATE_FOLDER,
    CMD_MOVE_FILE,
    CMD_CHECKPOINT,      // <-- NEW
    CMD_VIEW_CHECKPOINT, // <-- NEW
    CMD_REVERT,          // <-- NEW
    CMD_LIST_CHECKPOINTS, // <-- NEW
    CMD_REPLICATE,
    CMD_REQUEST_ACCESS,
    CMD_LIST_REQUESTS,
    CMD_APPROVE_REQUEST,
    CMD_DENY_REQUEST
} client_command_t;

// --- Access Level Enum ---
typedef enum {
    ACCESS_READ,
    ACCESS_WRITE
} access_level_t;


// --- Structs for SS <-> NM Registration ---
typedef struct {
    char ss_ip[MAX_IP_LEN];
    int client_port;
} ss_registration_t;

typedef struct {
    char filename[MAX_FILENAME_LEN];
} nm_ss_file_entry_t;


// --- Structs for Client <-> NM Communication ---

// Client sends this to NM
typedef struct {
    client_command_t command;
    char username[MAX_USERNAME_LEN];
    char filename[MAX_FILENAME_LEN];

    bool view_all; // -a
    bool view_long; // -l
    int request_id;
    char target_username[MAX_USERNAME_LEN];
    access_level_t access_level;
    char dest_path[MAX_FILENAME_LEN];
    char checkpoint_tag[50];
} client_request_t;
// --- NEW: Struct for LISTREQUESTS ---
// NM -> Client, sent file_count (as request_count) times after header
typedef struct {
    int request_id;
    char filename[MAX_FILENAME_LEN];
    char username[MAX_USERNAME_LEN]; // The user who wants access
} nm_access_request_entry_t;
// --- END NEW ---
// NM sends this back to Client (Generic)
typedef struct {
    response_status_t status;
    nfs_error_code_t error_code; // NEW
    char error_msg[MAX_ERROR_MSG_LEN]; 
    char ss_ip[MAX_IP_LEN];
    int ss_port;
    int file_count;
    char storage_filename[MAX_FILENAME_LEN]; 
} nm_response_t;

// NM sends this for each file in a VIEW list
typedef struct {
    char filename[MAX_FILENAME_LEN]; // Logical name (e.g., "folder/doc.txt")
    char owner[MAX_USERNAME_LEN]; 
    char ss_ip[MAX_IP_LEN];
    int ss_port;
    char storage_filename[MAX_FILENAME_LEN]; // NEW: Physical name (e.g., "doc.txt")
} nm_file_entry_t;

// --- Structs for INFO command ---

// NM -> Client response for INFO (Header)
typedef struct {
    response_status_t status;
    nfs_error_code_t error_code;
    char error_msg[MAX_ERROR_MSG_LEN];
    
    char owner[MAX_USERNAME_LEN];
    char ss_ip[MAX_IP_LEN];
    int ss_port;
    int acl_count;
    char storage_filename[MAX_FILENAME_LEN]; // NEW: Physical name
} nm_info_response_t;

// NM -> Client, sent acl_count times after header
typedef struct {
    char username[MAX_USERNAME_LEN];
    access_level_t level;
} nm_acl_entry_t;

// --- Structs for LIST command ---

// NM -> Client, sent file_count (as user_count) times after header
typedef struct {
    char username[MAX_USERNAME_LEN];
} nm_user_entry_t;


// SS -> Client (for CREATE/DELETE/Ready-to-Write)
typedef struct {
    response_status_t status;
    nfs_error_code_t error_code; // NEW
    char error_msg[MAX_ERROR_MSG_LEN]; 
} ss_response_t;

// --- (WRITE Protocol Structs are unchanged) ---
typedef struct {
    int sentence_index;
    int word_index;
    char content[FILE_BUFFER_SIZE];
    bool is_etirw;
} client_write_chunk_t;

typedef struct {
    response_status_t status;
    nfs_error_code_t error_code; // NEW
    char error_msg[MAX_ERROR_MSG_LEN];
    int new_active_sentence_index;  
    int new_total_sentence_count; 
} ss_write_chunk_response_t;

typedef struct {
    response_status_t status;
    nfs_error_code_t error_code; // NEW
    char error_msg[MAX_ERROR_MSG_LEN]; 
    int updated_sentence_count; 
} ss_write_response_t;


// --- Structs for READ/STATS ---
typedef struct {
    long char_count;
    long word_count;
    long line_count;
    time_t last_modified;
    time_t last_accessed; 
    time_t time_created;  
} ss_file_stats_t;

// SS -> Client (for GET_STATS, now with more info)
typedef struct {
    response_status_t status;
    nfs_error_code_t error_code; // NEW
    char error_msg[MAX_ERROR_MSG_LEN];
    ss_file_stats_t stats; 
} ss_stats_response_t;

// SS -> Client (for sending file data)
typedef struct {
    int data_size; 
    char data[FILE_BUFFER_SIZE];
} ss_file_data_chunk_t;


#endif // COMMON_H