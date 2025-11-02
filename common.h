#ifndef COMMON_H
#define COMMON_H

#include <unistd.h> // For ssize_t
#include <pthread.h>
#include <stdbool.h> 
#include <time.h>   // NEW: For time_t

// --- Network Configuration ---
#define NM_PORT 8080
#define MAX_IP_LEN 16

// --- File System Limits ---
#define MAX_FILENAME_LEN 256
#define MAX_ERROR_MSG_LEN 256
#define MAX_FILES 1000
#define MAX_USERNAME_LEN 50 

// --- Message Types (Client <-> NM Handshake) ---
typedef enum {
    MSG_SS_REGISTER,
    MSG_CLIENT_NM_REQUEST 
} message_type_t;

// --- Client Command Types ---
typedef enum {
    CMD_CREATE_FILE,
    CMD_VIEW_FILES,
    CMD_DELETE_FILE,
    CMD_GET_STATS   // NEW: Client asks SS for file details
} client_command_t;

// --- General Status Codes ---
typedef enum {
    STATUS_OK,
    STATUS_ERROR
} response_status_t;

// --- Structs for SS <-> NM Registration ---
typedef struct {
    char ss_ip[MAX_IP_LEN];
    int client_port;
} ss_registration_t;


// --- Structs for Client <-> NM Communication ---

// Client sends this to NM
typedef struct {
    client_command_t command;
    char username[MAX_USERNAME_LEN]; 
    char filename[MAX_FILENAME_LEN];

    bool view_all; // -a
    bool view_long; // -l
} client_request_t;

// NM sends this back to Client
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN]; 

    char ss_ip[MAX_IP_LEN];
    int ss_port;

    int file_count;
} nm_response_t;

// NM sends this for each file in a VIEW list
typedef struct {
    char filename[MAX_FILENAME_LEN];
    char owner[MAX_USERNAME_LEN]; 
    
    char ss_ip[MAX_IP_LEN];
    int ss_port;
} nm_file_entry_t;


// --- Structs for Client <-> SS Communication ---

// Client -> SS: client_request_t is re-used

// SS -> Client (for CREATE/DELETE)
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN]; 
} ss_response_t;

// NEW: Struct for file statistics
typedef struct {
    long char_count;
    long word_count;
    long line_count;
    time_t last_modified;
} ss_file_stats_t;

// NEW: SS -> Client (for GET_STATS)
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN];
    ss_file_stats_t stats; // The stats payload
} ss_stats_response_t;


#endif // COMMON_H