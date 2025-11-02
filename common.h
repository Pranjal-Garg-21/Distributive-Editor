#ifndef COMMON_H
#define COMMON_H

#include <unistd.h> // For ssize_t
#include <pthread.h>

// --- Network Configuration ---
#define NM_PORT 8080
#define MAX_IP_LEN 16

// --- File System Limits ---
#define MAX_FILENAME_LEN 256
#define MAX_ERROR_MSG_LEN 256

// --- Message Types (Client <-> NM Handshake) ---
typedef enum {
    MSG_SS_REGISTER,
    MSG_CLIENT_NM_REQUEST // Renamed from MSG_CLIENT_REQUEST
} message_type_t;

// --- Client Command Types ---
typedef enum {
    CMD_CREATE_FILE,
    // Future commands will go here:
    // CMD_DELETE_FILE,
    // CMD_READ_FILE,
    // CMD_WRITE_FILE,
    // CMD_LIST_FILES
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

// Client sends this to NM (after the initial handshake)
typedef struct {
    client_command_t command;
    char filename[MAX_FILENAME_LEN];
    // Other fields for read/write (e.g., size_t size) can be added later
} client_request_t;

// NM sends this back to Client
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN]; // If status is STATUS_ERROR

    // Info for the client to connect directly to an SS
    char ss_ip[MAX_IP_LEN];
    int ss_port;
} nm_response_t;


// --- Structs for Client <-> SS Communication ---

// Client sends this to SS (NOTE: We can reuse client_request_t)
// typedef client_request_t client_ss_request_t; 

// SS sends this back to Client
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN]; // If status is STATUS_ERROR
} ss_response_t;

#endif // COMMON_H