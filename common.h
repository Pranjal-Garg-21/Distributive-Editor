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
    MSG_CLIENT_NM_REQUEST 
} message_type_t;

// --- General Status Codes ---
typedef enum {
    STATUS_OK,
    STATUS_ERROR
} response_status_t;


// --- Command Enums ---
typedef enum {
    CMD_CREATE_FILE,
    CMD_VIEW_FILES,
    CMD_DELETE_FILE,
    CMD_READ_FILE,
    CMD_GET_STATS,
    CMD_WRITE_FILE
} client_command_t;


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

// SS -> Client (for CREATE/DELETE/Ready-to-Write)
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN]; 
} ss_response_t;

// --- Structs for the WRITE Protocol ---

// Client -> SS (for each write operation within a session)
typedef struct {
    int sentence_index;
    int word_index;
    char content[FILE_BUFFER_SIZE];
    bool is_etirw; // true if this is the final ETIRW command
} client_write_chunk_t;

// *** NEW/UPDATED STRUCT ***
// SS -> Client (sent after *each* non-ETIRW write chunk)
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN];
    int new_active_sentence_index;  // Lets client know which sentence to edit next
    int new_total_sentence_count; // Lets client know total
} ss_write_chunk_response_t;

// SS -> Client (sent once after ETIRW is received and file is saved)
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN]; 
    int updated_sentence_count; 
} ss_write_response_t;

// --- End WRITE Protocol Structs ---


// --- Structs for READ/STATS ---

// Struct for file statistics
typedef struct {
    long char_count;
    long word_count;
    long line_count;
    time_t last_modified;
} ss_file_stats_t;

// SS -> Client (for GET_STATS)
typedef struct {
    response_status_t status;
    char error_msg[MAX_ERROR_MSG_LEN];
    ss_file_stats_t stats; 
} ss_stats_response_t;

// SS -> Client (for sending file data)
typedef struct {
    int data_size; 
    char data[FILE_BUFFER_SIZE];
} ss_file_data_chunk_t;


#endif // COMMON_H