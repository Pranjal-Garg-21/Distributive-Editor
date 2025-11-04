#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <sys/stat.h> 
#include <ctype.h>    
#include "common.h"
#include "uthash.h" // --- NEW: Include the uthash header ---

#define MY_IP "127.0.0.1"
#define MY_CLIENT_PORT 9090 
#define NM_IP "127.0.0.1"

// --- NEW: Per-File Lock Management (Hash Map) ---

/**
 * @brief The struct that will be stored in the hash map.
 * It maps a filename (key) to its own rwlock (value).
 */
typedef struct {
    char filename[MAX_FILENAME_LEN]; // Key
    pthread_rwlock_t lock;           // Value
    UT_hash_handle hh;               // Uthash handle
} file_lock_t;

/**
 * @brief The global head of the hash map, initialized to NULL.
 */
file_lock_t* g_file_lock_map = NULL;

/**
 * @brief A single global mutex to protect the hash map *structure*.
 * This is locked ONLY when adding/removing entries, not during
 * file I/O, so it is extremely fast and low-contention.
 */
pthread_mutex_t g_lock_map_mutex;

// --- END NEW ---


/**
 * @brief Helper function to get word/line counts.
 */
void get_file_counts(const char* filename, long* word_count, long* line_count) {
    FILE* fp = fopen(filename, "r");
    *word_count = 0;
    *line_count = 0;
    if (fp == NULL) {
        perror("   [SS-Thread]: get_file_counts fopen failed");
        return; 
    }
    int c;
    bool in_word = false;
    while ((c = fgetc(fp)) != EOF) {
        if (c == '\n') {
            (*line_count)++;
        }
        if (isspace(c)) {
            in_word = false;
        } else if (!in_word) {
            in_word = true;
            (*word_count)++;
        }
    }
    // Handle file with content but no newline
    if (*line_count == 0 && *word_count > 0) *line_count = 1; 
    fclose(fp);
}


// --- NEW: Hash Map Lock Manager Function ---
/**
 * @brief Finds or creates a rwlock for a specific filename.
 * This function is thread-safe.
 * @return Pointer to the lock, or NULL on malloc failure.
 */
pthread_rwlock_t* get_lock_for_file(const char* filename) {
    file_lock_t* found_lock;

    // --- LOCK the "meta-mutex" ---
    // We must protect the global g_file_lock_map pointer
    pthread_mutex_lock(&g_lock_map_mutex);

    // 1. Try to find the lock (O(1) average case)
    HASH_FIND_STR(g_file_lock_map, filename, found_lock);

    if (found_lock) {
        // 2. Found it! Unlock the meta-mutex and return.
        // We are done touching the hash map structure.
        pthread_mutex_unlock(&g_lock_map_mutex);
        return &found_lock->lock;
    }

    // 3. Not found. We need to create a new one.
    found_lock = (file_lock_t*)malloc(sizeof(file_lock_t));
    if (found_lock == NULL) {
        perror("   [SS-LockMgr]: malloc failed for new lock");
        pthread_mutex_unlock(&g_lock_map_mutex); // Unlock on error
        return NULL;
    }

    // 4. Initialize the new lock entry
    strcpy(found_lock->filename, filename);
    if (pthread_rwlock_init(&found_lock->lock, NULL) != 0) {
        perror("   [SS-LockMgr]: rwlock_init failed");
        free(found_lock);
        pthread_mutex_unlock(&g_lock_map_mutex); // Unlock on error
        return NULL;
    }

    // 5. Add it to the hash map (O(1) average case)
    HASH_ADD_STR(g_file_lock_map, filename, found_lock);

    printf("   [SS-LockMgr]: Initialized new lock for file '%s'\n", filename);

    // 6. Unlock the meta-mutex and return the new lock
    pthread_mutex_unlock(&g_lock_map_mutex);
    return &found_lock->lock;
}
// --- END NEW ---


/**
 * @brief Handles a single client connection (for read/write/etc.)
 */
void* handle_client_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    printf("   [SS-Thread]: Got a connection! (conn_fd: %d)\n", conn_fd);

    client_request_t req;
    bool response_sent = false; 

    ssize_t n = recv(conn_fd, &req, sizeof(client_request_t), 0);
    if (n != sizeof(client_request_t)) {
        fprintf(stderr, "   [SS-Thread]: Error receiving client request.\n");
        close(conn_fd);
        return NULL;
    }

    // --- MODIFIED: Get the per-file lock ---
    pthread_rwlock_t* file_lock = get_lock_for_file(req.filename);

    if (file_lock == NULL) {
        // Server-side capacity issue (e.g., malloc failed)
        ss_response_t res = {0};
        res.status = STATUS_ERROR;
        strcpy(res.error_msg, "Storage Server internal error. Try again later.");
        if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
            perror("   [SS-Thread]: send capacity error response failed");
        }
        response_sent = true;
    } else {
        // --- We have a lock, now proceed with commands ---

        if (req.command == CMD_CREATE_FILE) {
            printf("   [SS-Thread]: Handling CMD_CREATE_FILE for '%s'\n", req.filename);
            
            // --- NEW: Write Lock ---
            pthread_rwlock_wrlock(file_lock);

            ss_response_t res = {0}; 
            FILE* fp = fopen(req.filename, "w"); 
            if (fp == NULL) {
                perror("   [SS-Thread]: fopen failed");
                res.status = STATUS_ERROR;
                strcpy(res.error_msg, "File creation failed on Storage Server");
            } else {
                fclose(fp);
                res.status = STATUS_OK;
                printf("   [SS-Thread]: Successfully created file '%s'\n", req.filename);
            }
            
            // --- NEW: Unlock ---
            pthread_rwlock_unlock(file_lock);

            if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
                perror("   [SS-Thread]: send create response failed");
            }
            response_sent = true;

        } else if (req.command == CMD_GET_STATS) {
            printf("   [SS-Thread]: Handling CMD_GET_STATS for '%s'\n", req.filename);
            
            // --- NEW: Read Lock ---
            pthread_rwlock_rdlock(file_lock);

            ss_stats_response_t res = {0}; 
            struct stat file_stat;
            if (stat(req.filename, &file_stat) < 0) {
                perror("   [SS-Thread]: stat failed");
                res.status = STATUS_ERROR;
                strcpy(res.error_msg, "File not found on Storage Server");
            } else {
                res.status = STATUS_OK;
                res.stats.char_count = file_stat.st_size;
                res.stats.last_modified = file_stat.st_mtime;
                get_file_counts(req.filename, &res.stats.word_count, &res.stats.line_count);
                printf("   [SS-Thread]: Stats for '%s': %ld chars, %ld words, %ld lines\n",
                       req.filename, res.stats.char_count, res.stats.word_count, res.stats.line_count);
            }
            
            // --- NEW: Unlock ---
            pthread_rwlock_unlock(file_lock);

            if (send(conn_fd, &res, sizeof(ss_stats_response_t), 0) < 0) {
                perror("   [SS-Thread]: send stats response failed");
            }
            response_sent = true;

        } else if (req.command == CMD_READ_FILE) {
            printf("   [SS-Thread]: Handling CMD_READ_FILE for '%s'\n", req.filename);
            
            // --- NEW: Read Lock ---
            pthread_rwlock_rdlock(file_lock);

            ss_response_t header_res = {0};
            FILE* fp = fopen(req.filename, "r");

            if (fp == NULL) {
                // 1. Send Error Header
                perror("   [SS-Thread]: fopen failed for read");
                header_res.status = STATUS_ERROR;
                strcpy(header_res.error_msg, "File not found or unreadable on Storage Server");
                if (send(conn_fd, &header_res, sizeof(ss_response_t), 0) < 0) {
                    perror("   [SS-Thread]: send read error response failed");
                }
            } else {
                // 2. Send OK Header
                header_res.status = STATUS_OK;
                if (send(conn_fd, &header_res, sizeof(ss_response_t), 0) < 0) {
                    perror("   [SS-Thread]: send read OK response failed");
                    fclose(fp);
                    pthread_rwlock_unlock(file_lock); // --- Unlock before early exit
                    close(conn_fd);
                    return NULL;
                }

                // 3. Send Data Chunks
                ss_file_data_chunk_t chunk;
                size_t bytes_read;
                while ((bytes_read = fread(chunk.data, 1, FILE_BUFFER_SIZE, fp)) > 0) {
                    chunk.data_size = bytes_read;
                    if (send(conn_fd, &chunk, sizeof(ss_file_data_chunk_t), 0) < 0) {
                        perror("   [SS-Thread]: send data chunk failed");
                        break; // Client disconnected
                    }
                }

                // 4. Send Terminator Chunk
                chunk.data_size = 0;
                if (send(conn_fd, &chunk, sizeof(ss_file_data_chunk_t), 0) < 0) {
                    perror("   [SS-Thread]: send terminator chunk failed");
                }
                
                fclose(fp);
                printf("   [SS-Thread]: File '%s' sent successfully.\n", req.filename);
            }
            
            // --- NEW: Unlock ---
            pthread_rwlock_unlock(file_lock);
            response_sent = true;
        } 
    } // --- End of 'else (file_lock != NULL)' block ---

    if (!response_sent) {
        // ... (Generic error logic is unchanged) ...
        ss_response_t res = {0};
        res.status = STATUS_ERROR;
        strcpy(res.error_msg, "Unknown command received by SS");
        if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
            perror("   [SS-Thread]: send error response failed");
        }
    }

    close(conn_fd);
    printf("   [SS-Thread]: Closing client connection.\n");
    return NULL;
}


// ------------------------------------------------------------------
// --- Main Function (Modified) ---
// ------------------------------------------------------------------
int main() {
    int sock_fd;
    struct sockaddr_in nm_addr;
    ss_registration_t reg_data;

    // --- 1. Register with Name Server ---
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0) {
        perror("invalid Name Server IP address");
        exit(EXIT_FAILURE);
    }

    printf("Attempting to connect to Name Server at %s:%d...\n", NM_IP, NM_PORT);
    if (connect(sock_fd, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
        perror("connection to Name Server failed");
        exit(EXIT_FAILURE);
    }

    printf("Connected to Name Server!\n");
    
    // Send message type
    message_type_t msg_type = MSG_SS_REGISTER;
    if (send(sock_fd, &msg_type, sizeof(message_type_t), 0) < 0) {
        perror("send message type failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }
    
    // Send registration data
    strcpy(reg_data.ss_ip, MY_IP);
    reg_data.client_port = MY_CLIENT_PORT;
    if (send(sock_fd, &reg_data, sizeof(reg_data), 0) < 0) {
        perror("send registration data failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }
    printf("Successfully registered with Name Server.\n");
    close(sock_fd); // Done with registration

    // --- NEW: Initialize the global lock map mutex ---
    if (pthread_mutex_init(&g_lock_map_mutex, NULL) != 0) {
        perror("g_lock_map_mutex init failed");
        exit(EXIT_FAILURE);
    }
    // --- END NEW ---


    // --- 2. Become a server for clients ---
    int listen_fd, conn_fd;
    struct sockaddr_in ss_serv_addr, client_addr;
    socklen_t client_len;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("SS server socket creation failed");
        exit(EXIT_FAILURE);
    }
    
    // Allow port reuse
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));

    memset(&ss_serv_addr, 0, sizeof(ss_serv_addr));
    ss_serv_addr.sin_family = AF_INET;
    ss_serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    ss_serv_addr.sin_port = htons(MY_CLIENT_PORT);

    if (bind(listen_fd, (struct sockaddr*)&ss_serv_addr, sizeof(ss_serv_addr)) < 0) {
        perror("SS server bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(listen_fd, 5) < 0) {
        perror("SS server listen failed");
        exit(EXIT_FAILURE);
    }
    printf("\n[SS-Main]: Storage Server now listening for clients on port %d...\n", MY_CLIENT_PORT);

    // --- 3. Main Accept Loop ---
    while (1) {
        client_len = sizeof(client_addr);
        conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            perror("SS server accept failed");
            continue; 
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("[SS-Main]: Accepted new client connection from %s\n", client_ip);

        // Create a new thread to handle this client
        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        
        if (pthread_create(&tid, NULL, handle_client_connection, p_conn_fd) != 0) {
            perror("pthread_create failed");
            free(p_conn_fd);
        }
        pthread_detach(tid); // We don't need to join it
    }

    // --- 4. Cleanup (in case of graceful shutdown) ---
    close(listen_fd);
    
    // --- NEW: Cleanup for hash map ---
    pthread_mutex_destroy(&g_lock_map_mutex);
    
    file_lock_t *current_lock, *tmp;
    HASH_ITER(hh, g_file_lock_map, current_lock, tmp) {
        pthread_rwlock_destroy(&current_lock->lock); // Destroy the rwlock
        HASH_DEL(g_file_lock_map, current_lock);    // Remove from hash map
        free(current_lock);                       // Free the struct
    }
    // --- END NEW ---

    return 0;
}