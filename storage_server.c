#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <sys/stat.h> // NEW: For stat()
#include <ctype.h>    // NEW: For isspace()
#include "common.h"

// ... (Your #defines for MY_IP, MY_CLIENT_PORT, NM_IP are all fine) ...
#define MY_IP "127.0.0.1"
#define MY_CLIENT_PORT 9090 
#define NM_IP "127.0.0.1"


/**
 * @brief NEW: Helper function to count words and lines in a file.
 */
void get_file_counts(const char* filename, long* word_count, long* line_count) {
    FILE* fp = fopen(filename, "r");
    *word_count = 0;
    *line_count = 0;
    
    if (fp == NULL) {
        perror("   [SS-Thread]: get_file_counts fopen failed");
        return; // Counts remain 0
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
    // Handle files that don't end with a newline
    if (*line_count == 0 && *word_count > 0) *line_count = 1;

    fclose(fp);
}


/**
 * @brief Handles a single client connection (for read/write/etc.)
 */
void* handle_client_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    printf("   [SS-Thread]: Got a connection! (conn_fd: %d)\n", conn_fd);

    client_request_t req;
    bool response_sent = false; // NEW: Flag to handle different response types

    // 1. Receive the request from the client
    ssize_t n = recv(conn_fd, &req, sizeof(client_request_t), 0);
    if (n != sizeof(client_request_t)) {
        fprintf(stderr, "   [SS-Thread]: Error receiving client request.\n");
        close(conn_fd);
        return NULL;
    }

    // 2. Process the command
    if (req.command == CMD_CREATE_FILE) {
        printf("   [SS-Thread]: Handling CMD_CREATE_FILE for '%s'\n", req.filename);
        
        ss_response_t res = {0}; // Use simple response
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
        
        // Send the simple response
        if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
            perror("   [SS-Thread]: send create response failed");
        }
        response_sent = true;

    } else if (req.command == CMD_GET_STATS) {
        printf("   [SS-Thread]: Handling CMD_GET_STATS for '%s'\n", req.filename);
        
        ss_stats_response_t res = {0}; // Use NEW stats response
        struct stat file_stat;

        // 1. Get stats from the OS (size, time)
        if (stat(req.filename, &file_stat) < 0) {
            perror("   [SS-Thread]: stat failed");
            res.status = STATUS_ERROR;
            strcpy(res.error_msg, "File not found on Storage Server");
        } else {
            res.status = STATUS_OK;
            res.stats.char_count = file_stat.st_size;
            res.stats.last_modified = file_stat.st_mtime;
            
            // 2. Get custom stats (word/line counts)
            get_file_counts(req.filename, &res.stats.word_count, &res.stats.line_count);
            printf("   [SS-Thread]: Stats for '%s': %ld chars, %ld words, %ld lines\n",
                   req.filename, res.stats.char_count, res.stats.word_count, res.stats.line_count);
        }
        
        // Send the stats response
        if (send(conn_fd, &res, sizeof(ss_stats_response_t), 0) < 0) {
            perror("   [SS-Thread]: send stats response failed");
        }
        response_sent = true;
    } 

    // 3. Send a generic error if command wasn't handled
    if (!response_sent) {
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
// --- Main Function (No changes needed) ---
// ------------------------------------------------------------------
int main() {
    // ... (Your existing PART 1: Register with Name Server is unchanged) ...
    int sock_fd;
    struct sockaddr_in nm_addr;
    ss_registration_t reg_data;

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
    message_type_t msg_type = MSG_SS_REGISTER;
    if (send(sock_fd, &msg_type, sizeof(message_type_t), 0) < 0) {
        perror("send message type failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }
    strcpy(reg_data.ss_ip, MY_IP);
    reg_data.client_port = MY_CLIENT_PORT;
    if (send(sock_fd, &reg_data, sizeof(reg_data), 0) < 0) {
        perror("send registration data failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }
    printf("Successfully registered with Name Server.\n");
    close(sock_fd);


    // ... (Your existing PART 2: Become a Server for Clients is unchanged) ...
    int listen_fd, conn_fd;
    struct sockaddr_in ss_serv_addr, client_addr;
    socklen_t client_len;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("SS server socket creation failed");
        exit(EXIT_FAILURE);
    }
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

        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        
        if (pthread_create(&tid, NULL, handle_client_connection, p_conn_fd) != 0) {
            perror("pthread_create failed");
            free(p_conn_fd);
        }
        pthread_detach(tid);
    }

    close(listen_fd);
    return 0;
}