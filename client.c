#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <stdbool.h> 
#include <time.h>     // NEW: For formatting time
#include "common.h"

#define NM_IP "127.0.0.1"
// NM_PORT is from common.h

/**
 * @brief Prints the usage of the client program.
 * (Unchanged)
 */
void print_usage(char* prog_name) {
    fprintf(stderr, "Usage: %s <username> <COMMAND> [options]\n\n", prog_name);
    fprintf(stderr, "Commands:\n");
    fprintf(stderr, "  CREATE <filename>\n");
    fprintf(stderr, "  VIEW [-a] [-l]\n");
    fprintf(stderr, "  DELETE <filename>\n");
}

/**
 * @brief Connects to a server at the given IP and port.
 * (Unchanged)
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
        // Suppress "connection failed" for stats, as SS might be down
        // This is a normal part of the -l loop
        close(sock_fd);
        return -1;
    }
    return sock_fd;
}

/**
 * @brief Parses flags for the VIEW command.
 * (Unchanged)
 */
void parse_view_flags(int argc, char* argv[], client_request_t* req) {
    req->view_all = false;
    req->view_long = false;
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "-a") == 0) {
            req->view_all = true;
        } else if (strcmp(argv[i], "-l") == 0) {
            req->view_long = true;
        } else if (strcmp(argv[i], "-al") == 0 || strcmp(argv[i], "-la") == 0) {
            req->view_all = true;
            req->view_long = true;
        }
    }
}


int main(int argc, char *argv[]) {
    // 1. Validate and parse command-line arguments
    // (Unchanged)
    if (argc < 3) { 
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    
    client_request_t req = {0};
    strncpy(req.username, argv[1], MAX_USERNAME_LEN - 1);
    char* command_str = argv[2];

    if (strcmp(command_str, "CREATE") == 0 && argc == 4) {
        req.command = CMD_CREATE_FILE;
        strncpy(req.filename, argv[3], MAX_FILENAME_LEN - 1);
    
    } else if (strcmp(command_str, "VIEW") == 0) {
        req.command = CMD_VIEW_FILES;
        parse_view_flags(argc, argv, &req);
    
    } else if (strcmp(command_str, "DELETE") == 0 && argc == 4) {
        req.command = CMD_DELETE_FILE;
        strncpy(req.filename, argv[3], MAX_FILENAME_LEN - 1);

    } else {
        fprintf(stderr, "Error: Unknown command or incorrect arguments\n");
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    // -------------------------------------------------
    // --- STEP 1: Connect to Name Server ---
    // (Unchanged)
    // -------------------------------------------------
    printf("Connecting to Name Server as user '%s'...\n", req.username);
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) exit(EXIT_FAILURE);

    printf("Connected to Name Server!\n");

    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    if (send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0) < 0) {
        perror("send handshake failed"); close(nm_sock_fd); exit(EXIT_FAILURE);
    }
    
    if (send(nm_sock_fd, &req, sizeof(client_request_t), 0) < 0) {
        perror("send request to NM failed"); close(nm_sock_fd); exit(EXIT_FAILURE);
    }

    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response header from NM failed"); close(nm_sock_fd); exit(EXIT_FAILURE);
    }

    if (nm_res.status == STATUS_ERROR) {
        fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg);
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }

    // -------------------------------------------------
    // --- STEP 2: Process the NM's response ---
    // -------------------------------------------------

    if (req.command == CMD_CREATE_FILE) {
        // --- CREATE logic (Unchanged) ---
        printf("NM approved. Contacting Storage Server at %s:%d...\n", nm_res.ss_ip, nm_res.ss_port);
        close(nm_sock_fd); 
        
        int ss_sock_fd = connect_to_server(nm_res.ss_ip, nm_res.ss_port);
        if (ss_sock_fd < 0) exit(EXIT_FAILURE);
        printf("Connected to Storage Server!\n");
        
        if (send(ss_sock_fd, &req, sizeof(client_request_t), 0) < 0) {
            perror("send request to SS failed"); close(ss_sock_fd); exit(EXIT_FAILURE);
        }
        ss_response_t ss_res;
        if (recv(ss_sock_fd, &ss_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
            perror("recv response from SS failed"); close(ss_sock_fd); exit(EXIT_FAILURE);
        }
        close(ss_sock_fd); 
        
        if (ss_res.status == STATUS_ERROR) {
            fprintf(stderr, "[Error from Storage Server]: %s\n", ss_res.error_msg);
            exit(EXIT_FAILURE);
        }
        printf("\nSuccess! File '%s' created.\n", req.filename);

    } else if (req.command == CMD_VIEW_FILES) {
        // --- UPDATED: VIEW logic ---
        int count = nm_res.file_count;
        printf("\n--- File List (%d files) ---\n", count);
        
        if (count == 0) {
            printf("(No files found)\n");
        } else {
            if (req.view_long) {
                // NEW: Print the full header
                printf("%-12s | %-20s | %-7s | %-7s | %-7s | %-19s\n",
                       "OWNER", "FILE", "CHARS", "WORDS", "LINES", "LAST MODIFIED");
                printf("-------------------------------------------------------------------------------------------\n");
            }

            nm_file_entry_t file_entry;
            for (int i = 0; i < count; i++) {
                // 1. Receive file info from NM
                if(recv(nm_sock_fd, &file_entry, sizeof(nm_file_entry_t), 0) != sizeof(nm_file_entry_t)) {
                     perror("recv file entry failed"); break;
                }
                
                if (req.view_long) {
                    // 2. If -l, connect to the SS to get stats
                    printf("%-12s | %-20s |", file_entry.owner, file_entry.filename);
                    
                    int ss_sock_fd = connect_to_server(file_entry.ss_ip, file_entry.ss_port);
                    if (ss_sock_fd < 0) {
                        printf(" (Stats unavailable: SS at %s:%d is down)\n", file_entry.ss_ip, file_entry.ss_port);
                        continue;
                    }

                    // 3. Send stats request
                    client_request_t stats_req = {0};
                    stats_req.command = CMD_GET_STATS;
                    strncpy(stats_req.filename, file_entry.filename, MAX_FILENAME_LEN - 1);
                    send(ss_sock_fd, &stats_req, sizeof(client_request_t), 0);

                    // 4. Receive stats response
                    ss_stats_response_t stats_res;
                    if(recv(ss_sock_fd, &stats_res, sizeof(ss_stats_response_t), 0) != sizeof(ss_stats_response_t)) {
                        printf(" (Error receiving stats)\n");
                        close(ss_sock_fd);
                        continue;
                    }
                    close(ss_sock_fd);

                    // 5. Print stats
                    if (stats_res.status == STATUS_OK) {
                        char time_str[30];
                        strftime(time_str, 30, "%Y-%m-%d %H:%M", localtime(&stats_res.stats.last_modified));
                        printf(" %-7ld | %-7ld | %-7ld | %s\n",
                               stats_res.stats.char_count,
                               stats_res.stats.word_count,
                               stats_res.stats.line_count,
                               time_str);
                    } else {
                        printf(" (Error: %s)\n", stats_res.error_msg);
                    }

                } else {
                    // Simple view
                    printf(" - %s\n", file_entry.filename);
                }
            }
        }
        printf("-------------------------------------------------------------------------------------------\n");
        close(nm_sock_fd); 
    }
    
    return 0;
}