#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <stdbool.h> 
#include <time.h>     
#include "common.h"
#include <ctype.h>

#define NM_IP "127.0.0.1"
// NM_PORT is from common.h

/**
 * @brief Prints the available commands in the interactive shell.
 */
void print_help() {
    printf("--- Network File System Client ---\n");
    printf("Available Commands:\n");
    printf("  CREATE <filename>\n");
    printf("  VIEW [-a] [-l] [-al]\n");
    printf("  READ <filename>\n");
    printf("  STREAM <filename>\n"); // NEW
    printf("  WRITE <filename> <sentence_number>\n");
    printf("  DELETE <filename>\n");
    printf("  ADDACCESS <R|W> <filename> <username>\n"); 
    printf("  REMACCESS <filename> <username>\n");     
    printf("  help     (Show this message)\n");
    printf("  exit     (Quit the client)\n\n");
}

/**
 * @brief Connects to a server at the given IP and port.
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

// =================================================================
// --- Command Handler Functions ---
// =================================================================

void do_create(const char* username, const char* filename) {
    // ... (unchanged) ...
    client_request_t req = {0};
    req.command = CMD_CREATE_FILE;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Name Server.\n"); return; }
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response header from NM failed"); close(nm_sock_fd); return;
    }
    close(nm_sock_fd); 
    if (nm_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg); return; }
    int ss_sock_fd = connect_to_server(nm_res.ss_ip, nm_res.ss_port);
    if (ss_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Storage Server at %s:%d.\n", nm_res.ss_ip, nm_res.ss_port); return; }
    send(ss_sock_fd, &req, sizeof(client_request_t), 0);
    ss_response_t ss_res;
    if (recv(ss_sock_fd, &ss_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("recv response from SS failed"); close(ss_sock_fd); return;
    }
    close(ss_sock_fd);
    if (ss_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Storage Server]: %s\n", ss_res.error_msg); }
    else { printf("Success! File '%s' created.\n", req.filename); }
}

void do_view(const char* username, bool view_all, bool view_long) {
    // ... (unchanged) ...
    client_request_t req = {0};
    req.command = CMD_VIEW_FILES;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    req.view_all = view_all;
    req.view_long = view_long;
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Name Server.\n"); return; }
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response header from NM failed"); close(nm_sock_fd); return;
    }
    if (nm_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg); close(nm_sock_fd); return; }
    int count = nm_res.file_count;
    printf("\n--- File List (%d files) ---\n", count);
    if (count == 0) { printf("(No files found)\n"); }
    else {
        if (req.view_long) {
            printf("%-12s | %-20s | %-7s | %-7s | %-7s | %-19s\n", "OWNER", "FILE", "CHARS", "WORDS", "LINES", "LAST MODIFIED");
            printf("-------------------------------------------------------------------------------------------\n");
        }
        nm_file_entry_t file_entry;
        for (int i = 0; i < count; i++) {
            if (recv(nm_sock_fd, &file_entry, sizeof(nm_file_entry_t), 0) != sizeof(nm_file_entry_t)) {
                perror("recv file entry failed"); break;
            }
            if (req.view_long) {
                printf("%-12s | %-20s |", file_entry.owner, file_entry.filename);
                int ss_sock_fd = connect_to_server(file_entry.ss_ip, file_entry.ss_port);
                if (ss_sock_fd < 0) { printf(" (Stats unavailable: SS at %s:%d is down)\n", file_entry.ss_ip, file_entry.ss_port); continue; }
                client_request_t stats_req = {0};
                stats_req.command = CMD_GET_STATS;
                strncpy(stats_req.filename, file_entry.filename, MAX_FILENAME_LEN - 1);
                send(ss_sock_fd, &stats_req, sizeof(client_request_t), 0);
                ss_stats_response_t stats_res;
                if (recv(ss_sock_fd, &stats_res, sizeof(ss_stats_response_t), 0) != sizeof(ss_stats_response_t)) {
                    printf(" (Error receiving stats)\n"); close(ss_sock_fd); continue;
                }
                close(ss_sock_fd);
                if (stats_res.status == STATUS_OK) {
                    char time_str[30];
                    strftime(time_str, 30, "%Y-%m-%d %H:%M", localtime(&stats_res.stats.last_modified));
                    printf(" %-7ld | %-7ld | %-7ld | %s\n", stats_res.stats.char_count, stats_res.stats.word_count, stats_res.stats.line_count, time_str);
                } else { printf(" (Error: %s)\n", stats_res.error_msg); }
            } else { printf(" - %s\n", file_entry.filename); }
        }
    }
    printf("-------------------------------------------------------------------------------------------\n");
    close(nm_sock_fd); 
}

void do_read(const char* username, const char* filename) {
    // ... (unchanged) ...
    client_request_t req = {0};
    req.command = CMD_READ_FILE;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Name Server.\n"); return; }
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response header from NM failed"); close(nm_sock_fd); return;
    }
    close(nm_sock_fd); 
    if (nm_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg); return; }
    int ss_sock_fd = connect_to_server(nm_res.ss_ip, nm_res.ss_port);
    if (ss_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Storage Server at %s:%d.\n", nm_res.ss_ip, nm_res.ss_port); return; }
    send(ss_sock_fd, &req, sizeof(client_request_t), 0);
    ss_response_t header_res;
    if (recv(ss_sock_fd, &header_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("recv header response from SS failed"); close(ss_sock_fd); return;
    }
    if (header_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Storage Server]: %s\n", header_res.error_msg); close(ss_sock_fd); return; }
    printf("\n--- Content of %s ---\n", req.filename);
    ss_file_data_chunk_t chunk;
    while (1) {
        ssize_t n = recv(ss_sock_fd, &chunk, sizeof(ss_file_data_chunk_t), 0);
        if (n < 0) { perror("recv data chunk failed"); break; }
        if (n == 0 || chunk.data_size == 0) { break; }
        fwrite(chunk.data, 1, chunk.data_size, stdout);
    }
    printf("\n--- End of File ---\n");
    close(ss_sock_fd);
}

// --- NEW: Handle STREAM ---
void do_stream(const char* username, const char* filename) {
    client_request_t req = {0};
    req.command = CMD_STREAM_FILE; // Ask NM for STREAM permission
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);

    // --- STEP 1: Connect to Name Server ---
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) {
        fprintf(stderr, "Error: Could not connect to Name Server.\n");
        return;
    }

    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);

    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response header from NM failed");
        close(nm_sock_fd);
        return;
    }
    close(nm_sock_fd); // Done with NM

    if (nm_res.status == STATUS_ERROR) {
        fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg);
        return;
    }

    // --- STEP 2: Connect to Storage Server ---
    int ss_sock_fd = connect_to_server(nm_res.ss_ip, nm_res.ss_port);
    if (ss_sock_fd < 0) {
        fprintf(stderr, "Error: Could not connect to Storage Server at %s:%d.\n", nm_res.ss_ip, nm_res.ss_port);
        return;
    }

    // IMPORTANT: Request a READ, not a STREAM.
    // The SS serves data the same way for both.
    req.command = CMD_READ_FILE; 
    if (send(ss_sock_fd, &req, sizeof(client_request_t), 0) < 0) {
        perror("send request to SS failed");
        close(ss_sock_fd);
        return;
    }

    ss_response_t header_res;
    if (recv(ss_sock_fd, &header_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("recv header response from SS failed");
        close(ss_sock_fd);
        return;
    }
    if (header_res.status == STATUS_ERROR) {
        fprintf(stderr, "[Error from Storage Server]: %s\n", header_res.error_msg);
        close(ss_sock_fd);
        return;
    }

    // --- STEP 3: Receive chunks and stream word-by-word ---
    printf("\n--- Streaming %s ---\n", req.filename);
    ss_file_data_chunk_t chunk;
    char word_buffer[FILE_BUFFER_SIZE + 1]; // Buffer for the current word
    int word_index = 0;

    while (1) {
        ssize_t n = recv(ss_sock_fd, &chunk, sizeof(ss_file_data_chunk_t), 0);
        
        if (n < 0) {
            perror("recv data chunk failed");
            break;
        }
        if (n == 0) {
            fprintf(stderr, "Storage Server disconnected unexpectedly.\n");
            break;
        }
        
        if (chunk.data_size == 0) {
            break; // Terminator chunk
        }

        // Parse the chunk character by character
        for (int i = 0; i < chunk.data_size; i++) {
            char c = chunk.data[i];
            
            if (isspace(c)) { // Found a word boundary
                if (word_index > 0) {
                    // We have a complete word
                    word_buffer[word_index] = '\0';
                    printf("%s ", word_buffer);
                    fflush(stdout); // CRITICAL: Flush output to print immediately
                    usleep(100000); // 100,000 microseconds = 0.1 seconds
                    word_index = 0; // Reset for next word
                }
                // (if word_index is 0, it's just multiple spaces, so we ignore)
            } else {
                // Not a space, add to word buffer
                if (word_index < FILE_BUFFER_SIZE) {
                    word_buffer[word_index++] = c;
                }
            }
        }
    }
    
    // After the loop, print any remaining word in the buffer
    if (word_index > 0) {
        word_buffer[word_index] = '\0';
        printf("%s", word_buffer);
        fflush(stdout);
    }
    
    printf("\n--- End of Stream ---\n");
    close(ss_sock_fd);
}


void do_write(const char* username, const char* filename, int initial_sentence_num) {
    // ... (unchanged) ...
    client_request_t req = {0};
    req.command = CMD_WRITE_FILE;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Name Server.\n"); return; }
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
         perror("recv response header from NM failed"); close(nm_sock_fd); return;
    }
    close(nm_sock_fd);
    if (nm_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg); return; }
    int ss_sock_fd = connect_to_server(nm_res.ss_ip, nm_res.ss_port);
    if (ss_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Storage Server at %s:%d.\n", nm_res.ss_ip, nm_res.ss_port); return; }
    send(ss_sock_fd, &req, sizeof(client_request_t), 0);
    ss_response_t ready_res;
    if (recv(ss_sock_fd, &ready_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("recv ready response from SS failed"); close(ss_sock_fd); return;
    }
    if (ready_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Storage Server]: %s\n", ready_res.error_msg); close(ss_sock_fd); return; }
    printf("File '%s' is locked. Enter write commands (e.g., '<word_index> <content>') or 'ETIRW' to save and exit.\n", filename);
    char write_input[FILE_BUFFER_SIZE + 100];
    int current_sentence = initial_sentence_num;
    while(1) {
        printf("(writing %s:%d) > ", filename, current_sentence);
        if (fgets(write_input, sizeof(write_input), stdin) == NULL) { break; }
        write_input[strcspn(write_input, "\n")] = 0; 
        client_write_chunk_t chunk = {0};
        chunk.sentence_index = current_sentence;
        if (strcmp(write_input, "ETIRW") == 0) {
            chunk.is_etirw = true;
            send(ss_sock_fd, &chunk, sizeof(client_write_chunk_t), 0);
            break; 
        }
        char* word_idx_str;
        char* content_str;
        char* first_space = strchr(write_input, ' ');
        if (first_space == NULL) {
            printf("Invalid format. Use: <word_index> <content> or ETIRW\n");
            continue;
        }
        *first_space = '\0';
        word_idx_str = write_input;
        content_str = first_space + 1; 
        while (isspace((unsigned char)*content_str)) { content_str++; }
        if (word_idx_str[0] == '\0' || content_str[0] == '\0') {
            printf("Invalid format. Use: <word_index> <content> or ETIRW\n");
            continue;
        }
        chunk.word_index = atoi(word_idx_str);
        strncpy(chunk.content, content_str, FILE_BUFFER_SIZE - 1);
        chunk.is_etirw = false;
        send(ss_sock_fd, &chunk, sizeof(client_write_chunk_t), 0);
    }
    ss_write_response_t final_res;
    if (recv(ss_sock_fd, &final_res, sizeof(ss_write_response_t), 0) != sizeof(ss_write_response_t)) {
         perror("recv final response from SS failed"); close(ss_sock_fd); return;
    }
    if (final_res.status == STATUS_OK) {
        printf("Write successful! (File now has %d sentences)\n", final_res.updated_sentence_count);
    } else { fprintf(stderr, "[Error from Storage Server]: %s\n", final_res.error_msg); }
    close(ss_sock_fd);
}

void do_addaccess(const char* username, const char* filename, const char* target_user, access_level_t level) {
    // ... (unchanged) ...
    client_request_t req = {0};
    req.command = CMD_ADD_ACCESS;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);
    strncpy(req.target_username, target_user, MAX_USERNAME_LEN - 1);
    req.access_level = level;
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Name Server.\n"); return; }
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response from NM failed"); close(nm_sock_fd); return;
    }
    close(nm_sock_fd);
    if (nm_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg); }
    else { printf("Access granted successfully!\n"); }
}

void do_remaccess(const char* username, const char* filename, const char* target_user) {
    // ... (unchanged) ...
    client_request_t req = {0};
    req.command = CMD_REM_ACCESS;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);
    strncpy(req.target_username, target_user, MAX_USERNAME_LEN - 1);
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Name Server.\n"); return; }
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response from NM failed"); close(nm_sock_fd); return;
    }
    close(nm_sock_fd);
    if (nm_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg); }
    else { printf("Access removed successfully!\n"); }
}

void do_delete(const char* username, const char* filename) {
    // ... (unchanged) ...
    client_request_t req = {0};
    req.command = CMD_DELETE_FILE;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Name Server.\n"); return; }
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response from NM failed"); close(nm_sock_fd); return;
    }
    close(nm_sock_fd); 
    if (nm_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg); return; }
    int ss_sock_fd = connect_to_server(nm_res.ss_ip, nm_res.ss_port);
    if (ss_sock_fd < 0) { fprintf(stderr, "Error: Could not connect to Storage Server at %s:%d.\n", nm_res.ss_ip, nm_res.ss_port); return; }
    send(ss_sock_fd, &req, sizeof(client_request_t), 0);
    ss_response_t ss_res;
    if (recv(ss_sock_fd, &ss_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("recv response from SS failed"); close(ss_sock_fd); return;
    }
    close(ss_sock_fd);
    if (ss_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Storage Server]: %s\n", ss_res.error_msg); }
    else { printf("File '%s' deleted successfully!\n", req.filename); }
}


// =================================================================
// --- Main Interactive Loop ---
// =================================================================

int main(int argc, char *argv[]) {
    char username[MAX_USERNAME_LEN];
    char input_line[1024];
    
    printf("Enter your username: ");
    if (fgets(username, MAX_USERNAME_LEN, stdin) == NULL) exit(EXIT_FAILURE);
    username[strcspn(username, "\n")] = 0; 

    printf("Welcome, %s! Type 'help' for commands.\n", username);

    while (1) {
        printf("(%s@nfs) > ", username);
        
        if (fgets(input_line, sizeof(input_line), stdin) == NULL) break;
        input_line[strcspn(input_line, "\n")] = 0;
        input_line[strcspn(input_line, "\r")] = 0;
        
        char* command = strtok(input_line, " \n");
        if (command == NULL) continue;

        if (strcmp(command, "exit") == 0) {
            break; 
        } else if (strcmp(command, "help") == 0) {
            print_help();
        } else if (strcmp(command, "CREATE") == 0) {
            char* filename = strtok(NULL, " \n");
            if (filename == NULL) fprintf(stderr, "Usage: CREATE <filename>\n");
            else do_create(username, filename);
        
        } else if (strcmp(command, "VIEW") == 0) {
            bool view_all = false;
            bool view_long = false;
            char* flag = strtok(NULL, " \n");
            while (flag != NULL) {
                if (strcmp(flag, "-a") == 0) view_all = true;
                else if (strcmp(flag, "-l") == 0) view_long = true;
                else if (strcmp(flag, "-al") == 0 || strcmp(flag, "-la") == 0) {
                    view_all = true; view_long = true;
                }
                flag = strtok(NULL, " \n");
            }
            do_view(username, view_all, view_long);

        } else if (strcmp(command, "READ") == 0) {
            char* filename = strtok(NULL, " \n");
            if (filename == NULL) fprintf(stderr, "Usage: READ <filename>\n");
            else do_read(username, filename);
        
        } else if (strcmp(command, "STREAM") == 0) { // NEW
            char* filename = strtok(NULL, " \n");
            if (filename == NULL) fprintf(stderr, "Usage: STREAM <filename>\n");
            else do_stream(username, filename);
        
        } else if (strcmp(command, "WRITE") == 0) { 
            char* filename = strtok(NULL, " \n");
            char* sentence_str = strtok(NULL, " \n");
            if (filename == NULL || sentence_str == NULL) {
                fprintf(stderr, "Usage: WRITE <filename> <sentence_number>\n");
            } else {
                do_write(username, filename, atoi(sentence_str));
            }

        } else if (strcmp(command, "ADDACCESS") == 0) { 
            char* level_str = strtok(NULL, " \n");
            char* filename = strtok(NULL, " \n");
            char* target_user = strtok(NULL, " \n");
            if (level_str == NULL || filename == NULL || target_user == NULL) {
                fprintf(stderr, "Usage: ADDACCESS <R|W> <filename> <username>\n");
            } else {
                access_level_t level;
                if (strcmp(level_str, "R") == 0) level = ACCESS_READ;
                else if (strcmp(level_str, "W") == 0) level = ACCESS_WRITE;
                else {
                    fprintf(stderr, "Error: Access level must be 'R' or 'W'\n");
                    continue;
                }
                do_addaccess(username, filename, target_user, level);
            }
        
        } else if (strcmp(command, "REMACCESS") == 0) { 
            char* filename = strtok(NULL, " \n");
            char* target_user = strtok(NULL, " \n");
            if (filename == NULL || target_user == NULL) {
                fprintf(stderr, "Usage: REMACCESS <filename> <username>\n");
            } else {
                do_remaccess(username, filename, target_user);
            }

        } else if (strcmp(command, "DELETE") == 0) {
            char* filename = strtok(NULL, " \n");
            if (filename == NULL) {
                fprintf(stderr, "Usage: DELETE <filename>\n");
            } else {
                do_delete(username, filename);
            }
        
        } else {
            fprintf(stderr, "Unknown command: '%s'. Type 'help' for commands.\n", command);
        }
    }

    printf("Goodbye, %s!\n", username);
    return 0;
}