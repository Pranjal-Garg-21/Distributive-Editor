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
    printf("  STREAM <filename>\n");
    printf("  WRITE <filename> <sentence_number>\n");
    printf("  UNDO <filename>\n");
    printf("  DELETE <filename>\n");
    printf("  INFO <filename>\n");
    printf("  LIST\n"); 
    printf("  ADDACCESS <R|W> <filename> <username>\n"); 
    printf("  REMACCESS <filename> <username>\n");  
    printf("  EXEC <filename>\n");    
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
// --- NEW HELPER FUNCTION ---
/**
 * @brief Checks if a string contains only digits.
 */
bool is_numeric(const char *s) {
    if (s == NULL || *s == '\0') { // Empty string
        return false;
    }
    while (*s) {
        if (!isdigit((unsigned char)*s)) {
            return false;
        }
        s++;
    }
    return true;
}
// --- END NEW HELPER FUNCTION ---
// =================================================================
// --- Command Handler Functions ---
// =================================================================

// (do_create, do_view, do_read, do_stream, do_write, do_addaccess, do_remaccess, do_delete, do_undo are all unchanged)

void do_create(const char* username, const char* filename) {
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

void do_stream(const char* username, const char* filename) {
    client_request_t req = {0};
    req.command = CMD_STREAM_FILE; 
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
    req.command = CMD_READ_FILE; 
    if (send(ss_sock_fd, &req, sizeof(client_request_t), 0) < 0) {
        perror("send request to SS failed"); close(ss_sock_fd); return;
    }
    ss_response_t header_res;
    if (recv(ss_sock_fd, &header_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("recv header response from SS failed"); close(ss_sock_fd); return;
    }
    if (header_res.status == STATUS_ERROR) { fprintf(stderr, "[Error from Storage Server]: %s\n", header_res.error_msg); close(ss_sock_fd); return; }
    printf("\n--- Streaming %s ---\n", req.filename);
    ss_file_data_chunk_t chunk;
    char word_buffer[FILE_BUFFER_SIZE + 1]; 
    int word_index = 0;
    while (1) {
        ssize_t n = recv(ss_sock_fd, &chunk, sizeof(ss_file_data_chunk_t), 0);
        if (n < 0) { perror("recv data chunk failed"); break; }
        if (n == 0) { fprintf(stderr, "Storage Server disconnected unexpectedly.\n"); break; }
        if (chunk.data_size == 0) { break; }
        for (int i = 0; i < chunk.data_size; i++) {
            char c = chunk.data[i];
            if (isspace(c)) { 
                if (word_index > 0) {
                    word_buffer[word_index] = '\0';
                    printf("%s ", word_buffer);
                    fflush(stdout); 
                    usleep(100000); 
                    word_index = 0; 
                }
            } else {
                if (word_index < FILE_BUFFER_SIZE) {
                    word_buffer[word_index++] = c;
                }
            }
        }
    }
    if (word_index > 0) {
        word_buffer[word_index] = '\0';
        printf("%s", word_buffer);
        fflush(stdout);
    }
    printf("\n--- End of Stream ---\n");
    close(ss_sock_fd);
}

void do_write(const char* username, const char* filename, int initial_sentence_num) {
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
        // --- NEW VALIDATION CHECK ---
        if (!is_numeric(word_idx_str)) {
            printf("Invalid format. First part must be a numeric <word_index>.\n");
            printf("Use: <word_index> <content> or ETIRW\n");
            continue; // Go back to the prompt
        }
        // --- END OF NEW CHECK ---
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

void do_undo(const char* username, const char* filename) {
    client_request_t req = {0};
    req.command = CMD_UNDO_FILE;
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
    else { printf("Undo successful! File '%s' has been reverted.\n", req.filename); }
}

void do_info(const char* username, const char* filename) {
    client_request_t req = {0};
    req.command = CMD_GET_INFO;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);

    // --- STEP 1: Connect to Name Server for ACL/Owner/Location ---
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) {
        fprintf(stderr, "Error: Could not connect to Name Server.\n");
        return;
    }

    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);

    // Receive the special INFO header
    nm_info_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_info_response_t), 0) != sizeof(nm_info_response_t)) {
        perror("recv info response header from NM failed");
        close(nm_sock_fd);
        return;
    }

    if (nm_res.status == STATUS_ERROR) {
        fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg);
        close(nm_sock_fd);
        return;
    }

    // --- Print NM Info ---
    printf("\n--- Info for %s ---\n", filename);
    printf("--> Owner: %s\n", nm_res.owner);
    printf("--> Access: ");
    
    nm_acl_entry_t acl_entry;
    for (int i = 0; i < nm_res.acl_count; i++) {
        if (recv(nm_sock_fd, &acl_entry, sizeof(nm_acl_entry_t), 0) != sizeof(nm_acl_entry_t)) {
            perror("recv ACL entry failed");
            break;
        }
        printf("%s (%s)%s", 
               acl_entry.username, 
               (acl_entry.level == ACCESS_WRITE) ? "RW" : "R",
               (i == nm_res.acl_count - 1) ? "" : ", ");
    }
    printf("\n");
    close(nm_sock_fd); // Done with NM

    // --- STEP 2: Connect to Storage Server for Stats ---
    int ss_sock_fd = connect_to_server(nm_res.ss_ip, nm_res.ss_port);
    if (ss_sock_fd < 0) {
        fprintf(stderr, "--> Stats: (Could not connect to Storage Server at %s:%d)\n", nm_res.ss_ip, nm_res.ss_port);
        printf("-----------------------------\n");
        return;
    }

    // We reuse CMD_GET_STATS, as it has all the info we need
    req.command = CMD_GET_STATS;
    if (send(ss_sock_fd, &req, sizeof(client_request_t), 0) < 0) {
        perror("send stats request to SS failed");
        close(ss_sock_fd);
        return;
    }

    ss_stats_response_t ss_res;
    if (recv(ss_sock_fd, &ss_res, sizeof(ss_stats_response_t), 0) != sizeof(ss_stats_response_t)) {
        perror("recv stats response from SS failed");
        close(ss_sock_fd);
        return;
    }
    close(ss_sock_fd);

    // --- Print SS Info ---
    if (ss_res.status == STATUS_OK) {
        char time_str[100];
        printf("--> Size: %ld bytes\n", ss_res.stats.char_count);
        
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&ss_res.stats.time_created));
        printf("--> Created: %s\n", time_str);
        
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&ss_res.stats.last_modified));
        printf("--> Last Modified: %s\n", time_str);
        
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&ss_res.stats.last_accessed));
        printf("--> Last Accessed: %s\n", time_str);
    } else {
        fprintf(stderr, "--> Stats: [Error from Storage Server]: %s\n", ss_res.error_msg);
    }
    printf("-----------------------------\n");
}


// --- ADD THIS NEW FUNCTION ---
void do_list(const char* username) {
    client_request_t req = {0};
    req.command = CMD_LIST_USERS;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);

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

    if (nm_res.status == STATUS_ERROR) {
        fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg);
        close(nm_sock_fd);
        return;
    }

    // We reuse file_count to mean user_count for this command
    int count = nm_res.file_count; 
    printf("\n--- Registered Users (%d) ---\n", count);
    if (count == 0) {
        printf("(No users found in the system)\n");
    } else {
        nm_user_entry_t user_entry;
        for (int i = 0; i < count; i++) {
            if (recv(nm_sock_fd, &user_entry, sizeof(nm_user_entry_t), 0) != sizeof(nm_user_entry_t)) {
                perror("recv user entry failed");
                break;
            }
            printf(" -> %s\n", user_entry.username);
        }
    }
    printf("-------------------------------\n");
    close(nm_sock_fd);
}

// client.c (add as a new function)

/**
 * @brief Handles the EXEC command.
 * Connects to the NM, sends the EXEC request, and then prints
 * the raw output piped back from the NM until the connection closes.
 */
void do_exec(const char* username, const char* filename) {
    client_request_t req = {0};
    req.command = CMD_EXEC;
    strncpy(req.username, username, MAX_USERNAME_LEN - 1);
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);

    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) {
        fprintf(stderr, "Error: Could not connect to Name Server.\n");
        return;
    }

    // Send the initial handshake and request
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0);
    send(nm_sock_fd, &req, sizeof(client_request_t), 0);

    // The NM will send ONE response header.
    // This tells us if the command is authorized and can begin.
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response header from NM failed");
        close(nm_sock_fd);
        return;
    }

    // If status is ERROR, the NM will close connection. Print error and stop.
    if (nm_res.status == STATUS_ERROR) {
        fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg);
        close(nm_sock_fd);
        return;
    }

    // If status is OK, the NM will now pipe the shell output.
    // We must read in a loop until the NM closes the connection.
    printf("\n--- Executing '%s' on server ---\n", filename);
    char buffer[4096];
    ssize_t bytes_read;
    while ((bytes_read = recv(nm_sock_fd, buffer, sizeof(buffer) - 1, 0)) > 0) {
        buffer[bytes_read] = '\0';
        // Use write() or fwrite() for raw output, not printf
        fwrite(buffer, 1, bytes_read, stdout); 
    }

    printf("\n--- Execution Finished ---\n");
    close(nm_sock_fd);
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
        
        } else if (strcmp(command, "STREAM") == 0) {
            char* filename = strtok(NULL, " \n");
            if (filename == NULL) fprintf(stderr, "Usage: STREAM <filename>\n");
            else do_stream(username, filename);
        
        } else if (strcmp(command, "INFO") == 0) {
            char* filename = strtok(NULL, " \n");
            if (filename == NULL) fprintf(stderr, "Usage: INFO <filename>\n");
            else do_info(username, filename);
        
        // --- ADDED THIS BLOCK ---
        } else if (strcmp(command, "LIST") == 0) {
            do_list(username);
        // --- END OF ADDED BLOCK ---

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
        
        } else if (strcmp(command, "UNDO") == 0) {
            char* filename = strtok(NULL, " \n");
            if (filename == NULL) {
                fprintf(stderr, "Usage: UNDO <filename>\n");
            } else {
                do_undo(username, filename);
            }
        } else if (strcmp(command, "EXEC") == 0) {
            char* filename = strtok(NULL, " \n");
            if (filename == NULL) fprintf(stderr, "Usage: EXEC <filename>\n");
            else do_exec(username, filename);
        }
        else {
            fprintf(stderr, "Unknown command: '%s'. Type 'help' for commands.\n", command);
        }
    }

    printf("Goodbye, %s!\n", username);
    return 0;
}

