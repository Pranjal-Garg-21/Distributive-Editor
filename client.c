#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "common.h"

#define NM_IP "127.0.0.1"
// NM_PORT is from common.h

/**
 * @brief Prints the usage of the client program.
 */
void print_usage(char* prog_name) {
    fprintf(stderr, "Usage: %s CREATE <filename>\n", prog_name);
    // Add other commands as you implement them
    // fprintf(stderr, "       %s DELETE <filename>\n", prog_name);
    // fprintf(stderr, "       %s READ <filename>\n", prog_name);
}

/**
 * @brief Connects to a server at the given IP and port.
 * @return The socket file descriptor, or -1 on failure.
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
        perror("connection failed");
        close(sock_fd);
        return -1;
    }
    return sock_fd;
}


int main(int argc, char *argv[]) {
    // 1. Validate and parse command-line arguments
    if (argc != 3) {
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    char* command_str = argv[1];
    char* filename = argv[2];

    client_request_t req = {0};

    if (strcmp(command_str, "CREATE") == 0) {
        req.command = CMD_CREATE_FILE;
    } else {
        fprintf(stderr, "Error: Unknown command '%s'\n", command_str);
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    strncpy(req.filename, filename, MAX_FILENAME_LEN - 1);


    // -------------------------------------------------
    // --- STEP 1: Connect to Name Server ---
    // -------------------------------------------------
    printf("Connecting to Name Server at %s:%d...\n", NM_IP, NM_PORT);
    int nm_sock_fd = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock_fd < 0) {
        exit(EXIT_FAILURE);
    }
    printf("Connected to Name Server!\n");

    // 2. Send handshake message
    message_type_t msg_type = MSG_CLIENT_NM_REQUEST;
    if (send(nm_sock_fd, &msg_type, sizeof(message_type_t), 0) < 0) {
        perror("send handshake failed");
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }

    // 3. Send the actual command request
    if (send(nm_sock_fd, &req, sizeof(client_request_t), 0) < 0) {
        perror("send request to NM failed");
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }

    // 4. Receive the NM's response
    nm_response_t nm_res;
    if (recv(nm_sock_fd, &nm_res, sizeof(nm_response_t), 0) != sizeof(nm_response_t)) {
        perror("recv response from NM failed");
        close(nm_sock_fd);
        exit(EXIT_FAILURE);
    }
    close(nm_sock_fd); // We are done with the Name Server


    // 5. Check the NM's response
    if (nm_res.status == STATUS_ERROR) {
        fprintf(stderr, "[Error from Name Server]: %s\n", nm_res.error_msg);
        exit(EXIT_FAILURE);
    }

    printf("NM approved. Contacting Storage Server at %s:%d...\n", nm_res.ss_ip, nm_res.ss_port);


    // -------------------------------------------------
    // --- STEP 2: Connect to Storage Server ---
    // -------------------------------------------------
    int ss_sock_fd = connect_to_server(nm_res.ss_ip, nm_res.ss_port);
    if (ss_sock_fd < 0) {
        exit(EXIT_FAILURE);
    }
    printf("Connected to Storage Server!\n");

    // 6. Send the *same* request to the SS
    if (send(ss_sock_fd, &req, sizeof(client_request_t), 0) < 0) {
        perror("send request to SS failed");
        close(ss_sock_fd);
        exit(EXIT_FAILURE);
    }

    // 7. Receive the SS's final response
    ss_response_t ss_res;
    if (recv(ss_sock_fd, &ss_res, sizeof(ss_response_t), 0) != sizeof(ss_response_t)) {
        perror("recv response from SS failed");
        close(ss_sock_fd);
        exit(EXIT_FAILURE);
    }
    close(ss_sock_fd); // Done with the Storage Server

    
    // 8. Report final status to the user
    if (ss_res.status == STATUS_ERROR) {
        fprintf(stderr, "[Error from Storage Server]: %s\n", ss_res.error_msg);
        exit(EXIT_FAILURE);
    }

    printf("\nSuccess! File '%s' created.\n", req.filename);

    return 0;
}