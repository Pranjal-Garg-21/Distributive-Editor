#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "common.h"

// For this simple start, we'll just use a global array.
// Your final project will need a more robust structure (like a hash map).
#define MAX_STORAGE_SERVERS 10
typedef struct {
    char ip[MAX_IP_LEN];
    int client_port;
} ss_info_t;

ss_info_t server_list[MAX_STORAGE_SERVERS];
int server_count = 0;

int main() {
    int listen_fd, conn_fd;
    struct sockaddr_in serv_addr, client_addr;
    socklen_t client_len;
    ss_registration_t reg_data;

    // 1. Create socket
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Optional: Allows kernel to reuse port immediately after server restart
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));

    // 2. Bind socket to the Name Server port
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); // Listen on all interfaces
    serv_addr.sin_port = htons(NM_PORT);

    if (bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // 3. Listen for connections
    if (listen(listen_fd, 5) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Name Server listening on port %d...\n", NM_PORT);

    // 4. Main accept loop
    while (1) {
        client_len = sizeof(client_addr);
        conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            perror("accept failed");
            continue; // Keep listening
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("Received connection from %s\n", client_ip);

        // 5. Receive registration data
        ssize_t n = recv(conn_fd, &reg_data, sizeof(ss_registration_t), 0);

        if (n == sizeof(ss_registration_t)) {
            // 6. Store and print the registration info
            if (server_count < MAX_STORAGE_SERVERS) {
                strcpy(server_list[server_count].ip, reg_data.ss_ip);
                server_list[server_count].client_port = reg_data.client_port;
                server_count++;

                printf("-> Registered new Storage Server:\n");
                printf("   IP = %s\n", reg_data.ss_ip);
                printf("   Client Port = %d\n", reg_data.client_port);
                printf("   Total SS count: %d\n\n", server_count);
            } else {
                printf("Storage server list is full. Ignoring new server.\n");
            }
        } else {
            fprintf(stderr, "Error receiving registration data.\n");
        }

        // For this simple handshake, we close the connection.
        // A real system might keep it for heartbeats.
        close(conn_fd);
    }

    close(listen_fd);
    return 0;
}