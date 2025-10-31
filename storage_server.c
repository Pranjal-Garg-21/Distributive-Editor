#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h> // <-- NEW
#include "common.h"

// --- Configuration for this SS ---
#define MY_IP "127.0.0.1"
#define MY_CLIENT_PORT 9090 // The port this SS will open for clients

// --- Name Server Configuration ---
#define NM_IP "127.0.0.1"
// NM_PORT is already defined in common.h


// ------------------------------------------------------------------
// --- NEW: Client Handler Function (Worker Thread) ---
// ------------------------------------------------------------------

/**
 * @brief Handles a single client connection (for read/write/etc.)
 * * @param p_conn_fd A pointer to the client's connection file descriptor.
 */
void* handle_client_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    printf("   [SS-Thread]: Got a connection! (conn_fd: %d)\n", conn_fd);

    // --- FUTURE WORK (Person 2) ---
    // This is where you will:
    // 1. recv() a command from the client (e.g., read, write, create).
    // 2. Perform the file operation on the local filesystem (e.g., fopen, fread, fwrite).
    // 3. send() a success/failure response (or file data) back to the client.

    printf("   [SS-Thread]: Closing client connection.\n");
    close(conn_fd);
    return NULL;
}


// ------------------------------------------------------------------
// --- Main Function ---
// ------------------------------------------------------------------

int main() {
    int sock_fd;
    struct sockaddr_in nm_addr;
    ss_registration_t reg_data;

    // ------------------------------------------------------------------
    // PART 1: Register with Name Server (Your existing code)
    // ------------------------------------------------------------------
    
    // 1. Create socket (for NM)
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // 2. Set up the Name Server's address
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0) {
        perror("invalid Name Server IP address");
        exit(EXIT_FAILURE);
    }

    // 3. Connect to the Name Server
    printf("Attempting to connect to Name Server at %s:%d...\n", NM_IP, NM_PORT);
    if (connect(sock_fd, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
        perror("connection to Name Server failed");
        exit(EXIT_FAILURE);
    }
    printf("Connected to Name Server!\n");

    // 4. Prepare and send registration data
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

    // 5. Close the connection to the Name Server
    close(sock_fd);

    // ------------------------------------------------------------------
    // --- NEW: PART 2 - Become a Server for Clients ---
    // ------------------------------------------------------------------

    int listen_fd, conn_fd;
    struct sockaddr_in ss_serv_addr, client_addr;
    socklen_t client_len;

    // 1. Create socket (for Clients)
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("SS server socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Optional: Allows kernel to reuse port immediately
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));

    // 2. Bind socket to the SS port (MY_CLIENT_PORT)
    memset(&ss_serv_addr, 0, sizeof(ss_serv_addr));
    ss_serv_addr.sin_family = AF_INET;
    ss_serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); // Listen on all interfaces
    ss_serv_addr.sin_port = htons(MY_CLIENT_PORT);

    if (bind(listen_fd, (struct sockaddr*)&ss_serv_addr, sizeof(ss_serv_addr)) < 0) {
        perror("SS server bind failed");
        exit(EXIT_FAILURE);
    }

    // 3. Listen for connections
    if (listen(listen_fd, 5) < 0) {
        perror("SS server listen failed");
        exit(EXIT_FAILURE);
    }

    printf("\n[SS-Main]: Storage Server now listening for clients on port %d...\n", MY_CLIENT_PORT);

    // 4. Main accept loop (multi-threaded)
    while (1) {
        client_len = sizeof(client_addr);
        conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            perror("SS server accept failed");
            continue; // Keep listening
        }

        // Print client info
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("[SS-Main]: Accepted new client connection from %s\n", client_ip);

        // Pass the connection to a new thread
        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        
        if (pthread_create(&tid, NULL, handle_client_connection, p_conn_fd) != 0) {
            perror("pthread_create failed");
            free(p_conn_fd);
        }
        pthread_detach(tid); // We don't need to join it
    }

    // 5. Clean up (this part is unreachable in this code)
    close(listen_fd);
    return 0;
}