#include <pthread.h>
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
pthread_mutex_t server_list_mutex;
/**
 * @brief Handles a single connection (either SS or Client) in its own thread.
 * * @param p_conn_fd A pointer to the connection's file descriptor.
 */
void* handle_connection(void* p_conn_fd) {
    // 1. Get the connection file descriptor from the pointer
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd); // Free the heap-allocated memory

    // 2. Receive the initial message type
    message_type_t msg_type;
    ssize_t n = recv(conn_fd, &msg_type, sizeof(message_type_t), 0);

    if (n != sizeof(message_type_t)) {
        fprintf(stderr, "Error receiving message type.\n");
        close(conn_fd);
        return NULL;
    }

    // 3. Route based on the message type
    if (msg_type == MSG_SS_REGISTER) {
        
        // --- This is the Storage Server registration logic ---
        ss_registration_t reg_data;
        n = recv(conn_fd, &reg_data, sizeof(ss_registration_t), 0);

        if (n == sizeof(ss_registration_t)) {
            
            // --- CRITICAL SECTION: Lock mutex ---
            // We lock to protect shared variables server_list and server_count
            pthread_mutex_lock(&server_list_mutex); 
            
            if (server_count < MAX_STORAGE_SERVERS) {
                // Add new server to our list
                strcpy(server_list[server_count].ip, reg_data.ss_ip);
                server_list[server_count].client_port = reg_data.client_port;
                server_count++;

                // Print registration info
                printf("-> Registered new Storage Server:\n");
                printf("   IP = %s\n", reg_data.ss_ip);
                printf("   Client Port = %d\n", reg_data.client_port);
                printf("   Total SS count: %d\n\n", server_count);
            } else {
                printf("Storage server list is full. Ignoring new server.\n");
            }
            
            // --- CRITICAL SECTION: Unlock mutex ---
            pthread_mutex_unlock(&server_list_mutex);

        } else {
            fprintf(stderr, "Error receiving SS registration data.\n");
        }
        
    } else if (msg_type == MSG_CLIENT_REQUEST) {
        
        // --- This is the Client handshake logic ---
        printf("-> Received a connection from a Client!\n");
        
        // In the future, you will:
        // 1. recv(conn_fd, &client_request_struct, ...);
        // 2. Process the request (e.g., ls, create, read, write)
        // 3. Send back a response to the client
        // send(conn_fd, &nm_response_struct, ...);
        
    } else {
        fprintf(stderr, "Unknown message type received: %d\n", msg_type);
    }

    // 4. We're done with this connection
    close(conn_fd);
    return NULL;
}
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
    if (pthread_mutex_init(&server_list_mutex, NULL) != 0) {
        perror("mutex init failed");
        exit(EXIT_FAILURE);
    }
    printf("Name Server listening on port %d...\n", NM_PORT);

 // 4. Main accept loop
    while (1) {
    client_len = sizeof(client_addr);
    conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
    if (conn_fd < 0) {
        perror("accept failed");
        continue;
    }

    // Pass the connection to a new thread
    pthread_t tid;
    int* p_conn_fd = malloc(sizeof(int));
    *p_conn_fd = conn_fd;
    pthread_create(&tid, NULL, handle_connection, p_conn_fd);
    pthread_detach(tid); // We don't need to join it
    } // End of while(1) loop
    close(listen_fd);
    return 0;
}