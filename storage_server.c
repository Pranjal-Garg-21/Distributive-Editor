#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "common.h"

// --- Configuration for this SS ---
// In a real scenario, this IP would be discovered, not hardcoded.
// We use 127.0.0.1 (localhost) for simple local testing.
#define MY_IP "127.0.0.1"
#define MY_CLIENT_PORT 9090 // The port this SS will open for clients

// --- Name Server Configuration ---
#define NM_IP "127.0.0.1"
// NM_PORT is already defined in common.h

int main() {
    int sock_fd;
    struct sockaddr_in nm_addr;
    ss_registration_t reg_data;

    // 1. Create socket
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // 2. Set up the Name Server's address
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);

    // Convert IP address from text to binary
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

   // ... inside main() after connect() ...

    printf("Connected to Name Server!\n");

    // 4. Prepare and send registration data
    
    // STEP 4A: Send the message type first
    message_type_t msg_type = MSG_SS_REGISTER;
    if (send(sock_fd, &msg_type, sizeof(message_type_t), 0) < 0) {
        perror("send message type failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    // STEP 4B: Now send the actual registration struct
    strcpy(reg_data.ss_ip, MY_IP);
    reg_data.client_port = MY_CLIENT_PORT;

    if (send(sock_fd, &reg_data, sizeof(reg_data), 0) < 0) {
        perror("send registration data failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    printf("Successfully registered with Name Server.\n");

// ... rest of the file ...

    // 5. Close the connection
    close(sock_fd);

    // In the real project, the SS would now proceed to open its
    // own server socket on MY_CLIENT_PORT (9090) to listen for
    // commands from clients or the NM.
    // printf("SS now listening for clients on port %d...\n", MY_CLIENT_PORT);
    // ... code to become a server ...

    return 0;
}