#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "common.h"

#define NM_IP "127.0.0.1"
// NM_PORT is from common.h

int main() {
    int sock_fd;
    struct sockaddr_in nm_addr;

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
    if (inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0) {
        perror("invalid Name Server IP address");
        exit(EXIT_FAILURE);
    }

    // 3. Connect to the Name Server
    if (connect(sock_fd, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
        perror("connection to Name Server failed");
        exit(EXIT_FAILURE);
    }

    printf("Connected to Name Server!\n");

    // 4. Send handshake message type
    message_type_t msg_type = MSG_CLIENT_REQUEST;
    if (send(sock_fd, &msg_type, sizeof(message_type_t), 0) < 0) {
        perror("send message type failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    printf("Sent client handshake. Closing connection.\n");

    // 5. Close the connection
    close(sock_fd);

    return 0;
}