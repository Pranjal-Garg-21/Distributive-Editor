#ifndef COMMON_H
#define COMMON_H

// Publicly known port for the Name Server
#define NM_PORT 8080

// Max length for an IP address string (e.g., "xxx.xxx.xxx.xxx")
#define MAX_IP_LEN 16

// This is the registration message the SS sends to the NM.
// It contains the SS's public-facing IP and the port it will
// use to listen for client connections.
typedef struct {
    char ss_ip[MAX_IP_LEN];
    int client_port;
} ss_registration_t;

#endif // COMMON_H