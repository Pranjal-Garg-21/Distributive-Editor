/* logger.h */
#ifndef LOGGER_H
#define LOGGER_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <stdarg.h> // For variadic functions
#include <string.h> // For strerror
#include <errno.h>  // For errno
#include <arpa/inet.h> // <-- THIS IS THE FIX

/* --- Log Globals (to be defined in each .c file) --- */
static FILE* g_log_fp = NULL;
static pthread_mutex_t g_log_mutex;

typedef enum {
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR
} log_level_t;

/* --- Core Logging Functions --- */

/**
 * @brief Initializes the global log file and mutex.
 * Must be called once at startup.
 */
static void log_init(const char* filename) {
    g_log_fp = fopen(filename, "a"); // Open in "append" mode
    if (g_log_fp == NULL) {
        perror("[Main] CRITICAL: Failed to open log file. Continuing without file logging.");
    }
    if (pthread_mutex_init(&g_log_mutex, NULL) != 0) {
        perror("[Main] CRITICAL: log mutex init failed");
        if (g_log_fp) fclose(g_log_fp);
        exit(EXIT_FAILURE); // This is a fatal error
    }
}

/**
 * @brief Closes the log file and destroys the mutex.
 * Must be called once at shutdown.
 */
static void log_shutdown() {
    pthread_mutex_lock(&g_log_mutex);
    if (g_log_fp) {
        fprintf(g_log_fp, "[%s] [INFO] [SYS:0] [SYS] --- Log Shutdown ---\n", __TIME__);
        fclose(g_log_fp);
        g_log_fp = NULL;
    }
    pthread_mutex_unlock(&g_log_mutex);
    pthread_mutex_destroy(&g_log_mutex);
}

/**
 * @brief The main, thread-safe logging function.
 */
static void server_log(log_level_t level, const char* ip, int port, const char* username, const char* format, ...) {
    // 1. Get timestamp
    char time_buf[100];
    time_t now = time(NULL);
    struct tm* t = localtime(&now);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", t);

    // 2. Map level to string
    const char* level_str = "INFO";
    if (level == LOG_ERROR) level_str = "ERROR";
    if (level == LOG_WARN) level_str = "WARN";

    // 3. Format the main message using varargs
    char msg_buf[4096]; 
    va_list args;
    va_start(args, format);
    vsnprintf(msg_buf, sizeof(msg_buf), format, args);
    va_end(args);

    // 4. Lock and write to the file
    pthread_mutex_lock(&g_log_mutex);
    if (g_log_fp) {
        fprintf(g_log_fp, "[%s] [%s] [%s:%d] [%s] %s\n",
                time_buf,
                level_str,
                ip ? ip : "N/A",      
                port,
                username ? username : "N/A", 
                msg_buf);
        fflush(g_log_fp); // Ensure it's written immediately
    }
    pthread_mutex_unlock(&g_log_mutex);
}

/* --- Helper struct for passing args to threads --- */

/**
 * @brief A helper struct to pass connection details to a new thread.
 */
typedef struct {
    int conn_fd;
    char ip_str[INET_ADDRSTRLEN];
    int port;
} thread_arg_t;


#endif // LOGGER_H