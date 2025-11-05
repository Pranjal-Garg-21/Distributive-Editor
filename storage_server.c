#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <sys/stat.h> 
#include <ctype.h>    
#include "common.h"
#include "uthash.h" 
#include <errno.h> 

#define MY_IP "127.0.0.1"
#define MY_CLIENT_PORT 9090 
#define NM_IP "127.0.0.1"



// --- Helper functions for WRITE ---

// A struct to hold the file in memory, split by sentences
typedef struct {
    char** sentences;
    int count;
} file_in_memory_t;

// Frees the in-memory file representation
void free_file_in_memory(file_in_memory_t* file_mem) {
    if (file_mem == NULL) return;
    for (int i = 0; i < file_mem->count; i++) {
        free(file_mem->sentences[i]);
    }
    free(file_mem->sentences);
    file_mem->sentences = NULL;
    file_mem->count = 0;
}

/**
 * @brief (REPLACED - FINAL FIX) A custom, robust sentence parser.
 * This replaces strtok and correctly handles all edge cases.
 */
file_in_memory_t parse_string_into_sentences(const char* content_str) {
    file_in_memory_t new_mem = { .sentences = NULL, .count = 0 };
    const char* start = content_str; // The start of the current sentence
    const char* p = content_str;     // The "current character" pointer
    bool is_first_sentence = true;

    // Trim leading space ONLY for the first sentence
    if (is_first_sentence) {
        while (isspace((unsigned char)*start)) {
            start++;
        }
        p = start; // Move p up with it
    }

    while (*p != '\0') {
        // Find the next delimiter from the current position
        const char* end = strpbrk(p, ".!?");

        if (end == NULL) {
            size_t remaining_len = strlen(start);
            if (remaining_len > 0) {
                // Check if it's just whitespace
                bool only_whitespace = true;
                for(size_t i = 0; i < remaining_len; i++) {
                    if (!isspace((unsigned char)start[i])) {
                        only_whitespace = false;
                        break;
                    }
                }
                
                if (!only_whitespace) {
                    char* last_sentence = strdup(start);
                    if (!last_sentence) {
                        perror("[SS-Parse] strdup failed for last sentence");
                        break;
                    }
                    
                    new_mem.count++;
                    new_mem.sentences = (char**)realloc(new_mem.sentences, new_mem.count * sizeof(char*));
                    new_mem.sentences[new_mem.count - 1] = last_sentence;
                    new_mem.count--;
                }
            }
            break; // End of string, exit loop
        }
        // We found a delimiter. The sentence is from 'start' to 'end' (inclusive).
        int sentence_len = (end - start) + 1;
        
        char* sentence = (char*)malloc(sentence_len + 1); // +1 for '\0'
        if (!sentence) {
            perror("[SS-Parse] malloc failed for sentence");
            break;
        }
        
        strncpy(sentence, start, sentence_len);
        sentence[sentence_len] = '\0';

        // Add to our list
        new_mem.count++;
        new_mem.sentences = (char**)realloc(new_mem.sentences, new_mem.count * sizeof(char*));
        if (!new_mem.sentences) {
            perror("[SS-Parse] realloc failed for sentences");
            free(sentence);
            break;
        }
        new_mem.sentences[new_mem.count - 1] = sentence;

        // Move 'start' and 'p' to the next character after the delimiter
        p = end + 1;
        start = p;

        // We are no longer on the first sentence
        is_first_sentence = false;
    }
    return new_mem;
}


// (MODIFIED to use new helper)
file_in_memory_t load_file_into_memory(const char* filename) {
    file_in_memory_t file_mem = { .sentences = NULL, .count = 0 };
    FILE* fp = fopen(filename, "r");
    if (fp == NULL) {
        if (errno == ENOENT) { 
             printf("   [SS-Write]: load_file: File not found, starting empty.\n");
             return file_mem; 
        }
        perror("   [SS-Write]: load_file fopen failed");
        return file_mem; 
    }

    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (fsize == 0) {
        fclose(fp);
        return file_mem; 
    }

    char* content = (char*)malloc(fsize + 1);
    if (!content) {
        perror("malloc file content failed");
        fclose(fp);
        return file_mem;
    }
    fread(content, 1, fsize, fp);
    fclose(fp);
    content[fsize] = 0;

    file_mem = parse_string_into_sentences(content); // This now calls the fixed func
    free(content);
    return file_mem;
}

// (UNCHANGED)
bool save_memory_to_file(const char* filename, file_in_memory_t* file_mem) {
    FILE* fp = fopen(filename, "w"); // "w" = overwrite
    if (fp == NULL) {
        perror("   [SS-Write]: save_file fopen failed");
        return false;
    }
    for (int i = 0; i < file_mem->count; i++) {
        fputs(file_mem->sentences[i], fp);
    }
    fclose(fp);
    return true;
}

/**
 * @brief (REPLACED) The core editing logic, with better safety checks.
 */
bool edit_sentence(file_in_memory_t* file_mem, int sentence_idx, int word_idx, const char* new_content) {
    
    // --- 1. Handle special case: new/empty file ---
    if (file_mem->count == 0) {
        if (sentence_idx != 0 || (word_idx != 0 && word_idx!=1)) {
             if (sentence_idx != 0) fprintf(stderr, "   [SS-Write]: File is empty. Can only write to sentence 0.\n");
             if (word_idx > 1) fprintf(stderr, "   [SS-Write]: File is empty. Can only write to word 0.\n");
             return false;
        }
        // Just parse the new content and assign it
        file_in_memory_t parsed_content = parse_string_into_sentences(new_content); 
        *file_mem = parsed_content; // Steal the pointers
        return true;
    }
    
    // --- 2. Check bounds ---
   if (sentence_idx < 0 || sentence_idx > file_mem->count) {
         fprintf(stderr, "   [SS-Write]: Sentence index %d out of bounds (0-%d)\n",
                 sentence_idx, file_mem->count - 1);
         return false;
    }

    // If client asked to edit at index == count, treat that as "append"
    if (sentence_idx == file_mem->count) {
        // Parse the new content into sentences and append them.
        file_in_memory_t parsed = parse_string_into_sentences(new_content);
        if (parsed.count == 0) {
            // nothing to append (empty or whitespace)
            if (parsed.sentences) free(parsed.sentences);
            return true;
        }

        int old_count = file_mem->count;
        int new_count = old_count + parsed.count;
        char **tmp = (char**)realloc(file_mem->sentences, new_count * sizeof(char*));
        if (!tmp) {
            perror("realloc failed while appending sentences");
            // free parsed sentences to avoid leak
            for (int i = 0; i < parsed.count; ++i) free(parsed.sentences[i]);
            free(parsed.sentences);
            return false;
        }
        file_mem->sentences = tmp;

        // move parsed sentences into file_mem (steal pointers)
        for (int i = 0; i < parsed.count; ++i) {
            file_mem->sentences[old_count + i] = parsed.sentences[i];
        }
        file_mem->count = new_count;

        // free the container (but not the strings; we moved them)
        free(parsed.sentences);
        return true;
    }

    // --- 3. Build the word list from the target sentence ---
    char* old_sentence = file_mem->sentences[sentence_idx];
    char delimiter = old_sentence[strlen(old_sentence) - 1];
    old_sentence[strlen(old_sentence) - 1] = '\0'; // Temporarily remove delimiter

    char* old_sentence_copy = strdup(old_sentence);
    if (!old_sentence_copy) {
        perror("strdup failed in edit_sentence");
        old_sentence[strlen(old_sentence)] = delimiter; 
        return false;
    }
    
    char* words[MAX_WORDS_PER_SENTENCE];
    int word_count = 0;
    char* token = strtok(old_sentence_copy, " ");
    while (token != NULL && word_count < MAX_WORDS_PER_SENTENCE) {
        words[word_count++] = token;
        token = strtok(NULL, " ");
    }

    if (word_idx < 0 || word_idx > word_count) {
        fprintf(stderr, "   [SS-Write]: Word index %d out of bounds (0-%d)\n", word_idx, word_count);
        free(old_sentence_copy);
        old_sentence[strlen(old_sentence)] = delimiter; 
        return false;
    }

    // --- 4. Rebuild the sentence in memory (using open_memstream) ---
    char* rebuilt_sentence = NULL;
    size_t rebuilt_size = 0;
    FILE* mem_stream = open_memstream(&rebuilt_sentence, &rebuilt_size);
    if (!mem_stream) {
        perror("open_memstream failed");
        free(old_sentence_copy);
        old_sentence[strlen(old_sentence)] = delimiter;
        return false;
    }
    
    for (int i = 0; i < word_count; i++) {
        if (i == word_idx) {
            fprintf(mem_stream, "%s ", new_content);
        }
        fprintf(mem_stream, "%s ", words[i]);
    }
    if (word_idx == word_count) { 
         fprintf(mem_stream, "%s", new_content);
    } else {
         fseek(mem_stream, -1, SEEK_CUR); 
    }
    
    fprintf(mem_stream, "%c", delimiter); 
    fclose(mem_stream);
    free(old_sentence_copy); 
    
    // --- 5. Re-parse the rebuilt sentence for new delimiters ---
    file_in_memory_t new_splits = parse_string_into_sentences(rebuilt_sentence); 
    free(rebuilt_sentence); 

    if (new_splits.count == 0) {
        return true; 
    }
    
    // --- 6. Splice the new sentences into the main list ---
    free(file_mem->sentences[sentence_idx]); 

    if (new_splits.count == 1) {
        file_mem->sentences[sentence_idx] = new_splits.sentences[0];
        free(new_splits.sentences); 
    } else {
        int split_count = new_splits.count;
        int old_count = file_mem->count;
        int new_count = old_count + split_count - 1;
        
        file_mem->sentences = (char**)realloc(file_mem->sentences, new_count * sizeof(char*));
        if (!file_mem->sentences) {
            perror("realloc failed in splice");
            return false;
        }
        
        memmove(&file_mem->sentences[sentence_idx + split_count], 
                &file_mem->sentences[sentence_idx + 1], 
                (old_count - (sentence_idx + 1)) * sizeof(char*));
                
        for (int i = 0; i < split_count; i++) {
            file_mem->sentences[sentence_idx + i] = new_splits.sentences[i];
        }
        
        file_mem->count = new_count;
        free(new_splits.sentences); 
    }
    return true;
}

// --- (Rest of file is unchanged) ---

// Creates a backup of a file
void create_backup_for_undo(const char* filename) {
    char backup_filename[MAX_FILENAME_LEN + 5];
    sprintf(backup_filename, "%s.bak", filename);

    FILE* orig_fp = fopen(filename, "r");
    if (orig_fp == NULL) {
        if (errno == ENOENT) return; 
        perror("   [SS-Write]: UNDO backup (read) failed");
        return;
    }

    FILE* bak_fp = fopen(backup_filename, "w");
    if (bak_fp == NULL) {
        perror("   [SS-Write]: UNDO backup (write) failed");
        fclose(orig_fp);
        return;
    }

    char buf[4096];
    size_t bytes;
    while ((bytes = fread(buf, 1, sizeof(buf), orig_fp)) > 0) {
        fwrite(buf, 1, bytes, bak_fp);
    }
    
    fclose(bak_fp);
    fclose(orig_fp);
    printf("   [SS-Thread]: Backup created: %s\n", backup_filename);
}

// --- Per-File Lock Management (Hash Map) ---

typedef struct {
    char filename[MAX_FILENAME_LEN]; 
    pthread_rwlock_t lock;           
    UT_hash_handle hh;               
} file_lock_t;

file_lock_t* g_file_lock_map = NULL;
pthread_mutex_t g_lock_map_mutex;

// Helper function to get word/line counts
void get_file_counts(const char* filename, long* word_count, long* line_count) {
    FILE* fp = fopen(filename, "r");
    *word_count = 0;
    *line_count = 0;
    if (fp == NULL) {
        perror("   [SS-Thread]: get_file_counts fopen failed");
        return; 
    }
    int c;
    bool in_word = false;
    while ((c = fgetc(fp)) != EOF) {
        if (c == '\n') {
            (*line_count)++;
        }
        if (isspace(c)) {
            in_word = false;
        } else if (!in_word) {
            in_word = true;
            (*word_count)++;
        }
    }
    if (*line_count == 0 && *word_count > 0) *line_count = 1; 
    fclose(fp);
}

// Hash Map Lock Manager Function
pthread_rwlock_t* get_lock_for_file(const char* filename) {
    file_lock_t* found_lock;

    pthread_mutex_lock(&g_lock_map_mutex);

    HASH_FIND_STR(g_file_lock_map, filename, found_lock);

    if (found_lock) {
        pthread_mutex_unlock(&g_lock_map_mutex);
        return &found_lock->lock;
    }

    found_lock = (file_lock_t*)malloc(sizeof(file_lock_t));
    if (found_lock == NULL) {
        perror("   [SS-LockMgr]: malloc failed for new lock");
        pthread_mutex_unlock(&g_lock_map_mutex); 
        return NULL;
    }

    strcpy(found_lock->filename, filename);
    if (pthread_rwlock_init(&found_lock->lock, NULL) != 0) {
        perror("   [SS-LockMgr]: rwlock_init failed");
        free(found_lock);
        pthread_mutex_unlock(&g_lock_map_mutex); 
        return NULL;
    }

    HASH_ADD_STR(g_file_lock_map, filename, found_lock);
    printf("   [SS-LockMgr]: Initialized new lock for file '%s'\n", filename);

    pthread_mutex_unlock(&g_lock_map_mutex);
    return &found_lock->lock;
}

// Handles a single client connection
void* handle_client_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    printf("   [SS-Thread]: Got a connection! (conn_fd: %d)\n", conn_fd);

    client_request_t req;
    bool response_sent = false; 

    ssize_t n = recv(conn_fd, &req, sizeof(client_request_t), 0);
    if (n != sizeof(client_request_t)) {
        fprintf(stderr, "   [SS-Thread]: Error receiving client request.\n");
        close(conn_fd);
        return NULL;
    }

    pthread_rwlock_t* file_lock = get_lock_for_file(req.filename);

    if (file_lock == NULL) {
        ss_response_t res = {0};
        res.status = STATUS_ERROR;
        strcpy(res.error_msg, "Storage Server internal error. Try again later.");
        if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
            perror("   [SS-Thread]: send capacity error response failed");
        }
        response_sent = true;
    } else {
        // --- We have a lock, now proceed with commands ---

        if (req.command == CMD_CREATE_FILE) {
            printf("   [SS-Thread]: Handling CMD_CREATE_FILE for '%s'\n", req.filename);
            
            pthread_rwlock_wrlock(file_lock);

            ss_response_t res = {0}; 
            FILE* fp = fopen(req.filename, "w"); 
            if (fp == NULL) {
                perror("   [SS-Thread]: fopen failed");
                res.status = STATUS_ERROR;
                strcpy(res.error_msg, "File creation failed on Storage Server");
            } else {
                fclose(fp);
                res.status = STATUS_OK;
                printf("   [SS-Thread]: Successfully created file '%s'\n", req.filename);
            }
            
            pthread_rwlock_unlock(file_lock);

            if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
                perror("   [SS-Thread]: send create response failed");
            }
            response_sent = true;

        } else if (req.command == CMD_GET_STATS) {
            printf("   [SS-Thread]: Handling CMD_GET_STATS for '%s'\n", req.filename);
            
            pthread_rwlock_rdlock(file_lock);

            ss_stats_response_t res = {0}; 
            struct stat file_stat;
            if (stat(req.filename, &file_stat) < 0) {
                perror("   [SS-Thread]: stat failed");
                res.status = STATUS_ERROR;
                strcpy(res.error_msg, "File not found on Storage Server");
            } else {
                res.status = STATUS_OK;
                res.stats.char_count = file_stat.st_size;
                res.stats.last_modified = file_stat.st_mtime;
                get_file_counts(req.filename, &res.stats.word_count, &res.stats.line_count);
                printf("   [SS-Thread]: Stats for '%s': %ld chars, %ld words, %ld lines\n",
                       req.filename, res.stats.char_count, res.stats.word_count, res.stats.line_count);
            }
            
            pthread_rwlock_unlock(file_lock);

            if (send(conn_fd, &res, sizeof(ss_stats_response_t), 0) < 0) {
                perror("   [SS-Thread]: send stats response failed");
            }
            response_sent = true;

        } else if (req.command == CMD_READ_FILE) {
            printf("   [SS-Thread]: Handling CMD_READ_FILE for '%s'\n", req.filename);
            
            pthread_rwlock_rdlock(file_lock);

            ss_response_t header_res = {0};
            FILE* fp = fopen(req.filename, "r");

            if (fp == NULL) {
                perror("   [SS-Thread]: fopen failed for read");
                header_res.status = STATUS_ERROR;
                strcpy(header_res.error_msg, "File not found or unreadable on Storage Server");
                if (send(conn_fd, &header_res, sizeof(ss_response_t), 0) < 0) {
                    perror("   [SS-Thread]: send read error response failed");
                }
            } else {
                header_res.status = STATUS_OK;
                if (send(conn_fd, &header_res, sizeof(ss_response_t), 0) < 0) {
                    perror("   [SS-Thread]: send read OK response failed");
                    fclose(fp);
                    pthread_rwlock_unlock(file_lock); 
                    close(conn_fd);
                    return NULL;
                }

                ss_file_data_chunk_t chunk;
                size_t bytes_read;
                while ((bytes_read = fread(chunk.data, 1, FILE_BUFFER_SIZE, fp)) > 0) {
                    chunk.data_size = bytes_read;
                    if (send(conn_fd, &chunk, sizeof(ss_file_data_chunk_t), 0) < 0) {
                        perror("   [SS-Thread]: send data chunk failed");
                        break; 
                    }
                }

                chunk.data_size = 0;
                if (send(conn_fd, &chunk, sizeof(ss_file_data_chunk_t), 0) < 0) {
                    perror("   [SS-Thread]: send terminator chunk failed");
                }
                
                fclose(fp);
                printf("   [SS-Thread]: File '%s' sent successfully.\n", req.filename);
            }
            
            pthread_rwlock_unlock(file_lock);
            response_sent = true;
        } 
        else if (req.command == CMD_WRITE_FILE) {
            printf("   [SS-Thread]: Handling CMD_WRITE_FILE for '%s'\n", req.filename);
            
            pthread_rwlock_wrlock(file_lock);
            
            ss_response_t ready_res = { .status = STATUS_OK };
            ss_write_response_t final_res = { .status = STATUS_OK };

            create_backup_for_undo(req.filename);

            file_in_memory_t file_mem = load_file_into_memory(req.filename);

            if (send(conn_fd, &ready_res, sizeof(ss_response_t), 0) < 0) {
                perror("   [SS-Thread]: send ready-for-write response failed");
                response_sent = true; 
            } else {
                client_write_chunk_t chunk;
                while (recv(conn_fd, &chunk, sizeof(client_write_chunk_t), 0) == sizeof(client_write_chunk_t)) {
                    if (chunk.is_etirw) {
                        break; // ETIRW received
                    }
                    
                    if (!edit_sentence(&file_mem, chunk.sentence_index, chunk.word_index, chunk.content)) {
                        final_res.status = STATUS_ERROR;
                        strcpy(final_res.error_msg, "Failed to edit sentence (e.g., index out of bounds)");
                        break;
                    }
                }

                if (final_res.status == STATUS_OK) {
                    if (!save_memory_to_file(req.filename, &file_mem)) {
                        final_res.status = STATUS_ERROR;
                        strcpy(final_res.error_msg, "Failed to save file to disk");
                    }
                }
                
                final_res.updated_sentence_count = file_mem.count;
                
                if (send(conn_fd, &final_res, sizeof(ss_write_response_t), 0) < 0) {
                    perror("   [SS-Thread]: send final write response failed");
                }
            }
            
            free_file_in_memory(&file_mem);
            pthread_rwlock_unlock(file_lock);
            response_sent = true;
        }
    } 

    if (!response_sent) {
        ss_response_t res = {0};
        res.status = STATUS_ERROR;
        strcpy(res.error_msg, "Unknown command received by SS");
        if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
            perror("   [SS-Thread]: send error response failed");
        }
    }

    close(conn_fd);
    printf("   [SS-Thread]: Closing client connection.\n");
    return NULL;
}


// --- Main Function ---
int main() {
    int sock_fd;
    struct sockaddr_in nm_addr;
    ss_registration_t reg_data;

    // --- 1. Register with Name Server ---
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0) {
        perror("invalid Name Server IP address");
        exit(EXIT_FAILURE);
    }

    printf("Attempting to connect to Name Server at %s:%d...\n", NM_IP, NM_PORT);
    if (connect(sock_fd, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
        perror("connection to Name Server failed");
        exit(EXIT_FAILURE);
    }

    printf("Connected to Name Server!\n");
    
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
    close(sock_fd); 

    // --- Initialize the global lock map mutex ---
    if (pthread_mutex_init(&g_lock_map_mutex, NULL) != 0) {
        perror("g_lock_map_mutex init failed");
        exit(EXIT_FAILURE);
    }

    // --- 2. Become a server for clients ---
    int listen_fd;
    struct sockaddr_in ss_serv_addr;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("SS server socket creation failed");
        exit(EXIT_FAILURE);
    }
    
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));

    memset(&ss_serv_addr, 0, sizeof(ss_serv_addr));
    ss_serv_addr.sin_family = AF_INET;
    ss_serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    ss_serv_addr.sin_port = htons(MY_CLIENT_PORT);

    if (bind(listen_fd, (struct sockaddr*)&ss_serv_addr, sizeof(ss_serv_addr)) < 0) {
        perror("SS server bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(listen_fd, 5) < 0) {
        perror("SS server listen failed");
        exit(EXIT_FAILURE);
    }
    printf("\n[SS-Main]: Storage Server now listening for clients on port %d...\n", MY_CLIENT_PORT);

    // --- 3. Main Accept Loop ---
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            perror("SS server accept failed");
            continue; 
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("[SS-Main]: Accepted new client connection from %s\n", client_ip);

        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        
        if (pthread_create(&tid, NULL, handle_client_connection, p_conn_fd) != 0) {
            perror("pthread_create failed");
            free(p_conn_fd);
        }
        pthread_detach(tid); 
    }

    // --- 4. Cleanup ---
    close(listen_fd);
    
    pthread_mutex_destroy(&g_lock_map_mutex);
    
    file_lock_t *current_lock, *tmp;
    HASH_ITER(hh, g_file_lock_map, current_lock, tmp) {
        pthread_rwlock_destroy(&current_lock->lock); 
        HASH_DEL(g_file_lock_map, current_lock);    
        free(current_lock);                       
    }
    
    return 0;
}

