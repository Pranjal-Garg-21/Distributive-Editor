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


// --- (All WRITE helper functions and lock manager are unchanged) ---
typedef struct { char** sentences; int count; } file_in_memory_t;
typedef struct { client_write_chunk_t* chunks; int count; int capacity; } edit_buffer_t;
void free_file_in_memory(file_in_memory_t* file_mem) {
    if (file_mem == NULL) return;
    for (int i = 0; i < file_mem->count; i++) { free(file_mem->sentences[i]); }
    free(file_mem->sentences);
    file_mem->sentences = NULL;
    file_mem->count = 0;
}
file_in_memory_t parse_string_into_sentences(const char* content_str) {
    file_in_memory_t new_mem = { .sentences = NULL, .count = 0 };
    const char* start = content_str;
    const char* p = content_str;
    bool is_first_sentence = true;
    if (is_first_sentence) { while (isspace((unsigned char)*start)) { start++; } p = start; }
    while (*p != '\0') {
        const char* end = strpbrk(p, ".!?");
        if (end == NULL) {
            size_t remaining_len = strlen(start);
            if (remaining_len > 0) {
                bool only_whitespace = true;
                for(size_t i = 0; i < remaining_len; i++) { if (!isspace((unsigned char)start[i])) { only_whitespace = false; break; } }
                if (!only_whitespace) {
                    char* last_sentence = strdup(start);
                    if (!last_sentence) { perror("[SS-Parse] strdup failed for last sentence"); break; }
                    new_mem.count++;
                    new_mem.sentences = (char**)realloc(new_mem.sentences, new_mem.count * sizeof(char*));
                    new_mem.sentences[new_mem.count - 1] = last_sentence;
                }
            }
            break; 
        }
        int sentence_len = (end - start) + 1;
        char* sentence = (char*)malloc(sentence_len + 1);
        if (!sentence) { perror("[SS-Parse] malloc failed for sentence"); break; }
        strncpy(sentence, start, sentence_len);
        sentence[sentence_len] = '\0';
        new_mem.count++;
        new_mem.sentences = (char**)realloc(new_mem.sentences, new_mem.count * sizeof(char*));
        if (!new_mem.sentences) { perror("[SS-Parse] realloc failed for sentences"); free(sentence); break; }
        new_mem.sentences[new_mem.count - 1] = sentence;
        p = end + 1;
        start = p;
        is_first_sentence = false;
    }
    return new_mem;
}
file_in_memory_t load_file_into_memory(const char* filename) {
    file_in_memory_t file_mem = { .sentences = NULL, .count = 0 };
    FILE* fp = fopen(filename, "r");
    if (fp == NULL) {
        if (errno == ENOENT) { printf("   [SS-Write]: load_file: File not found, starting empty.\n"); return file_mem; }
        perror("   [SS-Write]: load_file fopen failed");
        return file_mem; 
    }
    fseek(fp, 0, SEEK_END); long fsize = ftell(fp); fseek(fp, 0, SEEK_SET);
    if (fsize == 0) { fclose(fp); return file_mem; }
    char* content = (char*)malloc(fsize + 1);
    if (!content) { perror("malloc file content failed"); fclose(fp); return file_mem; }
    fread(content, 1, fsize, fp); fclose(fp); content[fsize] = 0;
    file_mem = parse_string_into_sentences(content);
    free(content);
    return file_mem;
}
bool save_memory_to_file(const char* filename, file_in_memory_t* file_mem) {
    FILE* fp = fopen(filename, "w");
    if (fp == NULL) { perror("   [SS-Write]: save_file fopen failed"); return false; }
    for (int i = 0; i < file_mem->count; i++) { fputs(file_mem->sentences[i], fp); }
    fclose(fp); return true;
}
bool edit_sentence(file_in_memory_t* file_mem, int sentence_idx, int word_idx, const char* new_content) {
    if (file_mem->count == 0) {
        if (sentence_idx != 0 || (word_idx != 0 && word_idx!=1)) {
             if (sentence_idx != 0) fprintf(stderr, "   [SS-Write]: File is empty. Can only write to sentence 0.\n");
             if (word_idx > 1) fprintf(stderr, "   [SS-Write]: File is empty. Can only write to word 0.\n");
             return false;
        }
        file_in_memory_t parsed_content = parse_string_into_sentences(new_content); 
        *file_mem = parsed_content; 
        return true;
    }
   if (sentence_idx < 0 || sentence_idx > file_mem->count) {
         fprintf(stderr, "   [SS-Write]: Sentence index %d out of bounds (0-%d)\n", sentence_idx, file_mem->count - 1);
         return false;
    }
    if (sentence_idx == file_mem->count) {
        file_in_memory_t parsed = parse_string_into_sentences(new_content);
        if (parsed.count == 0) { if (parsed.sentences) free(parsed.sentences); return true; }
        int old_count = file_mem->count; int new_count = old_count + parsed.count;
        char **tmp = (char**)realloc(file_mem->sentences, new_count * sizeof(char*));
        if (!tmp) {
            perror("realloc failed while appending sentences");
            for (int i = 0; i < parsed.count; ++i) free(parsed.sentences[i]);
            free(parsed.sentences);
            return false;
        }
        file_mem->sentences = tmp;
        for (int i = 0; i < parsed.count; ++i) { file_mem->sentences[old_count + i] = parsed.sentences[i]; }
        file_mem->count = new_count;
        free(parsed.sentences);
        return true;
    }
    char* old_sentence = file_mem->sentences[sentence_idx];
    char delimiter = old_sentence[strlen(old_sentence) - 1];
    old_sentence[strlen(old_sentence) - 1] = '\0'; 
    char* old_sentence_copy = strdup(old_sentence);
    if (!old_sentence_copy) { perror("strdup failed in edit_sentence"); old_sentence[strlen(old_sentence)] = delimiter; return false; }
    char* words[MAX_WORDS_PER_SENTENCE];
    int word_count = 0;
    char* token = strtok(old_sentence_copy, " ");
    while (token != NULL && word_count < MAX_WORDS_PER_SENTENCE) { words[word_count++] = token; token = strtok(NULL, " "); }
    if (word_idx < 0 || word_idx > word_count) {
        fprintf(stderr, "   [SS-Write]: Word index %d out of bounds (0-%d)\n", word_idx, word_count);
        free(old_sentence_copy);
        old_sentence[strlen(old_sentence)] = delimiter; 
        return false;
    }
    char* rebuilt_sentence = NULL;
    size_t rebuilt_size = 0;
    FILE* mem_stream = open_memstream(&rebuilt_sentence, &rebuilt_size);
    if (!mem_stream) { perror("open_memstream failed"); free(old_sentence_copy); old_sentence[strlen(old_sentence)] = delimiter; return false; }
    for (int i = 0; i < word_count; i++) {
        if (i == word_idx) { fprintf(mem_stream, "%s ", new_content); }
        fprintf(mem_stream, "%s ", words[i]);
    }
    if (word_idx == word_count) { fprintf(mem_stream, "%s", new_content); } else { fseek(mem_stream, -1, SEEK_CUR); }
    fprintf(mem_stream, "%c", delimiter); 
    fclose(mem_stream);
    free(old_sentence_copy); 
    file_in_memory_t new_splits = parse_string_into_sentences(rebuilt_sentence); 
    free(rebuilt_sentence); 
    if (new_splits.count == 0) { return true; }
    free(file_mem->sentences[sentence_idx]); 
    if (new_splits.count == 1) { file_mem->sentences[sentence_idx] = new_splits.sentences[0]; free(new_splits.sentences); }
    else {
        int split_count = new_splits.count; int old_count = file_mem->count; int new_count = old_count + split_count - 1;
        file_mem->sentences = (char**)realloc(file_mem->sentences, new_count * sizeof(char*));
        if (!file_mem->sentences) { perror("realloc failed in splice"); return false; }
        memmove(&file_mem->sentences[sentence_idx + split_count], &file_mem->sentences[sentence_idx + 1], (old_count - (sentence_idx + 1)) * sizeof(char*));
        for (int i = 0; i < split_count; i++) { file_mem->sentences[sentence_idx + i] = new_splits.sentences[i]; }
        file_mem->count = new_count;
        free(new_splits.sentences); 
    }
    return true;
}
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
        if (c == '\n') { (*line_count)++; }
        if (isspace(c)) { in_word = false; }
        else if (!in_word) { in_word = true; (*word_count)++; }
    }
    if (*line_count == 0 && *word_count > 0) *line_count = 1; 
    fclose(fp);
}
typedef struct {
    int index;
    UT_hash_handle hh;
} locked_sentence_t;
typedef struct {
    char filename[MAX_FILENAME_LEN]; 
    pthread_rwlock_t file_rwlock;
    pthread_mutex_t sentence_lock_mutex; 
    locked_sentence_t* locked_sentences_set; 
    pthread_cond_t sentence_lock_cond; 
    UT_hash_handle hh;               
} file_lock_t;
file_lock_t* g_file_lock_map = NULL;
pthread_mutex_t g_lock_map_mutex;
void lock_sentence(file_lock_t* file_lock, int sentence_index) {
    pthread_mutex_lock(&file_lock->sentence_lock_mutex);
    locked_sentence_t* found;
    HASH_FIND_INT(file_lock->locked_sentences_set, &sentence_index, found);
    while (found != NULL) {
        printf("   [SS-LockMgr]: Sentence %d is locked. Waiting...\n", sentence_index);
        pthread_cond_wait(&file_lock->sentence_lock_cond, &file_lock->sentence_lock_mutex);
        HASH_FIND_INT(file_lock->locked_sentences_set, &sentence_index, found);
    }
    locked_sentence_t* new_lock = (locked_sentence_t*)malloc(sizeof(locked_sentence_t));
    if (new_lock) {
        new_lock->index = sentence_index;
        HASH_ADD_INT(file_lock->locked_sentences_set, index, new_lock);
        printf("   [SS-LockMgr]: Sentence %d locked.\n", sentence_index);
    }
    pthread_mutex_unlock(&file_lock->sentence_lock_mutex);
}
void unlock_sentence(file_lock_t* file_lock, int sentence_index) {
    pthread_mutex_lock(&file_lock->sentence_lock_mutex);
    locked_sentence_t* found;
    HASH_FIND_INT(file_lock->locked_sentences_set, &sentence_index, found);
    if (found) {
        HASH_DEL(file_lock->locked_sentences_set, found);
        free(found);
        printf("   [SS-LockMgr]: Sentence %d unlocked.\n", sentence_index);
    } else {
        fprintf(stderr, "   [SS-LockMgr]: !!WARNING!! Tried to unlock sentence %d, but it wasn't locked.\n", sentence_index);
    }
    pthread_cond_broadcast(&file_lock->sentence_lock_cond);
    pthread_mutex_unlock(&file_lock->sentence_lock_mutex);
}
file_lock_t* get_lock_for_file(const char* filename) {
    file_lock_t* found_lock;
    pthread_mutex_lock(&g_lock_map_mutex);
    HASH_FIND_STR(g_file_lock_map, filename, found_lock);
    if (found_lock) {
        pthread_mutex_unlock(&g_lock_map_mutex);
        return found_lock;
    }
    found_lock = (file_lock_t*)malloc(sizeof(file_lock_t));
    if (found_lock == NULL) {
        perror("   [SS-LockMgr]: malloc failed for new lock");
        pthread_mutex_unlock(&g_lock_map_mutex); 
        return NULL;
    }
    memset(found_lock, 0, sizeof(file_lock_t));
    strcpy(found_lock->filename, filename);
    found_lock->locked_sentences_set = NULL;
    if (pthread_rwlock_init(&found_lock->file_rwlock, NULL) != 0) {
        perror("   [SS-LockMgr]: file_rwlock init failed");
        free(found_lock);
        pthread_mutex_unlock(&g_lock_map_mutex); 
        return NULL;
    }
    if (pthread_mutex_init(&found_lock->sentence_lock_mutex, NULL) != 0) {
        perror("   [SS-LockMgr]: sentence_lock_mutex init failed");
        pthread_rwlock_destroy(&found_lock->file_rwlock);
        free(found_lock);
        pthread_mutex_unlock(&g_lock_map_mutex); 
        return NULL;
    }
     if (pthread_cond_init(&found_lock->sentence_lock_cond, NULL) != 0) {
        perror("   [SS-LockMgr]: sentence_lock_cond init failed");
        pthread_rwlock_destroy(&found_lock->file_rwlock);
        pthread_mutex_destroy(&found_lock->sentence_lock_mutex);
        free(found_lock);
        pthread_mutex_unlock(&g_lock_map_mutex); 
        return NULL;
    }
    HASH_ADD_STR(g_file_lock_map, filename, found_lock);
    printf("   [SS-LockMgr]: Initialized new lock for file '%s'\n", filename);
    pthread_mutex_unlock(&g_lock_map_mutex);
    return found_lock;
}

// --- Main Connection Handler ---
void* handle_client_connection(void* p_conn_fd) {
    int conn_fd = *((int*)p_conn_fd);
    free(p_conn_fd);

    printf("   [SS-Thread]: Got a connection! (conn_fd: %d)\n", conn_fd);

    client_request_t req;
    bool response_sent = false; 

    ssize_t n = recv(conn_fd, &req, sizeof(client_request_t), 0);
    if (n != sizeof(client_request_t)) {
        fprintf(stderr, "   [SS-Thread]: Error receiving client request. Expected %zu, got %zd\n",
                sizeof(client_request_t), n);
        close(conn_fd);
        return NULL;
    }

    file_lock_t* file_lock = get_lock_for_file(req.filename);

    if (file_lock == NULL) {
        ss_response_t res = {0};
        res.status = STATUS_ERROR;
        strcpy(res.error_msg, "Storage Server internal error (Lock Manager). Try again.");
        if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
            perror("   [SS-Thread]: send lock error response failed");
        }
        response_sent = true;
    } else {
        // --- We have a lock, now proceed with commands ---

        if (req.command == CMD_CREATE_FILE) {
            printf("   [SS-Thread]: Handling CMD_CREATE_FILE for '%s'\n", req.filename);
            
            pthread_rwlock_wrlock(&file_lock->file_rwlock); 
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
            pthread_rwlock_unlock(&file_lock->file_rwlock); 

            if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
                perror("   [SS-Thread]: send create response failed");
            }
            response_sent = true;

        } else if (req.command == CMD_GET_STATS) {
            printf("   [SS-Thread]: Handling CMD_GET_STATS for '%s'\n", req.filename);
            
            pthread_rwlock_rdlock(&file_lock->file_rwlock); 
            ss_stats_response_t res = {0}; 
            struct stat file_stat;
            if (stat(req.filename, &file_stat) < 0) {
                perror("   [SS-Thread]: stat failed");
                res.status = STATUS_ERROR;
                strcpy(res.error_msg, "File not found on Storage Server");
            } else {
                res.status = STATUS_OK;
                // --- UPDATED BLOCK ---
                res.stats.char_count = file_stat.st_size;
                res.stats.last_modified = file_stat.st_mtime; // Time of last data modification
                res.stats.last_accessed = file_stat.st_atime; // Time of last access
                res.stats.time_created = file_stat.st_ctime;  // Time of last status change (e.g., creation, chmod)
                // --- END UPDATED BLOCK ---
                get_file_counts(req.filename, &res.stats.word_count, &res.stats.line_count);
            }
            pthread_rwlock_unlock(&file_lock->file_rwlock); 

            if (send(conn_fd, &res, sizeof(ss_stats_response_t), 0) < 0) {
                perror("   [SS-Thread]: send stats response failed");
            }
            response_sent = true;

        } else if (req.command == CMD_READ_FILE) {
            printf("   [SS-Thread]: Handling CMD_READ_FILE for '%s'\n", req.filename);
            
            pthread_rwlock_rdlock(&file_lock->file_rwlock); 
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
                    pthread_rwlock_unlock(&file_lock->file_rwlock); 
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
            pthread_rwlock_unlock(&file_lock->file_rwlock); 
            response_sent = true;
        
        } else if (req.command == CMD_WRITE_FILE) {
            printf("   [SS-Thread]: Handling CMD_WRITE_FILE for '%s'\n", req.filename);
            
            ss_response_t ready_res = { .status = STATUS_OK };
            ss_write_response_t final_res = { .status = STATUS_OK };
            
            int locked_sentence_index = -1;
            int final_sentence_count = 0;

            edit_buffer_t buffer = { .chunks = NULL, .count = 0, .capacity = 0 };

            if (send(conn_fd, &ready_res, sizeof(ss_response_t), 0) < 0) {
                perror("   [SS-Thread]: send ready-for-write response failed");
                response_sent = true; 
            } else {
                client_write_chunk_t chunk;
                while (recv(conn_fd, &chunk, sizeof(client_write_chunk_t), 0) == sizeof(client_write_chunk_t)) {
                    
                    if (chunk.is_etirw) {
                        break; 
                    }

                    if (locked_sentence_index == -1) {
                        printf("   [SS-Thread]: Validating sentence index %d...\n", chunk.sentence_index);
                        pthread_rwlock_rdlock(&file_lock->file_rwlock);
                        file_in_memory_t temp_file = load_file_into_memory(req.filename);
                        
                        if (chunk.sentence_index < 0 || chunk.sentence_index > temp_file.count) {
                            printf("   [SS-Thread]: Validation failed. Index %d out of bounds (0-%d).\n", 
                                   chunk.sentence_index, temp_file.count);
                            final_res.status = STATUS_ERROR;
                            sprintf(final_res.error_msg, "Sentence index %d is out of bounds (valid: 0-%d)", 
                                    chunk.sentence_index, temp_file.count);
                            
                            free_file_in_memory(&temp_file);
                            pthread_rwlock_unlock(&file_lock->file_rwlock);
                            break; 
                        }
                        
                        free_file_in_memory(&temp_file);
                        pthread_rwlock_unlock(&file_lock->file_rwlock);
                        
                        locked_sentence_index = chunk.sentence_index;
                        lock_sentence(file_lock, locked_sentence_index);
                    }

                    if (chunk.sentence_index != locked_sentence_index) {
                        final_res.status = STATUS_ERROR;
                        strcpy(final_res.error_msg, "Permission denied: Can only edit one sentence per session.");
                        break;
                    }
                    
                    if (buffer.count == buffer.capacity) {
                        buffer.capacity = (buffer.capacity == 0) ? 8 : buffer.capacity * 2;
                        client_write_chunk_t* new_buf = (client_write_chunk_t*)realloc(buffer.chunks, buffer.capacity * sizeof(client_write_chunk_t));
                        if (!new_buf) {
                            perror("   [SS-Thread]: realloc chunk buffer failed");
                            final_res.status = STATUS_ERROR;
                            strcpy(final_res.error_msg, "Server out of memory");
                            break;
                        }
                        buffer.chunks = new_buf;
                    }
                    memcpy(&buffer.chunks[buffer.count++], &chunk, sizeof(client_write_chunk_t));
                }
            }

            if (final_res.status == STATUS_OK && locked_sentence_index != -1) {
                
                printf("   [SS-Thread]: Acquiring file write lock for atomic R-M-W...\n");
                pthread_rwlock_wrlock(&file_lock->file_rwlock);
                
                file_in_memory_t file_mem = load_file_into_memory(req.filename);
                
                bool re_validation_passed = true;
                if (buffer.count > 0) { 
                    int first_index = buffer.chunks[0].sentence_index;
                    if (first_index < 0 || first_index > file_mem.count) {
                        re_validation_passed = false;
                        final_res.status = STATUS_ERROR;
                        sprintf(final_res.error_msg, "Write failed: File was modified by another user (index %d no longer valid)", first_index);
                    }
                }
                if (re_validation_passed) {
                    create_backup_for_undo(req.filename);

                    for (int i = 0; i < buffer.count; i++) {
                        client_write_chunk_t* c = &buffer.chunks[i];
                        if (!edit_sentence(&file_mem, c->sentence_index, c->word_index, c->content)) {
                            final_res.status = STATUS_ERROR; 
                            strcpy(final_res.error_msg, "Internal server error during edit");
                            break; 
                        }
                    }
                    
                    if (final_res.status == STATUS_OK) {
                        if (!save_memory_to_file(req.filename, &file_mem)) {
                            final_res.status = STATUS_ERROR;
                            strcpy(final_res.error_msg, "Failed to save file to disk");
                        }
                    }
                }
                final_sentence_count = file_mem.count;
                free_file_in_memory(&file_mem);
                
                pthread_rwlock_unlock(&file_lock->file_rwlock);
                printf("   [SS-Thread]: Atomic R-M-W complete, lock released.\n");
            }
            
            if (locked_sentence_index != -1) {
                unlock_sentence(file_lock, locked_sentence_index);
            }

            if (buffer.chunks) {
                free(buffer.chunks);
            }

            final_res.updated_sentence_count = final_sentence_count;
            
            if (send(conn_fd, &final_res, sizeof(ss_write_response_t), 0) < 0) {
                perror("   [SS-Thread]: send final write response failed");
            }

            response_sent = true;

        } else if (req.command == CMD_DELETE_FILE) {
            printf("   [SS-Thread]: Handling CMD_DELETE_FILE for '%s'\n", req.filename);
            
            pthread_rwlock_wrlock(&file_lock->file_rwlock); 
            ss_response_t res = {0};

            if (remove(req.filename) == 0) {
                res.status = STATUS_OK;
                printf("   [SS-Thread]: Successfully deleted file '%s'\n", req.filename);
            } else {
                perror("   [SS-Thread]: remove failed");
                res.status = STATUS_ERROR;
                strcpy(res.error_msg, "File not found or delete failed on Storage Server");
            }
            pthread_rwlock_unlock(&file_lock->file_rwlock); 

            if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
                perror("   [SS-Thread]: send delete response failed");
            }
            response_sent = true;
        }
        else if (req.command == CMD_UNDO_FILE) {
            printf("   [SS-Thread]: Handling CMD_UNDO_FILE for '%s'\n", req.filename);

            pthread_rwlock_wrlock(&file_lock->file_rwlock); 
            
            ss_response_t res = {0};
            char backup_filename[MAX_FILENAME_LEN + 5];
            sprintf(backup_filename, "%s.bak", req.filename);

            struct stat buffer;
            if (stat(backup_filename, &buffer) != 0) {
                if (errno == ENOENT) {
                    strcpy(res.error_msg, "No undo history found for this file.");
                } else {
                    perror("   [SS-Thread]: stat failed on .bak");
                    strcpy(res.error_msg, "Error checking undo history.");
                }
                res.status = STATUS_ERROR;
            } else {
                if (rename(backup_filename, req.filename) == 0) {
                    res.status = STATUS_OK;
                    printf("   [SS-Thread]: File '%s' reverted successfully.\n", req.filename);
                } else {
                    perror("   [SS-Thread]: rename failed");
                    res.status = STATUS_ERROR;
                    strcpy(res.error_msg, "Undo failed on Storage Server.");
                }
            }
            
            pthread_rwlock_unlock(&file_lock->file_rwlock); 

            if (send(conn_fd, &res, sizeof(ss_response_t), 0) < 0) {
                perror("   [SS-Thread]: send undo response failed");
            }
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


// --- (Main function is unchanged) ---
int main(int argc, char *argv[]) { 
    int sock_fd;
    struct sockaddr_in nm_addr;
    ss_registration_t reg_data;

    int my_client_port = MY_CLIENT_PORT;
    if (argc > 1) {
        my_client_port = atoi(argv[1]);
        if (my_client_port <= 0) {
            fprintf(stderr, "Invalid port number. Using default %d\n", MY_CLIENT_PORT);
            my_client_port = MY_CLIENT_PORT;
        }
    }

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("[Main] socket creation failed");
        exit(EXIT_FAILURE);
    }
    memset(&nm_addr, 0, sizeof(nm_addr));
    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &nm_addr.sin_addr) <= 0) {
        perror("[Main] invalid Name Server IP address");
        exit(EXIT_FAILURE);
    }
    printf("Attempting to connect to Name Server at %s:%d...\n", NM_IP, NM_PORT);
    if (connect(sock_fd, (struct sockaddr*)&nm_addr, sizeof(nm_addr)) < 0) {
        perror("[Main] connection to Name Server failed");
        exit(EXIT_FAILURE);
    }
    printf("Connected to Name Server!\n");
    message_type_t msg_type = MSG_SS_REGISTER;
    if (send(sock_fd, &msg_type, sizeof(message_type_t), 0) < 0) {
        perror("[Main] send message type failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }
    strcpy(reg_data.ss_ip, MY_IP);
    reg_data.client_port = my_client_port; 
    if (send(sock_fd, &reg_data, sizeof(reg_data), 0) < 0) {
        perror("[Main] send registration data failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }
    printf("Successfully registered with Name Server (Port: %d).\n", my_client_port);
    close(sock_fd); 

    if (pthread_mutex_init(&g_lock_map_mutex, NULL) != 0) {
        perror("[Main] g_lock_map_mutex init failed");
        exit(EXIT_FAILURE);
    }

    int listen_fd;
    struct sockaddr_in ss_serv_addr;
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("[Main] SS server socket creation failed");
        exit(EXIT_FAILURE);
    }
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
    memset(&ss_serv_addr, 0, sizeof(ss_serv_addr));
    ss_serv_addr.sin_family = AF_INET;
    ss_serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    ss_serv_addr.sin_port = htons(my_client_port); 
    if (bind(listen_fd, (struct sockaddr*)&ss_serv_addr, sizeof(ss_serv_addr)) < 0) {
        perror("[Main] SS server bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(listen_fd, 10) < 0) {
        perror("[Main] SS server listen failed");
        exit(EXIT_FAILURE);
    }
    printf("\n[SS-Main]: Storage Server now listening for clients on port %d...\n", my_client_port);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int conn_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (conn_fd < 0) {
            perror("[Main] SS server accept failed");
            continue; 
        }
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("[SS-Main]: Accepted new client connection from %s\n", client_ip);
        pthread_t tid;
        int* p_conn_fd = malloc(sizeof(int));
        *p_conn_fd = conn_fd;
        if (pthread_create(&tid, NULL, handle_client_connection, p_conn_fd) != 0) {
            perror("[Main] pthread_create failed");
            free(p_conn_fd);
            close(conn_fd);
        }
        pthread_detach(tid); 
    }

    close(listen_fd);
    pthread_mutex_destroy(&g_lock_map_mutex);
    file_lock_t *current_lock, *tmp;
    HASH_ITER(hh, g_file_lock_map, current_lock, tmp) {
        pthread_rwlock_destroy(&current_lock->file_rwlock); 
        pthread_mutex_destroy(&current_lock->sentence_lock_mutex);
        pthread_cond_destroy(&current_lock->sentence_lock_cond);
        locked_sentence_t *s_lock, *s_tmp;
        HASH_ITER(hh, current_lock->locked_sentences_set, s_lock, s_tmp) {
            HASH_DEL(current_lock->locked_sentences_set, s_lock);
            free(s_lock);
        }
        HASH_DEL(g_file_lock_map, current_lock);    
        free(current_lock);                       
    }
    
    return 0;
}
