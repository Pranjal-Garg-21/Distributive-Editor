# Distributive Editor

A terminal-based distributed editor and file service written in **C** using sockets and threads.

The system is built around three components:

- **Name Server (NM)**: metadata, permissions, routing, replication orchestration
- **Storage Server (SS)**: stores file contents and executes file operations
- **Client**: interactive CLI for users

---

## Features

- Multi-user file operations (`CREATE`, `READ`, `WRITE`, `DELETE`, `UNDO`, `STREAM`, `INFO`)
- Access control (owner-based ACL, request/approve workflow)
- Folder commands (`CREATEFOLDER`, `MOVE`, `VIEWFOLDER`)
- Checkpoints (`CHECKPOINT`, `REVERT`, `LISTCHECKPOINTS`, `VIEWCHECKPOINT`)
- Storage server registration + heartbeat tracking
- Replication support handled by the Name Server

---

## Project Structure

- `name_server.c` — central metadata + coordination service (listens on `NM_PORT`)
- `storage_server.c` — storage node process (one instance per storage node)
- `client.c` — interactive client shell
- `common.h` — shared protocol structs, enums, limits, and constants
- `logger.h` — logging utilities used by server components
- `uthash.h` — hash-map utility used internally

---

## Prerequisites

- Linux
- GCC
- POSIX threads (`pthread`)

Install build tools (Ubuntu/Debian):

```bash
sudo apt update
sudo apt install -y build-essential
```

---

## Build

From the project root:

```bash
gcc -o name_server name_server.c -pthread
gcc -o storage_server storage_server.c -pthread
gcc -o client client.c -pthread
```

This creates three executables:

- `./name_server`
- `./storage_server`
- `./client`

---

## Run (Local Machine)

By default, the Name Server port is defined in `common.h` as:

- `NM_PORT = 8080`

Start the services in this order using separate terminals.

### 1) Start Name Server

```bash
./name_server
```

### 2) Start one or more Storage Servers

Syntax:

```bash
./storage_server <NM_IP> <SS_IP> <SS_PORT>
```

Local examples:

```bash
./storage_server 127.0.0.1 127.0.0.1 9090
./storage_server 127.0.0.1 127.0.0.1 9091
```

Notes:

- Each storage server uses its own `<SS_PORT>`.
- On startup, each SS scans its current directory and reports existing `.txt` files to NM.

### 3) Start Client

Syntax:

```bash
./client <Name_Server_IP>
```

Local example:

```bash
./client 127.0.0.1
```

The client prompts for username, then opens the interactive shell.

---

## Quick Start Demo

Inside the client shell:

```text
CREATE notes.txt
WRITE notes.txt 0
READ notes.txt
VIEW -al
INFO notes.txt
UNDO notes.txt
```

Type `help` to see all available commands.

---

## Client Command Summary

### File Operations

- `CREATE <filename>`
- `DELETE <filename>`
- `READ <filename>`
- `WRITE <filename> <sentence_number>`
- `STREAM <filename>`
- `UNDO <filename>`
- `INFO <filename>`
- `EXEC <filename>`

### Folder Operations

- `CREATEFOLDER <foldername>`
- `MOVE <filename> <foldername>`
- `VIEWFOLDER <foldername>`

### Checkpoint Operations

- `CHECKPOINT <filename> <tag>`
- `REVERT <filename> <tag>`
- `LISTCHECKPOINTS <filename>`
- `VIEWCHECKPOINT <filename> <tag>`

### Access Control

- `ADDACCESS <R|W> <filename> <username>`
- `REMACCESS <filename> <username>`
- `REQUESTACCESS <filename>`
- `LISTREQUESTS`
- `APPROVEREQUEST <request_id> <R|W>`
- `DENYREQUEST <request_id>`

### System

- `VIEW [-a] [-l] [-al]`
- `LIST`
- `help`
- `exit`

---

## Logs and Runtime Files

- Name Server log: `nm.log`
- Storage Server logs: `ss_<port>.log` (for example `ss_9090.log`)
- Name Server metadata snapshot: `nm_metadata.dat`

---

## Troubleshooting

- **Bind/port errors**: ensure the port is free (`8080`, `9090`, etc.).
- **Client cannot connect to NM**: verify NM is running and IP/port are correct.
- **SS not visible to NM**: ensure SS started after NM and used correct `<NM_IP>`.
- **Permission denied for file operations**: verify owner/ACL using `INFO` and access-request commands.

---

## Notes

- This implementation is in **C** (not C++).
- For distributed setup across different machines, replace `127.0.0.1` with actual LAN IPs.
