Distributed Collaborative Editor (OSN)

A terminal-based real-time document synchronization system built in C++. This project implements a client-server architecture designed to handle concurrent multi-user read and write operations with low-latency performance, similar to the core mechanics of Google Docs.
Overview

This system enables multiple users to collaborate on documents simultaneously through a terminal interface. It handles the complexities of distributed systems, including network programming, data consistency, and thread synchronization.
Key Features

    Real-time Synchronization: Instant updates across all connected clients using custom synchronization primitives
    Multi-User Support: Robust handling of concurrent read/write operations to prevent data conflicts.
    Low-Latency Architecture: Optimized C++ backend for high-performance message passing.
    Client-Server Model: Centralized server managing document state with multiple terminal-based clients.

Technical Stack

    Language: C++
    Networking: Socket Programming (TCP/IP)
    Concurrency: POSIX Threads (Pthreads) and Mutexes
    Environment: Linux/Ubuntu

Architecture & Implementation

The project focuses on solving the challenges of distributed consistency:

    Server-Side: Manages the "Single Source of Truth" for the document, handling incoming change requests and broadcasting updates to all active subscribers.
    Client-Side: Provides a terminal interface for editing and displays live updates received from the server.
    Synchronization: Implements advanced locking mechanisms or versioning to ensure that concurrent edits do not result in race conditions or data corruption.
