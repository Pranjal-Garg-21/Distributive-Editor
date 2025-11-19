[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/0ek2UV58)


Hi there!
First compile the code, please use -pthread -lrt while compiling. Then run the name server on any device. To run the storage server, do ./<executable_name> <name_server_device_ip> <storage_server_device_ip> <storage_server_port>. Make sure name server and storage server are on the same network. The to run the client, do ./<executable_name> <name_server_device_ip> and you are good to go.

Our system makes these following assumptions:
->Storage Server Limit: The Name Server (NM) is hardcoded to handle a maximum of 10 Storage Servers (SS) at once (#define MAX_STORAGE_SERVERS 10). This assumes a relatively small cluster size for this MVP.
->Cache Size: The Least Recently Used (LRU) cache in the Name Server is strictly limited to 128 entries (#define MAX_CACHE_SIZE 128). If more than 128 files are accessed frequently, the system will incur performance penalties due to cache thrashing/eviction.
->Connection Backlog: The listen() system call uses a backlog of 10. This assumes that no more than 10 clients will attempt to initiate a TCP handshake simultaneously, or some connections may be dropped under high burst loads.
->Buffer Sizes: File operations use a buffer size of 4096 bytes (char buf[4096]) for reading and writing chunks. This assumes network packets and file chunks can be efficiently processed in 4KB blocks.

We have implemented all the bonus features as well. We have used flattening to store hierarchical folder structures. We store version based files to implement checkpoints.  We store requests in a list. And for fault tolerance- we have implemented replication using backup storage server and to keep track of storage servers, we have a heartbeat monitor as well.

Some unique factors:
->Round robin storage server selection.(Load Balancing so that no storage server gets overwhelmed).
->Name server persistence/SIGINT handling for name server-nm_metadata.dat file for backup.
->Robust WRITE implementation involving sentence validation and sentence level locking.

Thank you!
By,
Sarah Roomi, Pranjal Garg <3


{LLM CHAT LINKS: 
https://gemini.google.com/share/d729f5d7f81b, https://gemini.google.com/share/d729f5d7f81b, https://gemini.google.com/share/646f56b5fc8e, https://gemini.google.com/share/c9a9620c7bf9, https://gemini.google.com/share/c9a9620c7bf9, https://gemini.google.com/share/ecb8253793e1, https://gemini.google.com/share/763e4e1dfbc9, https://gemini.google.com/share/923840f64106, https://gemini.google.com/share/8f924fd44aa4
}
