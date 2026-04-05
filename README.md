# Redis-Lite 🚀
A high-performance, distributed Key-Value database engineered from scratch in C++17. 

## 🧠 Architecture
* **Networking:** Asynchronous `epoll` event loop handling 10,000+ concurrent connections.
* **Protocol:** Binary-safe RESP (REdis Serialization Protocol) parser. Fully compatible with the official `redis-cli`.
* **Storage Engine:** $O(1)$ Polymorphic LRU Cache supporting Strings, Lists, and Sets via `std::variant`.
* **Persistence:** Append-Only File (AOF) for SSD crash-recovery.
* **Distributed Routing:** Consistent Hashing with Virtual Nodes for horizontal scaling.
* **Orchestration:** Dynamic Service Registry with heartbeats and automated failover.

## ⚙️ Quick Start (Docker)
```bash
docker build -t redis-lite .
docker run -d -p 6379:6379 redis-lite
redis-cli -p 6379