#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <list>
#include <deque>
#include <mutex>
#include <atomic>
#include <cstdint>
#include "storage/store.h"
#include <openssl/ssl.h>
#include <openssl/err.h>

struct ClientState {
    std::string                     read_buf;
    TxState                         tx;
    bool                            authenticated = false;
    std::unordered_set<std::string> subscriptions;
    int64_t                         repl_offset   = 0;  // last confirmed offset (replicas)
    int resp_version = 2;
};

class RedisServer {
private:
    int port;
    int leader_port;
    int server_fd     = -1;
    int unix_fd       = -1;   // unix domain socket listener (-1 if disabled)
    int epoll_fd      = -1;
    int leader_fd     = -1;

    std::unordered_map<int, ClientState> clients;
    std::vector<int>                     replica_fds;

    int64_t last_ping_time;
    int64_t last_heartbeat;

    KeyValueStore store;

    // TLS / SSL Networking State
    SSL_CTX* ssl_ctx = nullptr;
    std::unordered_map<int, SSL*> client_ssl;
    
    void init_ssl();
    void secure_send(int fd, const std::string& data);

    // Pub/Sub
    std::unordered_map<std::string, std::set<int>> pubsub_channels;
    std::mutex                                     pubsub_mtx;

    // Replication
    std::string          repl_id;           // 40-char hex ID, generated at startup
    std::atomic<int64_t> repl_offset{0};    // bytes sent to all replicas (leader only)

    // Maps a list key to a queue of waiting client sockets
    std::unordered_map<std::string, std::list<int>> waiting_clients;
    
    // Maps a client socket to the keys they are waiting for (for cleanup on disconnect)
    std::unordered_map<int, std::vector<std::string>> client_blocked_on;

    std::atomic<int> active_connections{0}; // For Prometheus Metrics

    // Replication backlog for PSYNC partial resync.
    // Ring buffer: newest bytes at the front.
    static constexpr size_t BACKLOG_SIZE = 1024 * 1024;  // 1 MB
    std::deque<char>         repl_backlog;
    mutable std::mutex       backlog_mtx;

    int64_t get_time_ms();
    void    make_socket_non_blocking(int fd);
    void    handle_new_connection(int listening_fd);
    void    handle_client_data(int fd);
    void    close_client(int fd);
    void    check_heartbeats();
    void    start_heartbeat_thread();
    void    setup_signal_handlers();
    bool    handle_pubsub(int fd, const std::vector<std::string>& args);

    // Generate a random 40-hex-char replication ID
    std::string generate_repl_id() const;

    // Append bytes to the backlog, evicting old bytes when full.
    void append_to_backlog(const std::string& data);

    // Handle REPLCONF / PSYNC handshake from a connecting replica.
    bool handle_replication_handshake(int fd, const std::vector<std::string>& args);

    // Send a full RDB snapshot to a new replica.
    void send_full_resync(int replica_fd);

    // Broadcast a write command to all replicas + update offset + backlog.
    void replicate(const std::vector<std::string>& args);

    void start_metrics_thread();

public:
    RedisServer(int p, int lp = 0);
    ~RedisServer();
    void run();
};