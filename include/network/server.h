#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <stdint.h>
#include "storage/store.h"

class RedisServer {
private:
    int port;
    int leader_port;
    int server_fd;
    int epoll_fd;
    int leader_fd;

    std::vector<int> replica_fds;

    // Per-connection read buffers. Keyed by file descriptor.
    // Holds partial RESP data between epoll_wait cycles.
    std::unordered_map<int, std::string> client_buffers;

    int64_t last_ping_time;
    int64_t last_heartbeat;

    KeyValueStore store;

    int64_t get_time_ms();
    void make_socket_non_blocking(int socket_fd);
    void handle_new_connection();
    void handle_client_data(int client_fd);
    void check_heartbeats();
    void start_heartbeat_thread();

public:
    RedisServer(int p, int lp = 0);
    ~RedisServer();
    void run();
};