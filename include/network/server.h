#pragma once
#include <string>
#include <vector>
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
    int64_t last_ping_time;
    int64_t last_heartbeat;

    KeyValueStore store; // Unified variable name for the database

    int64_t get_time_ms();
    void make_socket_non_blocking(int socket_fd);
    void handle_new_connection();
    void handle_client_data(int client_fd);
    void check_heartbeats();
    void start_heartbeat_thread();
    std::vector<std::string> parse_resp(const std::string& input);

public:
    // Defaults leader_port to 0 if not provided
    RedisServer(int p, int lp = 0); 
    ~RedisServer();
    void run();
};