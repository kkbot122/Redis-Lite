#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <mutex>
#include <cstdint>
#include "storage/store.h"

// Per-connection state owned by the server.
struct ClientState {
    std::string              read_buf;
    TxState                  tx;
    bool                     authenticated = false;
    std::unordered_set<std::string> subscriptions;  // channels this fd is subscribed to
};

class RedisServer {
private:
    int port;
    int leader_port;
    int server_fd  = -1;
    int epoll_fd   = -1;
    int leader_fd  = -1;

    std::unordered_map<int, ClientState> clients;
    std::vector<int>                     replica_fds;

    int64_t last_ping_time;
    int64_t last_heartbeat;

    KeyValueStore store;

    // Pub/Sub: channel → set of subscriber fds
    std::unordered_map<std::string, std::set<int>> pubsub_channels;
    std::mutex                                     pubsub_mtx;

    int64_t get_time_ms();
    void    make_socket_non_blocking(int fd);
    void    handle_new_connection();
    void    handle_client_data(int fd);
    void    close_client(int fd);
    void    check_heartbeats();
    void    start_heartbeat_thread();
    void    setup_signal_handlers();

    // Returns false if the command was handled as a pub/sub command
    // (SUBSCRIBE, UNSUBSCRIBE, PUBLISH) and should not reach the store.
    bool    handle_pubsub(int fd, const std::vector<std::string>& args);

public:
    RedisServer(int p, int lp = 0);
    ~RedisServer();
    void run();
};