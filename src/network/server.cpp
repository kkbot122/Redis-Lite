#include "network/server.h"
#include "utils/logger.h"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <thread>

RedisServer::RedisServer(int p, int lp) : port(p), leader_port(lp), leader_fd(-1) {
    last_ping_time = get_time_ms();
    last_heartbeat = get_time_ms();
}

RedisServer::~RedisServer() {
    if (server_fd != -1) close(server_fd);
    if (epoll_fd != -1) close(epoll_fd);
}

int64_t RedisServer::get_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

void RedisServer::make_socket_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
}

std::vector<std::string> RedisServer::parse_resp(const std::string& input) {
    std::vector<std::string> args;
    if (input.empty()) return args;

    // FALLBACK: If it doesn't start with '*', it's an old-school netcat or test command!
    if (input[0] != '*') {
        std::stringstream ss(input);
        std::string token;
        while (ss >> token) args.push_back(token);
        return args;
    }

    // RESP PARSER
    size_t pos = 0;
    size_t rn = input.find("\r\n", pos);
    if (rn == std::string::npos) return args;

    int num_args = std::stoi(input.substr(pos + 1, rn - pos - 1));
    pos = rn + 2; 

    for (int i = 0; i < num_args; i++) {
        if (pos >= input.length() || input[pos] != '$') break;

        rn = input.find("\r\n", pos);
        if (rn == std::string::npos) break;

        int str_len = std::stoi(input.substr(pos + 1, rn - pos - 1));
        pos = rn + 2; 

        args.push_back(input.substr(pos, str_len));
        pos += str_len + 2; 
    }

    return args;
}

void RedisServer::run() {
    Logger::info("Starting server on port " + std::to_string(port));
    if (leader_port > 0) {
        Logger::info("Starting in REPLICA mode. Following port " + std::to_string(leader_port));
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    bind(server_fd, (struct sockaddr*)&address, sizeof(address));
    listen(server_fd, 5);
    make_socket_non_blocking(server_fd);

    epoll_fd = epoll_create1(0);
    struct epoll_event event, events[10];
    event.events = EPOLLIN;
    event.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);

    // Follower Startup Logic
    if (leader_port > 0) {
        leader_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in leader_addr;
        leader_addr.sin_family = AF_INET;
        leader_addr.sin_port = htons(leader_port);
        inet_pton(AF_INET, "127.0.0.1", &leader_addr.sin_addr); 
        
        if (connect(leader_fd, (struct sockaddr*)&leader_addr, sizeof(leader_addr)) == 0) {
            Logger::info("Successfully connected to Leader!");
            make_socket_non_blocking(leader_fd);
            
            struct epoll_event leader_event;
            leader_event.events = EPOLLIN;
            leader_event.data.fd = leader_fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, leader_fd, &leader_event);

            std::string handshake = "REPLICAOF\n";
            send(leader_fd, handshake.c_str(), handshake.length(), 0);
        } else {
            Logger::error("Failed to connect to Leader on port " + std::to_string(leader_port));
        }
    }

    start_heartbeat_thread();
    Logger::info("Heartbeat emitter started. Broadcasting presence to Router...");

    // The Event Loop
    while (true) {
        int num_events = epoll_wait(epoll_fd, events, 10, 1000);
        check_heartbeats();

        for (int i = 0; i < num_events; i++) {
            if (events[i].data.fd == server_fd) {
                handle_new_connection();
            } else {
                handle_client_data(events[i].data.fd);
            }
        }
    }
}

void RedisServer::check_heartbeats() {
    int64_t now = get_time_ms();
    
    // Leader Duty
    if (leader_port == 0 && (now - last_ping_time > 2000)) {
        for (int rep_fd : replica_fds) send(rep_fd, "PING\n", 5, 0);
        last_ping_time = now;
    }

    // Follower Duty (Failover)
    if (leader_port > 0 && leader_fd != -1 && (now - last_heartbeat > 5000)) {
        Logger::error("Leader has been silent for 5 seconds!");
        Logger::warn("Executing emergency failover promotion...");
        close(leader_fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, leader_fd, nullptr);
        leader_fd = -1;
        leader_port = 0; 
        Logger::info("Promoted to Leader! Accepting writes on port " + std::to_string(port));
    }
}

void RedisServer::handle_new_connection() {
    int client_fd = accept(server_fd, nullptr, nullptr);
    make_socket_non_blocking(client_fd);
    struct epoll_event client_event;
    client_event.events = EPOLLIN;
    client_event.data.fd = client_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event);
}

void RedisServer::handle_client_data(int client_fd) {
    char buffer[1024] = {0};
    int bytes_read = read(client_fd, buffer, 1024);

    if (bytes_read <= 0) {
        close(client_fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
        replica_fds.erase(std::remove(replica_fds.begin(), replica_fds.end(), client_fd), replica_fds.end());
        return;
    }

    if (client_fd == leader_fd) last_heartbeat = get_time_ms(); 

    std::string raw_input(buffer);
    std::vector<std::string> args = parse_resp(raw_input);
    if (args.empty()) return;

    std::string cmd = args[0];
    for (char &c : cmd) c = toupper(c);

    if (cmd == "REPLICAOF") {
        Logger::info("New Follower registered! FD: " + std::to_string(client_fd));
        replica_fds.push_back(client_fd);
        return; 
    }

    // FIXED: Changed kv_store to store to match the header file
    std::string response = store.execute_command(args);
    
    if (client_fd != leader_fd) send(client_fd, response.c_str(), response.length(), 0);

    if (leader_port == 0 && (cmd == "SET" || cmd == "SETEX") && response == "+OK\r\n") {
        std::string broadcast_msg = raw_input; 
        if (broadcast_msg.back() != '\n') broadcast_msg += "\n"; 
        for (int rep_fd : replica_fds) {
            send(rep_fd, broadcast_msg.c_str(), broadcast_msg.length(), 0);
        }
    }
}

void RedisServer::start_heartbeat_thread() {
    std::thread([this]() {
        while (true) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            struct sockaddr_in router_addr;
            router_addr.sin_family = AF_INET;
            router_addr.sin_port = htons(6379); 
            inet_pton(AF_INET, "127.0.0.1", &router_addr.sin_addr);

            if (connect(sock, (struct sockaddr*)&router_addr, sizeof(router_addr)) == 0) {
                std::string pulse = "HEARTBEAT " + std::to_string(port) + "\n";
                send(sock, pulse.c_str(), pulse.length(), 0);
            }
            close(sock);

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }).detach(); 
}