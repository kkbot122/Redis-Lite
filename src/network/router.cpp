#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <sstream>
#include <thread>
#include <mutex>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <csignal>
#include "network/hash_ring.h"
#include "utils/logger.h"

// ============================================================
// Cluster State & Hash Ring
// ============================================================
static std::unordered_map<int, int64_t> active_servers;
static HashRing ring;
static std::mutex cluster_mtx;

static int64_t get_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

void make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) flags = 0;
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// ============================================================
// Proxy State Machine
// ============================================================
struct ProxySession {
    int client_fd = -1;
    int backend_fd = -1;
    std::string client_buf;
    std::string backend_buf;
};

static std::unordered_map<int, ProxySession*> sessions;

void close_session(int epoll_fd, ProxySession* session) {
    if (!session) return;
    if (session->client_fd != -1) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, session->client_fd, nullptr);
        close(session->client_fd);
        sessions.erase(session->client_fd);
    }
    if (session->backend_fd != -1) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, session->backend_fd, nullptr);
        close(session->backend_fd);
        sessions.erase(session->backend_fd);
    }
    delete session;
}

// Extract the key. Supports both RESP arrays and inline ("SET key value\n")
static std::string extract_key_from_request(const std::string& raw) {
    if (raw.empty()) return "";
    if (raw[0] != '*') {
        size_t nl = raw.find('\n');
        if (nl == std::string::npos) return "";
        std::string line = raw.substr(0, nl);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        std::istringstream iss(line);
        std::string cmd, key;
        if (!(iss >> cmd >> key)) return "";
        return key;
    }

    size_t pos = raw.find("\r\n");
    if (pos == std::string::npos) return "";
    int argc = 0;
    try { argc = std::stoi(raw.substr(1, pos - 1)); } catch (...) { return ""; }
    if (argc < 2) return "";
    size_t cur = pos + 2;
    for (int i = 0; i < 2; ++i) {
        if (cur >= raw.size() || raw[cur] != '$') return "";
        size_t end = raw.find("\r\n", cur);
        if (end == std::string::npos) return "";
        int len = 0;
        try { len = std::stoi(raw.substr(cur + 1, end - cur - 1)); } catch (...) { return ""; }
        cur = end + 2;
        if (cur + len + 2 > raw.size()) return "";
        if (i == 1) return raw.substr(cur, len);
        cur += len + 2;
    }
    return "";
}

// ============================================================
// FIX #2: Robust Send (Never drop bytes on EAGAIN)
// ============================================================
void robust_send(int fd, const std::string& data) {
    size_t total_sent = 0;
    int retries = 0;
    while (total_sent < data.size()) {
        ssize_t sent = send(fd, data.c_str() + total_sent, data.size() - total_sent, MSG_NOSIGNAL);
        if (sent > 0) {
            total_sent += sent;
            retries = 0;
        } else if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (retries++ > 100) break; // Break out instead of deadlocking if connection truly dies
                std::this_thread::sleep_for(std::chrono::microseconds(500)); 
                continue;
            }
            break; 
        }
    }
}

// ============================================================
// Heartbeat Watcher Thread 
// ============================================================
static void heartbeat_watcher() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::vector<int> dead;
        {
            std::lock_guard<std::mutex> lk(cluster_mtx);
            int64_t now = get_now_ms();
            for (const auto& [port, last] : active_servers) {
                if (now - last > 5000) dead.push_back(port);
            }
            for (int p : dead) { 
                ring.remove_server(p); 
                active_servers.erase(p);
                Logger::error("Backend port " + std::to_string(p) + " evicted"); 
            }
        }
    }
}

// ============================================================
// MAIN EPOLL MULTIPLEXER
// ============================================================
int main() {
    Logger::init("router.log");
    Logger::info("Starting non-blocking epoll redis-router on port 6379");
    signal(SIGPIPE, SIG_IGN); // Prevent abrupt crashes

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        Logger::error("bind() failed"); return 1;
    }
    listen(server_fd, 1024);
    make_socket_non_blocking(server_fd);

    int epoll_fd = epoll_create1(0);
    struct epoll_event ev{}; ev.events = EPOLLIN | EPOLLET; ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::thread(heartbeat_watcher).detach();

    struct epoll_event events[128];
    char buf[8192];

    while (true) {
        int n = epoll_wait(epoll_fd, events, 128, -1);

        for (int i = 0; i < n; ++i) {
            int current_fd = events[i].data.fd;

            if (current_fd == server_fd) {
                while (true) {
                    int client_fd = accept(server_fd, nullptr, nullptr);
                    if (client_fd < 0) break;
                    
                    make_socket_non_blocking(client_fd);
                    ProxySession* session = new ProxySession();
                    session->client_fd = client_fd;
                    sessions[client_fd] = session;

                    ev.events = EPOLLIN | EPOLLET; ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                }
                continue;
            }

            if (sessions.find(current_fd) == sessions.end()) continue;
            ProxySession* session = sessions[current_fd];

            // 1. READ FROM CLIENT -> SEND TO BACKEND
            if (current_fd == session->client_fd) {
                bool closed = false;
                while (true) {
                    ssize_t bytes = recv(current_fd, buf, sizeof(buf), 0);
                    if (bytes > 0) session->client_buf.append(buf, bytes);
                    else if (bytes == 0) { closed = true; break; }
                    else { if (errno == EAGAIN || errno == EWOULDBLOCK) break; closed = true; break; }
                }

                if (closed && session->client_buf.empty()) { close_session(epoll_fd, session); continue; }
                if (session->client_buf.empty()) continue;

                if (session->client_buf.rfind("HEARTBEAT", 0) == 0) {
                    try {
                        int port = std::stoi(session->client_buf.substr(10));
                        std::lock_guard<std::mutex> lk(cluster_mtx);
                        if (!active_servers.count(port)) {
                            Logger::info("New backend mapped on port " + std::to_string(port));
                            ring.add_server(port);
                        }
                        active_servers[port] = get_now_ms();
                        robust_send(current_fd, "+OK\r\n");
                    } catch (...) {}
                    close_session(epoll_fd, session);
                    continue;
                }

                if (session->backend_fd == -1) {
                    std::string key = extract_key_from_request(session->client_buf);
                    if (key.empty()) continue;

                    int target_port;
                    { std::lock_guard<std::mutex> lk(cluster_mtx); target_port = ring.get_server_for_key(key); }

                    if (target_port == -1) {
                        robust_send(current_fd, "-ERR Cluster down\r\n");
                        close_session(epoll_fd, session); continue;
                    }

                    int bfd = socket(AF_INET, SOCK_STREAM, 0);
                    struct sockaddr_in baddr{};
                    baddr.sin_family = AF_INET; baddr.sin_port = htons(target_port);
                    inet_pton(AF_INET, "127.0.0.1", &baddr.sin_addr);

                    if (connect(bfd, (struct sockaddr*)&baddr, sizeof(baddr)) < 0) {
                        robust_send(current_fd, "-ERR Node offline\r\n");
                        close(bfd); close_session(epoll_fd, session); continue;
                    }
                    
                    make_socket_non_blocking(bfd);
                    session->backend_fd = bfd;
                    sessions[bfd] = session;

                    ev.events = EPOLLIN | EPOLLET; ev.data.fd = bfd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, bfd, &ev);
                }

                // USE ROBUST SEND
                robust_send(session->backend_fd, session->client_buf);
                session->client_buf.clear();
            }

            // 2. READ FROM BACKEND -> FORWARD TO CLIENT
            if (current_fd == session->backend_fd) {
                bool closed = false;
                while (true) {
                    ssize_t bytes = recv(current_fd, buf, sizeof(buf), 0);
                    if (bytes > 0) session->backend_buf.append(buf, bytes);
                    else if (bytes == 0) { closed = true; break; }
                    else { if (errno == EAGAIN || errno == EWOULDBLOCK) break; closed = true; break; }
                }

                if (!session->backend_buf.empty()) {
                    // USE ROBUST SEND
                    robust_send(session->client_fd, session->backend_buf);
                    session->backend_buf.clear();
                    
                    // FIX #1: DO NOT CALL close_session() HERE! 
                    // Let the proxy session stay alive to handle the next request!
                } else if (closed) {
                    close_session(epoll_fd, session);
                }
            }
        }
    }
    return 0;
}