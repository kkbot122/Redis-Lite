#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstring>
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
// The router tracks the relationship between a user and a backend node.
struct ProxySession {
    int client_fd = -1;
    int backend_fd = -1;
    std::string client_buf;
    std::string backend_buf;
};

// Maps socket FDs to their active sessions
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

// Extract the key from a RESP array to determine routing
static std::string extract_key_from_resp(const std::string& raw) {
    if (raw.empty() || raw[0] != '*') return "";
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
// Heartbeat Watcher Thread (Control Plane)
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
                Logger::error("Backend port " + std::to_string(p) + " evicted (missed heartbeats)"); 
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

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        Logger::error("bind() failed"); return 1;
    }
    listen(server_fd, 1024);
    make_socket_non_blocking(server_fd);

    int epoll_fd = epoll_create1(0);
    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::thread(heartbeat_watcher).detach();

    struct epoll_event events[128];
    char buf[8192];

    while (true) {
        int n = epoll_wait(epoll_fd, events, 128, -1);

        for (int i = 0; i < n; ++i) {
            int current_fd = events[i].data.fd;

            // ==========================================
            // 1. ACCEPT NEW CLIENTS
            // ==========================================
            if (current_fd == server_fd) {
                while (true) {
                    int client_fd = accept(server_fd, nullptr, nullptr);
                    if (client_fd < 0) break; // EAGAIN
                    
                    make_socket_non_blocking(client_fd);
                    
                    ProxySession* session = new ProxySession();
                    session->client_fd = client_fd;
                    sessions[client_fd] = session;

                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                }
                continue;
            }

            // Find the session for the socket that woke up
            if (sessions.find(current_fd) == sessions.end()) continue;
            ProxySession* session = sessions[current_fd];

            // ==========================================
            // 2. READ FROM CLIENT -> SEND TO BACKEND
            // ==========================================
            if (current_fd == session->client_fd) {
                bool closed = false;
                while (true) {
                    ssize_t bytes = recv(current_fd, buf, sizeof(buf), 0);
                    if (bytes > 0) session->client_buf.append(buf, bytes);
                    else if (bytes == 0) { closed = true; break; }
                    else { if (errno == EAGAIN || errno == EWOULDBLOCK) break; closed = true; break; }
                }

                if (closed) { close_session(epoll_fd, session); continue; }
                if (session->client_buf.empty()) continue;

                // Handle Heartbeats natively
                if (session->client_buf.rfind("HEARTBEAT", 0) == 0) {
                    try {
                        int port = std::stoi(session->client_buf.substr(10));
                        std::lock_guard<std::mutex> lk(cluster_mtx);
                        if (!active_servers.count(port)) {
                            Logger::info("New backend mapped on port " + std::to_string(port));
                            ring.add_server(port);
                        }
                        active_servers[port] = get_now_ms();
                        send(current_fd, "+OK\r\n", 5, 0);
                    } catch (...) {}
                    close_session(epoll_fd, session);
                    continue;
                }

                // If backend isn't connected yet, parse the key, find node, and connect
                if (session->backend_fd == -1) {
                    std::string key = extract_key_from_resp(session->client_buf);
                    if (key.empty()) continue; // Wait for full RESP frame

                    int target_port;
                    { std::lock_guard<std::mutex> lk(cluster_mtx); target_port = ring.get_server_for_key(key); }

                    if (target_port == -1) {
                        send(current_fd, "-ERR Cluster down\r\n", 19, 0);
                        close_session(epoll_fd, session);
                        continue;
                    }

                    int bfd = socket(AF_INET, SOCK_STREAM, 0);
                    struct sockaddr_in baddr{};
                    baddr.sin_family = AF_INET;
                    baddr.sin_port = htons(target_port);
                    inet_pton(AF_INET, "127.0.0.1", &baddr.sin_addr);

                    // Connect blocking (sub-millisecond locally), then make non-blocking
                    if (connect(bfd, (struct sockaddr*)&baddr, sizeof(baddr)) < 0) {
                        send(current_fd, "-ERR Node offline\r\n", 19, 0);
                        close(bfd); close_session(epoll_fd, session);
                        continue;
                    }
                    
                    make_socket_non_blocking(bfd);
                    session->backend_fd = bfd;
                    sessions[bfd] = session;

                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = bfd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, bfd, &ev);
                }

                // Forward client payload to the connected backend node
                send(session->backend_fd, session->client_buf.c_str(), session->client_buf.size(), 0);
                session->client_buf.clear();
            }

            // ==========================================
            // 3. READ FROM BACKEND -> FORWARD TO CLIENT
            // ==========================================
            if (current_fd == session->backend_fd) {
                bool closed = false;
                while (true) {
                    ssize_t bytes = recv(current_fd, buf, sizeof(buf), 0);
                    if (bytes > 0) session->backend_buf.append(buf, bytes);
                    else if (bytes == 0) { closed = true; break; }
                    else { if (errno == EAGAIN || errno == EWOULDBLOCK) break; closed = true; break; }
                }

                if (!session->backend_buf.empty()) {
                    send(session->client_fd, session->backend_buf.c_str(), session->backend_buf.size(), 0);
                    session->backend_buf.clear();
                    
                    // Drop the connection once response is fully streamed back to client
                    close_session(epoll_fd, session); 
                } else if (closed) {
                    close_session(epoll_fd, session);
                }
            }
        }
    }
    return 0;
}