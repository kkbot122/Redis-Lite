#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include "network/hash_ring.h"
#include "utils/logger.h"

// ============================================================
// Fixed-size thread pool
// ============================================================
// Replaces the previous "one detached thread per connection" approach.
// That model spawned unbounded threads — OS refusal starts around 8k threads,
// but memory pressure hits much earlier (each thread stack is ~8 MB by default).
// A pool of N workers shares the same N stacks regardless of connection count.
// Excess connections queue until a worker is free instead of crashing the OS.

class ThreadPool {
    std::vector<std::thread>          workers;
    std::queue<std::function<void()>> tasks;
    std::mutex                        queue_mtx;
    std::condition_variable           cv;
    bool                              stopping = false;

public:
    explicit ThreadPool(size_t n) {
        for (size_t i = 0; i < n; ++i) {
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lk(queue_mtx);
                        cv.wait(lk, [this] { return stopping || !tasks.empty(); });
                        if (stopping && tasks.empty()) return;
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    void submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lk(queue_mtx);
            tasks.push(std::move(task));
        }
        cv.notify_one();
    }

    ~ThreadPool() {
        { std::lock_guard<std::mutex> lk(queue_mtx); stopping = true; }
        cv.notify_all();
        for (auto& w : workers) w.join();
    }
};

// ============================================================
// Cluster state
// ============================================================

static std::unordered_map<int, int64_t> active_servers;
static HashRing                          ring;
static std::mutex                        cluster_mtx;

static int64_t get_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ============================================================
// Backend connection pool
// ============================================================

struct BackendPool {
    std::queue<int> sockets;
    std::mutex      mtx;
};

static std::unordered_map<int, BackendPool> pools;
static std::mutex                           pools_mtx;

static int borrow_backend_socket(int port) {
    {
        std::lock_guard<std::mutex> lk(pools_mtx);
        auto it = pools.find(port);
        if (it != pools.end()) {
            std::lock_guard<std::mutex> plk(it->second.mtx);
            if (!it->second.sockets.empty()) {
                int fd = it->second.sockets.front();
                it->second.sockets.pop();
                return fd;
            }
        }
    }
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(sock); return -1;
    }
    return sock;
}

static void return_backend_socket(int port, int fd) {
    const size_t MAX = 8;
    std::lock_guard<std::mutex> lk(pools_mtx);
    auto& pool = pools[port];
    std::lock_guard<std::mutex> plk(pool.mtx);
    if (pool.sockets.size() < MAX) pool.sockets.push(fd);
    else close(fd);
}

static void drain_pool(int port) {
    std::lock_guard<std::mutex> lk(pools_mtx);
    auto it = pools.find(port);
    if (it == pools.end()) return;
    std::lock_guard<std::mutex> plk(it->second.mtx);
    while (!it->second.sockets.empty()) { close(it->second.sockets.front()); it->second.sockets.pop(); }
    pools.erase(it);
}

// ============================================================
// RESP key extractor
// ============================================================

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
// Per-client handler (runs on a pool worker thread)
// ============================================================

static void handle_client(int client_fd) {
    struct timeval tv{}; tv.tv_sec = 5;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string raw_input;
    char buf[4096];
    while (true) {
        ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        raw_input.append(buf, n);
        if (raw_input.size() >= 2 &&
            raw_input[raw_input.size()-2] == '\r' &&
            raw_input[raw_input.size()-1] == '\n') break;
    }

    if (raw_input.empty()) { close(client_fd); return; }

    // Control plane: heartbeat
    if (raw_input.rfind("HEARTBEAT", 0) == 0) {
        try {
            int port = std::stoi(raw_input.substr(10));
            std::lock_guard<std::mutex> lk(cluster_mtx);
            if (!active_servers.count(port)) {
                Logger::info("New backend on port " + std::to_string(port));
                ring.add_server(port);
            }
            active_servers[port] = get_now_ms();
            send(client_fd, "+OK\r\n", 5, 0);
        } catch (...) { send(client_fd, "-ERR Malformed heartbeat\r\n", 26, 0); }
        close(client_fd);
        return;
    }

    // Data plane: route by key
    std::string key = extract_key_from_resp(raw_input);
    if (key.empty()) {
        send(client_fd, "-ERR Could not parse key from command\r\n", 38, 0);
        close(client_fd);
        return;
    }

    int target_port;
    { std::lock_guard<std::mutex> lk(cluster_mtx); target_port = ring.get_server_for_key(key); }

    if (target_port == -1) {
        send(client_fd, "-ERR No backends available\r\n", 28, 0);
        close(client_fd);
        return;
    }

    int bfd = borrow_backend_socket(target_port);
    if (bfd < 0) {
        send(client_fd, "-ERR Backend unreachable\r\n", 26, 0);
        close(client_fd);
        return;
    }

    if (send(bfd, raw_input.c_str(), raw_input.size(), 0) < 0) {
        close(bfd);
        send(client_fd, "-ERR Backend send failed\r\n", 26, 0);
        close(client_fd);
        return;
    }

    std::string response;
    while (true) {
        ssize_t n = recv(bfd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        response.append(buf, n);
        if (response.size() >= 2 &&
            response[response.size()-2] == '\r' &&
            response[response.size()-1] == '\n') break;
    }

    send(client_fd, response.c_str(), response.size(), 0);
    return_backend_socket(target_port, bfd);
    close(client_fd);
}

// ============================================================
// Background: evict stale backends
// ============================================================

static void heartbeat_watcher() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::vector<int> dead;
        { std::lock_guard<std::mutex> lk(cluster_mtx);
          int64_t now = get_now_ms();
          for (const auto& [port, last] : active_servers)
              if (now - last > 5000) dead.push_back(port);
          for (int p : dead) { ring.remove_server(p); active_servers.erase(p);
              Logger::error("Backend port " + std::to_string(p) + " evicted (missed heartbeats)"); }
        }
        for (int p : dead) drain_pool(p);
    }
}

// ============================================================
// main
// ============================================================

int main() {
    Logger::init("router.log");
    Logger::info("Starting redis-router on port 6379");

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { Logger::error("socket() failed"); return 1; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(6379);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        Logger::error("bind() failed"); return 1;
    }
    if (listen(server_fd, 128) < 0) { Logger::error("listen() failed"); return 1; }

    // Size the pool: 2× hardware threads, clamped to [8, 64].
    size_t hw      = std::thread::hardware_concurrency();
    size_t pool_sz = std::max(size_t(8), std::min(size_t(64), hw * 2));
    Logger::info("Thread pool size: " + std::to_string(pool_sz));
    ThreadPool pool(pool_sz);

    std::thread(heartbeat_watcher).detach();
    Logger::info("Router ready.");

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            Logger::error("accept() error: " + std::string(strerror(errno)));
            continue;
        }
        // Submit to the pool instead of spawning a new OS thread.
        pool.submit([client_fd]() { handle_client(client_fd); });
    }
    return 0;
}