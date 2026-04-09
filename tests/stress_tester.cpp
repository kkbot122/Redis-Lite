#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

std::atomic<int> successful_requests(0);
std::atomic<int> failed_requests(0);
std::atomic<int> reconnects(0);
std::atomic<int> timeouts(0);
std::atomic<int> total_processed(0); // <-- NEW: Tracks progress

static int connect_with_timeout(int port, int timeout_ms) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    int res = connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    if (res < 0 && errno != EINPROGRESS) {
        close(sock);
        return -1;
    }

    if (res < 0) {
        fd_set set;
        FD_ZERO(&set);
        FD_SET(sock, &set);
        struct timeval tv;
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;

        res = select(sock + 1, nullptr, &set, nullptr, &tv);
        if (res <= 0) {
            close(sock);
            return -1; 
        }
        int valopt;
        socklen_t lon = sizeof(int);
        getsockopt(sock, SOL_SOCKET, SO_ERROR, (void*)(&valopt), &lon);
        if (valopt) {
            close(sock);
            return -1;
        }
    }

    fcntl(sock, F_SETFL, flags);
    
    struct timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    return sock;
}

static bool send_all(int sock, const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(sock, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static std::string make_set_resp(const std::string& key, const std::string& value) {
    return "*3\r\n$3\r\nSET\r\n$" + std::to_string(key.size()) + "\r\n" + key +
           "\r\n$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

void simulate_client(int client_id, int requests_per_client, int port) {
    int sock = connect_with_timeout(port, 1500);
    if (sock < 0) {
        failed_requests += requests_per_client;
        total_processed += requests_per_client;
        return;
    }

    for (int i = 0; i < requests_per_client; ++i) {
        std::string key = "user_" + std::to_string(client_id) + "_" + std::to_string(i);
        std::string cmd = make_set_resp(key, "active");

        if (!send_all(sock, cmd)) {
            close(sock);
            sock = connect_with_timeout(port, 1500);
            reconnects++;
            if (sock < 0 || !send_all(sock, cmd)) {
                failed_requests++;
                total_processed++;
                continue;
            }
        }

        char buffer[64] = {0};
        int bytes_read = read(sock, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) timeouts++;
            close(sock);
            sock = connect_with_timeout(port, 1500);
            reconnects++;
            failed_requests++;
            total_processed++;
            if (sock < 0) break;
            continue;
        }

        if (std::strncmp(buffer, "+OK", 3) == 0) successful_requests++;
        else failed_requests++;
        
        // --- VISUAL PROGRESS BAR ---
        int current = ++total_processed;
        if (current % 500 == 0) {
            std::cout << "█" << std::flush;
        }
    }

    if (sock >= 0) close(sock);
}

int main() {
    int num_concurrent_users = 50;
    int requests_per_user = 1000;
    int target_port = 6379;
    int total_requests = num_concurrent_users * requests_per_user;

    std::cout << "🚀 Starting Distributed Stress Test...\n";
    std::cout << "Simulating " << num_concurrent_users << " concurrent connections.\n";
    std::cout << "Target: " << total_requests << " total database writes.\n";
    std::cout << "Progress: ";

    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(num_concurrent_users));
    for (int i = 0; i < num_concurrent_users; ++i)
        threads.emplace_back(simulate_client, i, requests_per_user, target_port);

    for (auto& t : threads) t.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;
    double req_per_sec = successful_requests / duration.count();

    std::cout << "\n\n--- STRESS TEST RESULTS ---\n";
    std::cout << "Time elapsed:        " << duration.count() << " seconds\n";
    std::cout << "Successful writes:   " << successful_requests << "\n";
    std::cout << "Failed requests:     " << failed_requests << "\n";
    std::cout << "Reconnects:          " << reconnects << "\n";
    std::cout << "Read timeouts:       " << timeouts << "\n";
    std::cout << "System Throughput:   " << req_per_sec << " requests/second\n";

    return failed_requests == 0 ? 0 : 2;
}