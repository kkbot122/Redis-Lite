#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include "network/hash_ring.h"
#include "utils/logger.h"

std::unordered_map<int, int64_t> active_servers;
HashRing ring;

int64_t get_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

void evict_dead_servers() {
    int64_t now = get_now_ms();
    std::vector<int> dead_ports;

    for (const auto& [port, last_seen] : active_servers) {
        // If 5 seconds pass without a heartbeat, mark for death
        if (now - last_seen > 5000) { 
            dead_ports.push_back(port);
        }
    }

    for (int port : dead_ports) {
        Logger::error("Server " + std::to_string(port) + " missed heartbeats. Evicting from cluster!");
        ring.remove_server(port);
        active_servers.erase(port);
    }
}

std::string extract_key(const std::string& raw_input) {
    size_t pos1 = raw_input.find("\r\n");
    if (pos1 == std::string::npos) return "";
    size_t pos2 = raw_input.find("\r\n", pos1 + 2);
    if (pos2 == std::string::npos) return "";
    size_t pos3 = raw_input.find("\r\n", pos2 + 2);
    if (pos3 == std::string::npos) return "";
    size_t pos4 = raw_input.find("\r\n", pos3 + 2);
    if (pos4 == std::string::npos) return "";
    
    return raw_input.substr(pos3 + 2, pos4 - pos3 - 2);
}

std::string forward_to_backend(int target_port, const std::string& command) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(target_port);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        return "-ERR Backend server offline\r\n";
    }

    send(sock, command.c_str(), command.length(), 0);
    char buffer[1024] = {0};
    int bytes_read = read(sock, buffer, 1024);
    close(sock);

    if (bytes_read > 0) return std::string(buffer, bytes_read);
    return "-ERR Empty response from backend\r\n";
}

int main() {
    Logger::init("router.log");
    Logger::info("Starting Dynamic Service Router on port 6379...");

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // ENSURE IT ACTUALLY BINDS!
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        Logger::error("Router failed to bind to port 6379! Is another app using it?");
        return 1;
    }
    
    listen(server_fd, 10);

    while (true) {
        // Run the Grim Reaper to check for dead servers
        evict_dead_servers();

        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) continue;

        char buffer[1024] = {0};
        int bytes_read = read(client_fd, buffer, 1024);
        
        if (bytes_read > 0) {
            std::string raw_input(buffer);

            // ==========================================
            // CONTROL PLANE: Intercept Heartbeats
            // ==========================================
            if (raw_input.find("HEARTBEAT") == 0) {
                try {
                    int p = std::stoi(raw_input.substr(10)); 
                    if (active_servers.find(p) == active_servers.end()) {
                        Logger::info("New Server Discovered! Adding " + std::to_string(p) + " to the ring.");
                        ring.add_server(p);
                    }
                    active_servers[p] = get_now_ms(); // Update the stopwatch
                    send(client_fd, "+OK\r\n", 5, 0);
                } catch (...) {} // Ignore malformed heartbeats
            } 
            // ==========================================
            // DATA PLANE: Standard User Traffic Routing
            // ==========================================
            else {
                std::string key = extract_key(raw_input);
                if (key.empty()) {
                    send(client_fd, "-ERR Need a key to route\r\n", 26, 0);
                } else {
                    int target_port = ring.get_server_for_key(key);
                    if (target_port == -1) {
                        send(client_fd, "-ERR No backend servers available\r\n", 35, 0);
                    } else {
                        std::string response = forward_to_backend(target_port, raw_input);
                        send(client_fd, response.c_str(), response.length(), 0);
                    }
                }
            }
        }
        
        // IMPORTANT: We close the connection immediately so the Router never gets "stuck"
        close(client_fd);
    }
    return 0;
}