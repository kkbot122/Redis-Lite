#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

// Thread-safe counters
std::atomic<int> successful_requests(0);
std::atomic<int> failed_requests(0);

// This function acts as a single "User" connecting to the database
void simulate_client(int client_id, int requests_per_client, int port) {
    // 1. Open a network socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    // 2. Connect to the Router
    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        failed_requests += requests_per_client;
        return;
    }

    // 3. Rapid-fire requests!
    for (int i = 0; i < requests_per_client; i++) {
        // Create a unique key for every single request (e.g., SET user_5_99 active)
        std::string key = "user_" + std::to_string(client_id) + "_" + std::to_string(i);
        std::string cmd = "SET " + key + " active\n";
        
        // Send the command
        if (send(sock, cmd.c_str(), cmd.length(), 0) < 0) {
            failed_requests++;
            continue;
        }

        // Wait for the "+OK\r\n" response
        char buffer[1024] = {0};
        int bytes_read = read(sock, buffer, 1024);
        
        if (bytes_read > 0) {
            successful_requests++;
        } else {
            failed_requests++;
        }
    }
    
    // Hang up the phone when done
    close(sock);
}

int main() {
    // === THE LOAD CONFIGURATION ===
    int num_concurrent_users = 50;      // 50 people using the app at once
    int requests_per_user = 1000;       // Each person clicks 1,000 times
    int target_port = 6379;             // Point this at your Router!

    int total_requests = num_concurrent_users * requests_per_user;

    std::cout << "🚀 Starting Distributed Stress Test...\n";
    std::cout << "Simulating " << num_concurrent_users << " concurrent connections.\n";
    std::cout << "Target: " << total_requests << " total database writes.\n\n";

    // Start the stopwatch!
    auto start_time = std::chrono::high_resolution_clock::now();

    // Spawn 50 separate threads (users) simultaneously
    std::vector<std::thread> threads;
    for (int i = 0; i < num_concurrent_users; i++) {
        threads.push_back(std::thread(simulate_client, i, requests_per_user, target_port));
    }

    // Wait for all 50 users to finish clicking
    for (auto& t : threads) {
        t.join();
    }

    // Stop the stopwatch!
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;

    // Calculate throughput
    double req_per_sec = successful_requests / duration.count();

    std::cout << "--- STRESS TEST RESULTS ---\n";
    std::cout << "Time elapsed:        " << duration.count() << " seconds\n";
    std::cout << "Successful writes:   " << successful_requests << "\n";
    std::cout << "Failed requests:     " << failed_requests << "\n";
    std::cout << "System Throughput:   " << req_per_sec << " requests/second\n";

    if (failed_requests > 0) {
        std::cout << "\n⚠️ WARNING: Your server dropped connections under load!\n";
    }

    return 0;
}