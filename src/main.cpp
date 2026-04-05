#include <iostream>
#include <string>
#include "network/server.h"
#include "utils/logger.h"
#include "utils/config.h" // NEW!

int main(int argc, char* argv[]) {
    Logger::init("server.log");
    Logger::info("Initializing Redis-lite boot sequence...");

    // 1. Read the config file!
    Config::load("redis.conf");

    // 2. Command line arguments OVERRIDE the config file 
    // (Useful for starting our Follower tests quickly)
    if (argc >= 2) Config::port = std::stoi(argv[1]);
    if (argc >= 3) Config::leader_port = std::stoi(argv[2]);

    Logger::info("Configuration loaded. Port: " + std::to_string(Config::port));
    Logger::info("Max Memory Limit: " + std::to_string(Config::max_memory) + " keys");

    // 3. Start the server using the Config!
    RedisServer server(Config::port, Config::leader_port);
    server.run();

    return 0;
}