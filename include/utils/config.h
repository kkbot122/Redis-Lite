#pragma once
#include <string>

class Config {
public:
    // Our global application settings, with safe defaults
    static int port;
    static int leader_port;
    static size_t max_memory;
    static std::string aof_file;

    // The function that reads redis.conf
    static void load(const std::string& filename);
};