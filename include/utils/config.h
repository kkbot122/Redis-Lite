#pragma once
#include <string>

class Config {
public:
    static int         port;
    static int         leader_port;
    static size_t      max_memory;
    static std::string aof_file;
    static std::string requirepass;       // empty = no auth required
    static int         slowlog_threshold; // ms; -1 = disabled
    static int         slowlog_max_len;
    static int         router_port;       // port the heartbeat thread sends to

    static void load(const std::string& filename);
};