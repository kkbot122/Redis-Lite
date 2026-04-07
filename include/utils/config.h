#pragma once
#include <string>

class Config {
public:
    static int         port;
    static int         leader_port;
    static size_t      max_memory;
    static std::string aof_file;
    static std::string rdb_file;
    static std::string requirepass;
    static int         slowlog_threshold;
    static int         slowlog_max_len;
    static int         router_port;
    static std::string unixsocket;       // path to unix domain socket; empty = disabled
    static int         rdb_save_seconds; // auto-save interval in seconds; 0 = disabled

    static std::string tls_cert_file;
    static std::string tls_key_file;

    static void load(const std::string& filename);
};