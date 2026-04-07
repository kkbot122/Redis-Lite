#include "utils/config.h"
#include "utils/logger.h"
#include <fstream>
#include <algorithm>

int         Config::port              = 6379;
int         Config::leader_port       = 0;
size_t      Config::max_memory        = 1000;
std::string Config::aof_file          = "database.aof";
std::string Config::rdb_file          = "dump.rdb";
std::string Config::requirepass       = "";
int         Config::slowlog_threshold = 10;
int         Config::slowlog_max_len   = 128;
int         Config::router_port       = 6379;
std::string Config::unixsocket        = "";
int         Config::rdb_save_seconds  = 900;

void Config::load(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) { Logger::warn("No config file — using defaults."); return; }
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        std::string k = line.substr(0, pos);
        std::string v = line.substr(pos + 1);
        k.erase(std::remove_if(k.begin(), k.end(), ::isspace), k.end());
        v.erase(std::remove_if(v.begin(), v.end(), ::isspace), v.end());
        if      (k == "port")              Config::port              = std::stoi(v);
        else if (k == "leader_port")       Config::leader_port       = std::stoi(v);
        else if (k == "max_memory")        Config::max_memory        = std::stoull(v) * 1024 * 1024;
        else if (k == "aof_file")          Config::aof_file          = v;
        else if (k == "rdb_file")          Config::rdb_file          = v;
        else if (k == "requirepass")       Config::requirepass       = v;
        else if (k == "slowlog_threshold") Config::slowlog_threshold = std::stoi(v);
        else if (k == "slowlog_max_len")   Config::slowlog_max_len   = std::stoi(v);
        else if (k == "router_port")       Config::router_port       = std::stoi(v);
        else if (k == "unixsocket")        Config::unixsocket        = v;
        else if (k == "rdb_save_seconds")  Config::rdb_save_seconds  = std::stoi(v);
    }
    Logger::info("Config loaded from " + filename);
}