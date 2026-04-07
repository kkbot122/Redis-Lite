#include "utils/config.h"
#include "utils/logger.h"
#include <fstream>
#include <algorithm>

int         Config::port               = 6379;
int         Config::leader_port        = 0;
size_t      Config::max_memory         = 1000;
std::string Config::aof_file           = "database.aof";
std::string Config::requirepass        = "";
int         Config::slowlog_threshold  = 10;
int         Config::slowlog_max_len    = 128;
int         Config::router_port        = 6379;

void Config::load(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        Logger::warn("Could not find " + filename + ". Using defaults.");
        return;
    }
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        std::string key   = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        key.erase(std::remove_if(key.begin(), key.end(), ::isspace), key.end());
        value.erase(std::remove_if(value.begin(), value.end(), ::isspace), value.end());
        if      (key == "port")               Config::port               = std::stoi(value);
        else if (key == "leader_port")        Config::leader_port        = std::stoi(value);
        else if (key == "max_memory")         Config::max_memory         = std::stoull(value);
        else if (key == "aof_file")           Config::aof_file           = value;
        else if (key == "requirepass")        Config::requirepass        = value;
        else if (key == "slowlog_threshold")  Config::slowlog_threshold  = std::stoi(value);
        else if (key == "slowlog_max_len")    Config::slowlog_max_len    = std::stoi(value);
        else if (key == "router_port")        Config::router_port        = std::stoi(value);
    }
    Logger::info("Config loaded from " + filename);
}