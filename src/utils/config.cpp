#include "utils/config.h"
#include "utils/logger.h"
#include <fstream>
#include <sstream>
#include <algorithm>

// Define the default values (just in case the config file is missing!)
int Config::port = 6379;
int Config::leader_port = 0;
size_t Config::max_memory = 3;
std::string Config::aof_file = "database.aof";

void Config::load(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        Logger::warn("Could not find " + filename + ". Using default settings.");
        return;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        // Ignore empty lines and comments (lines starting with #)
        if (line.empty() || line[0] == '#') continue;
        
        // Find the '=' sign
        auto delimiter_pos = line.find('=');
        if (delimiter_pos == std::string::npos) continue; // Skip invalid lines
        
        // Split the string into Key and Value
        std::string key = line.substr(0, delimiter_pos);
        std::string value = line.substr(delimiter_pos + 1);
        
        // Remove spaces just in case the user typed "port = 6379"
        key.erase(std::remove_if(key.begin(), key.end(), ::isspace), key.end());
        value.erase(std::remove_if(value.begin(), value.end(), ::isspace), value.end());
        
        // Update the system settings
        if (key == "port") Config::port = std::stoi(value);
        else if (key == "leader_port") Config::leader_port = std::stoi(value);
        else if (key == "max_memory") Config::max_memory = std::stoull(value);
        else if (key == "aof_file") Config::aof_file = value;
    }
    
    Logger::info("Successfully loaded configuration from " + filename);
}