#pragma once
#include <string>
#include <vector>
#include <fstream>
#include "storage/lru_cache.h" // NEW: Include our cache module

class KeyValueStore {
private:
    LRUCache cache;         // NEW: The engine handles its own memory now!
    std::ofstream aof_file; 

    int64_t get_current_time_ms();
    void append_to_aof(const std::vector<std::string>& args);
    void load_from_aof();

public:
    KeyValueStore();  
    ~KeyValueStore(); 

    std::string execute_command(const std::vector<std::string>& args);
};