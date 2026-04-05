#include "storage/store.h"
#include "utils/logger.h"
#include "utils/config.h"
#include <chrono>
#include <iostream>
#include <sstream>

// NEW: Initialize the cache with the config limit in the initializer list
KeyValueStore::KeyValueStore() : cache(Config::max_memory) {
    load_from_aof(); 
    
    aof_file.open(Config::aof_file, std::ios::app);
    if (!aof_file.is_open()) {
        Logger::error("Could not open AOF file for writing!"); 
    }
}

KeyValueStore::~KeyValueStore() {
    if (aof_file.is_open()) aof_file.close();
}

void KeyValueStore::load_from_aof() {
    std::ifstream file(Config::aof_file);
    if (!file.is_open()) {
        Logger::info("No AOF file found. Starting with a fresh database.");
        return;
    }

    Logger::info("Loading data from disk (AOF)...");
    std::string line;
    int restored_count = 0;

    while (std::getline(file, line)) {
        std::vector<std::string> args;
        std::stringstream ss(line);
        std::string token;
        while (ss >> token) args.push_back(token);

        if (!args.empty()) {
            execute_command(args);
            restored_count++;
        }
    }
    Logger::info("Successfully restored " + std::to_string(restored_count) + " commands from disk.");
}

void KeyValueStore::append_to_aof(const std::vector<std::string>& args) {
    if (!aof_file.is_open()) return;
    for (size_t i = 0; i < args.size(); ++i) {
        aof_file << args[i];
        if (i < args.size() - 1) aof_file << " ";
    }
    aof_file << "\n"; 
    aof_file.flush(); 
}

int64_t KeyValueStore::get_current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

std::string KeyValueStore::execute_command(const std::vector<std::string>& args) {
    if (args.empty()) return "-ERR Empty command\r\n";

    std::string cmd = args[0];
    for (char &c : cmd) c = toupper(c);

    if (cmd == "PING") {
        return "+PONG\r\n";
    } 
    // ==========================================
    // STRINGS
    // ==========================================
    else if (cmd == "SET" && args.size() >= 3) {
        cache.put(args[1], args[2]); // Stored as a string!
        append_to_aof(args);
        return "+OK\r\n";
    }
    else if (cmd == "GET" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], get_current_time_ms());
        if (!item) return "$-1\r\n"; // Null Bulk String

        // Type Safety Check! Is it a string?
        if (auto* str_val = std::get_if<std::string>(&item->value)) {
            return "$" + std::to_string(str_val->length()) + "\r\n" + *str_val + "\r\n";
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    // ==========================================
    // LISTS
    // ==========================================
    else if (cmd == "LPUSH" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], get_current_time_ms());
        int new_length = 0;

        if (!item) {
            // Key doesn't exist, create a new list!
            std::list<std::string> new_list = {args[2]};
            cache.put(args[1], new_list);
            new_length = 1;
        } else {
            // Key exists. Is it a list?
            if (auto* list_val = std::get_if<std::list<std::string>>(&item->value)) {
                list_val->push_front(args[2]);
                new_length = list_val->size();
            } else {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            }
        }
        append_to_aof(args);
        return ":" + std::to_string(new_length) + "\r\n"; // RESP Integer format
    }
    else if (cmd == "LRANGE" && args.size() >= 2) {
        // Note: For simplicity, our LRANGE ignores start/stop indices and just returns all items
        CacheItem* item = cache.get_item(args[1], get_current_time_ms());
        if (!item) return "*0\r\n"; // Empty RESP Array

        if (auto* list_val = std::get_if<std::list<std::string>>(&item->value)) {
            // Format output as a RESP Array!
            std::string resp = "*" + std::to_string(list_val->size()) + "\r\n";
            for (const auto& val : *list_val) {
                resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
            }
            return resp;
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    
    return "-ERR Unknown command\r\n";
}