#pragma once
#include "storage/lru_cache.h"
#include <string>
#include <vector>
#include <fstream>
#include <shared_mutex>
#include <atomic>
#include <cstdint>

class KeyValueStore {
private:
    LRUCache cache;
    std::fstream aof_file;
    bool loading_from_aof = false;

    mutable std::shared_mutex mtx;

    // Stats exposed by the INFO command.
    int64_t              start_time_ms;
    std::atomic<int64_t> total_commands{0};
    std::atomic<bool>    rewrite_in_progress{false};

    int64_t     get_current_time_ms() const;
    void        append_to_aof(const std::vector<std::string>& args);
    void        load_from_aof();
    std::string execute_command_locked(const std::vector<std::string>& args);

    // Used by BGREWRITEAOF to serialize one item back to RESP commands.
    std::string serialize_item_to_resp(const CacheItem& item) const;

    // Builds the INFO bulk-string payload.
    std::string build_info(const std::string& section) const;

public:
    KeyValueStore();
    ~KeyValueStore();

    std::string execute_command(const std::vector<std::string>& args);
};