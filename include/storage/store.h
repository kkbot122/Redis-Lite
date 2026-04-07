#pragma once
#include "storage/lru_cache.h"
#include <string>
#include <vector>
#include <deque>
#include <fstream>
#include <shared_mutex>
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <functional> // NEW: For our Command Handlers!

struct SlowlogEntry {
    int64_t                  id;
    int64_t                  timestamp_ms;
    int64_t                  duration_ms;
    std::vector<std::string> args;
};

struct TxState {
    bool                                      active  = false;
    bool                                      errored = false;
    std::vector<std::vector<std::string>>     queue;
    std::unordered_map<std::string, uint64_t> watched_keys; 
};

class KeyValueStore {
private:
    LRUCache cache;
    std::fstream aof_file;
    bool loading_from_aof = false;

    mutable std::shared_mutex mtx;

    std::unordered_map<std::string, uint64_t> key_versions; 
    std::atomic<uint64_t>                     global_flush_version{0};
    void track_mutations(const std::vector<std::string>& args); 

    int64_t              start_time_ms;
    std::atomic<int64_t> total_commands{0};
    std::atomic<bool>    rewrite_in_progress{false};
    std::atomic<bool>    rdb_save_in_progress{false};
    std::atomic<int64_t> last_save_time{0};      

    mutable std::mutex       slowlog_mtx;
    std::deque<SlowlogEntry> slowlog;
    std::atomic<int64_t>     slowlog_id{0};

    // =======================================================
    // NEW: The Command Registry
    // =======================================================
    using CommandHandler = std::function<std::string(const std::vector<std::string>&, int64_t)>;
    std::unordered_map<std::string, CommandHandler> command_registry;
    void init_commands(); // Registers all functions on boot
    // =======================================================

    int64_t     get_current_time_ms() const;
    void        append_to_aof(const std::vector<std::string>& args);
    void        load_from_aof();
    std::string execute_command_locked(const std::vector<std::string>& args);
    std::string serialize_item_to_resp(const CacheItem& item) const;
    std::string build_info(const std::string& section) const;
    void        record_slowlog(const std::vector<std::string>& args, int64_t duration_ms);

    bool        rdb_save_snapshot(const std::string& path,
                                  const std::vector<CacheItem>& items) const;
    bool        rdb_load_snapshot(const std::string& path);

public:
    KeyValueStore();
    ~KeyValueStore();

    std::string execute_command(const std::vector<std::string>& args,
                                TxState& tx, bool& authenticated);
    
    void        maybe_auto_save();
};