#pragma once
#include "storage/lru_cache.h"
#include <string>
#include <vector>
#include <deque>
#include <fstream>
#include <shared_mutex>
#include <mutex>
#include <atomic>
#include <cstdint>

struct SlowlogEntry {
    int64_t                  id;
    int64_t                  timestamp_ms;
    int64_t                  duration_ms;
    std::vector<std::string> args;
};

// Per-connection transaction state. The server allocates one per client fd.
struct TxState {
    bool                              active  = false;   // MULTI issued
    bool                              errored = false;   // command error inside MULTI
    std::vector<std::vector<std::string>> queue;
};

class KeyValueStore {
private:
    LRUCache cache;
    std::fstream aof_file;
    bool loading_from_aof = false;

    mutable std::shared_mutex mtx;

    int64_t              start_time_ms;
    std::atomic<int64_t> total_commands{0};
    std::atomic<bool>    rewrite_in_progress{false};

    // Slowlog
    mutable std::mutex   slowlog_mtx;
    std::deque<SlowlogEntry> slowlog;
    std::atomic<int64_t> slowlog_id{0};

    int64_t     get_current_time_ms() const;
    void        append_to_aof(const std::vector<std::string>& args);
    void        load_from_aof();
    std::string execute_command_locked(const std::vector<std::string>& args);
    std::string serialize_item_to_resp(const CacheItem& item) const;
    std::string build_info(const std::string& section) const;
    void        record_slowlog(const std::vector<std::string>& args, int64_t duration_ms);

public:
    KeyValueStore();
    ~KeyValueStore();

    // Main entry — thread-safe, records slowlog, handles AUTH.
    // tx: per-connection transaction state (owned by the server, passed in).
    // authenticated: per-connection auth flag.
    std::string execute_command(const std::vector<std::string>& args,
                                TxState& tx,
                                bool& authenticated);
};