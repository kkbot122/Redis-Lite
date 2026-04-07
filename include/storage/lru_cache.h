#pragma once
#include <string>
#include <unordered_map>
#include <list>
#include <unordered_set>
#include <variant>
#include <vector>
#include <utility>
#include <cstdint>
#include <set>

struct ZSet {
    std::unordered_map<std::string, double> dict;   // O(1) score lookups
    std::set<std::pair<double, std::string>> tree;  // O(log N) ordered iteration
};

using RedisValue = std::variant<
    std::string,
    std::list<std::string>,
    std::unordered_set<std::string>,
    std::unordered_map<std::string, std::string>,
    ZSet
>;

struct CacheItem {
    std::string key;
    RedisValue  value;
    int64_t     expires_at;
    size_t      mem_size = 0;
};

class LRUCache {
private:
    size_t max_memory_bytes;
    size_t current_memory_bytes = 0; // NEW: Global memory tracker

    std::list<CacheItem> items;
    std::unordered_map<std::string, std::list<CacheItem>::iterator> map;

    void   evict_if_needed();
    size_t calculate_item_size(const std::string& key, const RedisValue& value) const;

public:
    explicit LRUCache(size_t max_bytes);

    void       put       (const std::string& key, const RedisValue& value, int64_t expires_at = 0);
    CacheItem* get_item  (const std::string& key, int64_t current_time_ms);
    bool       exists    (const std::string& key, int64_t current_time_ms);
    bool       remove    (const std::string& key);
    bool       set_expiry(const std::string& key, int64_t expires_at);
    size_t     size      () const;
    size_t     memory_usage() const; // For the INFO command

    // NEW: Recalculates size when a value is modified in-place (e.g., ZADD, HSET)
    void       recalculate_size(const std::string& key);

    std::vector<CacheItem> get_all_items(int64_t current_time_ms) const;
    std::pair<size_t, std::vector<std::string>> scan(size_t cursor, size_t count, int64_t current_time_ms) const;
};