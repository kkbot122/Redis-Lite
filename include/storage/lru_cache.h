#pragma once
#include <string>
#include <unordered_map>
#include <list>
#include <unordered_set>
#include <variant>
#include <vector>
#include <utility>
#include <cstdint>

using RedisValue = std::variant<
    std::string,
    std::list<std::string>,
    std::unordered_set<std::string>,
    std::unordered_map<std::string, std::string>   // hash type
>;

struct CacheItem {
    std::string key;
    RedisValue  value;
    int64_t     expires_at;   // ms epoch; 0 = no expiry
};

class LRUCache {
private:
    size_t capacity;
    std::list<CacheItem> items;
    std::unordered_map<std::string, std::list<CacheItem>::iterator> map;

    void evict_if_needed();

public:
    explicit LRUCache(size_t cap);

    void      put       (const std::string& key, const RedisValue& value, int64_t expires_at = 0);
    CacheItem* get_item (const std::string& key, int64_t current_time_ms);
    bool      exists    (const std::string& key, int64_t current_time_ms);
    bool      remove    (const std::string& key);
    bool      set_expiry(const std::string& key, int64_t expires_at);
    size_t    size      () const;

    // Full snapshot for BGREWRITEAOF — returns a copy of every live item.
    std::vector<CacheItem> get_all_items(int64_t current_time_ms) const;

    // Cursor-based SCAN. cursor=0 starts over; next_cursor=0 means done.
    std::pair<size_t, std::vector<std::string>> scan(
        size_t cursor, size_t count, int64_t current_time_ms) const;
};