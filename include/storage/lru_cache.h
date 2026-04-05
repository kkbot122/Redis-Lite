#pragma once
#include <string>
#include <unordered_map>
#include <list>
#include <unordered_set>
#include <variant>

using RedisValue = std::variant<std::string, std::list<std::string>, std::unordered_set<std::string>>;

struct CacheItem {
    std::string key;
    RedisValue value;
    int64_t expires_at; 
};

class LRUCache {
private:
    size_t capacity;
    std::list<CacheItem> items;
    std::unordered_map<std::string, std::list<CacheItem>::iterator> map;

    void evict_if_needed();

public:
    LRUCache(size_t cap);
    
    // Put a new item in the cache
    void put(const std::string& key, const RedisValue& value, int64_t expires_at = 0);
    
    // NEW: Get a pointer to the item so we can modify Lists and Sets in memory!
    CacheItem* get_item(const std::string& key, int64_t current_time_ms);
};