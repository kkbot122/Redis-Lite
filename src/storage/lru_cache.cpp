#include "storage/lru_cache.h"
#include "utils/logger.h"

LRUCache::LRUCache(size_t cap) : capacity(cap) {}

void LRUCache::evict_if_needed() {
    if (map.size() > capacity) {
        auto last_item = items.back();
        Logger::warn("Memory Full. Evicting oldest key: " + last_item.key);
        map.erase(last_item.key); 
        items.pop_back();        
    }
}

// Updated to accept RedisValue instead of std::string
void LRUCache::put(const std::string& key, const RedisValue& value, int64_t expires_at) {
    if (map.find(key) != map.end()) {
        auto it = map[key];
        it->value = value;
        it->expires_at = expires_at;
        items.splice(items.begin(), items, it); 
    } else {
        items.push_front({key, value, expires_at});
        map[key] = items.begin();
        evict_if_needed();
    }
}

// Updated to return a pointer to the CacheItem!
CacheItem* LRUCache::get_item(const std::string& key, int64_t current_time_ms) {
    if (map.find(key) != map.end()) {
        auto it = map[key]; 
        
        if (it->expires_at > 0 && it->expires_at < current_time_ms) {
            items.erase(it);
            map.erase(key);
            return nullptr; 
        }
        
        items.splice(items.begin(), items, it);
        return &(*it); // Return the pointer to the actual item in memory
    }
    return nullptr; 
}