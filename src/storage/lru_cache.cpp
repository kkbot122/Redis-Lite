#include "storage/lru_cache.h"
#include "utils/logger.h"
#include <algorithm>

LRUCache::LRUCache(size_t cap) : capacity(cap) {}

void LRUCache::evict_if_needed() {
    while (map.size() > capacity) {
        auto& last = items.back();
        Logger::warn("Memory full — evicting LRU key: " + last.key);
        map.erase(last.key);
        items.pop_back();
    }
}

void LRUCache::put(const std::string& key, const RedisValue& value, int64_t expires_at) {
    auto it = map.find(key);
    if (it != map.end()) {
        it->second->value      = value;
        it->second->expires_at = expires_at;
        items.splice(items.begin(), items, it->second);
    } else {
        items.push_front({key, value, expires_at});
        map[key] = items.begin();
        evict_if_needed();
    }
}

CacheItem* LRUCache::get_item(const std::string& key, int64_t now) {
    auto it = map.find(key);
    if (it == map.end()) return nullptr;
    auto lit = it->second;
    if (lit->expires_at > 0 && lit->expires_at <= now) {
        items.erase(lit);
        map.erase(it);
        return nullptr;
    }
    items.splice(items.begin(), items, lit);
    return &(*lit);
}

bool LRUCache::exists(const std::string& key, int64_t now) {
    auto it = map.find(key);
    if (it == map.end()) return false;
    auto lit = it->second;
    if (lit->expires_at > 0 && lit->expires_at <= now) {
        items.erase(lit);
        map.erase(it);
        return false;
    }
    return true;
}

bool LRUCache::remove(const std::string& key) {
    auto it = map.find(key);
    if (it == map.end()) return false;
    items.erase(it->second);
    map.erase(it);
    return true;
}

bool LRUCache::set_expiry(const std::string& key, int64_t expires_at) {
    auto it = map.find(key);
    if (it == map.end()) return false;
    it->second->expires_at = expires_at;
    return true;
}

size_t LRUCache::size() const { return map.size(); }

// Returns a value-copy of every live item in LRU order (MRU first).
// Expired items are skipped but NOT evicted (this is a const method).
std::vector<CacheItem> LRUCache::get_all_items(int64_t now) const {
    std::vector<CacheItem> result;
    result.reserve(map.size());
    for (const auto& item : items) {
        if (item.expires_at == 0 || item.expires_at > now) {
            result.push_back(item);
        }
    }
    return result;
}

// Simple index-based cursor. Not as rehash-safe as Redis's reverse-bit cursor,
// but stateless, correct, and sufficient for our keyspace sizes.
std::pair<size_t, std::vector<std::string>> LRUCache::scan(
    size_t cursor, size_t count, int64_t now) const
{
    // Build a stable ordered list of live keys.
    std::vector<std::string> all_keys;
    all_keys.reserve(map.size());
    for (const auto& item : items) {
        if (item.expires_at == 0 || item.expires_at > now) {
            all_keys.push_back(item.key);
        }
    }

    if (cursor >= all_keys.size()) return {0, {}};

    size_t end = std::min(cursor + count, all_keys.size());
    std::vector<std::string> page(all_keys.begin() + cursor,
                                  all_keys.begin() + end);

    size_t next_cursor = (end >= all_keys.size()) ? 0 : end;
    return {next_cursor, page};
}