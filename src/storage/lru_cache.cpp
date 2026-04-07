#include "storage/lru_cache.h"
#include "utils/logger.h"
#include <algorithm>

LRUCache::LRUCache(size_t max_bytes) : max_memory_bytes(max_bytes), current_memory_bytes(0) {}

size_t LRUCache::memory_usage() const { return current_memory_bytes; }

// ============================================================
// THE RSS MEMORY ESTIMATOR
// ============================================================
size_t LRUCache::calculate_item_size(const std::string& key, const RedisValue& value) const {
    // Base size: The struct itself + the dynamically allocated key string
    size_t total = sizeof(CacheItem) + key.capacity();
    
    if (const auto* s = std::get_if<std::string>(&value)) {
        total += s->capacity();
    } 
    else if (const auto* l = std::get_if<std::list<std::string>>(&value)) {
        total += sizeof(std::list<std::string>);
        // Linked list overhead: ~2 pointers per node + string capacity
        for (const auto& str : *l) total += (sizeof(void*) * 2) + str.capacity();
    } 
    else if (const auto* st = std::get_if<std::unordered_set<std::string>>(&value)) {
        total += sizeof(std::unordered_set<std::string>);
        // Hash set overhead: buckets + pointers
        for (const auto& str : *st) total += (sizeof(void*) * 2) + str.capacity();
    } 
    else if (const auto* h = std::get_if<std::unordered_map<std::string, std::string>>(&value)) {
        total += sizeof(std::unordered_map<std::string, std::string>);
        for (const auto& [k, v] : *h) total += (sizeof(void*) * 2) + k.capacity() + v.capacity();
    } 
    else if (const auto* z = std::get_if<ZSet>(&value)) {
        total += sizeof(ZSet);
        // Add the Hash Map footprint
        for (const auto& [k, score] : z->dict) total += (sizeof(void*) * 2) + k.capacity() + sizeof(double);
        // Add the Red-Black Tree footprint (~3 pointers per node: left, right, parent)
        for (const auto& [score, k] : z->tree) total += (sizeof(void*) * 3) + k.capacity() + sizeof(double);
    }
    return total;
}

void LRUCache::evict_if_needed() {
    // Keep deleting the oldest items until we are back under the byte limit!
    while (current_memory_bytes > max_memory_bytes && !items.empty()) {
        auto& last = items.back();
        Logger::warn("Memory limit exceeded (" + std::to_string(current_memory_bytes) + " bytes). Evicting: " + last.key);
        current_memory_bytes -= last.mem_size;
        map.erase(last.key);
        items.pop_back();
    }
}

void LRUCache::put(const std::string& key, const RedisValue& value, int64_t expires_at) {
    size_t new_size = calculate_item_size(key, value);
    
    auto it = map.find(key);
    if (it != map.end()) {
        current_memory_bytes -= it->second->mem_size; // Remove old size
        it->second->value = value;
        it->second->expires_at = expires_at;
        it->second->mem_size = new_size;             // Apply new size
        current_memory_bytes += new_size;
        items.splice(items.begin(), items, it->second);
    } else {
        current_memory_bytes += new_size;
        items.push_front({key, value, expires_at, new_size});
        map[key] = items.begin();
    }
    evict_if_needed();
}

void LRUCache::recalculate_size(const std::string& key) {
    auto it = map.find(key);
    if (it == map.end()) return;
    
    size_t updated_size = calculate_item_size(key, it->second->value);
    current_memory_bytes -= it->second->mem_size;
    current_memory_bytes += updated_size;
    it->second->mem_size = updated_size;
    
    evict_if_needed();
}

CacheItem* LRUCache::get_item(const std::string& key, int64_t now) {
    auto it = map.find(key);
    if (it == map.end()) return nullptr;
    auto lit = it->second;
    if (lit->expires_at > 0 && lit->expires_at <= now) {
        current_memory_bytes -= lit->mem_size;
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
        current_memory_bytes -= lit->mem_size;
        items.erase(lit);
        map.erase(it);
        return false;
    }
    return true;
}

bool LRUCache::remove(const std::string& key) {
    auto it = map.find(key);
    if (it == map.end()) return false;
    current_memory_bytes -= it->second->mem_size; // Free the bytes!
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