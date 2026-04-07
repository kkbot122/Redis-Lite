#include <gtest/gtest.h>
#include "storage/lru_cache.h"
#include "utils/logger.h"
#include <variant>

// Helper function to cleanly unpack our shapeshifting RedisValue in tests
std::string get_string_val(CacheItem* item) {
    if (!item) return ""; // Item was deleted or expired!
    if (auto* str = std::get_if<std::string>(&item->value)) {
        return *str;
    }
    return ""; // Wrong type
}

// ---------------------------------------------------------
// TEST 1: Basic Storage
// ---------------------------------------------------------
TEST(LRUCacheTest, BasicPutAndGet) {
    LRUCache cache(1024); // Capacity of 3
    
    cache.put("user:1", "Alice");
    cache.put("user:2", "Bob");

    // Use our new get_item() and unpack it!
    EXPECT_EQ(get_string_val(cache.get_item("user:1", 100)), "Alice");
    EXPECT_EQ(get_string_val(cache.get_item("user:2", 100)), "Bob");
    
    // Asking for a key that doesn't exist should return a null pointer
    EXPECT_EQ(cache.get_item("user:99", 100), nullptr); 
}

// ---------------------------------------------------------
// TEST 2: The Eviction Engine
// ---------------------------------------------------------
TEST(LRUCacheTest, EvictsLeastRecentlyUsed) {
    LRUCache cache(250);

    cache.put("A", "1");
    cache.put("B", "2");
    cache.put("C", "3");
    
    // Make "A" the most recently used.
    cache.get_item("A", 100); 

    // Push 4th item, killing "B"
    cache.put("D", "4");

    EXPECT_EQ(get_string_val(cache.get_item("A", 100)), "1"); 
    EXPECT_EQ(get_string_val(cache.get_item("C", 100)), "3"); 
    EXPECT_EQ(get_string_val(cache.get_item("D", 100)), "4"); 
    
    // B should be a null pointer now!
    EXPECT_EQ(cache.get_item("B", 100), nullptr); 
}

// ---------------------------------------------------------
// TEST 3: Time-To-Live (TTL) Expiration
// ---------------------------------------------------------
TEST(LRUCacheTest, HandlesExpiration) {
    LRUCache cache(1024);
    
    cache.put("temp_key", "secret", 5000);

    EXPECT_EQ(get_string_val(cache.get_item("temp_key", 4999)), "secret");

    // Instantly deleted
    EXPECT_EQ(cache.get_item("temp_key", 5001), nullptr);
}

int main(int argc, char **argv) {
    Logger::init("test_run.log");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}