#include <gtest/gtest.h>
#include "network/hash_ring.h"

TEST(HashRingTest, RoutesKeysConsistently) {
    HashRing ring;
    ring.add_server(8001);
    ring.add_server(8002);
    ring.add_server(8003);

    // If we hash "apple" 100 times, it MUST return the exact same server 
    // every single time. Otherwise, our database is broken.
    int target_port = ring.get_server_for_key("apple");
    
    for (int i = 0; i < 100; i++) {
        EXPECT_EQ(ring.get_server_for_key("apple"), target_port);
    }
}

TEST(HashRingTest, HandlesEmptyRing) {
    HashRing ring;
    // If the router boots up before any backend servers are added, 
    // it should gracefully return -1 instead of crashing.
    EXPECT_EQ(ring.get_server_for_key("apple"), -1);
}