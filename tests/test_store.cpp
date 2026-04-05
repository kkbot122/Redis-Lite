#include <gtest/gtest.h>
#include "storage/store.h"
#include "utils/config.h"
#include "utils/logger.h"
#include <cstdio> // For std::remove (deleting files)

// GTest "Fixtures" let us set up a clean environment before every single test
class StoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Force the tests to use a temporary config and a fake database file
        Config::max_memory = 10;
        Config::aof_file = "test_database.aof";
        std::remove("test_database.aof"); // Ensure we start with a clean slate
    }

    void TearDown() override {
        // Clean up the hard drive after the test finishes
        std::remove("test_database.aof");
    }
};

// TEST 1: The Command Parser
TEST_F(StoreTest, HandlesCaseInsensitiveCommands) {
    KeyValueStore store;
    
    // Send lowercase 'set' and 'get'
    EXPECT_EQ(store.execute_command({"set", "user", "Alice"}), "+OK\r\n");
    EXPECT_EQ(store.execute_command({"gEt", "user"}), "Alice\r\n");
}

// TEST 2: Disk Persistence (The Crash Simulation)
TEST_F(StoreTest, AOFPersistenceSurvivesCrash) {
    // Block 1: Simulate the first server booting up
    {
        KeyValueStore store1;
        store1.execute_command({"SET", "hero", "Batman"});
        EXPECT_EQ(store1.execute_command({"GET", "hero"}), "Batman\r\n");
    } // store1 is destroyed here. The AOF file is saved and closed.

    // Block 2: Simulate a brand new server rebooting after a crash
    {
        KeyValueStore store2; // This constructor should read test_database.aof
        EXPECT_EQ(store2.execute_command({"GET", "hero"}), "Batman\r\n");
    }
}