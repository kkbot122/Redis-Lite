#include <gtest/gtest.h>
#include "storage/store.h"
#include "utils/config.h"
#include "utils/logger.h"
#include <cstdio> // For std::remove

class StoreTest : public ::testing::Test {
protected:
void SetUp() override {
Config::max_memory = 10;
Config::aof_file = "test_database.aof";
std::remove("test_database.aof");
}


void TearDown() override {
    std::remove("test_database.aof");
}


};

// TEST 1: Case-insensitive commands
TEST_F(StoreTest, HandlesCaseInsensitiveCommands) {
KeyValueStore store;


TxState tx;
bool authenticated = true;

EXPECT_EQ(store.execute_command({"set", "user", "Alice"}, tx, authenticated), "+OK\r\n");
EXPECT_EQ(store.execute_command({"gEt", "user"}, tx, authenticated), "$5\r\nAlice\r\n");


}

// TEST 2: AOF Persistence (Crash simulation)
TEST_F(StoreTest, AOFPersistenceSurvivesCrash) {


// Simulate first server instance
{
    KeyValueStore store1;

    TxState tx;
    bool authenticated = true;

    store1.execute_command({"SET", "hero", "Batman"}, tx, authenticated);
    EXPECT_EQ(store1.execute_command({"GET", "hero"}, tx, authenticated), "$6\r\nBatman\r\n");
}

// Simulate restart
{
    KeyValueStore store2;

    TxState tx;
    bool authenticated = true;

    EXPECT_EQ(store2.execute_command({"GET", "hero"}, tx, authenticated), "$6\r\nBatman\r\n");
}


};
