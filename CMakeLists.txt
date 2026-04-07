cmake_minimum_required(VERSION 3.14)
project(RedisLite)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})



# ==========================================
# 1. CORE SERVERS
# ==========================================
add_executable(redis-server 
    src/main.cpp 
    src/network/server.cpp 
    src/storage/store.cpp
    src/storage/commands.cpp
    src/storage/persistence.cpp
    src/storage/lru_cache.cpp
    src/utils/logger.cpp   
    src/utils/config.cpp   
)

target_link_libraries(redis-server OpenSSL::SSL OpenSSL::Crypto)

add_executable(redis-router 
    src/network/router.cpp
    src/network/hash_ring.cpp
    src/utils/logger.cpp   
)

# ==========================================
# 2. AUTOMATED TESTING (Google Test)
# ==========================================
enable_testing()

# Go to GitHub and download Google Test automatically!
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
FetchContent_MakeAvailable(googletest)

# Build our test executable
add_executable(run_tests 
    tests/test_lru.cpp 
    tests/test_store.cpp       
    tests/test_hash_ring.cpp   
    src/storage/lru_cache.cpp 
    src/storage/store.cpp  
    src/storage/commands.cpp    
    src/storage/persistence.cpp
    src/network/hash_ring.cpp  
    src/utils/logger.cpp
    src/utils/config.cpp       
)

# Link the Google Test framework to our test executable
target_link_libraries(run_tests gtest_main)

# Tell CMake this is a test suite
include(GoogleTest)
gtest_discover_tests(run_tests)

# ==========================================
# 3. LOAD TESTING TOOLS
# ==========================================
# Find the multithreading library for Linux
find_package(Threads REQUIRED)

add_executable(stress_tester tests/stress_tester.cpp)
target_link_libraries(stress_tester Threads::Threads)