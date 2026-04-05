#include "utils/logger.h"
#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

// Define the static variables
std::ofstream Logger::log_file;
std::mutex Logger::log_mutex;

void Logger::init(const std::string& filename) {
    // Open in Append mode so we don't delete yesterday's logs!
    log_file.open(filename, std::ios::app);
    if (!log_file.is_open()) {
        std::cerr << "CRITICAL FATAL: Could not open log file: " << filename << std::endl;
        exit(1); // If we can't log, we shouldn't start the server.
    }
}

std::string Logger::get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    
    std::stringstream ss;
    // Format: YYYY-MM-DD HH:MM:SS
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");
    return ss.str();
}

std::string Logger::level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
        default:              return "UNKNOWN";
    }
}

void Logger::log(LogLevel level, const std::string& message) {
    // Lock the mutex. If another part of the code is already logging,
    // this will politely wait in line until the other code is done.
    std::lock_guard<std::mutex> lock(log_mutex);

    if (log_file.is_open()) {
        // Construct the enterprise log format
        std::string log_entry = "[" + get_current_timestamp() + "] " 
                              + "[" + level_to_string(level) + "] " 
                              + message + "\n";
        
        // Write to file and flush immediately
        log_file << log_entry;
        log_file.flush(); 

        // We also print to std::cout JUST for our own local debugging convenience.
        // In a pure production mode, we might turn this off.
        std::cout << log_entry; 
    }
}

void Logger::info(const std::string& message) { log(LogLevel::INFO, message); }
void Logger::warn(const std::string& message) { log(LogLevel::WARN, message); }
void Logger::error(const std::string& message) { log(LogLevel::ERROR, message); }