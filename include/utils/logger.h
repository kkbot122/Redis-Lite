#pragma once
#include <string>
#include <fstream>
#include <mutex>

// The 3 levels of severity in enterprise logging
enum class LogLevel {
    INFO,   // Routine operations (e.g., "Server started")
    WARN,   // Something weird happened, but we survived (e.g., "Memory at 90%")
    ERROR   // Critical failure (e.g., "Could not open AOF file")
};

class Logger {
private:
    static std::ofstream log_file;
    static std::mutex log_mutex; // The "Seatbelt" to prevent scrambled text

    // Helper functions
    static std::string get_current_timestamp();
    static std::string level_to_string(LogLevel level);

public:
    // Initialize the pipeline to the log file
    static void init(const std::string& filename);
    
    // The main logging function
    static void log(LogLevel level, const std::string& message);
    
    // Quick helpers so we can just type Logger::info("Message")
    static void info(const std::string& message);
    static void warn(const std::string& message);
    static void error(const std::string& message);
};