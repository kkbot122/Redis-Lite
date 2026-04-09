#include "storage/store.h"
#include "utils/logger.h"
#include "utils/config.h"
#include <fstream>
#include <sstream>
#include <thread>
#include <cstdio>
#include <cstring>
#include <unistd.h>    
#include <sys/wait.h>
#include <chrono>

// ============================================================
// RDB binary format
// ============================================================
static constexpr uint8_t  RDB_TYPE_STRING = 0;
static constexpr uint8_t  RDB_TYPE_LIST   = 1;
static constexpr uint8_t  RDB_TYPE_SET    = 2;
static constexpr uint8_t  RDB_TYPE_HASH   = 3;
static constexpr uint8_t  RDB_TYPE_ZSET   = 4;
static constexpr uint8_t  RDB_EOF         = 0xFF;
static const     char     RDB_MAGIC[]     = "REDISLITE";
static constexpr uint32_t RDB_VERSION     = 1;

// ---- Binary write helpers ----
static void w8 (std::ofstream& f, uint8_t  v) { f.write(reinterpret_cast<const char*>(&v), 1); }
static void w32(std::ofstream& f, uint32_t v) { f.write(reinterpret_cast<const char*>(&v), 4); }
static void w64(std::ofstream& f, int64_t  v) { f.write(reinterpret_cast<const char*>(&v), 8); }
static void wstr(std::ofstream& f, const std::string& s) {
    w32(f, static_cast<uint32_t>(s.size()));
    f.write(s.data(), s.size());
}
static void wdouble(std::ofstream& f, double v) { f.write(reinterpret_cast<const char*>(&v), 8); }

// ---- Binary read helpers ----
static bool r8 (std::ifstream& f, uint8_t&  v) { return static_cast<bool>(f.read(reinterpret_cast<char*>(&v), 1)); }
static bool r32(std::ifstream& f, uint32_t& v) { return static_cast<bool>(f.read(reinterpret_cast<char*>(&v), 4)); }
static bool r64(std::ifstream& f, int64_t&  v) { return static_cast<bool>(f.read(reinterpret_cast<char*>(&v), 8)); }
static bool rstr(std::ifstream& f, std::string& s) {
    uint32_t len = 0;
    if (!r32(f, len)) return false;
    s.resize(len);
    return static_cast<bool>(f.read(&s[0], len));
}
static bool rdouble(std::ifstream& f, double& v) { return static_cast<bool>(f.read(reinterpret_cast<char*>(&v), 8)); }

// ============================================================
// AOF Persistence
// ============================================================
void KeyValueStore::append_to_aof(const std::vector<std::string>& args) {
    if (loading_from_aof || !aof_file.is_open()) return;
    aof_file << "*" << args.size() << "\r\n";
    for (const auto& a : args)
        aof_file << "$" << a.size() << "\r\n" << a << "\r\n";
    aof_file.flush();

    // FIXED: Buffer raw bytes to the string directly, avoiding massive heap fragmentation
    if (aof_child_pid != -1) {
        aof_rewrite_buffer += "*" + std::to_string(args.size()) + "\r\n";
        for (const auto& a : args) {
            aof_rewrite_buffer += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
        }
    }
}

void KeyValueStore::load_from_aof() {
    std::ifstream file(Config::aof_file);
    if (!file.is_open()) { Logger::info("No AOF — skipping."); return; }
    Logger::info("Replaying AOF…");
    loading_from_aof = true;
    int n = 0; std::string line;
    while (std::getline(file, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty() || line[0] != '*') continue;
        int argc = 0;
        try { argc = std::stoi(line.substr(1)); } catch (...) { continue; }
        std::vector<std::string> args; args.reserve(argc);
        for (int i = 0; i < argc; ++i) {
            if (!std::getline(file, line)) break;
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty() || line[0] != '$') break;
            int len = 0;
            try { len = std::stoi(line.substr(1)); } catch (...) { break; }
            if (!std::getline(file, line)) break;
            if (!line.empty() && line.back() == '\r') line.pop_back();
            args.push_back(line.substr(0, len));
        }
        if (static_cast<int>(args.size()) == argc) { execute_command_locked(args); ++n; }
    }
    loading_from_aof = false;
    Logger::info("AOF replay done — " + std::to_string(n) + " commands.");
}

std::string KeyValueStore::serialize_item_to_resp(const CacheItem& item) const {
    auto rc = [](const std::vector<std::string>& a) {
        std::string s = "*" + std::to_string(a.size()) + "\r\n";
        for (const auto& x : a) s += "$" + std::to_string(x.size()) + "\r\n" + x + "\r\n";
        return s;
    };
    std::string out;
    if (const auto* s = std::get_if<std::string>(&item.value)) {
        out += rc({"SET", item.key, *s});
    } else if (const auto* l = std::get_if<std::list<std::string>>(&item.value)) {
        if (!l->empty()) {
            std::vector<std::string> a = {"RPUSH", item.key};
            for (const auto& v : *l) a.push_back(v);
            out += rc(a);
        }
    } else if (const auto* st = std::get_if<std::unordered_set<std::string>>(&item.value)) {
        if (!st->empty()) {
            std::vector<std::string> a = {"SADD", item.key};
            for (const auto& m : *st) a.push_back(m);
            out += rc(a);
        }
    } else if (const auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item.value)) {
        if (!h->empty()) {
            std::vector<std::string> a = {"HSET", item.key};
            for (const auto& [f,v] : *h) { a.push_back(f); a.push_back(v); }
            out += rc(a);
        }
    } else if (const auto* zs = std::get_if<ZSet>(&item.value)) {
        if (!zs->dict.empty()) {
            std::vector<std::string> a = {"ZADD", item.key};
            for (const auto& [score, member] : zs->tree) {
                std::ostringstream oss;
                oss << score; 
                a.push_back(oss.str());
                a.push_back(member);
            }
            out += rc(a);
        }
    }
    if (item.expires_at > 0)
        out += rc({"PEXPIREAT", item.key, std::to_string(item.expires_at)});
    return out;
}

// ============================================================
// RDB Persistence
// ============================================================
bool KeyValueStore::rdb_save_snapshot(const std::string& path, int64_t now) const {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f.is_open()) return false;

    f.write(RDB_MAGIC, 9);
    w32(f, RDB_VERSION);

    // FIXED: Iterate the cache directly without allocating memory
    cache.for_each([&](const CacheItem& item) {
        if (std::holds_alternative<std::string>(item.value)) {
            w8(f, RDB_TYPE_STRING); w64(f, item.expires_at); wstr(f, item.key); wstr(f, std::get<std::string>(item.value));
        } else if (std::holds_alternative<std::list<std::string>>(item.value)) {
            const auto& l = std::get<std::list<std::string>>(item.value);
            w8(f, RDB_TYPE_LIST); w64(f, item.expires_at); wstr(f, item.key); w32(f, static_cast<uint32_t>(l.size()));
            for (const auto& v : l) wstr(f, v);
        } else if (std::holds_alternative<std::unordered_set<std::string>>(item.value)) {
            const auto& s = std::get<std::unordered_set<std::string>>(item.value);
            w8(f, RDB_TYPE_SET); w64(f, item.expires_at); wstr(f, item.key); w32(f, static_cast<uint32_t>(s.size()));
            for (const auto& m : s) wstr(f, m);
        } else if (std::holds_alternative<std::unordered_map<std::string,std::string>>(item.value)) {
            const auto& h = std::get<std::unordered_map<std::string,std::string>>(item.value);
            w8(f, RDB_TYPE_HASH); w64(f, item.expires_at); wstr(f, item.key); w32(f, static_cast<uint32_t>(h.size()));
            for (const auto& [fld, val] : h) { wstr(f, fld); wstr(f, val); }
        } else if (std::holds_alternative<ZSet>(item.value)) {
            const auto& z = std::get<ZSet>(item.value);
            w8(f, RDB_TYPE_ZSET); w64(f, item.expires_at); wstr(f, item.key); w32(f, static_cast<uint32_t>(z.dict.size()));
            for (const auto& [member, score] : z.dict) { wstr(f, member); wdouble(f, score); }
        }
    });
    
    w8(f, RDB_EOF);
    f.flush();
    return f.good();
}

bool KeyValueStore::rdb_load_snapshot(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return false;

    char magic[9];
    if (!f.read(magic, 9) || std::memcmp(magic, RDB_MAGIC, 9) != 0) {
        Logger::error("RDB: bad magic in " + path); return false;
    }
    uint32_t ver = 0;
    if (!r32(f, ver) || ver != RDB_VERSION) {
        Logger::error("RDB: unsupported version"); return false;
    }
    Logger::info("Loading RDB snapshot: " + path);
    int loaded = 0;

    while (true) {
        uint8_t type = 0;
        if (!r8(f, type)) break;
        if (type == RDB_EOF) break;

        int64_t  expires_at = 0;
        std::string key;
        if (!r64(f, expires_at) || !rstr(f, key)) break;

        int64_t now = get_current_time_ms();
        bool expired = (expires_at > 0 && expires_at <= now);

        if (type == RDB_TYPE_STRING) {
            std::string val; if (!rstr(f, val)) break;
            if (!expired) { cache.put(key, val, expires_at); ++loaded; }
        } else if (type == RDB_TYPE_LIST) {
            uint32_t count = 0; if (!r32(f, count)) break;
            std::list<std::string> l;
            for (uint32_t i = 0; i < count; ++i) {
                std::string v; if (!rstr(f, v)) goto done;
                l.push_back(std::move(v));
            }
            if (!expired) { cache.put(key, l, expires_at); ++loaded; }
        } else if (type == RDB_TYPE_SET) {
            uint32_t count = 0; if (!r32(f, count)) break;
            std::unordered_set<std::string> s;
            for (uint32_t i = 0; i < count; ++i) {
                std::string m; if (!rstr(f, m)) goto done;
                s.insert(std::move(m));
            }
            if (!expired) { cache.put(key, s, expires_at); ++loaded; }
        } else if (type == RDB_TYPE_HASH) {
            uint32_t count = 0; if (!r32(f, count)) break;
            std::unordered_map<std::string,std::string> h;
            for (uint32_t i = 0; i < count; ++i) {
                std::string fld, val; if (!rstr(f, fld) || !rstr(f, val)) goto done;
                h[std::move(fld)] = std::move(val);
            }
            if (!expired) { cache.put(key, h, expires_at); ++loaded; }
        } else if (type == RDB_TYPE_ZSET) {
            uint32_t count = 0; if (!r32(f, count)) break;
            ZSet z;
            for (uint32_t i = 0; i < count; ++i) {
                std::string m; double score = 0;
                if (!rstr(f, m) || !rdouble(f, score)) goto done;
                z.dict[m] = score; z.tree.insert({score, m});
            }
            if (!expired) { cache.put(key, z, expires_at); ++loaded; }
        }
    }

done:
    Logger::info("RDB load done — " + std::to_string(loaded) + " keys.");
    last_save_time.store(get_current_time_ms() / 1000);
    return true;
}

void KeyValueStore::maybe_auto_save() {
    if (Config::rdb_save_seconds <= 0) return;
    if (rdb_save_in_progress.load() || rdb_child_pid != -1) return;
    int64_t now_s = get_current_time_ms() / 1000;
    if (now_s - last_save_time.load() < Config::rdb_save_seconds) return;

    rdb_save_in_progress.store(true);
    
    // OS MAGIC: Fork the process!
    pid_t pid = fork();
    if (pid == 0) {
        // CHILD PROCESS: We now have a perfectly frozen, zero-cost clone of RAM.
        // We do NOT need to lock the mutex or copy data!
        std::string tmp = Config::rdb_file + ".bgsave.tmp";
        rdb_save_snapshot(tmp, get_current_time_ms());
        _exit(0); // Self-destruct silently
    } else if (pid > 0) {
        // PARENT PROCESS: Instantly continue serving users.
        rdb_child_pid = pid;
        Logger::info("Auto-save RDB started in background (Child PID: " + std::to_string(pid) + ")");
    } else {
        rdb_save_in_progress.store(false);
        Logger::error("Auto-save RDB failed to fork.");
    }
}

void KeyValueStore::check_background_tasks() {
    // 1. Check if the RDB Child finished
    if (rdb_child_pid != -1) {
        int stat;
        if (waitpid(rdb_child_pid, &stat, WNOHANG) == rdb_child_pid) {
            rdb_child_pid = -1;
            rdb_save_in_progress.store(false);
            last_save_time.store(get_current_time_ms() / 1000);
            std::string tmp = Config::rdb_file + ".bgsave.tmp";
            std::rename(tmp.c_str(), Config::rdb_file.c_str());
            Logger::info("BGSAVE complete (Forked child finished).");
        }
    }
    
    // 2. Check if the AOF Child finished
    if (aof_child_pid != -1) {
        int stat;
        if (waitpid(aof_child_pid, &stat, WNOHANG) == aof_child_pid) {
            std::string tmp = Config::aof_file + ".rewrite.tmp";
            std::string pending_commands;

            // Phase 1: Lock the parent briefly just to extract the buffer and clear it
            {
                std::unique_lock<std::shared_mutex> lk(mtx);
                pending_commands = std::move(aof_rewrite_buffer);
                aof_rewrite_buffer.clear(); 
            }

            // Phase 2: Perform SLOW Disk I/O completely outside the lock!
            // The main thread can continue processing commands simultaneously.
            std::ofstream f(tmp, std::ios::app | std::ios::out);
            f.write(pending_commands.c_str(), pending_commands.size());
            f.flush(); 
            f.close();
            
            // Phase 3: Lock briefly again just to swap the file handles
            {
                std::unique_lock<std::shared_mutex> lk(mtx);
                if (aof_file.is_open()) aof_file.close();
                std::rename(tmp.c_str(), Config::aof_file.c_str());
                aof_file.open(Config::aof_file, std::ios::app | std::ios::out);
            }

            aof_child_pid = -1;
            rewrite_in_progress.store(false);
            Logger::info("BGREWRITEAOF complete and files swapped.");
        }
    }
}