#include "storage/store.h"
#include "utils/logger.h"
#include "utils/config.h"
#include <chrono>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <thread>
#include <cstdio>
#include <cstring>
#include <fnmatch.h>

// ============================================================
// RDB binary format
//
// Header  : "REDISLITE" (9 bytes) + uint32_t version (4 bytes)
// Per item: uint8_t  type  (0=string 1=list 2=set 3=hash)
//           int64_t  expires_at (ms epoch; 0 = none)
//           uint32_t key_len + key bytes
//           type-specific payload (see below)
// Footer  : uint8_t 0xFF  (EOF marker)
//
// String payload : uint32_t len + bytes
// List payload   : uint32_t count; each element: uint32_t len + bytes
// Set  payload   : uint32_t count; each element: uint32_t len + bytes
// Hash payload   : uint32_t count; each field: uint32_t flen + bytes
//                                              uint32_t vlen + bytes
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

// ---- Binary read helpers (return false on underflow) ----
static bool r8 (std::ifstream& f, uint8_t&  v) { return static_cast<bool>(f.read(reinterpret_cast<char*>(&v), 1)); }
static bool r32(std::ifstream& f, uint32_t& v) { return static_cast<bool>(f.read(reinterpret_cast<char*>(&v), 4)); }
static bool r64(std::ifstream& f, int64_t&  v) { return static_cast<bool>(f.read(reinterpret_cast<char*>(&v), 8)); }
static bool rstr(std::ifstream& f, std::string& s) {
    uint32_t len = 0;
    if (!r32(f, len)) return false;
    s.resize(len);
    return static_cast<bool>(f.read(&s[0], len));
}

static void wdouble(std::ofstream& f, double v) { f.write(reinterpret_cast<const char*>(&v), 8); }
static bool rdouble(std::ifstream& f, double& v) { return static_cast<bool>(f.read(reinterpret_cast<char*>(&v), 8)); }

// ============================================================
// Constructor / Destructor
// ============================================================

KeyValueStore::KeyValueStore() : cache(Config::max_memory) {
    init_commands();
    start_time_ms = get_current_time_ms();
    // Prefer RDB if it exists and is newer than the AOF.
    rdb_load_snapshot(Config::rdb_file);
    load_from_aof();
    aof_file.open(Config::aof_file, std::ios::app | std::ios::out);
    if (!aof_file.is_open())
        Logger::error("Could not open AOF: " + Config::aof_file);
    last_save_time.store(get_current_time_ms() / 1000);
}

KeyValueStore::~KeyValueStore() {
    if (aof_file.is_open()) aof_file.close();
}

// ============================================================
// Private helpers
// ============================================================

int64_t KeyValueStore::get_current_time_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

void KeyValueStore::append_to_aof(const std::vector<std::string>& args) {
    if (loading_from_aof || !aof_file.is_open()) return;
    aof_file << "*" << args.size() << "\r\n";
    for (const auto& a : args)
        aof_file << "$" << a.size() << "\r\n" << a << "\r\n";
    aof_file.flush();
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

// ============================================================
// RDB save
// ============================================================

bool KeyValueStore::rdb_save_snapshot(const std::string& path,
                                       const std::vector<CacheItem>& items) const {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f.is_open()) return false;

    f.write(RDB_MAGIC, 9);
    w32(f, RDB_VERSION);

    for (const auto& item : items) {
        if (std::holds_alternative<std::string>(item.value)) {
            w8(f, RDB_TYPE_STRING);
            w64(f, item.expires_at);
            wstr(f, item.key);
            wstr(f, std::get<std::string>(item.value));

        } else if (std::holds_alternative<std::list<std::string>>(item.value)) {
            const auto& l = std::get<std::list<std::string>>(item.value);
            w8(f, RDB_TYPE_LIST);
            w64(f, item.expires_at);
            wstr(f, item.key);
            w32(f, static_cast<uint32_t>(l.size()));
            for (const auto& v : l) wstr(f, v);

        } else if (std::holds_alternative<std::unordered_set<std::string>>(item.value)) {
            const auto& s = std::get<std::unordered_set<std::string>>(item.value);
            w8(f, RDB_TYPE_SET);
            w64(f, item.expires_at);
            wstr(f, item.key);
            w32(f, static_cast<uint32_t>(s.size()));
            for (const auto& m : s) wstr(f, m);

        } else if (std::holds_alternative<std::unordered_map<std::string,std::string>>(item.value)) {
            const auto& h = std::get<std::unordered_map<std::string,std::string>>(item.value);
            w8(f, RDB_TYPE_HASH);
            w64(f, item.expires_at);
            wstr(f, item.key);
            w32(f, static_cast<uint32_t>(h.size()));
            for (const auto& [fld, val] : h) { wstr(f, fld); wstr(f, val); }
        } else if (std::holds_alternative<ZSet>(item.value)) {
            const auto& z = std::get<ZSet>(item.value);
            w8(f, RDB_TYPE_ZSET);
            w64(f, item.expires_at);
            wstr(f, item.key);
            w32(f, static_cast<uint32_t>(z.dict.size()));
            for (const auto& [member, score] : z.dict) { 
                wstr(f, member); 
                wdouble(f, score); 
            }
        }
    }

    w8(f, RDB_EOF);
    f.flush();
    return f.good();
}

// ============================================================
// RDB load
// ============================================================

bool KeyValueStore::rdb_load_snapshot(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return false;

    char magic[9];
    if (!f.read(magic, 9) || std::memcmp(magic, RDB_MAGIC, 9) != 0) {
        Logger::error("RDB: bad magic in " + path);
        return false;
    }
    uint32_t ver = 0;
    if (!r32(f, ver) || ver != RDB_VERSION) {
        Logger::error("RDB: unsupported version");
        return false;
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
        // Skip already-expired items — don't load them into the cache.
        bool expired = (expires_at > 0 && expires_at <= now);

        if (type == RDB_TYPE_STRING) {
            std::string val;
            if (!rstr(f, val)) break;
            if (!expired) { cache.put(key, val, expires_at); ++loaded; }

        } else if (type == RDB_TYPE_LIST) {
            uint32_t count = 0;
            if (!r32(f, count)) break;
            std::list<std::string> l;
            for (uint32_t i = 0; i < count; ++i) {
                std::string v;
                if (!rstr(f, v)) goto done;
                l.push_back(std::move(v));
            }
            if (!expired) { cache.put(key, l, expires_at); ++loaded; }

        } else if (type == RDB_TYPE_SET) {
            uint32_t count = 0;
            if (!r32(f, count)) break;
            std::unordered_set<std::string> s;
            for (uint32_t i = 0; i < count; ++i) {
                std::string m;
                if (!rstr(f, m)) goto done;
                s.insert(std::move(m));
            }
            if (!expired) { cache.put(key, s, expires_at); ++loaded; }

        } else if (type == RDB_TYPE_HASH) {
            uint32_t count = 0;
            if (!r32(f, count)) break;
            std::unordered_map<std::string,std::string> h;
            for (uint32_t i = 0; i < count; ++i) {
                std::string fld, val;
                if (!rstr(f, fld) || !rstr(f, val)) goto done;
                h[std::move(fld)] = std::move(val);
            }
            if (!expired) { cache.put(key, h, expires_at); ++loaded; }
        } else if (type == RDB_TYPE_ZSET) {
            uint32_t count = 0;
            if (!r32(f, count)) break;
            ZSet z;
            for (uint32_t i = 0; i < count; ++i) {
                std::string m; 
                double score = 0;
                if (!rstr(f, m) || !rdouble(f, score)) goto done;
                z.dict[m] = score;
                z.tree.insert({score, m});
            }
            if (!expired) { cache.put(key, z, expires_at); ++loaded; }
        }
    }

done:
    Logger::info("RDB load done — " + std::to_string(loaded) + " keys.");
    last_save_time.store(get_current_time_ms() / 1000);
    return true;
}

// ============================================================
// Auto-save (called by server event loop)
// ============================================================

void KeyValueStore::maybe_auto_save() {
    if (Config::rdb_save_seconds <= 0) return;
    if (rdb_save_in_progress.load()) return;
    int64_t now_s = get_current_time_ms() / 1000;
    if (now_s - last_save_time.load() < Config::rdb_save_seconds) return;

    auto snapshot = cache.get_all_items(get_current_time_ms());
    rdb_save_in_progress.store(true);

    std::thread([this, snapshot = std::move(snapshot)]() {
        std::string tmp = Config::rdb_file + ".tmp";
        if (rdb_save_snapshot(tmp, snapshot)) {
            std::unique_lock<std::shared_mutex> lk(mtx);
            std::rename(tmp.c_str(), Config::rdb_file.c_str());
            last_save_time.store(get_current_time_ms() / 1000);
            Logger::info("Auto-save RDB complete.");
        } else {
            Logger::error("Auto-save RDB failed.");
        }
        rdb_save_in_progress.store(false);
    }).detach();
}

// ============================================================
// Serialise one item back to RESP (for BGREWRITEAOF)
// ============================================================

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
// INFO
// ============================================================

std::string KeyValueStore::build_info(const std::string& section) const {
    int64_t now     = get_current_time_ms();
    int64_t up_s    = (now - start_time_ms) / 1000;
    size_t  nkeys   = cache.size();
    auto    all     = cache.get_all_items(now);
    size_t  expires = 0;
    for (const auto& i : all) if (i.expires_at > 0) ++expires;

    auto sec = [](const std::string& n) { return "# " + n + "\r\n"; };
    auto kv  = [](const std::string& k, int64_t v) { return k+":"+std::to_string(v)+"\r\n"; };
    auto kvs = [](const std::string& k, const std::string& v) { return k+":"+v+"\r\n"; };

    bool all_sec = section.empty() || section == "all" || section == "everything";
    std::string info;

    if (all_sec || section == "server") {
        info += sec("Server");
        info += kvs("redis_version",    "7.0.0-lite");
        info += kv ("uptime_in_seconds", up_s);
        info += kv ("uptime_in_days",    up_s / 86400);
        info += "\r\n";
    }
    if (all_sec || section == "persistence") {
        info += sec("Persistence");
        info += kv ("rdb_last_save_time",        last_save_time.load());
        info += kvs("rdb_bgsave_in_progress",    rdb_save_in_progress.load() ? "1" : "0");
        info += kvs("aof_rewrite_in_progress",   rewrite_in_progress.load()  ? "1" : "0");
        info += "\r\n";
    }
    if (all_sec || section == "memory") {
        info += sec("Memory");
        info += kv ("used_memory",       cache.memory_usage()); // Shows exact bytes!
        info += kv ("maxmemory",         Config::max_memory);
        info += kv ("used_memory_keys", nkeys);
        info += kv ("maxmemory_keys",   Config::max_memory);
        info += kvs("maxmemory_policy", "allkeys-lru");
        info += "\r\n";
    }
    if (all_sec || section == "stats") {
        info += sec("Stats");
        info += kv ("total_commands_processed", total_commands.load());
        info += "\r\n";
    }
    if (all_sec || section == "replication") {
        info += sec("Replication");
        info += kvs("role",            "master");
        info += kv ("connected_slaves", 0);
        info += "\r\n";
    }
    if (all_sec || section == "keyspace") {
        info += sec("Keyspace");
        if (nkeys > 0)
            info += "db0:keys=" + std::to_string(nkeys) +
                    ",expires=" + std::to_string(expires) + "\r\n";
        info += "\r\n";
    }
    return info;
}

// ============================================================
// Slowlog
// ============================================================

void KeyValueStore::record_slowlog(const std::vector<std::string>& args, int64_t dur) {
    if (Config::slowlog_threshold < 0 || dur < Config::slowlog_threshold) return;
    SlowlogEntry e; e.id = slowlog_id.fetch_add(1);
    e.timestamp_ms = get_current_time_ms(); e.duration_ms = dur; e.args = args;
    std::lock_guard<std::mutex> lk(slowlog_mtx);
    slowlog.push_front(std::move(e));
    while (static_cast<int>(slowlog.size()) > Config::slowlog_max_len) slowlog.pop_back();
}

// ============================================================
// Internal Command Routing (O(1) Dispatcher)
// ============================================================
std::string KeyValueStore::execute_command_locked(const std::vector<std::string>& args) {
    if (args.empty()) return "-ERR Empty command\r\n";
    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
    
    auto it = command_registry.find(cmd);
    if (it != command_registry.end()) {
        return it->second(args, get_current_time_ms()); 
    }
    return "-ERR Unknown command '" + args[0] + "'\r\n";
}

// NEW: Helper to identify which keys were modified so we can bump their version
void KeyValueStore::track_mutations(const std::vector<std::string>& args) {
    if (args.empty()) return;
    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    static const std::unordered_set<std::string> write_cmds = {
        "SET","SETEX","GETSET","APPEND","DEL","RENAME",
        "EXPIRE","PEXPIRE","PEXPIREAT","PERSIST",
        "INCR","INCRBY","DECR","DECRBY",
        "LPUSH","RPUSH","LPOP","RPOP",
        "SADD","SREM","HSET","HDEL","HINCRBY","HINCRBYFLOAT","HSETNX",
        "ZADD", "FLUSHDB", "BGREWRITEAOF"
    };

    if (!write_cmds.count(cmd)) return;

    if (cmd == "FLUSHDB") {
        global_flush_version++; 
    } else if (cmd == "DEL") {
        for (size_t i = 1; i < args.size(); ++i) key_versions[args[i]]++;
    } else if (cmd == "RENAME" && args.size() >= 3) {
        key_versions[args[1]]++; cache.recalculate_size(args[1]);
        key_versions[args[2]]++; cache.recalculate_size(args[2]);
    } else if (args.size() > 1) {
        key_versions[args[1]]++; 
        cache.recalculate_size(args[1]); // NEW: Update the memory footprint!
    }
}

// ============================================================
// Public thread-safe entry point
// ============================================================

std::string KeyValueStore::execute_command(const std::vector<std::string>& args,
                                            TxState& tx, bool& authenticated) {
    if (args.empty()) return "-ERR Empty command\r\n";
    ++total_commands;

    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    if (cmd == "AUTH") {
        if (args.size() < 2) return "-ERR wrong number of arguments\r\n";
        if (Config::requirepass.empty()) { authenticated = true; return "-ERR no password set\r\n"; }
        if (args[1] == Config::requirepass) { authenticated = true; return "+OK\r\n"; }
        return "-WRONGPASS invalid username-password pair\r\n";
    }
    if (!Config::requirepass.empty() && !authenticated)
        return "-NOAUTH Authentication required\r\n";

    // ============================================================
    // OPTIMISTIC LOCKING: WATCH / UNWATCH
    // ============================================================
    if (cmd == "WATCH") {
        if (tx.active) return "-ERR WATCH inside MULTI is not allowed\r\n";
        if (args.size() < 2) return "-ERR wrong number of arguments for 'watch' command\r\n";
        
        std::shared_lock<std::shared_mutex> lock(mtx);
        for (size_t i = 1; i < args.size(); ++i) {
            // Record the exact version of the key at the time of WATCH
            tx.watched_keys[args[i]] = key_versions[args[i]] + global_flush_version.load();
        }
        return "+OK\r\n";
    }
    if (cmd == "UNWATCH") {
        tx.watched_keys.clear();
        return "+OK\r\n";
    }

    // ============================================================
    // TRANSACTION QUEUE
    // ============================================================
    if (cmd == "MULTI") {
        if (tx.active) return "-ERR MULTI calls can not be nested\r\n";
        tx.active = true; tx.errored = false; tx.queue.clear(); return "+OK\r\n";
    }
    if (cmd == "DISCARD") {
        if (!tx.active) return "-ERR DISCARD without MULTI\r\n";
        tx.active = false; tx.errored = false; tx.queue.clear(); tx.watched_keys.clear(); return "+OK\r\n";
    }
    if (cmd == "EXEC") {
        if (!tx.active) return "-ERR EXEC without MULTI\r\n";
        if (tx.errored) { 
            tx.active=false; tx.queue.clear(); tx.watched_keys.clear(); 
            return "-EXECABORT Transaction discarded because of previous errors\r\n"; 
        }
        
        std::unique_lock<std::shared_mutex> lock(mtx);

        // 1. Verify all watched keys haven't been modified by another client
        for (const auto& [k, expected_version] : tx.watched_keys) {
            uint64_t current_version = key_versions[k] + global_flush_version.load();
            if (current_version != expected_version) {
                // ABORT! Another client touched this key.
                tx.active = false; tx.queue.clear(); tx.watched_keys.clear();
                return "*-1\r\n"; // RESP Null Array indicating aborted transaction
            }
        }

        // 2. Safely execute the queued commands atomically
        std::string r = "*"+std::to_string(tx.queue.size())+"\r\n";
        for (const auto& qa : tx.queue) {
            std::string res = execute_command_locked(qa);
            r += res;
            if (res[0] != '-') track_mutations(qa); // Increment version on success
        }
        
        tx.active = false; tx.queue.clear(); tx.watched_keys.clear(); return r;
    }
    
    if (tx.active) { tx.queue.push_back(args); return "+QUEUED\r\n"; }

    // ============================================================
    // STANDARD EXECUTION
    // ============================================================
    static const std::unordered_set<std::string> read_only = {
        "GET","EXISTS","TYPE","TTL","PTTL","STRLEN","LRANGE","LLEN",
        "SMEMBERS","SISMEMBER","SCARD","HGET","HMGET","HEXISTS","HLEN",
        "HKEYS","HVALS","HGETALL","DBSIZE","PING","INFO","SCAN","KEYS",
        "SLOWLOG","CONFIG","LASTSAVE", "ZRANGE"
    };

    int64_t t0 = get_current_time_ms();
    std::string result;
    
    if (read_only.count(cmd)) { 
        std::shared_lock<std::shared_mutex> lk(mtx); 
        result = execute_command_locked(args); 
    } else { 
        std::unique_lock<std::shared_mutex> lk(mtx); 
        result = execute_command_locked(args); 
        if (result[0] != '-') track_mutations(args); // Increment version on success
    }
    
    record_slowlog(args, get_current_time_ms() - t0);
    return result;
}