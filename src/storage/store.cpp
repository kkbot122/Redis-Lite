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
// Internal dispatch
// ============================================================

std::string KeyValueStore::execute_command_locked(const std::vector<std::string>& args) {
    if (args.empty()) return "-ERR Empty command\r\n";
    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
    const int64_t now = get_current_time_ms();

    if (cmd == "PING")
        return args.size() >= 2
            ? "$"+std::to_string(args[1].size())+"\r\n"+args[1]+"\r\n" : "+PONG\r\n";

    if (cmd == "INFO") {
        std::string s = args.size() >= 2 ? args[1] : "";
        for (char& c : s) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
        std::string p = build_info(s);
        return "$"+std::to_string(p.size())+"\r\n"+p+"\r\n";
    }

    // ---------------------------------------- SAVE / BGSAVE / LASTSAVE / BGREWRITEAOF
    if (cmd == "SAVE") {
        auto snap = cache.get_all_items(now);
        if (rdb_save_snapshot(Config::rdb_file, snap)) {
            last_save_time.store(now / 1000);
            return "+OK\r\n";
        }
        return "-ERR RDB save failed\r\n";
    }

    if (cmd == "BGSAVE") {
        if (rdb_save_in_progress.load())
            return "+Background save already in progress\r\n";
        auto snap = cache.get_all_items(now);
        rdb_save_in_progress.store(true);
        std::thread([this, snap = std::move(snap)]() {
            std::string tmp = Config::rdb_file + ".bgsave.tmp";
            if (rdb_save_snapshot(tmp, snap)) {
                std::unique_lock<std::shared_mutex> lk(mtx);
                std::rename(tmp.c_str(), Config::rdb_file.c_str());
                last_save_time.store(get_current_time_ms() / 1000);
                Logger::info("BGSAVE complete.");
            } else { Logger::error("BGSAVE failed."); }
            rdb_save_in_progress.store(false);
        }).detach();
        return "+Background saving started\r\n";
    }

    if (cmd == "LASTSAVE")
        return ":"+std::to_string(last_save_time.load())+"\r\n";

    if (cmd == "BGREWRITEAOF") {
        if (rewrite_in_progress.load())
            return "+Background AOF rewrite already in progress\r\n";
        auto snap = cache.get_all_items(now);
        rewrite_in_progress.store(true);
        std::thread([this, snap = std::move(snap)]() {
            std::string tmp = Config::aof_file + ".rewrite.tmp";
            { std::ofstream f(tmp, std::ios::trunc);
              if (!f.is_open()) { Logger::error("BGREWRITEAOF: cannot open tmp"); rewrite_in_progress.store(false); return; }
              for (const auto& item : snap) f << serialize_item_to_resp(item);
              f.flush(); }
            { std::unique_lock<std::shared_mutex> lk(mtx);
              if (aof_file.is_open()) aof_file.close();
              std::rename(tmp.c_str(), Config::aof_file.c_str());
              aof_file.open(Config::aof_file, std::ios::app | std::ios::out); }
            rewrite_in_progress.store(false);
            Logger::info("BGREWRITEAOF complete.");
        }).detach();
        return "+Background append only file rewriting started\r\n";
    }

    // ---------------------------------------- CONFIG
    if (cmd == "CONFIG" && args.size() >= 3) {
        std::string sub = args[1];
        for (char& c : sub) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
        if (sub == "GET") {
            std::string p = args[2];
            for (char& c : p) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
            std::vector<std::pair<std::string,std::string>> m;
            auto add = [&](const std::string& k, const std::string& v) {
                if (fnmatch(p.c_str(), k.c_str(), 0) == 0) m.push_back({k,v});
            };
            add("port",              std::to_string(Config::port));
            add("maxmemory",         std::to_string(Config::max_memory));
            add("aof_file",          Config::aof_file);
            add("rdb_file",          Config::rdb_file);
            add("requirepass",       Config::requirepass);
            add("slowlog-threshold", std::to_string(Config::slowlog_threshold));
            add("slowlog-max-len",   std::to_string(Config::slowlog_max_len));
            add("rdb_save_seconds",  std::to_string(Config::rdb_save_seconds));
            std::string r = "*"+std::to_string(m.size()*2)+"\r\n";
            for (const auto& [k,v] : m)
                r += "$"+std::to_string(k.size())+"\r\n"+k+"\r\n"
                   + "$"+std::to_string(v.size())+"\r\n"+v+"\r\n";
            return r;
        }
        if (sub == "SET" && args.size() >= 4) {
            std::string p = args[2], v = args[3];
            for (char& c : p) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
            if      (p == "maxmemory")         Config::max_memory        = std::stoull(v);
            else if (p == "requirepass")       Config::requirepass       = v;
            else if (p == "slowlog-threshold") Config::slowlog_threshold = std::stoi(v);
            else if (p == "slowlog-max-len")   Config::slowlog_max_len   = std::stoi(v);
            else if (p == "rdb_save_seconds")  Config::rdb_save_seconds  = std::stoi(v);
            else return "-ERR Unknown config parameter\r\n";
            return "+OK\r\n";
        }
        if (sub == "RESETSTAT") { total_commands.store(0); return "+OK\r\n"; }
    }

    // ---------------------------------------- SLOWLOG
    if (cmd == "SLOWLOG" && args.size() >= 2) {
        std::string sub = args[1];
        for (char& c : sub) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
        std::lock_guard<std::mutex> lk(slowlog_mtx);
        if (sub == "LEN")   return ":"+std::to_string(slowlog.size())+"\r\n";
        if (sub == "RESET") { slowlog.clear(); return "+OK\r\n"; }
        if (sub == "GET") {
            size_t cnt = 128;
            if (args.size() >= 3) try { cnt = std::stoull(args[2]); } catch (...) {}
            cnt = std::min(cnt, slowlog.size());
            std::string r = "*"+std::to_string(cnt)+"\r\n";
            for (size_t i = 0; i < cnt; ++i) {
                const auto& e = slowlog[i];
                r += "*4\r\n:"+std::to_string(e.id)+"\r\n:"+std::to_string(e.timestamp_ms)+
                     "\r\n:"+std::to_string(e.duration_ms)+"\r\n";
                r += "*"+std::to_string(e.args.size())+"\r\n";
                for (const auto& a : e.args) r += "$"+std::to_string(a.size())+"\r\n"+a+"\r\n";
            }
            return r;
        }
    }

    // ---------------------------------------- DBSIZE / FLUSHDB
    if (cmd == "DBSIZE") return ":"+std::to_string(cache.size())+"\r\n";
    if (cmd == "FLUSHDB") { cache = LRUCache(Config::max_memory); append_to_aof(args); return "+OK\r\n"; }

    // ---------------------------------------- DEL / EXISTS / TYPE / RENAME
    if (cmd == "DEL" && args.size() >= 2) {
        int d = 0;
        for (size_t i=1;i<args.size();++i) if(cache.remove(args[i]))++d;
        append_to_aof(args); return ":"+std::to_string(d)+"\r\n";
    }
    if (cmd == "EXISTS" && args.size() >= 2) return cache.exists(args[1],now)?":1\r\n":":0\r\n";
    if (cmd == "TYPE" && args.size() >= 2) {
        CacheItem* item=cache.get_item(args[1],now);
        if (!item) return "+none\r\n";
        if (std::holds_alternative<std::string>(item->value))                                  return "+string\r\n";
        if (std::holds_alternative<std::list<std::string>>(item->value))                       return "+list\r\n";
        if (std::holds_alternative<std::unordered_set<std::string>>(item->value))              return "+set\r\n";
        if (std::holds_alternative<std::unordered_map<std::string,std::string>>(item->value))  return "+hash\r\n";
        if (std::holds_alternative<ZSet>(item->value)) return "+zset\r\n";
    }
    if (cmd == "RENAME" && args.size() >= 3) {
        CacheItem* item=cache.get_item(args[1],now);
        if(!item) return "-ERR no such key\r\n";
        cache.put(args[2],item->value,item->expires_at); cache.remove(args[1]);
        append_to_aof(args); return "+OK\r\n";
    }

    // ---------------------------------------- KEYS / SCAN
    if (cmd == "KEYS" && args.size() >= 2) {
        auto all = cache.get_all_items(now);
        std::string r = ""; int cnt = 0;
        for (const auto& item : all)
            if (fnmatch(args[1].c_str(), item.key.c_str(), 0) == 0) {
                r += "$"+std::to_string(item.key.size())+"\r\n"+item.key+"\r\n"; ++cnt;
            }
        return "*"+std::to_string(cnt)+"\r\n"+r;
    }
    if (cmd == "SCAN" && args.size() >= 2) {
        size_t cursor=0,count=10; std::string pat="*";
        try{cursor=std::stoull(args[1]);}catch(...){return"-ERR invalid cursor\r\n";}
        for (size_t i=2;i+1<args.size();++i) {
            std::string opt=args[i]; for(char&c:opt)c=toupper(c);
            if(opt=="COUNT") try{count=std::stoull(args[i+1]);}catch(...){}
            if(opt=="MATCH") pat=args[i+1];
        }
        auto [next,keys]=cache.scan(cursor,count,now);
        std::vector<std::string> filtered;
        for(const auto& k:keys) if(fnmatch(pat.c_str(),k.c_str(),0)==0) filtered.push_back(k);
        std::string nc=std::to_string(next);
        std::string r="*2\r\n$"+std::to_string(nc.size())+"\r\n"+nc+"\r\n";
        r+="*"+std::to_string(filtered.size())+"\r\n";
        for(const auto& k:filtered) r+="$"+std::to_string(k.size())+"\r\n"+k+"\r\n";
        return r;
    }

    // ---------------------------------------- TTL family
    if (cmd=="TTL"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return":-2\r\n";if(i->expires_at==0)return":-1\r\n";int64_t s=(i->expires_at-now)/1000;return":"+std::to_string(s>0?s:0)+"\r\n";}
    if (cmd=="PTTL"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return":-2\r\n";if(i->expires_at==0)return":-1\r\n";int64_t ms=i->expires_at-now;return":"+std::to_string(ms>0?ms:0)+"\r\n";}
    if (cmd=="EXPIRE"&&args.size()>=3){int64_t s=0;try{s=std::stoll(args[2]);}catch(...){return"-ERR\r\n";}bool ok=cache.set_expiry(args[1],now+s*1000);if(ok)append_to_aof(args);return ok?":1\r\n":":0\r\n";}
    if (cmd=="PEXPIRE"&&args.size()>=3){int64_t ms=0;try{ms=std::stoll(args[2]);}catch(...){return"-ERR\r\n";}bool ok=cache.set_expiry(args[1],now+ms);if(ok)append_to_aof(args);return ok?":1\r\n":":0\r\n";}
    if (cmd=="PEXPIREAT"&&args.size()>=3){int64_t a=0;try{a=std::stoll(args[2]);}catch(...){return"-ERR\r\n";}bool ok=cache.set_expiry(args[1],a);if(ok)append_to_aof(args);return ok?":1\r\n":":0\r\n";}
    if (cmd=="PERSIST"&&args.size()>=2){bool ok=cache.set_expiry(args[1],0);if(ok)append_to_aof(args);return ok?":1\r\n":":0\r\n";}

    // ---------------------------------------- STRING
    if (cmd=="SET"&&args.size()>=3) {
        int64_t exp=0;
        for(size_t i=3;i+1<args.size();++i){
            std::string f=args[i];for(char&c:f)c=toupper(c);
            try{if(f=="EX")exp=now+std::stoll(args[i+1])*1000;if(f=="PX")exp=now+std::stoll(args[i+1]);}catch(...){return"-ERR\r\n";}
        }
        cache.put(args[1],args[2],exp); append_to_aof(args); return "+OK\r\n";
    }
    if (cmd=="GET"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return"$-1\r\n";if(auto*s=std::get_if<std::string>(&i->value))return"$"+std::to_string(s->size())+"\r\n"+*s+"\r\n";return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if (cmd=="APPEND"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);std::string r;if(!i){cache.put(args[1],args[2]);r=args[2];}else{if(auto*s=std::get_if<std::string>(&i->value)){*s+=args[2];r=*s;}else return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}append_to_aof(args);return":"+std::to_string(r.size())+"\r\n";}
    if (cmd=="STRLEN"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return":0\r\n";if(auto*s=std::get_if<std::string>(&i->value))return":"+std::to_string(s->size())+"\r\n";return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if (cmd=="GETSET"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);std::string old="$-1\r\n";if(i){if(auto*s=std::get_if<std::string>(&i->value))old="$"+std::to_string(s->size())+"\r\n"+*s+"\r\n";else return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}cache.put(args[1],args[2]);append_to_aof(args);return old;}

    auto num_op=[&](const std::string&key,int64_t delta)->std::string{
        CacheItem*i=cache.get_item(key,now);int64_t val=0;
        if(i){if(auto*s=std::get_if<std::string>(&i->value)){try{val=std::stoll(*s);}catch(...){return"-ERR value not integer\r\n";}}else return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
        val+=delta;cache.put(key,std::to_string(val));return":"+std::to_string(val)+"\r\n";
    };
    if(cmd=="INCR"  &&args.size()>=2){auto r=num_op(args[1], 1);if(r[0]==':')append_to_aof(args);return r;}
    if(cmd=="DECR"  &&args.size()>=2){auto r=num_op(args[1],-1);if(r[0]==':')append_to_aof(args);return r;}
    if(cmd=="INCRBY"&&args.size()>=3){int64_t d=0;try{d=std::stoll(args[2]);}catch(...){return"-ERR\r\n";}auto r=num_op(args[1],d);if(r[0]==':')append_to_aof(args);return r;}
    if(cmd=="DECRBY"&&args.size()>=3){int64_t d=0;try{d=std::stoll(args[2]);}catch(...){return"-ERR\r\n";}auto r=num_op(args[1],-d);if(r[0]==':')append_to_aof(args);return r;}

    // ---------------------------------------- LIST
    if((cmd=="LPUSH"||cmd=="RPUSH")&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);int nl=0;if(!i){std::list<std::string>l;for(size_t x=2;x<args.size();++x)cmd=="LPUSH"?l.push_front(args[x]):l.push_back(args[x]);nl=l.size();cache.put(args[1],l);}else{if(auto*l=std::get_if<std::list<std::string>>(&i->value)){for(size_t x=2;x<args.size();++x)cmd=="LPUSH"?l->push_front(args[x]):l->push_back(args[x]);nl=l->size();}else return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}append_to_aof(args);return":"+std::to_string(nl)+"\r\n";}
    if((cmd=="LPOP"||cmd=="RPOP")&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return"$-1\r\n";if(auto*l=std::get_if<std::list<std::string>>(&i->value)){if(l->empty())return"$-1\r\n";std::string v=cmd=="LPOP"?l->front():l->back();cmd=="LPOP"?l->pop_front():l->pop_back();append_to_aof(args);return"$"+std::to_string(v.size())+"\r\n"+v+"\r\n";}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="LLEN"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return":0\r\n";if(auto*l=std::get_if<std::list<std::string>>(&i->value))return":"+std::to_string(l->size())+"\r\n";return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="LRANGE"&&args.size()>=4){CacheItem*i=cache.get_item(args[1],now);if(!i)return"*0\r\n";if(auto*l=std::get_if<std::list<std::string>>(&i->value)){int len=l->size(),start=0,stop=0;try{start=std::stoi(args[2]);stop=std::stoi(args[3]);}catch(...){return"-ERR\r\n";}if(start<0)start=std::max(0,len+start);if(stop<0)stop=len+stop;stop=std::min(stop,len-1);if(start>stop||start>=len)return"*0\r\n";std::string r;int cnt=0,idx=0;for(const auto&v:*l){if(idx>stop)break;if(idx>=start){r+="$"+std::to_string(v.size())+"\r\n"+v+"\r\n";++cnt;}++idx;}return"*"+std::to_string(cnt)+"\r\n"+r;}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}

    // ---------------------------------------- SET
    if(cmd=="SADD"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);int a=0;if(!i){std::unordered_set<std::string>s;for(size_t x=2;x<args.size();++x)if(s.insert(args[x]).second)++a;cache.put(args[1],s);}else{if(auto*s=std::get_if<std::unordered_set<std::string>>(&i->value))for(size_t x=2;x<args.size();++x)if(s->insert(args[x]).second)++a;else return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}append_to_aof(args);return":"+std::to_string(a)+"\r\n";}
    if(cmd=="SISMEMBER"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);if(!i)return":0\r\n";if(auto*s=std::get_if<std::unordered_set<std::string>>(&i->value))return s->count(args[2])?":1\r\n":":0\r\n";return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="SMEMBERS"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return"*0\r\n";if(auto*s=std::get_if<std::unordered_set<std::string>>(&i->value)){std::string r="*"+std::to_string(s->size())+"\r\n";for(const auto&m:*s)r+="$"+std::to_string(m.size())+"\r\n"+m+"\r\n";return r;}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="SREM"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);if(!i)return":0\r\n";if(auto*s=std::get_if<std::unordered_set<std::string>>(&i->value)){int rm=0;for(size_t x=2;x<args.size();++x)rm+=s->erase(args[x]);append_to_aof(args);return":"+std::to_string(rm)+"\r\n";}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="SCARD"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return":0\r\n";if(auto*s=std::get_if<std::unordered_set<std::string>>(&i->value))return":"+std::to_string(s->size())+"\r\n";return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}

    // ---------------------------------------- HASH
    if(cmd=="HSET"&&args.size()>=4&&(args.size()%2)==0){CacheItem*i=cache.get_item(args[1],now);int a=0;if(!i){std::unordered_map<std::string,std::string>h;for(size_t x=2;x<args.size();x+=2){if(h.find(args[x])==h.end())++a;h[args[x]]=args[x+1];}cache.put(args[1],h);}else{if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){for(size_t x=2;x<args.size();x+=2){if(h->find(args[x])==h->end())++a;(*h)[args[x]]=args[x+1];}}else return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}append_to_aof(args);return":"+std::to_string(a)+"\r\n";}
    if(cmd=="HSETNX"&&args.size()>=4){CacheItem*i=cache.get_item(args[1],now);if(!i){cache.put(args[1],std::unordered_map<std::string,std::string>{{args[2],args[3]}});append_to_aof(args);return":1\r\n";}if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){if(h->count(args[2]))return":0\r\n";(*h)[args[2]]=args[3];append_to_aof(args);return":1\r\n";}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="HGET"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);if(!i)return"$-1\r\n";if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){auto it=h->find(args[2]);if(it==h->end())return"$-1\r\n";return"$"+std::to_string(it->second.size())+"\r\n"+it->second+"\r\n";}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="HMGET"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);std::string r="*"+std::to_string(args.size()-2)+"\r\n";for(size_t x=2;x<args.size();++x){if(!i){r+="$-1\r\n";continue;}if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){auto it=h->find(args[x]);if(it==h->end())r+="$-1\r\n";else r+="$"+std::to_string(it->second.size())+"\r\n"+it->second+"\r\n";}else return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}return r;}
    if(cmd=="HDEL"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);if(!i)return":0\r\n";if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){int rm=0;for(size_t x=2;x<args.size();++x)rm+=h->erase(args[x]);append_to_aof(args);return":"+std::to_string(rm)+"\r\n";}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="HEXISTS"&&args.size()>=3){CacheItem*i=cache.get_item(args[1],now);if(!i)return":0\r\n";if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value))return h->count(args[2])?":1\r\n":":0\r\n";return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="HLEN"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return":0\r\n";if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value))return":"+std::to_string(h->size())+"\r\n";return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="HKEYS"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return"*0\r\n";if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){std::string r="*"+std::to_string(h->size())+"\r\n";for(const auto&[k,v]:*h)r+="$"+std::to_string(k.size())+"\r\n"+k+"\r\n";return r;}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="HVALS"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return"*0\r\n";if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){std::string r="*"+std::to_string(h->size())+"\r\n";for(const auto&[k,v]:*h)r+="$"+std::to_string(v.size())+"\r\n"+v+"\r\n";return r;}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="HGETALL"&&args.size()>=2){CacheItem*i=cache.get_item(args[1],now);if(!i)return"*0\r\n";if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){std::string r="*"+std::to_string(h->size()*2)+"\r\n";for(const auto&[k,v]:*h){r+="$"+std::to_string(k.size())+"\r\n"+k+"\r\n$"+std::to_string(v.size())+"\r\n"+v+"\r\n";}return r;}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    if(cmd=="HINCRBY"&&args.size()>=4){int64_t d=0;try{d=std::stoll(args[3]);}catch(...){return"-ERR\r\n";}CacheItem*i=cache.get_item(args[1],now);int64_t val=0;if(!i){cache.put(args[1],std::unordered_map<std::string,std::string>{{args[2],std::to_string(d)}});append_to_aof(args);return":"+std::to_string(d)+"\r\n";}if(auto*h=std::get_if<std::unordered_map<std::string,std::string>>(&i->value)){auto it=h->find(args[2]);if(it!=h->end())try{val=std::stoll(it->second);}catch(...){return"-ERR hash value not integer\r\n";}val+=d;(*h)[args[2]]=std::to_string(val);append_to_aof(args);return":"+std::to_string(val)+"\r\n";}return"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";}
    // ================================================================
    // SORTED SET commands
    // ================================================================

    if (cmd == "ZADD" && args.size() >= 4 && (args.size() % 2) == 0) {
        CacheItem* item = cache.get_item(args[1], now);
        int added = 0;

        if (!item) {
            ZSet z;
            for (size_t i = 2; i < args.size(); i += 2) {
                double score = 0;
                try { score = std::stod(args[i]); }
                catch (...) { return "-ERR value is not a valid float\r\n"; }
                z.dict[args[i+1]] = score;
                z.tree.insert({score, args[i+1]});
                added++;
            }
            cache.put(args[1], z);
        } else {
            if (auto* z = std::get_if<ZSet>(&item->value)) {
                for (size_t i = 2; i < args.size(); i += 2) {
                    double score = 0;
                    try { score = std::stod(args[i]); }
                    catch (...) { return "-ERR value is not a valid float\r\n"; }

                    auto it = z->dict.find(args[i+1]);
                    if (it != z->dict.end()) {
                        if (it->second != score) {
                            z->tree.erase({it->second, args[i+1]}); // O(log N)
                            z->tree.insert({score, args[i+1]});     // O(log N)
                            it->second = score;
                        }
                    } else {
                        z->dict[args[i+1]] = score;
                        z->tree.insert({score, args[i+1]});
                        added++;
                    }
                }
            } else return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        append_to_aof(args);
        return ":" + std::to_string(added) + "\r\n";
    }

    if (cmd == "ZRANGE" && args.size() >= 4) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "*0\r\n";

        if (auto* z = std::get_if<ZSet>(&item->value)) {
            int len = z->tree.size();
            int start = 0, stop = 0;
            try { start = std::stoi(args[2]); stop = std::stoi(args[3]); }
            catch (...) { return "-ERR value is not an integer or out of range\r\n"; }

            bool withscores = false;
            if (args.size() >= 5) {
                std::string opt = args[4];
                for (char& c : opt) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
                if (opt == "WITHSCORES") withscores = true;
            }

            if (start < 0) start = std::max(0, len + start);
            if (stop < 0) stop = len + stop;
            stop = std::min(stop, len - 1);

            if (start > stop || start >= len) return "*0\r\n";

            int count = 0;
            std::string resp = "";
            auto it = z->tree.begin();
            std::advance(it, start); 

            for (int i = start; i <= stop && it != z->tree.end(); ++i, ++it) {
                resp += "$" + std::to_string(it->second.size()) + "\r\n" + it->second + "\r\n";
                count++;
                if (withscores) {
                    std::ostringstream oss;
                    oss << it->first; 
                    std::string s_str = oss.str();
                    resp += "$" + std::to_string(s_str.size()) + "\r\n" + s_str + "\r\n";
                    count++;
                }
            }
            return "*" + std::to_string(count) + "\r\n" + resp;
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    return "-ERR Unknown command '"+args[0]+"'\r\n";
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

    if (cmd == "MULTI") {
        if (tx.active) return "-ERR MULTI calls can not be nested\r\n";
        tx.active = true; tx.errored = false; tx.queue.clear(); return "+OK\r\n";
    }
    if (cmd == "DISCARD") {
        if (!tx.active) return "-ERR DISCARD without MULTI\r\n";
        tx.active = false; tx.errored = false; tx.queue.clear(); return "+OK\r\n";
    }
    if (cmd == "EXEC") {
        if (!tx.active) return "-ERR EXEC without MULTI\r\n";
        if (tx.errored) { tx.active=false; tx.queue.clear(); return "-EXECABORT Transaction discarded because of previous errors\r\n"; }
        std::unique_lock<std::shared_mutex> lock(mtx);
        std::string r = "*"+std::to_string(tx.queue.size())+"\r\n";
        for (const auto& qa : tx.queue) r += execute_command_locked(qa);
        tx.active = false; tx.queue.clear(); return r;
    }
    if (tx.active) { tx.queue.push_back(args); return "+QUEUED\r\n"; }

    static const std::unordered_set<std::string> read_only = {
        "GET","EXISTS","TYPE","TTL","PTTL","STRLEN","LRANGE","LLEN",
        "SMEMBERS","SISMEMBER","SCARD","HGET","HMGET","HEXISTS","HLEN",
        "HKEYS","HVALS","HGETALL","DBSIZE","PING","INFO","SCAN","KEYS",
        "SLOWLOG","CONFIG","LASTSAVE"
    };

    int64_t t0 = get_current_time_ms();
    std::string result;
    if (read_only.count(cmd)) { std::shared_lock<std::shared_mutex> lk(mtx); result = execute_command_locked(args); }
    else                      { std::unique_lock<std::shared_mutex> lk(mtx); result = execute_command_locked(args); }
    record_slowlog(args, get_current_time_ms() - t0);
    return result;
}