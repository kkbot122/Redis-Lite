#include "storage/store.h"
#include "utils/logger.h"
#include "utils/config.h"
#include <chrono>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <stdexcept>
#include <thread>
#include <cstdio>   // std::rename, std::remove

// ============================================================
// Constructor / Destructor
// ============================================================

KeyValueStore::KeyValueStore() : cache(Config::max_memory) {
    start_time_ms = get_current_time_ms();
    load_from_aof();
    aof_file.open(Config::aof_file, std::ios::app | std::ios::out);
    if (!aof_file.is_open())
        Logger::error("Could not open AOF file: " + Config::aof_file);
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
    if (!file.is_open()) {
        Logger::info("No AOF file — starting fresh.");
        return;
    }
    Logger::info("Replaying AOF…");
    loading_from_aof = true;
    int restored = 0;
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty() || line[0] != '*') continue;
        int argc = 0;
        try { argc = std::stoi(line.substr(1)); } catch (...) { continue; }
        std::vector<std::string> args;
        args.reserve(argc);
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
        if (static_cast<int>(args.size()) == argc) {
            execute_command_locked(args);
            ++restored;
        }
    }
    loading_from_aof = false;
    Logger::info("AOF replay done — " + std::to_string(restored) + " commands.");
}

// Converts one CacheItem back into the RESP command(s) needed to recreate it.
// Used exclusively by BGREWRITEAOF.
std::string KeyValueStore::serialize_item_to_resp(const CacheItem& item) const {
    std::string out;

    auto resp_cmd = [](const std::vector<std::string>& args) {
        std::string s = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto& a : args)
            s += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
        return s;
    };

    if (const auto* s = std::get_if<std::string>(&item.value)) {
        out += resp_cmd({"SET", item.key, *s});
    } else if (const auto* l = std::get_if<std::list<std::string>>(&item.value)) {
        if (!l->empty()) {
            std::vector<std::string> args = {"RPUSH", item.key};
            for (const auto& v : *l) args.push_back(v);
            out += resp_cmd(args);
        }
    } else if (const auto* st = std::get_if<std::unordered_set<std::string>>(&item.value)) {
        if (!st->empty()) {
            std::vector<std::string> args = {"SADD", item.key};
            for (const auto& m : *st) args.push_back(m);
            out += resp_cmd(args);
        }
    } else if (const auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item.value)) {
        if (!h->empty()) {
            std::vector<std::string> args = {"HSET", item.key};
            for (const auto& [f, v] : *h) { args.push_back(f); args.push_back(v); }
            out += resp_cmd(args);
        }
    }

    // Append a PEXPIREAT if this item has a TTL.
    if (item.expires_at > 0) {
        out += resp_cmd({"PEXPIREAT", item.key, std::to_string(item.expires_at)});
    }

    return out;
}

// ============================================================
// INFO builder
// ============================================================

std::string KeyValueStore::build_info(const std::string& section) const {
    int64_t now       = get_current_time_ms();
    int64_t uptime_s  = (now - start_time_ms) / 1000;
    int64_t uptime_d  = uptime_s / 86400;

    // Count live keys and keys with expiry (approximate — no eviction here).
    size_t total_keys   = cache.size();
    auto   all_items    = cache.get_all_items(now);
    size_t expires_keys = 0;
    for (const auto& item : all_items)
        if (item.expires_at > 0) ++expires_keys;

    auto sec = [](const std::string& name) { return "# " + name + "\r\n"; };
    auto kv  = [](const std::string& k, const auto& v) {
        return k + ":" + std::to_string(v) + "\r\n";
    };
    auto kvs = [](const std::string& k, const std::string& v) {
        return k + ":" + v + "\r\n";
    };

    bool all = (section.empty() || section == "all" || section == "everything");

    std::string info;

    if (all || section == "server") {
        info += sec("Server");
        info += kvs("redis_version",         "7.0.0-lite");
        info += kvs("redis_mode",            "standalone");
        info += kvs("os",                    "Linux");
        info += kv ("uptime_in_seconds",     uptime_s);
        info += kv ("uptime_in_days",        uptime_d);
        info += kvs("executable",            "redis-server");
        info += kvs("config_file",           Config::aof_file);
        info += "\r\n";
    }

    if (all || section == "memory") {
        info += sec("Memory");
        info += kv ("used_memory_keys",  total_keys);
        info += kv ("maxmemory_keys",    Config::max_memory);
        info += kvs("maxmemory_policy",  "allkeys-lru");
        info += "\r\n";
    }

    if (all || section == "stats") {
        info += sec("Stats");
        info += kv ("total_commands_processed", total_commands.load());
        info += kvs("aof_rewrite_in_progress",
                    rewrite_in_progress.load() ? "1" : "0");
        info += "\r\n";
    }

    if (all || section == "replication") {
        info += sec("Replication");
        info += kvs("role",              "master");
        info += kv ("connected_slaves",  0);
        info += "\r\n";
    }

    if (all || section == "keyspace") {
        info += sec("Keyspace");
        if (total_keys > 0) {
            info += "db0:keys=" + std::to_string(total_keys)
                  + ",expires=" + std::to_string(expires_keys)
                  + "\r\n";
        }
        info += "\r\n";
    }

    return info;
}

// ============================================================
// Internal command dispatch (caller holds an appropriate lock)
// ============================================================

std::string KeyValueStore::execute_command_locked(const std::vector<std::string>& args) {
    if (args.empty()) return "-ERR Empty command\r\n";

    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    const int64_t now = get_current_time_ms();

    // ------------------------------------------------------------------ PING
    if (cmd == "PING")
        return args.size() >= 2
            ? "$" + std::to_string(args[1].size()) + "\r\n" + args[1] + "\r\n"
            : "+PONG\r\n";

    // ------------------------------------------------------------------ INFO
    if (cmd == "INFO") {
        std::string section = (args.size() >= 2) ? args[1] : "";
        for (char& c : section) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
        std::string payload = build_info(section);
        return "$" + std::to_string(payload.size()) + "\r\n" + payload + "\r\n";
    }

    // ---------------------------------------------------------------- DBSIZE
    if (cmd == "DBSIZE")
        return ":" + std::to_string(cache.size()) + "\r\n";

    // ---------------------------------------------------------------- FLUSHDB
    if (cmd == "FLUSHDB") {
        cache = LRUCache(Config::max_memory);
        append_to_aof(args);
        return "+OK\r\n";
    }

    // ---------------------------------------------------------------- DEL
    if (cmd == "DEL" && args.size() >= 2) {
        int deleted = 0;
        for (size_t i = 1; i < args.size(); ++i)
            if (cache.remove(args[i])) ++deleted;
        append_to_aof(args);
        return ":" + std::to_string(deleted) + "\r\n";
    }

    // --------------------------------------------------------------- EXISTS
    if (cmd == "EXISTS" && args.size() >= 2)
        return cache.exists(args[1], now) ? ":1\r\n" : ":0\r\n";

    // ---------------------------------------------------------------- TYPE
    if (cmd == "TYPE" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "+none\r\n";
        if (std::holds_alternative<std::string>(item->value))                              return "+string\r\n";
        if (std::holds_alternative<std::list<std::string>>(item->value))                   return "+list\r\n";
        if (std::holds_alternative<std::unordered_set<std::string>>(item->value))          return "+set\r\n";
        if (std::holds_alternative<std::unordered_map<std::string,std::string>>(item->value)) return "+hash\r\n";
        return "+unknown\r\n";
    }

    // -------------------------------------------------------------- RENAME
    if (cmd == "RENAME" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "-ERR no such key\r\n";
        cache.put(args[2], item->value, item->expires_at);
        cache.remove(args[1]);
        append_to_aof(args);
        return "+OK\r\n";
    }

    // ---------------------------------------------------------------- SCAN
    if (cmd == "SCAN" && args.size() >= 2) {
        size_t cursor = 0;
        try { cursor = std::stoull(args[1]); } catch (...) {
            return "-ERR invalid cursor\r\n";
        }
        size_t count = 10;
        for (size_t i = 2; i + 1 < args.size(); ++i) {
            std::string opt = args[i];
            for (char& c : opt) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
            if (opt == "COUNT") {
                try { count = std::stoull(args[i + 1]); } catch (...) {}
            }
        }
        auto [next, keys] = cache.scan(cursor, count, now);
        std::string resp = "*2\r\n$" + std::to_string(std::to_string(next).size())
                         + "\r\n" + std::to_string(next) + "\r\n";
        resp += "*" + std::to_string(keys.size()) + "\r\n";
        for (const auto& k : keys)
            resp += "$" + std::to_string(k.size()) + "\r\n" + k + "\r\n";
        return resp;
    }

    // ------------------------------------------------------------ TTL / PTTL
    if (cmd == "TTL" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":-2\r\n";
        if (item->expires_at == 0) return ":-1\r\n";
        int64_t s = (item->expires_at - now) / 1000;
        return ":" + std::to_string(s > 0 ? s : 0) + "\r\n";
    }
    if (cmd == "PTTL" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":-2\r\n";
        if (item->expires_at == 0) return ":-1\r\n";
        int64_t ms = item->expires_at - now;
        return ":" + std::to_string(ms > 0 ? ms : 0) + "\r\n";
    }
    if (cmd == "EXPIRE" && args.size() >= 3) {
        int64_t s = 0;
        try { s = std::stoll(args[2]); } catch (...) { return "-ERR value not integer\r\n"; }
        bool ok = cache.set_expiry(args[1], now + s * 1000);
        if (ok) append_to_aof(args);
        return ok ? ":1\r\n" : ":0\r\n";
    }
    if (cmd == "PEXPIRE" && args.size() >= 3) {
        int64_t ms = 0;
        try { ms = std::stoll(args[2]); } catch (...) { return "-ERR value not integer\r\n"; }
        bool ok = cache.set_expiry(args[1], now + ms);
        if (ok) append_to_aof(args);
        return ok ? ":1\r\n" : ":0\r\n";
    }
    if (cmd == "PEXPIREAT" && args.size() >= 3) {
        int64_t abs_ms = 0;
        try { abs_ms = std::stoll(args[2]); } catch (...) { return "-ERR value not integer\r\n"; }
        bool ok = cache.set_expiry(args[1], abs_ms);
        if (ok) append_to_aof(args);
        return ok ? ":1\r\n" : ":0\r\n";
    }
    if (cmd == "PERSIST" && args.size() >= 2) {
        bool ok = cache.set_expiry(args[1], 0);
        if (ok) append_to_aof(args);
        return ok ? ":1\r\n" : ":0\r\n";
    }

    // ------------------------------------------------------- BGREWRITEAOF
    if (cmd == "BGREWRITEAOF") {
        if (rewrite_in_progress.load())
            return "+Background AOF rewrite already in progress\r\n";

        // Snapshot under shared lock (we're already under shared lock here
        // because execute_command() routed us here — safe to call get_all_items).
        auto snapshot = cache.get_all_items(now);
        rewrite_in_progress.store(true);

        std::thread([this, snapshot = std::move(snapshot)]() {
            std::string tmp_path = Config::aof_file + ".rewrite.tmp";
            {
                std::ofstream tmp(tmp_path, std::ios::trunc);
                if (!tmp.is_open()) {
                    Logger::error("BGREWRITEAOF: failed to open temp file");
                    rewrite_in_progress.store(false);
                    return;
                }
                for (const auto& item : snapshot)
                    tmp << serialize_item_to_resp(item);
                tmp.flush();
            }

            // Swap the file under an exclusive lock so no writer races us.
            {
                std::unique_lock<std::shared_mutex> lock(mtx);
                if (aof_file.is_open()) aof_file.close();
                if (std::rename(tmp_path.c_str(), Config::aof_file.c_str()) != 0) {
                    Logger::error("BGREWRITEAOF: rename failed");
                    rewrite_in_progress.store(false);
                    return;
                }
                aof_file.open(Config::aof_file, std::ios::app | std::ios::out);
            }

            rewrite_in_progress.store(false);
            Logger::info("BGREWRITEAOF complete — AOF compacted.");
        }).detach();

        return "+Background append only file rewriting started\r\n";
    }

    // ================================================================
    // STRING commands
    // ================================================================

    if (cmd == "SET" && args.size() >= 3) {
        int64_t expires_at = 0;
        for (size_t i = 3; i + 1 < args.size(); ++i) {
            std::string flag = args[i];
            for (char& c : flag) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
            try {
                if (flag == "EX")  expires_at = now + std::stoll(args[i+1]) * 1000;
                if (flag == "PX")  expires_at = now + std::stoll(args[i+1]);
            } catch (...) { return "-ERR value not integer\r\n"; }
        }
        cache.put(args[1], args[2], expires_at);
        append_to_aof(args);
        return "+OK\r\n";
    }
    if (cmd == "GET" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "$-1\r\n";
        if (auto* s = std::get_if<std::string>(&item->value))
            return "$" + std::to_string(s->size()) + "\r\n" + *s + "\r\n";
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    if (cmd == "GETSET" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        std::string old_resp = "$-1\r\n";
        if (item) {
            if (auto* s = std::get_if<std::string>(&item->value))
                old_resp = "$" + std::to_string(s->size()) + "\r\n" + *s + "\r\n";
            else return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        cache.put(args[1], args[2]);
        append_to_aof(args);
        return old_resp;
    }
    if (cmd == "APPEND" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        std::string result;
        if (!item) { cache.put(args[1], args[2]); result = args[2]; }
        else {
            if (auto* s = std::get_if<std::string>(&item->value)) { *s += args[2]; result = *s; }
            else return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        append_to_aof(args);
        return ":" + std::to_string(result.size()) + "\r\n";
    }
    if (cmd == "STRLEN" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":0\r\n";
        if (auto* s = std::get_if<std::string>(&item->value))
            return ":" + std::to_string(s->size()) + "\r\n";
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // Numeric helper for INCR/DECR family.
    auto numeric_op = [&](const std::string& key, int64_t delta) -> std::string {
        CacheItem* item = cache.get_item(key, now);
        int64_t val = 0;
        if (item) {
            if (auto* s = std::get_if<std::string>(&item->value)) {
                try { val = std::stoll(*s); }
                catch (...) { return "-ERR value is not an integer or out of range\r\n"; }
            } else return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        val += delta;
        cache.put(key, std::to_string(val));
        return ":" + std::to_string(val) + "\r\n";
    };

    if (cmd == "INCR"   && args.size() >= 2) { auto r = numeric_op(args[1],  1); if (r[0]==':') append_to_aof(args); return r; }
    if (cmd == "DECR"   && args.size() >= 2) { auto r = numeric_op(args[1], -1); if (r[0]==':') append_to_aof(args); return r; }
    if (cmd == "INCRBY" && args.size() >= 3) {
        int64_t d=0; try{d=std::stoll(args[2]);}catch(...){return"-ERR value not integer\r\n";}
        auto r=numeric_op(args[1],d); if(r[0]==':')append_to_aof(args); return r;
    }
    if (cmd == "DECRBY" && args.size() >= 3) {
        int64_t d=0; try{d=std::stoll(args[2]);}catch(...){return"-ERR value not integer\r\n";}
        auto r=numeric_op(args[1],-d); if(r[0]==':')append_to_aof(args); return r;
    }

    // ================================================================
    // LIST commands
    // ================================================================

    if ((cmd == "LPUSH" || cmd == "RPUSH") && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        int new_len = 0;
        if (!item) {
            std::list<std::string> nl;
            for (size_t i=2; i<args.size(); ++i)
                cmd=="LPUSH" ? nl.push_front(args[i]) : nl.push_back(args[i]);
            new_len = nl.size();
            cache.put(args[1], nl);
        } else {
            if (auto* l = std::get_if<std::list<std::string>>(&item->value)) {
                for (size_t i=2; i<args.size(); ++i)
                    cmd=="LPUSH" ? l->push_front(args[i]) : l->push_back(args[i]);
                new_len = l->size();
            } else return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        append_to_aof(args);
        return ":" + std::to_string(new_len) + "\r\n";
    }
    if ((cmd=="LPOP"||cmd=="RPOP") && args.size()>=2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "$-1\r\n";
        if (auto* l = std::get_if<std::list<std::string>>(&item->value)) {
            if (l->empty()) return "$-1\r\n";
            std::string val = cmd=="LPOP" ? l->front() : l->back();
            cmd=="LPOP" ? l->pop_front() : l->pop_back();
            append_to_aof(args);
            return "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    if (cmd == "LLEN" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":0\r\n";
        if (auto* l = std::get_if<std::list<std::string>>(&item->value))
            return ":" + std::to_string(l->size()) + "\r\n";
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    if (cmd == "LRANGE" && args.size() >= 4) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "*0\r\n";
        if (auto* l = std::get_if<std::list<std::string>>(&item->value)) {
            int len = l->size(), start = 0, stop = 0;
            try { start=std::stoi(args[2]); stop=std::stoi(args[3]); }
            catch(...) { return "-ERR value not integer\r\n"; }
            if (start < 0) start = std::max(0, len + start);
            if (stop  < 0) stop  = len + stop;
            stop = std::min(stop, len - 1);
            if (start > stop || start >= len) return "*0\r\n";
            std::string resp; int count = 0, idx = 0;
            for (const auto& v : *l) {
                if (idx > stop) break;
                if (idx >= start) { resp += "$"+std::to_string(v.size())+"\r\n"+v+"\r\n"; ++count; }
                ++idx;
            }
            return "*" + std::to_string(count) + "\r\n" + resp;
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // ================================================================
    // SET commands
    // ================================================================

    if (cmd == "SADD" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        int added = 0;
        if (!item) {
            std::unordered_set<std::string> ns;
            for (size_t i=2; i<args.size(); ++i) if (ns.insert(args[i]).second) ++added;
            cache.put(args[1], ns);
        } else {
            if (auto* s = std::get_if<std::unordered_set<std::string>>(&item->value))
                for (size_t i=2; i<args.size(); ++i) if (s->insert(args[i]).second) ++added;
            else return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        append_to_aof(args);
        return ":" + std::to_string(added) + "\r\n";
    }
    if (cmd == "SISMEMBER" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":0\r\n";
        if (auto* s = std::get_if<std::unordered_set<std::string>>(&item->value))
            return s->count(args[2]) ? ":1\r\n" : ":0\r\n";
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    if (cmd == "SMEMBERS" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "*0\r\n";
        if (auto* s = std::get_if<std::unordered_set<std::string>>(&item->value)) {
            std::string resp = "*" + std::to_string(s->size()) + "\r\n";
            for (const auto& m : *s) resp += "$"+std::to_string(m.size())+"\r\n"+m+"\r\n";
            return resp;
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    if (cmd == "SREM" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":0\r\n";
        if (auto* s = std::get_if<std::unordered_set<std::string>>(&item->value)) {
            int rm=0; for (size_t i=2;i<args.size();++i) rm+=s->erase(args[i]);
            append_to_aof(args);
            return ":" + std::to_string(rm) + "\r\n";
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }
    if (cmd == "SCARD" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":0\r\n";
        if (auto* s = std::get_if<std::unordered_set<std::string>>(&item->value))
            return ":" + std::to_string(s->size()) + "\r\n";
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // ================================================================
    // HASH commands
    // ================================================================

    // HSET key field value [field value ...]
    if (cmd == "HSET" && args.size() >= 4 && (args.size() % 2) == 0) {
        CacheItem* item = cache.get_item(args[1], now);
        int added = 0;
        if (!item) {
            std::unordered_map<std::string,std::string> nh;
            for (size_t i = 2; i < args.size(); i += 2) {
                if (nh.find(args[i]) == nh.end()) ++added;
                nh[args[i]] = args[i+1];
            }
            cache.put(args[1], nh);
        } else {
            if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
                for (size_t i = 2; i < args.size(); i += 2) {
                    if (h->find(args[i]) == h->end()) ++added;
                    (*h)[args[i]] = args[i+1];
                }
            } else return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        append_to_aof(args);
        return ":" + std::to_string(added) + "\r\n";
    }

    // HSETNX key field value
    if (cmd == "HSETNX" && args.size() >= 4) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) {
            cache.put(args[1], std::unordered_map<std::string,std::string>{{args[2], args[3]}});
            append_to_aof(args);
            return ":1\r\n";
        }
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
            if (h->count(args[2])) return ":0\r\n";
            (*h)[args[2]] = args[3];
            append_to_aof(args);
            return ":1\r\n";
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HGET key field
    if (cmd == "HGET" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "$-1\r\n";
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
            auto it = h->find(args[2]);
            if (it == h->end()) return "$-1\r\n";
            return "$" + std::to_string(it->second.size()) + "\r\n" + it->second + "\r\n";
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HMGET key field [field ...]
    if (cmd == "HMGET" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        std::string resp = "*" + std::to_string(args.size() - 2) + "\r\n";
        for (size_t i = 2; i < args.size(); ++i) {
            if (!item) { resp += "$-1\r\n"; continue; }
            if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
                auto it = h->find(args[i]);
                if (it == h->end()) resp += "$-1\r\n";
                else resp += "$" + std::to_string(it->second.size()) + "\r\n" + it->second + "\r\n";
            } else return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        return resp;
    }

    // HDEL key field [field ...]
    if (cmd == "HDEL" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":0\r\n";
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
            int rm = 0;
            for (size_t i = 2; i < args.size(); ++i) rm += h->erase(args[i]);
            append_to_aof(args);
            return ":" + std::to_string(rm) + "\r\n";
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HEXISTS key field
    if (cmd == "HEXISTS" && args.size() >= 3) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":0\r\n";
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value))
            return h->count(args[2]) ? ":1\r\n" : ":0\r\n";
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HLEN key
    if (cmd == "HLEN" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return ":0\r\n";
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value))
            return ":" + std::to_string(h->size()) + "\r\n";
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HKEYS key
    if (cmd == "HKEYS" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "*0\r\n";
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
            std::string resp = "*" + std::to_string(h->size()) + "\r\n";
            for (const auto& [k,v] : *h) resp += "$"+std::to_string(k.size())+"\r\n"+k+"\r\n";
            return resp;
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HVALS key
    if (cmd == "HVALS" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "*0\r\n";
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
            std::string resp = "*" + std::to_string(h->size()) + "\r\n";
            for (const auto& [k,v] : *h) resp += "$"+std::to_string(v.size())+"\r\n"+v+"\r\n";
            return resp;
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HGETALL key
    if (cmd == "HGETALL" && args.size() >= 2) {
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return "*0\r\n";
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
            std::string resp = "*" + std::to_string(h->size() * 2) + "\r\n";
            for (const auto& [k,v] : *h) {
                resp += "$"+std::to_string(k.size())+"\r\n"+k+"\r\n";
                resp += "$"+std::to_string(v.size())+"\r\n"+v+"\r\n";
            }
            return resp;
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HINCRBY key field increment
    if (cmd == "HINCRBY" && args.size() >= 4) {
        int64_t delta = 0;
        try { delta = std::stoll(args[3]); }
        catch (...) { return "-ERR value not integer\r\n"; }

        CacheItem* item = cache.get_item(args[1], now);
        int64_t val = 0;

        if (!item) {
            cache.put(args[1], std::unordered_map<std::string,std::string>{{args[2], std::to_string(delta)}});
            append_to_aof(args);
            return ":" + std::to_string(delta) + "\r\n";
        }
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
            auto it = h->find(args[2]);
            if (it != h->end()) {
                try { val = std::stoll(it->second); }
                catch (...) { return "-ERR hash value is not an integer\r\n"; }
            }
            val += delta;
            (*h)[args[2]] = std::to_string(val);
            append_to_aof(args);
            return ":" + std::to_string(val) + "\r\n";
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    // HINCRBYFLOAT key field increment
    if (cmd == "HINCRBYFLOAT" && args.size() >= 4) {
        double delta = 0;
        try { delta = std::stod(args[3]); }
        catch (...) { return "-ERR value not a float\r\n"; }

        CacheItem* item = cache.get_item(args[1], now);
        double val = 0;

        if (!item) {
            std::ostringstream os; os << delta;
            cache.put(args[1], std::unordered_map<std::string,std::string>{{args[2], os.str()}});
            append_to_aof(args);
            std::ostringstream r; r << delta;
            return "$" + std::to_string(r.str().size()) + "\r\n" + r.str() + "\r\n";
        }
        if (auto* h = std::get_if<std::unordered_map<std::string,std::string>>(&item->value)) {
            auto it = h->find(args[2]);
            if (it != h->end()) {
                try { val = std::stod(it->second); }
                catch (...) { return "-ERR hash value is not a float\r\n"; }
            }
            val += delta;
            std::ostringstream os; os << val;
            (*h)[args[2]] = os.str();
            append_to_aof(args);
            std::ostringstream r; r << val;
            return "$" + std::to_string(r.str().size()) + "\r\n" + r.str() + "\r\n";
        }
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    return "-ERR Unknown command '" + args[0] + "'\r\n";
}

// ============================================================
// Public thread-safe entry point
// ============================================================

std::string KeyValueStore::execute_command(const std::vector<std::string>& args) {
    if (args.empty()) return "-ERR Empty command\r\n";
    ++total_commands;

    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    static const std::unordered_set<std::string> read_only = {
        "GET", "EXISTS", "TYPE", "TTL", "PTTL", "STRLEN", "GETRANGE",
        "LRANGE", "LLEN",
        "SMEMBERS", "SISMEMBER", "SCARD",
        "HGET", "HMGET", "HEXISTS", "HLEN", "HKEYS", "HVALS", "HGETALL",
        "DBSIZE", "PING", "INFO", "SCAN"
    };

    if (read_only.count(cmd)) {
        std::shared_lock<std::shared_mutex> lock(mtx);
        return execute_command_locked(args);
    }

    std::unique_lock<std::shared_mutex> lock(mtx);
    return execute_command_locked(args);
}