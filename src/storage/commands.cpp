#include "storage/store.h"
#include "utils/config.h"
#include "utils/logger.h"
#include <sstream>
#include <algorithm>
#include <thread>
#include <fnmatch.h>
#include <unistd.h>
#include <chrono>

void KeyValueStore::init_commands() {

    // ==========================================================
    // SERVER & ADMIN COMMANDS
    // ==========================================================
    command_registry["PING"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        return args.size() >= 2 ? "$" + std::to_string(args[1].size()) + "\r\n" + args[1] + "\r\n" : "+PONG\r\n";
    };

    command_registry["INFO"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        std::string s = args.size() >= 2 ? args[1] : "";
        for (char& c : s) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
        std::string p = build_info(s);
        return "$" + std::to_string(p.size()) + "\r\n" + p + "\r\n";
    };

    command_registry["DBSIZE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        return ":" + std::to_string(cache.size()) + "\r\n";
    };

    command_registry["FLUSHDB"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        cache = LRUCache(Config::max_memory); 
        append_to_aof(args); 
        return std::string("+OK\r\n");
    };

    // ==========================================================
    // PERSISTENCE COMMANDS
    // ==========================================================
    command_registry["SAVE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        // Just use the 'now' parameter directly!
        if (rdb_save_snapshot(Config::rdb_file, now)) {
            last_save_time.store(now / 1000);
            return std::string("+OK\r\n");
        }
        return std::string("-ERR RDB save failed\r\n");
    };

   command_registry["BGSAVE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
    if (rdb_save_in_progress.load() || rdb_child_pid != -1) return std::string("+Background save already in progress\r\n");
    rdb_save_in_progress.store(true);
    
    pid_t pid = fork();
    if (pid == 0) {
        // Child Process: Do NOT copy the cache into a vector.
        // Pass 'now' to rdb_save_snapshot so it iterates over the memory directly.
        std::string tmp = Config::rdb_file + ".bgsave.tmp";
        rdb_save_snapshot(tmp, now);
        _exit(0); 
        return std::string(""); 
    } else if (pid > 0) {
        rdb_child_pid = pid;
        return std::string("+Background saving started\r\n");
    } else {
        rdb_save_in_progress.store(false);
        return std::string("-ERR fork failed\r\n");
    }
};

    command_registry["LASTSAVE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        return ":" + std::to_string(last_save_time.load()) + "\r\n";
    };

    command_registry["BGREWRITEAOF"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
    if (rewrite_in_progress.load() || aof_child_pid != -1) return std::string("+Background AOF rewrite already in progress\r\n");
    rewrite_in_progress.store(true);
    
    // Clear the string buffer 
    aof_rewrite_buffer.clear();
    
    pid_t pid = fork();
    if (pid == 0) {
        // Child Process: Iterate over the cache directly, NO vectors.
        std::string tmp = Config::aof_file + ".rewrite.tmp";
        std::ofstream f(tmp, std::ios::trunc);
        if (f.is_open()) {
            // NOTE: You will need to add a for_each() method to your LRUCache (see Step 4)
            cache.for_each([&](const CacheItem& item) {
                f << serialize_item_to_resp(item);
            });
            f.flush();
        }
        _exit(0);
        return std::string(""); 
    } else if (pid > 0) {
        aof_child_pid = pid;
        return std::string("+Background append only file rewriting started\r\n");
    } else {
        rewrite_in_progress.store(false);
        return std::string("-ERR fork failed\r\n");
    }
};

    // ==========================================================
    // KEYSPACE COMMANDS
    // ==========================================================
    command_registry["DEL"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        int d = 0;
        for (size_t i = 1; i < args.size(); ++i) if (cache.remove(args[i])) ++d;
        append_to_aof(args); 
        return ":" + std::to_string(d) + "\r\n";
    };

    command_registry["EXISTS"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        return cache.exists(args[1], now) ? std::string(":1\r\n") : std::string(":0\r\n");
    };

    command_registry["TYPE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return std::string("+none\r\n");
        if (std::holds_alternative<std::string>(item->value)) return std::string("+string\r\n");
        if (std::holds_alternative<std::list<std::string>>(item->value)) return std::string("+list\r\n");
        if (std::holds_alternative<std::unordered_set<std::string>>(item->value)) return std::string("+set\r\n");
        if (std::holds_alternative<std::unordered_map<std::string,std::string>>(item->value)) return std::string("+hash\r\n");
        if (std::holds_alternative<ZSet>(item->value)) return std::string("+zset\r\n");
        return std::string("+none\r\n");
    };

    command_registry["RENAME"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return std::string("-ERR no such key\r\n");
        cache.put(args[2], item->value, item->expires_at); 
        cache.remove(args[1]);
        append_to_aof(args); 
        return std::string("+OK\r\n");
    };

    command_registry["SCAN"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        size_t cursor = 0, count = 10; std::string pat = "*";
        try { cursor = std::stoull(args[1]); } catch (...) { return std::string("-ERR invalid cursor\r\n"); }
        for (size_t i = 2; i + 1 < args.size(); ++i) {
            std::string opt = args[i]; for(char& c : opt) c = toupper(c);
            if (opt == "COUNT") try { count = std::stoull(args[i+1]); } catch (...) {}
            if (opt == "MATCH") pat = args[i+1];
        }
        auto [next, keys] = cache.scan(cursor, count, now);
        std::vector<std::string> filtered;
        for (const auto& k : keys) if (fnmatch(pat.c_str(), k.c_str(), 0) == 0) filtered.push_back(k);
        std::string nc = std::to_string(next);
        std::string r = "*2\r\n$" + std::to_string(nc.size()) + "\r\n" + nc + "\r\n*" + std::to_string(filtered.size()) + "\r\n";
        for (const auto& k : filtered) r += "$" + std::to_string(k.size()) + "\r\n" + k + "\r\n";
        return r;
    };

    // ==========================================================
    // TTL COMMANDS
    // ==========================================================
    command_registry["TTL"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now);
        if (!i) return std::string(":-2\r\n");
        if (i->expires_at == 0) return std::string(":-1\r\n");
        int64_t s = (i->expires_at - now) / 1000;
        return ":" + std::to_string(s > 0 ? s : 0) + "\r\n";
    };

    command_registry["PTTL"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now);
        if (!i) return std::string(":-2\r\n");
        if (i->expires_at == 0) return std::string(":-1\r\n");
        int64_t ms = i->expires_at - now;
        return ":" + std::to_string(ms > 0 ? ms : 0) + "\r\n";
    };

    command_registry["EXPIRE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        int64_t s = 0; try { s = std::stoll(args[2]); } catch (...) { return std::string("-ERR\r\n"); }
        bool ok = cache.set_expiry(args[1], now + s * 1000);
        if (ok) append_to_aof(args);
        return ok ? std::string(":1\r\n") : std::string(":0\r\n");
    };

    command_registry["PEXPIRE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        int64_t ms = 0; try { ms = std::stoll(args[2]); } catch (...) { return std::string("-ERR\r\n"); }
        bool ok = cache.set_expiry(args[1], now + ms);
        if (ok) append_to_aof(args);
        return ok ? std::string(":1\r\n") : std::string(":0\r\n");
    };

    command_registry["PERSIST"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        bool ok = cache.set_expiry(args[1], 0);
        if (ok) append_to_aof(args);
        return ok ? std::string(":1\r\n") : std::string(":0\r\n");
    };

    // ==========================================================
    // STRING COMMANDS
    // ==========================================================
    command_registry["SET"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        int64_t exp = 0;
        for (size_t i = 3; i + 1 < args.size(); ++i) {
            std::string f = args[i]; for (char& c : f) c = toupper(c);
            try { 
                if (f == "EX") exp = now + std::stoll(args[i + 1]) * 1000; 
                if (f == "PX") exp = now + std::stoll(args[i + 1]); 
            } catch (...) { return std::string("-ERR syntax error\r\n"); }
        }
        cache.put(args[1], args[2], exp); 
        append_to_aof(args); 
        return std::string("+OK\r\n");
    };

    command_registry["GET"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now);
        if (!i) return std::string("$-1\r\n");
        if (auto* s = std::get_if<std::string>(&i->value)) return "$" + std::to_string(s->size()) + "\r\n" + *s + "\r\n";
        return std::string("-WRONGTYPE\r\n");
    };

    auto num_op = [this](const std::string& key, int64_t delta, int64_t now) -> std::string {
        CacheItem* i = cache.get_item(key, now); int64_t val = 0;
        if (i) {
            if (auto* s = std::get_if<std::string>(&i->value)) {
                try { val = std::stoll(*s); } catch (...) { return "-ERR value not integer\r\n"; }
            } else return "-WRONGTYPE\r\n";
        }
        val += delta; cache.put(key, std::to_string(val)); return ":" + std::to_string(val) + "\r\n";
    };

    command_registry["INCR"] = [this, num_op](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        auto r = num_op(args[1], 1, now); if (r[0] == ':') append_to_aof(args); return r;
    };
    command_registry["DECR"] = [this, num_op](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        auto r = num_op(args[1], -1, now); if (r[0] == ':') append_to_aof(args); return r;
    };
    command_registry["INCRBY"] = [this, num_op](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        int64_t d = 0; try { d = std::stoll(args[2]); } catch (...) { return std::string("-ERR\r\n"); }
        auto r = num_op(args[1], d, now); if (r[0] == ':') append_to_aof(args); return r;
    };
    command_registry["DECRBY"] = [this, num_op](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        int64_t d = 0; try { d = std::stoll(args[2]); } catch (...) { return std::string("-ERR\r\n"); }
        auto r = num_op(args[1], -d, now); if (r[0] == ':') append_to_aof(args); return r;
    };

    // ==========================================================
    // LIST COMMANDS
    // ==========================================================
    command_registry["LPUSH"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now); int nl = 0;
        if (!i) {
            std::list<std::string> l; for (size_t x = 2; x < args.size(); ++x) l.push_front(args[x]);
            nl = l.size(); cache.put(args[1], l);
        } else {
            if (auto* l = std::get_if<std::list<std::string>>(&i->value)) {
                for (size_t x = 2; x < args.size(); ++x) l->push_front(args[x]); nl = l->size();
            } else return std::string("-WRONGTYPE\r\n");
        }
        append_to_aof(args); return ":" + std::to_string(nl) + "\r\n";
    };

    command_registry["RPUSH"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now); int nl = 0;
        if (!i) {
            std::list<std::string> l; for (size_t x = 2; x < args.size(); ++x) l.push_back(args[x]);
            nl = l.size(); cache.put(args[1], l);
        } else {
            if (auto* l = std::get_if<std::list<std::string>>(&i->value)) {
                for (size_t x = 2; x < args.size(); ++x) l->push_back(args[x]); nl = l->size();
            } else return std::string("-WRONGTYPE\r\n");
        }
        append_to_aof(args); return ":" + std::to_string(nl) + "\r\n";
    };

    command_registry["LPOP"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now); if (!i) return std::string("$-1\r\n");
        if (auto* l = std::get_if<std::list<std::string>>(&i->value)) {
            if (l->empty()) return std::string("$-1\r\n");
            std::string v = l->front(); l->pop_front(); append_to_aof(args);
            return "$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
        }
        return std::string("-WRONGTYPE\r\n");
    };

    command_registry["RPOP"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now); if (!i) return std::string("$-1\r\n");
        if (auto* l = std::get_if<std::list<std::string>>(&i->value)) {
            if (l->empty()) return std::string("$-1\r\n");
            std::string v = l->back(); l->pop_back(); append_to_aof(args);
            return "$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
        }
        return std::string("-WRONGTYPE\r\n");
    };

    command_registry["LRANGE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 4) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now); if (!i) return std::string("*0\r\n");
        if (auto* l = std::get_if<std::list<std::string>>(&i->value)) {
            int len = l->size(), start = 0, stop = 0;
            try { start = std::stoi(args[2]); stop = std::stoi(args[3]); } catch (...) { return std::string("-ERR\r\n"); }
            if (start < 0) start = std::max(0, len + start); if (stop < 0) stop = len + stop; stop = std::min(stop, len - 1);
            if (start > stop || start >= len) return std::string("*0\r\n");
            std::string r; int cnt = 0, idx = 0;
            for (const auto& v : *l) {
                if (idx > stop) break;
                if (idx >= start) { r += "$" + std::to_string(v.size()) + "\r\n" + v + "\r\n"; ++cnt; }
                ++idx;
            }
            return "*" + std::to_string(cnt) + "\r\n" + r;
        }
        return std::string("-WRONGTYPE\r\n");
    };

    // ==========================================================
    // SET COMMANDS (UPDATED FOR RESP3)
    // ==========================================================
    command_registry["SADD"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now); int a = 0;
        if (!i) {
            std::unordered_set<std::string> s; for (size_t x = 2; x < args.size(); ++x) if (s.insert(args[x]).second) ++a;
            cache.put(args[1], s);
        } else {
            if (auto* s = std::get_if<std::unordered_set<std::string>>(&i->value)) {
                for (size_t x = 2; x < args.size(); ++x) if (s->insert(args[x]).second) ++a;
            } else return std::string("-WRONGTYPE\r\n");
        }
        append_to_aof(args); return ":" + std::to_string(a) + "\r\n";
    };

    command_registry["SMEMBERS"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now);
        if (!i) return std::string("*0\r\n");
        if (auto* s = std::get_if<std::unordered_set<std::string>>(&i->value)) {
            // RESP3 uses '~' for Sets, RESP2 uses '*' for Arrays
            std::string prefix = (resp_version == 3) ? "~" : "*";
            std::string r = prefix + std::to_string(s->size()) + "\r\n";
            for (const auto& m : *s) r += "$" + std::to_string(m.size()) + "\r\n" + m + "\r\n";
            return r;
        }
        return std::string("-WRONGTYPE\r\n");
    };

    // ==========================================================
    // HASH COMMANDS (UPDATED FOR RESP3)
    // ==========================================================
    command_registry["HSET"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 4 || (args.size() % 2) != 0) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now); int a = 0;
        if (!i) {
            std::unordered_map<std::string, std::string> h;
            for (size_t x = 2; x < args.size(); x += 2) { if (h.find(args[x]) == h.end()) ++a; h[args[x]] = args[x+1]; }
            cache.put(args[1], h);
        } else {
            if (auto* h = std::get_if<std::unordered_map<std::string, std::string>>(&i->value)) {
                for (size_t x = 2; x < args.size(); x += 2) { if (h->find(args[x]) == h->end()) ++a; (*h)[args[x]] = args[x+1]; }
            } else return std::string("-WRONGTYPE\r\n");
        }
        append_to_aof(args); return ":" + std::to_string(a) + "\r\n";
    };

    command_registry["HGET"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now); if (!i) return std::string("$-1\r\n");
        if (auto* h = std::get_if<std::unordered_map<std::string, std::string>>(&i->value)) {
            auto it = h->find(args[2]); if (it == h->end()) return std::string("$-1\r\n");
            return "$" + std::to_string(it->second.size()) + "\r\n" + it->second + "\r\n";
        }
        return std::string("-WRONGTYPE\r\n");
    };

    command_registry["HGETALL"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 2) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* i = cache.get_item(args[1], now);
        if (!i) return std::string("*0\r\n");
        if (auto* h = std::get_if<std::unordered_map<std::string, std::string>>(&i->value)) {
            if (resp_version == 3) {
                // RESP3 Map format (%)
                std::string r = "%" + std::to_string(h->size()) + "\r\n";
                for (const auto& [k, v] : *h) {
                    r += "$" + std::to_string(k.size()) + "\r\n" + k + "\r\n$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
                }
                return r;
            } else {
                // RESP2 Array format (*)
                std::string r = "*" + std::to_string(h->size() * 2) + "\r\n";
                for (const auto& [k, v] : *h) {
                    r += "$" + std::to_string(k.size()) + "\r\n" + k + "\r\n$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
                }
                return r;
            }
        }
        return std::string("-WRONGTYPE\r\n");
    };

    // ==========================================================
    // SORTED SET COMMANDS
    // ==========================================================
    command_registry["ZADD"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 4 || (args.size() % 2) != 0) return std::string("-ERR wrong number of arguments\r\n");
        std::vector<std::pair<double, std::string>> new_elements;
        for (size_t i = 2; i < args.size(); i += 2) {
            try { new_elements.push_back({std::stod(args[i]), args[i+1]}); }
            catch (...) { return std::string("-ERR value is not a valid float\r\n"); }
        }
        CacheItem* item = cache.get_item(args[1], now); int added = 0;
        if (!item) {
            ZSet z;
            for (const auto& el : new_elements) { z.dict[el.second] = el.first; z.tree.insert(el); added++; }
            cache.put(args[1], z);
        } else {
            if (auto* z = std::get_if<ZSet>(&item->value)) {
                for (const auto& el : new_elements) {
                    auto it = z->dict.find(el.second);
                    if (it != z->dict.end()) {
                        if (it->second != el.first) {
                            z->tree.erase({it->second, el.second}); z->tree.insert({el.first, el.second}); it->second = el.first;
                        }
                    } else { z->dict[el.second] = el.first; z->tree.insert(el); added++; }
                }
            } else return std::string("-WRONGTYPE\r\n");
        }
        append_to_aof(args);
        return ":" + std::to_string(added) + "\r\n";
    };

    command_registry["ZRANGE"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 4) return std::string("-ERR wrong number of arguments\r\n");
        CacheItem* item = cache.get_item(args[1], now);
        if (!item) return std::string("*0\r\n");
        if (auto* z = std::get_if<ZSet>(&item->value)) {
            int len = z->tree.size(); int start = 0, stop = 0;
            try { start = std::stoi(args[2]); stop = std::stoi(args[3]); } catch (...) { return std::string("-ERR syntax error\r\n"); }
            bool withscores = false;
            if (args.size() >= 5) { std::string opt = args[4]; for (char& c : opt) c = toupper(c); if (opt == "WITHSCORES") withscores = true; }
            if (start < 0) start = std::max(0, len + start);
            if (stop < 0) stop = len + stop;
            stop = std::min(stop, len - 1);
            if (start > stop || start >= len) return std::string("*0\r\n");
            int count = 0; std::string resp = "";
            auto it = z->tree.begin(); std::advance(it, start);
            for (int i = start; i <= stop && it != z->tree.end(); ++i, ++it) {
                resp += "$" + std::to_string(it->second.size()) + "\r\n" + it->second + "\r\n"; count++;
                if (withscores) {
                    std::ostringstream oss; oss << it->first; std::string s_str = oss.str();
                    resp += "$" + std::to_string(s_str.size()) + "\r\n" + s_str + "\r\n"; count++;
                }
            }
            return "*" + std::to_string(count) + "\r\n" + resp;
        }
        return std::string("-WRONGTYPE\r\n");
    };

    // ==========================================================
    // BLOCKING QUEUE COMMANDS
    // ==========================================================
    command_registry["BLPOP"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        if (args.size() < 3) return std::string("-ERR wrong number of arguments\r\n");
        
        // Loop through the provided keys to see if any have data
        for (size_t i = 1; i < args.size() - 1; ++i) {
            CacheItem* item = cache.get_item(args[i], now);
            if (item) {
                if (auto* l = std::get_if<std::list<std::string>>(&item->value)) {
                    if (!l->empty()) {
                        std::string v = l->front();
                        l->pop_front();
                        append_to_aof({"LPOP", args[i]});
                        
                        // Returns the RESP Array: [key_name, popped_value]
                        return "*2\r\n$" + std::to_string(args[i].size()) + "\r\n" + args[i] + 
                               "\r\n$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
                    }
                } else return std::string("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            }
        }
        // If all lists are empty, return the secret internal wait flag
        return std::string("*WAIT\r\n"); 
    };

    // ==========================================================
    // LUA SCRIPTING
    // ==========================================================
    command_registry["EVAL"] = [this](const std::vector<std::string>& args, int64_t now, int resp_version) {
        // Syntax: EVAL "script" numkeys key1 key2 arg1 arg2
        if (args.size() < 3) return std::string("-ERR wrong number of arguments for 'eval' command\r\n");

        std::string script = args[1];
        int numkeys = 0;
        try { numkeys = std::stoi(args[2]); } catch(...) { return std::string("-ERR value is not an integer\r\n"); }
        if (args.size() < 3 + numkeys) return std::string("-ERR invalid number of arguments\r\n");

        // 1. Setup the KEYS array in Lua
        lua_newtable(lua_vm);
        for (int i = 0; i < numkeys; ++i) {
            lua_pushstring(lua_vm, args[3 + i].c_str());
            lua_rawseti(lua_vm, -2, i + 1); // Lua is 1-indexed!
        }
        lua_setglobal(lua_vm, "KEYS");

        // 2. Setup the ARGV array in Lua
        lua_newtable(lua_vm);
        int numargs = args.size() - 3 - numkeys;
        for (int i = 0; i < numargs; ++i) {
            lua_pushstring(lua_vm, args[3 + numkeys + i].c_str());
            lua_rawseti(lua_vm, -2, i + 1);
        }
        lua_setglobal(lua_vm, "ARGV");

        // 3. Execute the script!
        if (luaL_dostring(lua_vm, script.c_str()) != LUA_OK) {
            std::string err = lua_tostring(lua_vm, -1);
            lua_pop(lua_vm, 1);
            return "-ERR Error running script: " + err + "\r\n";
        }

        // 4. Capture the return value
        std::string result;
        if (lua_isstring(lua_vm, -1)) {
            std::string res = lua_tostring(lua_vm, -1);
            // If it's already RESP (returned by redis.call), pass it. Otherwise, format it.
            if (!res.empty() && (res[0] == '+' || res[0] == '-' || res[0] == ':' || res[0] == '$' || res[0] == '*')) {
                result = res;
            } else {
                result = "$" + std::to_string(res.size()) + "\r\n" + res + "\r\n";
            }
        } else if (lua_isinteger(lua_vm, -1)) {
            result = ":" + std::to_string(lua_tointeger(lua_vm, -1)) + "\r\n";
        } else {
            result = "$-1\r\n"; // nil
        }
        lua_pop(lua_vm, 1);

        append_to_aof(args);
        return result;
    };
}