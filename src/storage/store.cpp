#include "storage/store.h"
#include "utils/logger.h"
#include "utils/config.h"
#include <chrono>
#include <unordered_set>

// ============================================================
// Constructor / Destructor
// ============================================================
KeyValueStore::KeyValueStore() : cache(Config::max_memory) {
    init_commands(); // Boot up the O(1) Command Registry
    init_lua();
    start_time_ms = get_current_time_ms();
    
    rdb_load_snapshot(Config::rdb_file);
    load_from_aof();
    
    aof_file.open(Config::aof_file, std::ios::app | std::ios::out);
    if (!aof_file.is_open()) {
        Logger::error("Could not open AOF: " + Config::aof_file);
    }
    last_save_time.store(get_current_time_ms() / 1000);
}

KeyValueStore::~KeyValueStore() {
    if (aof_file.is_open()) aof_file.close();
    if (lua_vm) lua_close(lua_vm);
}

// ============================================================
// LUA VIRTUAL MACHINE INITIALIZATION
// ============================================================
void KeyValueStore::init_lua() {
    lua_vm = luaL_newstate();
    luaL_openlibs(lua_vm); // Load standard Lua math/string libraries

    // Create the global 'redis' table in Lua
    lua_newtable(lua_vm); 
    
    // Push the 'this' pointer into Lua so the static function knows which database to talk to
    lua_pushlightuserdata(lua_vm, this); 
    lua_pushcclosure(lua_vm, lua_redis_call, 1); 
    lua_setfield(lua_vm, -2, "call"); // Maps 'redis.call' to our C++ function
    
    lua_setglobal(lua_vm, "redis"); 
}

int KeyValueStore::lua_redis_call(lua_State* L) {
    // 1. Grab the 'this' pointer we hid inside Lua
    KeyValueStore* store = static_cast<KeyValueStore*>(lua_touserdata(L, lua_upvalueindex(1)));

    // 2. Unpack the arguments Lua gave us (e.g., "SET", "mykey", "10")
    int argc = lua_gettop(L);
    std::vector<std::string> args;
    for (int i = 1; i <= argc; ++i) {
        if (lua_isstring(L, i)) args.push_back(lua_tostring(L, i));
    }

    // 3. Execute the command in the native C++ engine! (Defaulting to RESP2)
    std::string resp = store->execute_command_locked(args, 2);

    // 4. Send the result back to Lua
    lua_pushstring(L, resp.c_str());
    return 1; // We are returning 1 value
}

// ============================================================
// Private Helpers
// ============================================================
int64_t KeyValueStore::get_current_time_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ============================================================
// INFO & Slowlog
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
        info += kv ("used_memory",       cache.memory_usage()); 
        info += kv ("maxmemory",         Config::max_memory);
        info += kvs("maxmemory_policy", "allkeys-lru");
        info += "\r\n";
    }
    if (all_sec || section == "stats") {
        info += sec("Stats");
        info += kv ("total_commands_processed", total_commands.load());
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

void KeyValueStore::record_slowlog(const std::vector<std::string>& args, int64_t dur) {
    if (Config::slowlog_threshold < 0 || dur < Config::slowlog_threshold) return;
    SlowlogEntry e; e.id = slowlog_id.fetch_add(1);
    e.timestamp_ms = get_current_time_ms(); e.duration_ms = dur; e.args = args;
    std::lock_guard<std::mutex> lk(slowlog_mtx);
    slowlog.push_front(std::move(e));
    while (static_cast<int>(slowlog.size()) > Config::slowlog_max_len) slowlog.pop_back();
}

// ============================================================
// Internal Command Engine (O(1) Dispatcher)
// ============================================================
std::string KeyValueStore::execute_command_locked(const std::vector<std::string>& args, int resp_version) {
    if (args.empty()) return "-ERR Empty command\r\n";
    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    auto it = command_registry.find(cmd);
    if (it != command_registry.end()) {
        return it->second(args, get_current_time_ms(), resp_version); 
    }
    return "-ERR Unknown command '" + args[0] + "'\r\n";
}

bool KeyValueStore::is_write_command(const std::string& cmd) {
    static const std::unordered_set<std::string> writes = {
        "SET","SETEX","GETSET","APPEND","DEL","RENAME","EXPIRE","PEXPIRE","PEXPIREAT",
        "PERSIST","INCR","INCRBY","DECR","DECRBY","LPUSH","RPUSH","LPOP","RPOP",
        "SADD","SREM","HSET","HDEL","HINCRBY","HINCRBYFLOAT","HSETNX","ZADD", 
        "FLUSHDB", "BGREWRITEAOF", "EVAL"
    };
    return writes.count(cmd) > 0;
}

// ============================================================
// Mutation Tracking for WATCH and Cache Sizing
// ============================================================
void KeyValueStore::track_mutations(const std::vector<std::string>& args) {
    if (args.empty()) return;
    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    if (!is_write_command(cmd)) return;

    if (cmd == "FLUSHDB") {
        global_flush_version++; 
    } else if (cmd == "DEL") {
        for (size_t i = 1; i < args.size(); ++i) key_versions[args[i]]++;
    } else if (cmd == "RENAME" && args.size() >= 3) {
        key_versions[args[1]]++; cache.recalculate_size(args[1]);
        key_versions[args[2]]++; cache.recalculate_size(args[2]);
    } else if (cmd == "EVAL" && args.size() >= 3) {
        // NEW: Bump versions for any key the Lua script touches!
        int numkeys = 0; try { numkeys = std::stoi(args[2]); } catch(...) {}
        for (int i = 0; i < numkeys && 3 + i < args.size(); ++i) {
            key_versions[args[3 + i]]++;
            cache.recalculate_size(args[3 + i]);
        }
    }   
    else if (args.size() > 1) {
        key_versions[args[1]]++; 
        cache.recalculate_size(args[1]); 
    }
}

// ============================================================
// Public Thread-Safe Entry Point (ACID Transactions)
// ============================================================
std::string KeyValueStore::execute_command(const std::vector<std::string>& args,
                                            TxState& tx, bool& authenticated, int resp_version) {
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

    if (cmd == "WATCH") {
        if (tx.active) return "-ERR WATCH inside MULTI is not allowed\r\n";
        if (args.size() < 2) return "-ERR wrong number of arguments\r\n";
        std::unique_lock<std::shared_mutex> lock(mtx);
        for (size_t i = 1; i < args.size(); ++i) {
            tx.watched_keys[args[i]] = key_versions[args[i]] + global_flush_version.load();
        }
        return "+OK\r\n";
    }
    if (cmd == "UNWATCH") { tx.watched_keys.clear(); return "+OK\r\n"; }
    if (cmd == "MULTI")   { if (tx.active) return "-ERR MULTI calls can not be nested\r\n"; tx.active = true; tx.errored = false; tx.queue.clear(); return "+OK\r\n"; }
    if (cmd == "DISCARD") { if (!tx.active) return "-ERR DISCARD without MULTI\r\n"; tx.active = false; tx.errored = false; tx.queue.clear(); tx.watched_keys.clear(); return "+OK\r\n"; }
    
    if (cmd == "EXEC") {
        if (!tx.active) return "-ERR EXEC without MULTI\r\n";
        if (tx.errored) { tx.active=false; tx.queue.clear(); tx.watched_keys.clear(); return "-EXECABORT Transaction discarded because of previous errors\r\n"; }
        
        std::unique_lock<std::shared_mutex> lock(mtx);
        for (const auto& [k, expected_version] : tx.watched_keys) {
            uint64_t current_version = key_versions[k] + global_flush_version.load();
            if (current_version != expected_version) {
                tx.active = false; tx.queue.clear(); tx.watched_keys.clear();
                return "*-1\r\n"; 
            }
        }

        std::string r = "*"+std::to_string(tx.queue.size())+"\r\n";
        for (const auto& qa : tx.queue) {
            std::string res = execute_command_locked(qa, resp_version);
            r += res;
            if (res[0] != '-') track_mutations(qa);
        }
        tx.active = false; tx.queue.clear(); tx.watched_keys.clear(); return r;
    }
    
    if (tx.active) { tx.queue.push_back(args); return "+QUEUED\r\n"; }

    int64_t t0 = get_current_time_ms();
    std::string result;
    
    { 
        std::unique_lock<std::shared_mutex> lk(mtx); 
        result = execute_command_locked(args, resp_version); 
        if (result[0] != '-') track_mutations(args); 
    }
    
    record_slowlog(args, get_current_time_ms() - t0);
    return result;
}