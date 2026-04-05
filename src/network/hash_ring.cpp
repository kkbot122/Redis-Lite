#include "network/hash_ring.h"
#include "utils/logger.h"
#include <functional>

void HashRing::add_server(int port) {
    std::hash<std::string> hasher;
    for (int i = 0; i < virtual_nodes; i++) {
        std::string vnode_id = "server_" + std::to_string(port) + "_vnode_" + std::to_string(i);
        size_t hash_val = hasher(vnode_id);
        ring[hash_val] = port;
    }
}

void HashRing::remove_server(int port) {
    std::hash<std::string> hasher;
    for (int i = 0; i < virtual_nodes; i++) {
        // We recreate the exact same Virtual Node IDs and erase them from the map!
        std::string vnode_id = "server_" + std::to_string(port) + "_vnode_" + std::to_string(i);
        size_t hash_val = hasher(vnode_id);
        ring.erase(hash_val);
    }
}

int HashRing::get_server_for_key(const std::string& key) {
    if (ring.empty()) return -1;
    std::hash<std::string> hasher;
    size_t key_hash = hasher(key);
    
    auto it = ring.lower_bound(key_hash);
    if (it == ring.end()) {
        return ring.begin()->second;
    }
    return it->second;
}