#include "network/hash_ring.h"
#include <functional>

// Virtual node key format: "server:<port>#<i>"
// Using std::hash<std::string> is fast and consistent within a process.
// If you need cross-process consistency (e.g. multiple routers), swap this
// for MurmurHash3 or xxHash — std::hash is not standardised across platforms.

void HashRing::add_server(int port) {
    std::hash<std::string> hasher;
    for (int i = 0; i < virtual_nodes; ++i) {
        size_t h = hasher("server:" + std::to_string(port) + "#" + std::to_string(i));
        ring[h]  = port;
    }
}

void HashRing::remove_server(int port) {
    std::hash<std::string> hasher;
    for (int i = 0; i < virtual_nodes; ++i) {
        size_t h = hasher("server:" + std::to_string(port) + "#" + std::to_string(i));
        ring.erase(h);
    }
}

int HashRing::get_server_for_key(const std::string& key) {
    if (ring.empty()) return -1;
    std::hash<std::string> hasher;
    size_t h = hasher(key);
    // Walk clockwise to the first virtual node >= h.
    auto it = ring.lower_bound(h);
    if (it == ring.end()) it = ring.begin();   // Wrap around.
    return it->second;
}