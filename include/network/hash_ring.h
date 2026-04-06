#pragma once
#include <string>
#include <map>

class HashRing {
private:
    std::map<size_t, int> ring;

    // 150 virtual nodes gives good distribution across up to ~20 backends.
    // With 3 nodes the ring was severely unbalanced — one backend could own
    // 60%+ of the keyspace depending on where its slots landed.
    static constexpr int virtual_nodes = 150;

public:
    void add_server   (int port);
    void remove_server(int port);
    int  get_server_for_key(const std::string& key);
};