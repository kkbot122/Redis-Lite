#pragma once
#include <string>
#include <map>

class HashRing {
private:
    std::map<size_t, int> ring;
    int virtual_nodes = 3; 

public:
    void add_server(int port);
    void remove_server(int port);
    int get_server_for_key(const std::string& key);
};