#include "network/server.h"
#include "utils/logger.h"
#include "utils/config.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <thread>
#include <csignal>
#include <atomic>

// ============================================================
// Graceful shutdown via SIGTERM / SIGINT
// ============================================================

static std::atomic<bool> g_shutdown{false};

static void signal_handler(int) {
    g_shutdown.store(true);
}

// ============================================================
// Write commands that must be replicated to followers
// ============================================================

static const std::unordered_set<std::string> WRITE_COMMANDS = {
    "SET","SETEX","GETSET","APPEND",
    "DEL","RENAME",
    "EXPIRE","PEXPIRE","PEXPIREAT","PERSIST",
    "INCR","INCRBY","DECR","DECRBY",
    "LPUSH","RPUSH","LPOP","RPOP",
    "SADD","SREM",
    "HSET","HDEL","HINCRBY","HINCRBYFLOAT","HSETNX",
    "FLUSHDB","BGREWRITEAOF"
};

// ============================================================
// RESP frame parser (returns bytes consumed, 0 = incomplete)
// ============================================================

static size_t try_parse_one_resp(const std::string& buf,
                                  std::vector<std::string>& args) {
    if (buf.empty()) return 0;

    // Inline fallback
    if (buf[0] != '*') {
        size_t nl = buf.find('\n');
        if (nl == std::string::npos) return 0;
        std::string line = buf.substr(0, nl);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        std::stringstream ss(line); std::string t;
        while (ss >> t) args.push_back(t);
        return nl + 1;
    }

    size_t pos = 0;
    size_t rn  = buf.find("\r\n", pos);
    if (rn == std::string::npos) return 0;
    int argc = 0;
    try { argc = std::stoi(buf.substr(1, rn - 1)); } catch (...) { return 0; }
    pos = rn + 2;

    for (int i = 0; i < argc; ++i) {
        if (pos >= buf.size() || buf[pos] != '$') return 0;
        rn = buf.find("\r\n", pos);
        if (rn == std::string::npos) return 0;
        int len = 0;
        try { len = std::stoi(buf.substr(pos + 1, rn - pos - 1)); } catch (...) { return 0; }
        pos = rn + 2;
        if (pos + static_cast<size_t>(len) + 2 > buf.size()) return 0;
        args.push_back(buf.substr(pos, len));
        pos += len + 2;
    }
    return pos;
}

// ============================================================
// Constructor / Destructor
// ============================================================

RedisServer::RedisServer(int p, int lp)
    : port(p), leader_port(lp), leader_fd(-1) {
    last_ping_time = get_time_ms();
    last_heartbeat = get_time_ms();
}

RedisServer::~RedisServer() {
    if (server_fd != -1) close(server_fd);
    if (epoll_fd  != -1) close(epoll_fd);
}

int64_t RedisServer::get_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

void RedisServer::make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) flags = 0;
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// ============================================================
// Signal handler setup
// ============================================================

void RedisServer::setup_signal_handlers() {
    struct sigaction sa{};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT,  &sa, nullptr);
    // Ignore SIGPIPE so a broken client connection doesn't kill the server.
    signal(SIGPIPE, SIG_IGN);
}

// ============================================================
// Pub/Sub
// ============================================================

bool RedisServer::handle_pubsub(int fd, const std::vector<std::string>& args) {
    if (args.empty()) return false;
    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    if (cmd == "SUBSCRIBE" && args.size() >= 2) {
        std::lock_guard<std::mutex> lk(pubsub_mtx);
        auto& state = clients[fd];
        for (size_t i = 1; i < args.size(); ++i) {
            pubsub_channels[args[i]].insert(fd);
            state.subscriptions.insert(args[i]);
            // RESP: *3\r\n$9\r\nsubscribe\r\n$<n>\r\n<ch>\r\n:<count>\r\n
            std::string resp = "*3\r\n$9\r\nsubscribe\r\n";
            resp += "$" + std::to_string(args[i].size()) + "\r\n" + args[i] + "\r\n";
            resp += ":" + std::to_string(state.subscriptions.size()) + "\r\n";
            send(fd, resp.c_str(), resp.size(), 0);
        }
        return true;
    }

    if (cmd == "UNSUBSCRIBE") {
        std::lock_guard<std::mutex> lk(pubsub_mtx);
        auto& state = clients[fd];
        std::vector<std::string> channels =
            args.size() >= 2
                ? std::vector<std::string>(args.begin() + 1, args.end())
                : std::vector<std::string>(state.subscriptions.begin(), state.subscriptions.end());

        for (const auto& ch : channels) {
            pubsub_channels[ch].erase(fd);
            if (pubsub_channels[ch].empty()) pubsub_channels.erase(ch);
            state.subscriptions.erase(ch);
            std::string resp = "*3\r\n$11\r\nunsubscribe\r\n";
            resp += "$" + std::to_string(ch.size()) + "\r\n" + ch + "\r\n";
            resp += ":" + std::to_string(state.subscriptions.size()) + "\r\n";
            send(fd, resp.c_str(), resp.size(), 0);
        }
        return true;
    }

    if (cmd == "PUBLISH" && args.size() >= 3) {
        const std::string& channel = args[1];
        const std::string& message = args[2];
        int receivers = 0;
        {
            std::lock_guard<std::mutex> lk(pubsub_mtx);
            auto it = pubsub_channels.find(channel);
            if (it != pubsub_channels.end()) {
                std::string msg = "*3\r\n$7\r\nmessage\r\n";
                msg += "$" + std::to_string(channel.size()) + "\r\n" + channel + "\r\n";
                msg += "$" + std::to_string(message.size()) + "\r\n" + message + "\r\n";
                for (int sub_fd : it->second) {
                    send(sub_fd, msg.c_str(), msg.size(), 0);
                    ++receivers;
                }
            }
        }
        // PUBLISH returns the number of subscribers that received the message.
        std::string r = ":" + std::to_string(receivers) + "\r\n";
        send(fd, r.c_str(), r.size(), 0);
        return true;
    }

    return false;
}

// ============================================================
// close_client — cleans up all per-connection state
// ============================================================

void RedisServer::close_client(int fd) {
    {
        std::lock_guard<std::mutex> lk(pubsub_mtx);
        auto it = clients.find(fd);
        if (it != clients.end()) {
            for (const auto& ch : it->second.subscriptions) {
                pubsub_channels[ch].erase(fd);
                if (pubsub_channels[ch].empty()) pubsub_channels.erase(ch);
            }
        }
    }
    clients.erase(fd);
    replica_fds.erase(std::remove(replica_fds.begin(), replica_fds.end(), fd), replica_fds.end());
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
}

// ============================================================
// run()
// ============================================================

void RedisServer::run() {
    setup_signal_handlers();

    Logger::info("Starting server on port " + std::to_string(port));
    if (leader_port > 0)
        Logger::info("Replica mode — following port " + std::to_string(leader_port));

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { Logger::error("socket() failed"); return; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        Logger::error("bind() failed on port " + std::to_string(port)); return;
    }
    listen(server_fd, 128);
    make_socket_non_blocking(server_fd);

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) { Logger::error("epoll_create1() failed"); return; }

    struct epoll_event ev{};
    ev.events = EPOLLIN; ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    // Replica startup
    if (leader_port > 0) {
        leader_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in la{};
        la.sin_family = AF_INET; la.sin_port = htons(leader_port);
        inet_pton(AF_INET, "127.0.0.1", &la.sin_addr);
        if (connect(leader_fd, reinterpret_cast<struct sockaddr*>(&la), sizeof(la)) == 0) {
            Logger::info("Connected to leader on port " + std::to_string(leader_port));
            make_socket_non_blocking(leader_fd);
            struct epoll_event le{}; le.events = EPOLLIN; le.data.fd = leader_fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, leader_fd, &le);
            clients[leader_fd]; // allocate state
            const std::string hs = "*1\r\n$9\r\nREPLICAOF\r\n";
            send(leader_fd, hs.c_str(), hs.size(), 0);
        } else {
            Logger::error("Failed to connect to leader on port " + std::to_string(leader_port));
            close(leader_fd); leader_fd = -1;
        }
    }

    start_heartbeat_thread();
    Logger::info("Server ready. SIGTERM/SIGINT will trigger graceful shutdown.");

    struct epoll_event events[64];
    while (!g_shutdown.load()) {
        int n = epoll_wait(epoll_fd, events, 64, 1000);
        check_heartbeats();
        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == server_fd) handle_new_connection();
            else                                 handle_client_data(events[i].data.fd);
        }
    }

    // ---- Graceful shutdown ----
    Logger::info("Shutdown signal received — flushing AOF and closing connections.");
    // Flush the store's AOF via BGREWRITEAOF before exit is possible but
    // a simple approach is to just let the KeyValueStore destructor close
    // the file (it calls aof_file.close() which flushes).
    // Close all client sockets cleanly.
    std::vector<int> all_fds;
    for (const auto& [fd, _] : clients) all_fds.push_back(fd);
    for (int fd : all_fds) close_client(fd);
    close(server_fd); server_fd = -1;
    Logger::info("Server stopped cleanly.");
}

// ============================================================
// handle_new_connection
// ============================================================

void RedisServer::handle_new_connection() {
    int client_fd = accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) return;
    make_socket_non_blocking(client_fd);

    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = client_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

    ClientState& s  = clients[client_fd];
    s.read_buf      = "";
    s.authenticated = Config::requirepass.empty(); // pre-auth if no password set
}

// ============================================================
// handle_client_data
// ============================================================

void RedisServer::handle_client_data(int fd) {
    char buf[4096];
    while (true) {
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n > 0) {
            clients[fd].read_buf.append(buf, n);
        } else if (n == 0) {
            close_client(fd); return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_client(fd); return;
        }
    }

    if (fd == leader_fd) last_heartbeat = get_time_ms();

    auto& state = clients[fd];
    while (!state.read_buf.empty()) {
        std::vector<std::string> args;
        size_t consumed = try_parse_one_resp(state.read_buf, args);
        if (consumed == 0) break;
        state.read_buf.erase(0, consumed);
        if (args.empty()) continue;

        std::string cmd = args[0];
        for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

        // Replica handshake
        if (cmd == "REPLICAOF") {
            Logger::info("Replica registered fd=" + std::to_string(fd));
            replica_fds.push_back(fd);
            continue;
        }

        // Pub/sub commands are handled separately and never reach the store.
        if (cmd == "SUBSCRIBE" || cmd == "UNSUBSCRIBE" || cmd == "PUBLISH") {
            handle_pubsub(fd, args);
            continue;
        }

        std::string response = store.execute_command(args, state.tx, state.authenticated);

        if (fd != leader_fd)
            send(fd, response.c_str(), response.size(), 0);

        // Replicate successful writes to followers
        if (leader_port == 0 && WRITE_COMMANDS.count(cmd) && response[0] != '-') {
            std::string repl = "*" + std::to_string(args.size()) + "\r\n";
            for (const auto& a : args)
                repl += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
            for (int rep_fd : replica_fds)
                send(rep_fd, repl.c_str(), repl.size(), 0);
        }
    }
}

// ============================================================
// check_heartbeats
// ============================================================

void RedisServer::check_heartbeats() {
    int64_t now = get_time_ms();
    if (leader_port == 0 && now - last_ping_time > 2000) {
        const std::string ping = "*1\r\n$4\r\nPING\r\n";
        for (int rep_fd : replica_fds) send(rep_fd, ping.c_str(), ping.size(), 0);
        last_ping_time = now;
    }
    if (leader_port > 0 && leader_fd != -1 && now - last_heartbeat > 5000) {
        Logger::error("Leader silent — promoting self to leader.");
        close_client(leader_fd);
        leader_fd = -1; leader_port = 0;
        Logger::info("Promoted to leader on port " + std::to_string(port));
    }
}

// ============================================================
// start_heartbeat_thread
// ============================================================

void RedisServer::start_heartbeat_thread() {
    std::thread([this]() {
        while (!g_shutdown.load()) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock >= 0) {
                struct sockaddr_in ra{};
                ra.sin_family = AF_INET;
                ra.sin_port   = htons(Config::router_port);
                inet_pton(AF_INET, "127.0.0.1", &ra.sin_addr);
                if (connect(sock, reinterpret_cast<struct sockaddr*>(&ra), sizeof(ra)) == 0) {
                    std::string pulse = "HEARTBEAT " + std::to_string(port) + "\r\n";
                    send(sock, pulse.c_str(), pulse.size(), 0);
                }
                close(sock);
            }
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }).detach();
}