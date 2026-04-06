#include "network/server.h"
#include "utils/logger.h"
#include "utils/config.h"
#include <iostream>
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
#include <unordered_set>

// ============================================================
// The set of commands that must be forwarded to replicas.
// Extend this as you add more write commands.
// ============================================================
static const std::unordered_set<std::string> WRITE_COMMANDS = {
    "SET", "SETEX", "GETSET", "APPEND",
    "DEL", "RENAME",
    "EXPIRE", "PEXPIRE", "PERSIST",
    "INCR", "INCRBY", "DECR", "DECRBY",
    "LPUSH", "RPUSH", "LPOP", "RPOP",
    "SADD", "SREM",
    "FLUSHDB"
};

// ============================================================
// Constructor / Destructor
// ============================================================

RedisServer::RedisServer(int p, int lp)
    : port(p), leader_port(lp),
      server_fd(-1), epoll_fd(-1), leader_fd(-1)
{
    last_ping_time = get_time_ms();
    last_heartbeat = get_time_ms();
}

RedisServer::~RedisServer() {
    if (server_fd != -1) close(server_fd);
    if (epoll_fd  != -1) close(epoll_fd);
}

// ============================================================
// Helpers
// ============================================================

int64_t RedisServer::get_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

void RedisServer::make_socket_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1) flags = 0;
    fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
}

// ============================================================
// RESP parser
// ============================================================
// Tries to consume ONE complete RESP command from the front of `buf`.
// On success: populates `args` and returns the number of bytes consumed.
// On incomplete input: returns 0 and leaves `args` empty.
//
// Also handles inline (non-RESP) commands for netcat / telnet use.

static size_t try_parse_one_resp(const std::string& buf,
                                  std::vector<std::string>& args) {
    if (buf.empty()) return 0;

    // ---- Inline fallback -----------------------------------------------
    // If the client is sending plain text (e.g. "SET foo bar\r\n"), parse
    // it as a whitespace-delimited line.  Values containing spaces are NOT
    // supported in inline mode — that is the same limitation Redis has.
    if (buf[0] != '*') {
        size_t nl = buf.find('\n');
        if (nl == std::string::npos) return 0;  // Incomplete line.

        std::string line = buf.substr(0, nl);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        std::stringstream ss(line);
        std::string token;
        while (ss >> token) args.push_back(token);

        return nl + 1;
    }

    // ---- RESP array -------------------------------------------------------
    size_t pos = 0;
    size_t rn  = buf.find("\r\n", pos);
    if (rn == std::string::npos) return 0;

    int argc = 0;
    try { argc = std::stoi(buf.substr(1, rn - 1)); }
    catch (...) { return 0; }

    pos = rn + 2;

    for (int i = 0; i < argc; ++i) {
        if (pos >= buf.size() || buf[pos] != '$') return 0;

        rn = buf.find("\r\n", pos);
        if (rn == std::string::npos) return 0;

        int len = 0;
        try { len = std::stoi(buf.substr(pos + 1, rn - pos - 1)); }
        catch (...) { return 0; }

        pos = rn + 2;

        // Guard against a truncated payload.
        if (pos + static_cast<size_t>(len) + 2 > buf.size()) return 0;

        args.push_back(buf.substr(pos, len));
        pos += len + 2;   // Skip data + trailing \r\n.
    }

    return pos;   // Number of bytes consumed.
}

// ============================================================
// run()
// ============================================================

void RedisServer::run() {
    Logger::info("Starting server on port " + std::to_string(port));
    if (leader_port > 0) {
        Logger::info("Running in REPLICA mode, following port " +
                     std::to_string(leader_port));
    }

    // ---- Create and bind the listening socket ----------------------------
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { Logger::error("socket() failed"); return; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address{};
    address.sin_family      = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port        = htons(port);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&address),
             sizeof(address)) < 0) {
        Logger::error("bind() failed on port " + std::to_string(port));
        return;
    }

    // Backlog of 128 lets the kernel queue bursts of connections while
    // the event loop is busy processing other events.
    listen(server_fd, 128);
    make_socket_non_blocking(server_fd);

    // ---- Set up epoll ----------------------------------------------------
    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) { Logger::error("epoll_create1() failed"); return; }

    struct epoll_event ev{};
    ev.events   = EPOLLIN;
    ev.data.fd  = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    // ---- Replica startup: connect to leader ------------------------------
    if (leader_port > 0) {
        leader_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in la{};
        la.sin_family = AF_INET;
        la.sin_port   = htons(leader_port);
        inet_pton(AF_INET, "127.0.0.1", &la.sin_addr);

        if (connect(leader_fd,
                    reinterpret_cast<struct sockaddr*>(&la), sizeof(la)) == 0) {
            Logger::info("Connected to leader on port " +
                         std::to_string(leader_port));
            make_socket_non_blocking(leader_fd);

            struct epoll_event le{};
            le.events  = EPOLLIN;
            le.data.fd = leader_fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, leader_fd, &le);

            // Announce ourselves to the leader using RESP.
            // *1\r\n$9\r\nREPLICAOF\r\n
            const std::string hs = "*1\r\n$9\r\nREPLICAOF\r\n";
            send(leader_fd, hs.c_str(), hs.size(), 0);
        } else {
            Logger::error("Failed to connect to leader on port " +
                          std::to_string(leader_port));
            close(leader_fd);
            leader_fd = -1;
        }
    }

    start_heartbeat_thread();
    Logger::info("Heartbeat thread started. Announcing presence to router.");

    // ---- Event loop ------------------------------------------------------
    // 64 events per epoll_wait call is a reasonable batch size.
    struct epoll_event events[64];

    while (true) {
        int n = epoll_wait(epoll_fd, events, 64, 1000 /* ms timeout */);
        check_heartbeats();

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == server_fd) {
                handle_new_connection();
            } else {
                handle_client_data(events[i].data.fd);
            }
        }
    }
}

// ============================================================
// handle_new_connection
// ============================================================

void RedisServer::handle_new_connection() {
    int client_fd = accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) return;

    make_socket_non_blocking(client_fd);

    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLET;   // Edge-triggered for lower overhead.
    ev.data.fd = client_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

    // Allocate an empty read buffer for this new connection.
    client_buffers[client_fd] = "";
}

// ============================================================
// handle_client_data  (pipelining-aware)
// ============================================================

void RedisServer::handle_client_data(int client_fd) {
    // ---- Read all available bytes into the per-client buffer -------------
    // With edge-triggered epoll we MUST drain the socket completely or we
    // won't get another EPOLLIN notification until more data arrives.
    char buf[4096];
    while (true) {
        ssize_t n = read(client_fd, buf, sizeof(buf));
        if (n > 0) {
            client_buffers[client_fd].append(buf, n);
        } else if (n == 0) {
            // Client closed the connection.
            close(client_fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
            client_buffers.erase(client_fd);
            replica_fds.erase(
                std::remove(replica_fds.begin(), replica_fds.end(), client_fd),
                replica_fds.end());
            return;
        } else {
            // EAGAIN / EWOULDBLOCK means we've drained the socket.
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            // Any other error — close the connection.
            close(client_fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
            client_buffers.erase(client_fd);
            return;
        }
    }

    // Update the failover timer whenever the leader sends us anything.
    if (client_fd == leader_fd) last_heartbeat = get_time_ms();

    // ---- Parse and execute all complete commands in the buffer -----------
    std::string& rbuf = client_buffers[client_fd];

    while (!rbuf.empty()) {
        std::vector<std::string> args;
        size_t consumed = try_parse_one_resp(rbuf, args);

        if (consumed == 0) break;   // Incomplete frame — wait for more data.

        // Remove the consumed bytes from the front of the buffer.
        rbuf.erase(0, consumed);

        if (args.empty()) continue;

        std::string cmd = args[0];
        for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

        // ---- Replica registration handshake ------------------------------
        if (cmd == "REPLICAOF") {
            Logger::info("New replica registered. fd=" + std::to_string(client_fd));
            replica_fds.push_back(client_fd);
            // No response needed for an internal handshake.
            continue;
        }

        // ---- Execute and respond ------------------------------------------
        std::string response = store.execute_command(args);

        // Don't echo a response back if this fd is our upstream leader —
        // we are replaying their write commands, not answering them.
        if (client_fd != leader_fd) {
            send(client_fd, response.c_str(), response.size(), 0);
        }

        // ---- Replicate writes to all registered replicas -----------------
        // Only the leader (leader_port == 0) broadcasts. Forward the raw
        // bytes we received so replicas replay the exact same RESP command.
        if (leader_port == 0 &&
            WRITE_COMMANDS.count(cmd) &&
            response[0] != '-') {   // Don't replicate commands that errored.

            // Re-serialise as a clean RESP array so replicas always get a
            // well-formed command regardless of how the client sent it.
            std::string repl = "*" + std::to_string(args.size()) + "\r\n";
            for (const auto& arg : args) {
                repl += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
            }

            for (int rep_fd : replica_fds) {
                send(rep_fd, repl.c_str(), repl.size(), 0);
            }
        }
    }
}

// ============================================================
// check_heartbeats  (called once per epoll_wait cycle)
// ============================================================

void RedisServer::check_heartbeats() {
    int64_t now = get_time_ms();

    // Leader: ping all replicas every 2 seconds to keep them from promoting.
    if (leader_port == 0 && now - last_ping_time > 2000) {
        const std::string ping = "*1\r\n$4\r\nPING\r\n";
        for (int rep_fd : replica_fds) {
            send(rep_fd, ping.c_str(), ping.size(), 0);
        }
        last_ping_time = now;
    }

    // Replica failover: promote to leader if the upstream has been silent
    // for more than 5 seconds.
    if (leader_port > 0 && leader_fd != -1 && now - last_heartbeat > 5000) {
        Logger::error("Leader silent for 5 s — promoting self to leader.");
        close(leader_fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, leader_fd, nullptr);
        client_buffers.erase(leader_fd);
        leader_fd   = -1;
        leader_port = 0;
        Logger::info("Promoted to leader on port " + std::to_string(port));
    }
}

// ============================================================
// start_heartbeat_thread
// ============================================================
// Opens a fresh TCP connection to the router every 2 seconds and
// sends a HEARTBEAT message so the router knows we are alive.

void RedisServer::start_heartbeat_thread() {
    std::thread([this]() {
        // The router always listens on its own fixed port.  We read that
        // from the binary's own port value: if we are a replica we were
        // launched with a non-6379 port, but the router is always on 6379.
        // Make this configurable if you later run the router on a different
        // port (add a router_port field to Config).
        const int router_port = 6379;

        while (true) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock >= 0) {
                struct sockaddr_in ra{};
                ra.sin_family = AF_INET;
                ra.sin_port   = htons(router_port);
                inet_pton(AF_INET, "127.0.0.1", &ra.sin_addr);

                if (connect(sock, reinterpret_cast<struct sockaddr*>(&ra),
                            sizeof(ra)) == 0) {
                    std::string pulse = "HEARTBEAT " + std::to_string(port) + "\r\n";
                    send(sock, pulse.c_str(), pulse.size(), 0);
                }
                close(sock);
            }

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }).detach();
}