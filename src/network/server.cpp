#include "network/server.h"
#include "utils/logger.h"
#include "utils/config.h"
#include <sys/socket.h>
#include <sys/un.h>
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
#include <random>
#include <iomanip>
#include <fstream>

static std::atomic<bool> g_shutdown{false};
static void signal_handler(int) { g_shutdown.store(true); }

static const std::unordered_set<std::string> WRITE_COMMANDS = {
    "SET","SETEX","GETSET","APPEND","DEL","RENAME",
    "EXPIRE","PEXPIRE","PEXPIREAT","PERSIST",
    "INCR","INCRBY","DECR","DECRBY",
    "LPUSH","RPUSH","LPOP","RPOP",
    "SADD","SREM","SMEMBERS",
    "HSET","HDEL","HINCRBY","HINCRBYFLOAT","HSETNX","HGETALL",
    "FLUSHDB","BGREWRITEAOF",
    "ZADD"
};

// ============================================================
// RESP frame parser
// ============================================================
static size_t try_parse_one_resp(const std::string& buf, std::vector<std::string>& args) {
    if (buf.empty()) return 0;
    if (buf[0] != '*') {
        size_t nl = buf.find('\n');
        if (nl == std::string::npos) return 0;
        std::string line = buf.substr(0, nl);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        std::stringstream ss(line); std::string t;
        while (ss >> t) args.push_back(t);
        return nl + 1;
    }
    size_t pos = 0, rn = buf.find("\r\n", pos);
    if (rn == std::string::npos) return 0;
    int argc = 0;
    try { argc = std::stoi(buf.substr(1, rn - 1)); } catch (...) { return 0; }
    pos = rn + 2;
    for (int i = 0; i < argc; ++i) {
        if (pos >= buf.size() || buf[pos] != '$') return 0;
        rn = buf.find("\r\n", pos);
        if (rn == std::string::npos) return 0;
        int len = 0;
        try { len = std::stoi(buf.substr(pos+1, rn-pos-1)); } catch (...) { return 0; }
        pos = rn + 2;
        if (pos + static_cast<size_t>(len) + 2 > buf.size()) return 0;
        args.push_back(buf.substr(pos, len));
        pos += len + 2;
    }
    return pos;
}

// ============================================================
// Constructor & Destructor & TLS Init
// ============================================================
RedisServer::RedisServer(int p, int lp)
    : port(p), leader_port(lp), leader_fd(-1) {
    last_ping_time = get_time_ms();
    last_heartbeat = get_time_ms();
    repl_id = generate_repl_id();
}

RedisServer::~RedisServer() {
    if (server_fd != -1) close(server_fd);
    if (unix_fd   != -1) close(unix_fd);
    if (epoll_fd  != -1) close(epoll_fd);
    if (!Config::unixsocket.empty()) unlink(Config::unixsocket.c_str());
    if (ssl_ctx) SSL_CTX_free(ssl_ctx);
}

void RedisServer::init_ssl() {
    if (Config::tls_cert_file.empty() || Config::tls_key_file.empty()) return;

    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
    const SSL_METHOD* method = TLS_server_method();
    ssl_ctx = SSL_CTX_new(method);

    if (!ssl_ctx) {
        Logger::error("Unable to create SSL context.");
        return;
    }

    if (SSL_CTX_use_certificate_file(ssl_ctx, Config::tls_cert_file.c_str(), SSL_FILETYPE_PEM) <= 0) {
        Logger::error("Failed to load TLS Certificate: " + Config::tls_cert_file);
        exit(EXIT_FAILURE);
    }
    if (SSL_CTX_use_PrivateKey_file(ssl_ctx, Config::tls_key_file.c_str(), SSL_FILETYPE_PEM) <= 0 ) {
        Logger::error("Failed to load TLS Private Key: " + Config::tls_key_file);
        exit(EXIT_FAILURE);
    }
    Logger::info("TLS/SSL Engine Enabled. Encrypted connections online.");
}

void RedisServer::secure_send(int fd, const std::string& data) {
    if (client_ssl.count(fd)) {
        SSL_write(client_ssl[fd], data.c_str(), data.size());
    } else {
        send(fd, data.c_str(), data.size(), 0);
    }
}

int64_t RedisServer::get_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

void RedisServer::make_socket_non_blocking(int fd) {
    int f = fcntl(fd, F_GETFL, 0);
    if (f == -1) f = 0;
    fcntl(fd, F_SETFL, f | O_NONBLOCK);
}

std::string RedisServer::generate_repl_id() const {
    std::mt19937_64 rng(std::chrono::steady_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<uint64_t> dist;
    std::ostringstream oss;
    for (int i = 0; i < 5; ++i)
        oss << std::hex << std::setfill('0') << std::setw(8) << (dist(rng) & 0xFFFFFFFF);
    return oss.str();
}

// ============================================================
// Replication functions
// ============================================================
void RedisServer::append_to_backlog(const std::string& data) {
    std::lock_guard<std::mutex> lk(backlog_mtx);
    for (char c : data) repl_backlog.push_back(c);
    while (repl_backlog.size() > BACKLOG_SIZE) repl_backlog.pop_front();
}

void RedisServer::replicate(const std::vector<std::string>& args) {
    std::string repl = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& a : args) repl += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
    append_to_backlog(repl);
    repl_offset.fetch_add(static_cast<int64_t>(repl.size()));
    for (int rep_fd : replica_fds) secure_send(rep_fd, repl);
}

void RedisServer::send_full_resync(int replica_fd) {
    std::string header = "+FULLRESYNC " + repl_id + " " + std::to_string(repl_offset.load()) + "\r\n";
    secure_send(replica_fd, header);

    std::ifstream rdb(Config::rdb_file, std::ios::binary | std::ios::ate);
    if (!rdb.is_open()) {
        const unsigned char empty_rdb[] = {
            0x52,0x45,0x44,0x49,0x53, 0x30,0x30,0x31,0x31, 0xFF, 
            0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00 
        };
        std::string preamble = "$" + std::to_string(sizeof(empty_rdb)) + "\r\n";
        secure_send(replica_fd, preamble);
        if (client_ssl.count(replica_fd)) SSL_write(client_ssl[replica_fd], empty_rdb, sizeof(empty_rdb));
        else send(replica_fd, reinterpret_cast<const char*>(empty_rdb), sizeof(empty_rdb), 0);
        return;
    }

    std::streamsize size = rdb.tellg();
    rdb.seekg(0, std::ios::beg);
    std::string preamble = "$" + std::to_string(size) + "\r\n";
    secure_send(replica_fd, preamble);

    char buf[8192];
    while (rdb.read(buf, sizeof(buf)) || rdb.gcount() > 0) {
        if (client_ssl.count(replica_fd)) SSL_write(client_ssl[replica_fd], buf, rdb.gcount());
        else send(replica_fd, buf, static_cast<size_t>(rdb.gcount()), 0);
    }
}

bool RedisServer::handle_replication_handshake(int fd, const std::vector<std::string>& args) {
    if (args.empty()) return false;
    std::string cmd = args[0];
    for (char& c : cmd) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    if (cmd == "REPLCONF") {
        if (args.size() >= 3) {
            std::string sub = args[1]; for (char& c : sub) c = toupper(c);
            if (sub == "ACK") {
                try { clients[fd].repl_offset = std::stoll(args[2]); } catch (...) {}
                return true; 
            }
        }
        secure_send(fd, "+OK\r\n"); return true;
    }

    if (cmd == "PSYNC" && args.size() >= 3) {
        const std::string& peer_id = args[1];
        int64_t peer_offset = -1; try { peer_offset = std::stoll(args[2]); } catch (...) {}

        bool can_partial = false;
        if (peer_id == repl_id && peer_offset >= 0) {
            std::lock_guard<std::mutex> lk(backlog_mtx);
            int64_t backlog_start = repl_offset.load() - static_cast<int64_t>(repl_backlog.size());
            can_partial = (peer_offset >= backlog_start);

            if (can_partial) {
                secure_send(fd, "+CONTINUE " + repl_id + "\r\n");
                size_t skip = static_cast<size_t>(peer_offset - backlog_start);
                std::string missing(repl_backlog.begin() + skip, repl_backlog.end());
                if (!missing.empty()) secure_send(fd, missing);
                replica_fds.push_back(fd);
                return true;
            }
        }
        send_full_resync(fd);
        replica_fds.push_back(fd);
        return true;
    }
    return false;
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
            std::string r = "*3\r\n$9\r\nsubscribe\r\n$"+std::to_string(args[i].size())+"\r\n"+args[i]+"\r\n:"+std::to_string(state.subscriptions.size())+"\r\n";
            secure_send(fd, r);
        }
        return true;
    }
    if (cmd == "UNSUBSCRIBE") {
        std::lock_guard<std::mutex> lk(pubsub_mtx);
        auto& state = clients[fd];
        std::vector<std::string> channels = args.size() >= 2
            ? std::vector<std::string>(args.begin()+1, args.end())
            : std::vector<std::string>(state.subscriptions.begin(), state.subscriptions.end());
        for (const auto& ch : channels) {
            pubsub_channels[ch].erase(fd);
            if (pubsub_channels[ch].empty()) pubsub_channels.erase(ch);
            state.subscriptions.erase(ch);
            std::string r = "*3\r\n$11\r\nunsubscribe\r\n$"+std::to_string(ch.size())+"\r\n"+ch+"\r\n:"+std::to_string(state.subscriptions.size())+"\r\n";
            secure_send(fd, r);
        }
        return true;
    }
    if (cmd == "PUBLISH" && args.size() >= 3) {
        int recv = 0;
        {
            std::lock_guard<std::mutex> lk(pubsub_mtx);
            auto it = pubsub_channels.find(args[1]);
            if (it != pubsub_channels.end()) {
                std::string msg = "*3\r\n$7\r\nmessage\r\n$"+std::to_string(args[1].size())+"\r\n"+args[1]+"\r\n$"+std::to_string(args[2].size())+"\r\n"+args[2]+"\r\n";
                for (int s : it->second) { secure_send(s, msg); ++recv; }
            }
        }
        secure_send(fd, ":" + std::to_string(recv) + "\r\n");
        return true;
    }
    return false;
}

// ============================================================
// Cleanup
// ============================================================
void RedisServer::close_client(int fd) {
    {
        std::lock_guard<std::mutex> lk(pubsub_mtx);
        auto it = clients.find(fd);
        if (it != clients.end())
            for (const auto& ch : it->second.subscriptions) {
                pubsub_channels[ch].erase(fd);
                if (pubsub_channels[ch].empty()) pubsub_channels.erase(ch);
            }
    }
    if (client_blocked_on.count(fd)) {
        for (const auto& key : client_blocked_on[fd]) waiting_clients[key].remove(fd);
        client_blocked_on.erase(fd);
    }

    // TLS Cleanup
    if (client_ssl.count(fd)) {
        SSL_free(client_ssl[fd]);
        client_ssl.erase(fd);
    }

    clients.erase(fd);
    replica_fds.erase(std::remove(replica_fds.begin(), replica_fds.end(), fd), replica_fds.end());
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
}

// ============================================================
// Run Loop
// ============================================================
void RedisServer::setup_signal_handlers() {
    struct sigaction sa{};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT,  &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);
}

void RedisServer::run() {
    setup_signal_handlers();
    init_ssl(); // Boot up TLS if configured
    Logger::info("Starting server on port " + std::to_string(port));

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(port);
    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        Logger::error("bind() failed"); return;
    }
    listen(server_fd, 128); make_socket_non_blocking(server_fd);

    epoll_fd = epoll_create1(0);
    auto epoll_add = [&](int fd) {
        struct epoll_event ev{}; ev.events = EPOLLIN | EPOLLET; ev.data.fd = fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
    };
    epoll_add(server_fd);

    if (!Config::unixsocket.empty()) {
        unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (unix_fd >= 0) {
            unlink(Config::unixsocket.c_str());
            struct sockaddr_un un_addr{}; un_addr.sun_family = AF_UNIX;
            strncpy(un_addr.sun_path, Config::unixsocket.c_str(), sizeof(un_addr.sun_path)-1);
            if (bind(unix_fd, reinterpret_cast<struct sockaddr*>(&un_addr), sizeof(un_addr)) == 0) {
                listen(unix_fd, 128); make_socket_non_blocking(unix_fd); epoll_add(unix_fd);
            }
        }
    }

    if (leader_port > 0) {
        leader_fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in la{}; la.sin_family = AF_INET; la.sin_port = htons(leader_port);
        inet_pton(AF_INET, "127.0.0.1", &la.sin_addr);
        if (connect(leader_fd, reinterpret_cast<struct sockaddr*>(&la), sizeof(la)) == 0) {
            make_socket_non_blocking(leader_fd); epoll_add(leader_fd); clients[leader_fd].authenticated = true;
            auto send_resp = [&](const std::vector<std::string>& a) {
                std::string s = "*"+std::to_string(a.size())+"\r\n";
                for (const auto& x : a) s += "$"+std::to_string(x.size())+"\r\n"+x+"\r\n";
                secure_send(leader_fd, s);
            };
            send_resp({"REPLCONF", "listening-port", std::to_string(port)});
            send_resp({"REPLCONF", "capa", "eof"});
            send_resp({"PSYNC", "?", "-1"});
        }
    }

    //start_heartbeat_thread();

    struct epoll_event events[64];
    while (!g_shutdown.load()) {
        int n = epoll_wait(epoll_fd, events, 64, 1000);
        check_heartbeats();
        store.maybe_auto_save();
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            if (fd == server_fd || fd == unix_fd) handle_new_connection(fd);
            else handle_client_data(fd);
        }
    }

    TxState tx; bool auth = true;
    store.execute_command({"SAVE"}, tx, auth, 2);
    std::vector<int> all_fds; for (const auto& [fd,_] : clients) all_fds.push_back(fd);
    for (int fd : all_fds) close_client(fd);
}

void RedisServer::handle_new_connection(int listening_fd) {
    int client_fd = accept(listening_fd, nullptr, nullptr);
    if (client_fd < 0) return;

    if (ssl_ctx && listening_fd == server_fd) {
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

        SSL* ssl = SSL_new(ssl_ctx);
        SSL_set_fd(ssl, client_fd);
        
        if (SSL_accept(ssl) <= 0) {
            Logger::error("TLS Handshake failed (or timed out). Dropping connection.");
            SSL_free(ssl);
            close(client_fd);
            return;
        }
        
        tv.tv_sec = 0;
        setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
        
        client_ssl[client_fd] = ssl;
    }

    make_socket_non_blocking(client_fd);
    struct epoll_event ev{}; ev.events = EPOLLIN | EPOLLET; ev.data.fd = client_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

    ClientState& s = clients[client_fd];
    s.read_buf = ""; s.authenticated = Config::requirepass.empty();
}

void RedisServer::handle_client_data(int fd) {
    char buf[4096];
    SSL* ssl = client_ssl.count(fd) ? client_ssl[fd] : nullptr;

    while (true) {
        // Read from the encrypted TLS stream, OR the raw TCP stream!
        ssize_t n = ssl ? SSL_read(ssl, buf, sizeof(buf)) : read(fd, buf, sizeof(buf));
        if (n > 0) { clients[fd].read_buf.append(buf, n); }
        else if (n == 0) { close_client(fd); return; }
        else {
            if (ssl) {
                int err = SSL_get_error(ssl, n);
                // Standard OpenSSL "I'm empty" signal
                if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) break;
                // NEW: OpenSSL sometimes throws SYSCALL when the OS returns EAGAIN. Ignore it!
                if (err == SSL_ERROR_SYSCALL && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            }
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

        std::string cmd = args[0]; for (char& c : cmd) c = toupper(c);

        if (cmd == "HELLO") {
            int v = 2; if (args.size() >= 2) { try { v = std::stoi(args[1]); } catch (...) {} }
            if (v == 3) {
                state.resp_version = 3;
                secure_send(fd, "%2\r\n$6\r\nserver\r\n$10\r\nredis-lite\r\n$7\r\nversion\r\n$5\r\n7.0.0\r\n");
            } else {
                state.resp_version = 2;
                secure_send(fd, "*4\r\n$6\r\nserver\r\n$10\r\nredis-lite\r\n$7\r\nversion\r\n$5\r\n7.0.0\r\n");
            }
            continue;
        }

        if ((cmd == "REPLCONF" || cmd == "PSYNC") && leader_port == 0) {
            handle_replication_handshake(fd, args); continue;
        }
        if (fd == leader_fd && cmd == "PING") continue;
        if (cmd == "SUBSCRIBE" || cmd == "UNSUBSCRIBE" || cmd == "PUBLISH") {
            handle_pubsub(fd, args); continue;
        }

        std::string response = store.execute_command(args, state.tx, state.authenticated, state.resp_version);

        if (response == "*WAIT\r\n") {
            for (size_t i = 1; i < args.size() - 1; ++i) {
                waiting_clients[args[i]].push_back(fd); client_blocked_on[fd].push_back(args[i]);
            }
            continue; 
        }

        if (fd != leader_fd) secure_send(fd, response);

        if (cmd == "LPUSH" || cmd == "RPUSH") {
            std::string list_key = args[1];
            if (waiting_clients.count(list_key) && !waiting_clients[list_key].empty()) {
                int parked_fd = waiting_clients[list_key].front(); waiting_clients[list_key].pop_front();
                if (client_blocked_on.count(parked_fd)) client_blocked_on.erase(parked_fd);
                TxState dummy_tx; bool dummy_auth = true;
                std::string wake_response = store.execute_command({"BLPOP", list_key, "0"}, dummy_tx, dummy_auth, clients[parked_fd].resp_version);
                secure_send(parked_fd, wake_response);
            }
        }

        if (leader_port == 0 && WRITE_COMMANDS.count(cmd) && response[0] != '-') replicate(args);
    }
}

void RedisServer::check_heartbeats() {
    int64_t now = get_time_ms();
    if (leader_port == 0 && now - last_ping_time > 2000) {
        for (int rep_fd : replica_fds) {
            secure_send(rep_fd, "*1\r\n$4\r\nPING\r\n");
            secure_send(rep_fd, "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
        }
        last_ping_time = now;
    }
    if (leader_port > 0 && leader_fd != -1 && now - last_heartbeat > 5000) {
        close_client(leader_fd); leader_fd = -1; leader_port = 0;
        repl_id = generate_repl_id();  
    }
}

void RedisServer::start_heartbeat_thread() {
    if (Config::router_port <= 0 || Config::router_port == port) {
        return; 
    }
    std::thread([this]() {
        while (!g_shutdown.load()) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock >= 0) {
                struct sockaddr_in ra{}; ra.sin_family = AF_INET; ra.sin_port = htons(Config::router_port);
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