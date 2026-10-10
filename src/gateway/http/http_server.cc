#include "gateway/http/http_server.h"

#include "gateway/http/http_parser.h"
#include "gateway/http/http_response.h"
#include "network/buffer.h"
#include "network/reactor/channel.h"
#include "network/reactor/event_loop.h"
#include "network/reactor/tcp_acceptor.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <sys/socket.h>
#include <unistd.h>

namespace live::gateway::http {
namespace {

std::string nextRequestId() {
    static std::atomic<std::uint64_t> sequence{1};
    return "req-" + std::to_string(sequence.fetch_add(1, std::memory_order_relaxed));
}

bool validRequestId(const std::string& value) {
    return !value.empty() && value.size() <= 128 &&
           value.find_first_of("\r\n\t") == std::string::npos;
}

}  // namespace

struct HttpServer::Connection : public std::enable_shared_from_this<HttpServer::Connection> {
    Connection(HttpServer* owner, int fd, std::string client_ip)
        : owner(owner), fd(fd), client_ip(std::move(client_ip)), channel(owner->loop_, fd) {}

    void start() {
        const auto weak = weak_from_this();
        channel.setReadCallback([weak] { if (auto self = weak.lock()) self->onRead(); });
        channel.setWriteCallback([weak] { if (auto self = weak.lock()) self->onWrite(); });
        channel.setCloseCallback([weak] { if (auto self = weak.lock()) self->close(); });
        channel.setErrorCallback([weak] { if (auto self = weak.lock()) self->close(); });
        channel.enableReading();
    }

    void onRead() {
        if (request_in_flight || closed) return;
        int saved_errno = 0;
        const ssize_t bytes = input.readFd(fd, &saved_errno);
        if (bytes == 0 || (bytes < 0 && saved_errno != EAGAIN && saved_errno != EWOULDBLOCK)) {
            close();
            return;
        }
        if (input.readableBytes() > 8 * 1024 * 1024 + 64 * 1024) {
            queueResponse(HttpResponse{413, "Payload Too Large", {}, "request too large"}, true);
            return;
        }
        while (input.readableBytes() != 0 && !closed && !request_in_flight) {
            HttpRequest request;
            request.client_ip = client_ip;
            std::size_t consumed = 0;
            const std::string raw(input.peek(), input.readableBytes());
            const auto parse_status = HttpRequestParser::parse(raw, &request, &consumed);
            if (parse_status.isIncomplete()) return;
            if (!parse_status.ok()) {
                queueResponse(HttpResponse{400, "Bad Request", {}, parse_status.message()}, true);
                return;
            }
            const auto request_id_it = request.headers.find("x-request-id");
            request.request_id = request_id_it != request.headers.end() && validRequestId(request_id_it->second)
                ? request_id_it->second : nextRequestId();
            const auto connection_it = request.headers.find("connection");
            const bool keep_alive = connection_it == request.headers.end() ||
                connection_it->second.find("close") == std::string::npos;
            input.retrieve(consumed);
            if (owner->worker_pool_ != nullptr) {
                request_in_flight = true;
                channel.disableReading();
                const auto weak = weak_from_this();
                HttpServer* server = owner;
                const auto started_at = std::chrono::steady_clock::now();
                if (!server->worker_pool_->submit([server, weak, request = std::move(request), keep_alive, started_at]() mutable {
                    auto response = server->router_.dispatch(request);
                    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - started_at).count();
                    server->loop_->runInLoop([weak, request = std::move(request), response = std::move(response),
                                              keep_alive, elapsed]() mutable {
                        if (auto self = weak.lock()) self->onAsyncResponse(std::move(request), std::move(response), keep_alive,
                                                                            static_cast<std::uint64_t>(std::max<std::int64_t>(0, elapsed)));
                    });
                })) {
                    request_in_flight = false;
                    queueResponse(HttpResponse{503, "Service Unavailable", {}, "request queue is full"}, true);
                }
                return;
            }
            const auto started_at = std::chrono::steady_clock::now();
            auto response = owner->router_.dispatch(request);
            response.headers["X-Request-Id"] = request.request_id;
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - started_at).count();
            owner->observe(request, response, elapsed);
            queueResponse(std::move(response), !keep_alive);
            if (!keep_alive) break;
        }
        if (output.readableBytes() != 0) channel.enableWriting();
    }

    void onAsyncResponse(HttpRequest request, HttpResponse response, bool keep_alive, std::uint64_t elapsed_us) {
        if (closed) return;
        response.headers["X-Request-Id"] = request.request_id;
        owner->observe(request, response, static_cast<std::int64_t>(elapsed_us));
        request_in_flight = false;
        queueResponse(std::move(response), !keep_alive);
    }

    void queueResponse(HttpResponse response, bool close_after_write) {
        response.headers["Connection"] = close_after_write ? "close" : "keep-alive";
        std::string wire = response.serialize();
        if (output.readableBytes() + wire.size() > 8 * 1024 * 1024) {
            output.retrieveAll();
            HttpResponse overload{503, "Service Unavailable", {}, "response queue is full"};
            overload.headers["Connection"] = "close";
            wire = overload.serialize();
            close_after_write = true;
        }
        output.append(wire);
        close_after_write_ = close_after_write_ || close_after_write;
        if (close_after_write_) channel.disableReading();
        channel.enableWriting();
    }

    void onWrite() {
        while (output.readableBytes() > 0) {
            int send_flags = 0;
#ifdef MSG_NOSIGNAL
            send_flags = MSG_NOSIGNAL;
#endif
            const ssize_t written = ::send(fd, output.peek(), output.readableBytes(), send_flags);
            if (written > 0) {
                output.retrieve(static_cast<std::size_t>(written));
                continue;
            }
            if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
            close();
            return;
        }
        if (close_after_write_) close();
        else {
            channel.enableReading();
            if (!request_in_flight && input.readableBytes() != 0) onRead();
        }
    }

    void close() {
        if (closed) return;
        closed = true;
        owner->removeConnection(fd);
    }

    HttpServer* owner;
    int fd;
    std::string client_ip;
    network::Channel channel;
    network::Buffer input;
    network::Buffer output;
    bool closed{false};
    bool close_after_write_{false};
    bool request_in_flight{false};
};

HttpServer::HttpServer(network::EventLoop* loop, std::string host, std::uint16_t port,
                       std::size_t max_connections, RequestObserver observer,
                       std::size_t worker_threads, std::size_t worker_queue_capacity)
    : loop_(loop), max_connections_(max_connections), observer_(std::move(observer)),
      acceptor_(std::make_unique<network::TcpAcceptor>(loop, std::move(host), port,
          [this](int fd, const std::string& client_ip) { onConnection(fd, client_ip); })) {
    if (worker_threads != 0) worker_pool_ = std::make_unique<common::ThreadPool>(worker_threads, worker_queue_capacity);
}

HttpServer::~HttpServer() { stop(); }

common::Status HttpServer::start() {
    if (loop_ == nullptr) return common::Status::InvalidArgument("event loop must not be null");
    return acceptor_->start();
}

void HttpServer::stop() {
    stopWorkers();
    stopInLoop();
}

void HttpServer::stopWorkers() {
    if (worker_pool_) worker_pool_->stop();
}

void HttpServer::stopAcceptingInLoop() {
    if (acceptor_) acceptor_->stop();
}

void HttpServer::stopInLoop() {
    stopAcceptingInLoop();
    while (!connections_.empty()) {
        const auto it = connections_.begin();
        removeConnection(it->first);
    }
}

void HttpServer::observe(const http::HttpRequest& request, const http::HttpResponse& response, std::int64_t elapsed_us) {
    if (observer_) observer_(request, response, static_cast<std::uint64_t>(std::max<std::int64_t>(0, elapsed_us)));
}

std::uint16_t HttpServer::port() const { return acceptor_ == nullptr ? 0 : acceptor_->port(); }

void HttpServer::onConnection(int fd, const std::string& client_ip) {
    if (connections_.size() >= max_connections_) {
        ::close(fd);
        return;
    }
    auto connection = std::make_shared<Connection>(this, fd, client_ip);
    connections_.emplace(fd, connection);
    active_connections_.store(connections_.size(), std::memory_order_relaxed);
    connection->start();
}

void HttpServer::removeConnection(int fd) {
    const auto it = connections_.find(fd);
    if (it == connections_.end()) return;
    loop_->removeChannel(&it->second->channel);
    ::close(fd);
    connections_.erase(it);
    active_connections_.store(connections_.size(), std::memory_order_relaxed);
}

}  // namespace live::gateway::http
