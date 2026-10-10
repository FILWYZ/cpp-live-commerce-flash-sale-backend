#include "network/reactor/tcp_acceptor.h"

#include "network/reactor/channel.h"
#include "network/reactor/event_loop.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace live::network {
namespace {

bool setNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

}  // namespace

TcpAcceptor::TcpAcceptor(EventLoop* loop, std::string host, std::uint16_t port,
                         NewConnectionCallback callback)
    : loop_(loop), host_(std::move(host)), requested_port_(port), callback_(std::move(callback)) {}

TcpAcceptor::~TcpAcceptor() { stop(); }

common::Status TcpAcceptor::start() {
    if (loop_ == nullptr || !callback_) return common::Status::InvalidArgument("invalid TCP acceptor");
    if (listen_fd_ >= 0) return common::Status::AlreadyExists("acceptor already started");
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return common::Status::Internal("socket failed");
    int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (!setNonBlocking(listen_fd_)) {
        stop();
        return common::Status::Internal("failed to set non-blocking socket");
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(requested_port_);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    if (!host_.empty() && host_ != "0.0.0.0") {
        if (::inet_pton(AF_INET, host_.c_str(), &address.sin_addr) != 1) {
            stop();
            return common::Status::InvalidArgument("host must be an IPv4 address");
        }
    }
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        const std::string message = std::string("bind failed: ") + std::strerror(errno);
        stop();
        return common::Status::Internal(message);
    }
    if (::listen(listen_fd_, SOMAXCONN) < 0) {
        const std::string message = std::string("listen failed: ") + std::strerror(errno);
        stop();
        return common::Status::Internal(message);
    }
    sockaddr_in bound{};
    socklen_t length = sizeof(bound);
    if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &length) == 0) {
        bound_port_ = ntohs(bound.sin_port);
    }
    channel_ = std::make_unique<Channel>(loop_, listen_fd_);
    channel_->setReadCallback([this] { handleRead(); });
    channel_->enableReading();
    return common::Status::Ok();
}

void TcpAcceptor::stop() {
    if (channel_ != nullptr) {
        loop_->removeChannel(channel_.get());
        channel_.reset();
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    bound_port_ = 0;
}

void TcpAcceptor::handleRead() {
    while (listen_fd_ >= 0) {
        sockaddr_in address{};
        socklen_t length = sizeof(address);
        const int connection_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&address), &length);
        if (connection_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
            return;
        }
        if (!setNonBlocking(connection_fd)) {
            ::close(connection_fd);
            continue;
        }
        char peer_address[INET_ADDRSTRLEN]{};
        const char* peer = ::inet_ntop(AF_INET, &address.sin_addr, peer_address, sizeof(peer_address));
        callback_(connection_fd, peer == nullptr ? std::string{} : std::string(peer));
    }
}

}  // namespace live::network
