#pragma once

#include "common/error/status.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace live::network {

class Channel;
class EventLoop;

class TcpAcceptor {
public:
    using NewConnectionCallback = std::function<void(int, const std::string&)>;

    TcpAcceptor(EventLoop* loop, std::string host, std::uint16_t port, NewConnectionCallback callback);
    ~TcpAcceptor();

    common::Status start();
    void stop();
    std::uint16_t port() const { return bound_port_; }

private:
    void handleRead();

    EventLoop* loop_;
    std::string host_;
    std::uint16_t requested_port_;
    std::uint16_t bound_port_{0};
    NewConnectionCallback callback_;
    int listen_fd_{-1};
    std::unique_ptr<Channel> channel_;
};

}  // namespace live::network
