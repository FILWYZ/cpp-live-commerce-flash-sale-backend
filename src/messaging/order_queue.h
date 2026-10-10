#pragma once

#include "common/error/status.h"
#include "messaging/order_message.h"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <string>

namespace live::messaging {

class IOrderQueue {
public:
    virtual ~IOrderQueue() = default;
    virtual common::Status push(const FlashSaleOrderMessage& message) = 0;
    virtual common::Status pop(FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) = 0;
    // Durable transports acknowledge only after business processing succeeds.
    // In-process queues have no broker acknowledgement, so the default is a no-op.
    virtual common::Status acknowledge(const FlashSaleOrderMessage& message, bool processed) {
        (void)message; (void)processed;
        return common::Status::Ok();
    }
    virtual std::size_t pending() const = 0;
};

class InMemoryOrderQueue final : public IOrderQueue {
public:
    common::Status push(const FlashSaleOrderMessage& message) override;
    common::Status pop(FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) override;
    std::size_t pending() const override;

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::deque<FlashSaleOrderMessage> messages_;
};

}  // namespace live::messaging
