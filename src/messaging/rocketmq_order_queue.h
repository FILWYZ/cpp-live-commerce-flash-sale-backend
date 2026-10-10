#pragma once

#include "messaging/in_memory_broker.h"
#include "messaging/order_queue.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace live::messaging {

struct RocketMqDlqMessage {
    std::string message_id;
    std::string payload;
};

class RocketMqOrderQueue final : public IOrderQueue, public IEventPublisher {
public:
    RocketMqOrderQueue();
    ~RocketMqOrderQueue() override;
    RocketMqOrderQueue(const RocketMqOrderQueue&) = delete;
    RocketMqOrderQueue& operator=(const RocketMqOrderQueue&) = delete;

    common::Status start(const std::string& endpoint, const std::string& command_topic,
                         const std::string& consumer_group, bool enable_consumer = true,
                         bool enable_dlq_operations = false);
    void stop();
    common::Status push(const FlashSaleOrderMessage& message) override;
    common::Status pop(FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) override;
    common::Status acknowledge(const FlashSaleOrderMessage& message, bool processed) override;
    common::Status peekDlq(std::size_t limit, std::vector<RocketMqDlqMessage>* messages);
    common::Status replayDlq(std::size_t limit, std::size_t* replayed);
    std::size_t pending() const override { return 0; }  // RocketMQ owns the backlog/lag counters.
    common::Status publish(const Event& event) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace live::messaging
