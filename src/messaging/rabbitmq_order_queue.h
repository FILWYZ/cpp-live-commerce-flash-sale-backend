#pragma once

#include "messaging/in_memory_broker.h"
#include "messaging/order_queue.h"

#include <memory>
#include <string>
#include <vector>

namespace live::messaging {

// Durable RabbitMQ transport for flash-sale commands and transactional outbox
// events. Publisher confirms are enabled and command deliveries use manual ACK.
class RabbitMqOrderQueue final : public IOrderQueue, public IEventPublisher {
public:
    RabbitMqOrderQueue();
    ~RabbitMqOrderQueue() override;
    RabbitMqOrderQueue(const RabbitMqOrderQueue&) = delete;
    RabbitMqOrderQueue& operator=(const RabbitMqOrderQueue&) = delete;

    common::Status start(const std::string& host, int port, const std::string& username,
                         const std::string& password, const std::string& vhost,
                         const std::string& exchange, const std::string& command_queue,
                         const std::string& routing_key, bool enable_consumer);
    void stop();
    common::Status push(const FlashSaleOrderMessage& message) override;
    common::Status pop(FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) override;
    common::Status acknowledge(const FlashSaleOrderMessage& message, bool processed) override;
    common::Status peekDlq(std::size_t limit, std::vector<FlashSaleOrderMessage>* messages);
    common::Status replayDlq(RabbitMqOrderQueue* destination, std::size_t limit, std::size_t* replayed);
    std::size_t pending() const override;
    common::Status publish(const Event& event) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace live::messaging
