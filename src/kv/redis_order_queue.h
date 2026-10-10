#pragma once

#include "messaging/order_queue.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace live::kv { class RedisKVStore; }

namespace live::kv {

class RedisOrderQueue final : public ::live::messaging::IOrderQueue {
public:
    RedisOrderQueue(RedisKVStore* redis, std::string queue_key, std::size_t max_length = 100000)
        : redis_(redis), queue_key_(std::move(queue_key)), max_length_(max_length) {}

    // BRPOP is blocking. A consumer must use a dedicated Redis connection so
    // request-path GET/SET/EVAL operations are never serialized behind it.
    common::Status connectBlockingConsumer(const std::string& host, std::uint16_t port,
                                           std::chrono::milliseconds timeout = std::chrono::seconds(2),
                                           std::size_t pool_size = 8);

    common::Status push(const ::live::messaging::FlashSaleOrderMessage& message) override;
    common::Status pop(::live::messaging::FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) override;
    std::size_t pending() const override;
    common::Status peekRaw(std::size_t limit, std::vector<std::string>* payloads) const;
    common::Status peek(std::size_t limit, std::vector<::live::messaging::FlashSaleOrderMessage>* messages) const;
    common::Status replayTo(RedisOrderQueue* destination, std::size_t limit, std::size_t* moved);

private:
    RedisKVStore* redis_{nullptr};
    std::unique_ptr<RedisKVStore> blocking_redis_;
    std::string queue_key_;
    std::size_t max_length_{100000};
};

}  // namespace live::kv
