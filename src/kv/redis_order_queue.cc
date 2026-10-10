#include "kv/redis_order_queue.h"

#include "kv/redis_kv_store.h"

#include <vector>

namespace live::kv {
namespace {

std::string hashTag(const std::string& key) {
    const auto open = key.find('{');
    if (open == std::string::npos) return key;
    const auto close = key.find('}', open + 1);
    if (close == std::string::npos || close == open + 1) return key;
    return key.substr(open + 1, close - open - 1);
}

}  // namespace

common::Status RedisOrderQueue::connectBlockingConsumer(const std::string& host, std::uint16_t port,
                                                        std::chrono::milliseconds timeout, std::size_t pool_size) {
    auto connection = std::make_unique<RedisKVStore>();
    if (const auto status = connection->connect(host, port, timeout, pool_size); !status.ok()) return status;
    blocking_redis_ = std::move(connection);
    return common::Status::Ok();
}

common::Status RedisOrderQueue::push(const ::live::messaging::FlashSaleOrderMessage& message) {
    if (redis_ == nullptr || queue_key_.empty()) return common::Status::FailedPrecondition("Redis order queue is not configured");
    const std::string payload = message.serialize();
    if (payload.empty()) return common::Status::InvalidArgument("invalid order queue message");
    return redis_->listPushBounded(queue_key_, payload, max_length_);
}

common::Status RedisOrderQueue::pop(::live::messaging::FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) {
    RedisKVStore* consumer_redis = blocking_redis_ != nullptr ? blocking_redis_.get() : redis_;
    if (consumer_redis == nullptr || queue_key_.empty()) return common::Status::FailedPrecondition("Redis order queue is not configured");
    std::string payload;
    if (const auto status = consumer_redis->listBlockingPop(queue_key_, timeout, &payload); !status.ok()) return status;
    return ::live::messaging::FlashSaleOrderMessage::deserialize(payload, message);
}

std::size_t RedisOrderQueue::pending() const {
    if (redis_ == nullptr || queue_key_.empty()) return 0;
    std::size_t size = 0;
    return redis_->listLength(queue_key_, &size).ok() ? size : 0;
}

common::Status RedisOrderQueue::peekRaw(std::size_t limit, std::vector<std::string>* payloads) const {
    if (redis_ == nullptr || queue_key_.empty() || payloads == nullptr || limit == 0 || limit > 1000)
        return common::Status::InvalidArgument("invalid Redis order queue inspection");
    return redis_->listRange(queue_key_, 0, static_cast<std::int64_t>(limit - 1), payloads);
}

common::Status RedisOrderQueue::peek(std::size_t limit,
    std::vector<::live::messaging::FlashSaleOrderMessage>* messages) const {
    if (messages == nullptr) return common::Status::InvalidArgument("invalid Redis order queue inspection");
    std::vector<std::string> payloads;
    if (const auto status = peekRaw(limit, &payloads); !status.ok()) return status;
    messages->clear();
    messages->reserve(payloads.size());
    for (const auto& payload : payloads) {
        ::live::messaging::FlashSaleOrderMessage message;
        if (const auto status = ::live::messaging::FlashSaleOrderMessage::deserialize(payload, &message); !status.ok()) return status;
        messages->push_back(std::move(message));
    }
    return common::Status::Ok();
}

common::Status RedisOrderQueue::replayTo(RedisOrderQueue* destination, std::size_t limit, std::size_t* moved) {
    if (destination == nullptr || moved == nullptr || limit == 0 || limit > 1000 || redis_ == nullptr ||
        destination->redis_ != redis_ || queue_key_.empty() || destination->queue_key_.empty() ||
        hashTag(queue_key_) != hashTag(destination->queue_key_))
        return common::Status::InvalidArgument("invalid Redis DLQ replay target");
    *moved = 0;
    for (std::size_t i = 0; i < limit; ++i) {
        bool completed = false;
        for (int attempt = 0; attempt < 3 && !completed; ++attempt) {
            std::size_t length = 0;
            if (const auto status = redis_->listLength(queue_key_, &length); !status.ok()) return status;
            if (length == 0) return common::Status::Ok();
            std::vector<std::string> oldest;
            if (const auto status = redis_->listRange(queue_key_, static_cast<std::int64_t>(length - 1),
                                                       static_cast<std::int64_t>(length - 1), &oldest); !status.ok()) return status;
            if (oldest.empty()) return common::Status::Ok();
            ::live::messaging::FlashSaleOrderMessage decoded;
            if (const auto status = ::live::messaging::FlashSaleOrderMessage::deserialize(oldest.front(), &decoded); !status.ok())
                return common::Status::FailedPrecondition("oldest DLQ entry is malformed; inspect and repair it before replay");
            std::int64_t result = 0;
            if (const auto status = redis_->listMoveBounded(queue_key_, destination->queue_key_,
                    destination->max_length_, oldest.front(), &result); !status.ok()) return status;
            if (result == -2) continue;
            if (result == 0) return common::Status::Ok();
            if (result == -1) return common::Status::ResourceExhausted("destination Redis order queue is full");
            ++*moved;
            completed = true;
        }
        if (!completed) return common::Status::Aborted("DLQ changed repeatedly during replay; retry the operation");
    }
    return common::Status::Ok();
}

}  // namespace live::kv
