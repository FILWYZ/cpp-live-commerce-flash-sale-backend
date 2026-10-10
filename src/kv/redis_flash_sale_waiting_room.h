#pragma once

#include "common/error/status.h"

#include <chrono>
#include <cstddef>
#include <string>
#include <utility>

namespace live::kv { class RedisKVStore; }
namespace live::messaging { struct FlashSaleOrderMessage; }

namespace live::kv {

// Shared per-campaign FIFO ticket queue. Claims are leased so a process crash
// returns a ticket to circulation after the lease expires.
class RedisFlashSaleWaitingRoom final {
public:
    RedisFlashSaleWaitingRoom(RedisKVStore* redis, std::string campaign_id,
                              std::size_t capacity = 100000)
        : redis_(redis), campaign_id_(std::move(campaign_id)), capacity_(capacity) {}

    common::Status enqueue(const ::live::messaging::FlashSaleOrderMessage& message,
                           std::size_t* position);
    common::Status position(const std::string& ticket, std::size_t* position) const;
    common::Status size(std::size_t* count) const;
    common::Status claim(const std::string& lease_owner, std::chrono::milliseconds lease,
                         std::string* ticket, ::live::messaging::FlashSaleOrderMessage* message);
    common::Status complete(const std::string& ticket, const std::string& lease_owner,
                            bool* completed);
    common::Status release(const std::string& ticket, const std::string& lease_owner,
                           bool* released);

private:
    std::string queueKey() const;
    std::string payloadKey() const;
    std::string leasesKey() const;
    std::string leaseExpiryKey() const;
    std::string sequenceKey() const;

    RedisKVStore* redis_{nullptr};
    std::string campaign_id_;
    std::size_t capacity_{100000};
};

}  // namespace live::kv
