#pragma once

#include "common/error/status.h"

#include <cstdint>
#include <string>

namespace live::kv { class RedisKVStore; }

namespace live::traffic {

// Shared fixed-point token bucket. All Gateway instances using the same key
// consume from one Redis bucket, so configured rate and burst are cluster-wide.
class RedisTokenBucket {
public:
    RedisTokenBucket(::live::kv::RedisKVStore* redis, std::string key,
                     double rate_per_second, double burst);

    common::Status tryAcquire(bool* allowed, double tokens = 1.0);

private:
    ::live::kv::RedisKVStore* redis_;
    std::string key_;
    std::uint64_t rate_milli_per_second_;
    std::uint64_t capacity_milli_;
    std::uint64_t ttl_milliseconds_;
};

}  // namespace live::traffic
