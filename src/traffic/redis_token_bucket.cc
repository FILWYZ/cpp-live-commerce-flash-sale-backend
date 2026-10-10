#include "traffic/redis_token_bucket.h"

#include "kv/redis_kv_store.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

namespace live::traffic {
namespace {

constexpr const char* kTryAcquireScript =
    "local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000); "
    "local capacity=tonumber(ARGV[1]); local rate=tonumber(ARGV[2]); local cost=tonumber(ARGV[3]); "
    "local tokens=tonumber(redis.call('HGET',KEYS[1],'tokens')) or capacity; "
    "local last=tonumber(redis.call('HGET',KEYS[1],'last_ms')) or now; "
    "if now>last then tokens=math.min(capacity,tokens+(now-last)*rate/1000); last=now end; "
    "local allowed=0; if tokens>=cost then tokens=tokens-cost; allowed=1 end; "
    "redis.call('HSET',KEYS[1],'tokens',tokens,'last_ms',last); "
    "redis.call('PEXPIRE',KEYS[1],ARGV[4]); return allowed";

std::uint64_t toMilli(double value) {
    if (!std::isfinite(value) || value <= 0.0 || value >= static_cast<double>(std::numeric_limits<std::uint64_t>::max() / 1000)) return 0;
    return static_cast<std::uint64_t>(std::round(value * 1000.0));
}

}  // namespace

RedisTokenBucket::RedisTokenBucket(::live::kv::RedisKVStore* redis, std::string key,
                                   double rate_per_second, double burst)
    : redis_(redis), key_(std::move(key)), rate_milli_per_second_(toMilli(rate_per_second)),
      capacity_milli_(toMilli(burst)), ttl_milliseconds_(0) {
    if (rate_milli_per_second_ != 0 && capacity_milli_ != 0) {
        const long double refill_ms = static_cast<long double>(capacity_milli_) * 1000.0L /
            static_cast<long double>(rate_milli_per_second_);
        const auto ttl = std::max<long double>(60000.0L, refill_ms * 2.0L);
        ttl_milliseconds_ = static_cast<std::uint64_t>(std::min<long double>(ttl,
            static_cast<long double>(std::numeric_limits<std::uint64_t>::max())));
    }
}

common::Status RedisTokenBucket::tryAcquire(bool* allowed, double tokens) {
    if (allowed == nullptr || redis_ == nullptr || key_.empty() || rate_milli_per_second_ == 0 ||
        capacity_milli_ == 0 || !std::isfinite(tokens) || tokens <= 0.0) {
        return common::Status::InvalidArgument("invalid Redis token bucket configuration or request");
    }
    const auto cost = toMilli(tokens);
    if (cost == 0 || cost > capacity_milli_) {
        *allowed = false;
        return common::Status::Ok();
    }
    std::int64_t result = 0;
    const auto status = redis_->evalInteger(kTryAcquireScript, {key_},
        {std::to_string(capacity_milli_), std::to_string(rate_milli_per_second_),
         std::to_string(cost), std::to_string(ttl_milliseconds_)}, &result);
    if (!status.ok()) return status;
    *allowed = result == 1;
    return common::Status::Ok();
}

}  // namespace live::traffic
