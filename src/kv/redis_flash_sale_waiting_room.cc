#include "kv/redis_flash_sale_waiting_room.h"

#include "kv/redis_kv_store.h"
#include "messaging/order_message.h"

#include <charconv>
#include <cstdint>

namespace live::kv {
namespace {

constexpr const char* kEnqueueScript =
    "local rank=redis.call('ZRANK',KEYS[1],ARGV[1]); if rank then return rank+1 end; "
    "if redis.call('ZCARD',KEYS[1])>=tonumber(ARGV[3]) then return -1 end; "
    "local seq=redis.call('INCR',KEYS[5]); redis.call('ZADD',KEYS[1],seq,ARGV[1]); "
    "redis.call('HSET',KEYS[2],ARGV[1],ARGV[2]); return redis.call('ZCARD',KEYS[1])";

constexpr const char* kPositionScript =
    "local rank=redis.call('ZRANK',KEYS[1],ARGV[1]); if not rank then return 0 end; return rank+1";

constexpr const char* kSizeScript = "return redis.call('ZCARD',KEYS[1])";

constexpr const char* kClaimScript =
    "local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000); "
    "local candidates=redis.call('ZRANGE',KEYS[1],0,tonumber(ARGV[3])-1); "
    "for _,ticket in ipairs(candidates) do "
    "local expires=tonumber(redis.call('ZSCORE',KEYS[4],ticket) or '0'); "
    "if expires<=now then local payload=redis.call('HGET',KEYS[2],ticket); "
    "if payload then redis.call('HSET',KEYS[3],ticket,ARGV[1]); "
    "redis.call('ZADD',KEYS[4],now+tonumber(ARGV[2]),ticket); "
    "return string.len(ticket)..':'..ticket..payload; "
    "else redis.call('ZREM',KEYS[1],ticket); end end end; return nil";

constexpr const char* kCompleteScript =
    "if redis.call('HGET',KEYS[3],ARGV[1])~=ARGV[2] then return 0 end; "
    "redis.call('ZREM',KEYS[1],ARGV[1]); redis.call('HDEL',KEYS[2],ARGV[1]); "
    "redis.call('HDEL',KEYS[3],ARGV[1]); redis.call('ZREM',KEYS[4],ARGV[1]); "
    "if redis.call('ZCARD',KEYS[1])==0 then redis.call('DEL',KEYS[1],KEYS[2],KEYS[3],KEYS[4],KEYS[5]); end; return 1";

constexpr const char* kReleaseScript =
    "if redis.call('HGET',KEYS[1],ARGV[1])~=ARGV[2] then return 0 end; "
    "redis.call('HDEL',KEYS[1],ARGV[1]); redis.call('ZREM',KEYS[2],ARGV[1]); return 1";

common::Status parseClaim(const std::string& result, std::string* ticket,
                          ::live::messaging::FlashSaleOrderMessage* message) {
    if (ticket == nullptr || message == nullptr) return common::Status::InvalidArgument("claim outputs must not be null");
    const auto colon = result.find(':');
    if (colon == std::string::npos) return common::Status::Internal("corrupt Redis waiting-room claim");
    std::size_t ticket_size = 0;
    const auto parsed = std::from_chars(result.data(), result.data() + colon, ticket_size);
    if (parsed.ec != std::errc{} || parsed.ptr != result.data() + colon ||
        ticket_size > result.size() - colon - 1) {
        return common::Status::Internal("corrupt Redis waiting-room claim length");
    }
    const auto payload_start = colon + 1 + ticket_size;
    ticket->assign(result.data() + colon + 1, ticket_size);
    if (ticket->empty()) return common::Status::Internal("Redis waiting-room claim has an empty ticket");
    return ::live::messaging::FlashSaleOrderMessage::deserialize(result.substr(payload_start), message);
}

}  // namespace

std::string RedisFlashSaleWaitingRoom::queueKey() const {
    return "flashsale:waiting:{" + campaign_id_ + "}:queue";
}
std::string RedisFlashSaleWaitingRoom::payloadKey() const {
    return "flashsale:waiting:{" + campaign_id_ + "}:payloads";
}
std::string RedisFlashSaleWaitingRoom::leasesKey() const {
    return "flashsale:waiting:{" + campaign_id_ + "}:leases";
}
std::string RedisFlashSaleWaitingRoom::leaseExpiryKey() const {
    return "flashsale:waiting:{" + campaign_id_ + "}:lease-expiry";
}
std::string RedisFlashSaleWaitingRoom::sequenceKey() const {
    return "flashsale:waiting:{" + campaign_id_ + "}:sequence";
}

common::Status RedisFlashSaleWaitingRoom::enqueue(
    const ::live::messaging::FlashSaleOrderMessage& message, std::size_t* position) {
    if (redis_ == nullptr || campaign_id_.empty() || capacity_ == 0 || position == nullptr ||
        message.promotion_id != campaign_id_ || message.idempotency_key.empty()) {
        return common::Status::InvalidArgument("invalid Redis waiting-room enqueue");
    }
    if (const auto status = message.validate(); !status.ok()) return status;
    std::int64_t rank = 0;
    if (const auto status = redis_->evalInteger(kEnqueueScript,
            {queueKey(), payloadKey(), leasesKey(), leaseExpiryKey(), sequenceKey()},
            {message.idempotency_key, message.serialize(), std::to_string(capacity_)}, &rank); !status.ok()) return status;
    if (rank < 0) return common::Status::ResourceExhausted("flash-sale waiting room is full");
    *position = static_cast<std::size_t>(rank);
    return common::Status::Ok();
}

common::Status RedisFlashSaleWaitingRoom::position(const std::string& ticket, std::size_t* position) const {
    if (redis_ == nullptr || campaign_id_.empty() || ticket.empty() || position == nullptr)
        return common::Status::InvalidArgument("invalid Redis waiting-room position query");
    std::int64_t rank = 0;
    if (const auto status = redis_->evalInteger(kPositionScript, {queueKey()}, {ticket}, &rank); !status.ok()) return status;
    *position = static_cast<std::size_t>(rank);
    return common::Status::Ok();
}

common::Status RedisFlashSaleWaitingRoom::size(std::size_t* count) const {
    if (redis_ == nullptr || campaign_id_.empty() || count == nullptr)
        return common::Status::InvalidArgument("invalid Redis waiting-room size query");
    std::int64_t value = 0;
    if (const auto status = redis_->evalInteger(kSizeScript, {queueKey()}, {}, &value); !status.ok()) return status;
    *count = static_cast<std::size_t>(value);
    return common::Status::Ok();
}

common::Status RedisFlashSaleWaitingRoom::claim(
    const std::string& lease_owner, std::chrono::milliseconds lease,
    std::string* ticket, ::live::messaging::FlashSaleOrderMessage* message) {
    if (redis_ == nullptr || campaign_id_.empty() || lease_owner.empty() ||
        lease.count() <= 0 || ticket == nullptr || message == nullptr) {
        return common::Status::InvalidArgument("invalid Redis waiting-room claim");
    }
    std::string encoded;
    if (const auto status = redis_->evalString(kClaimScript,
            {queueKey(), payloadKey(), leasesKey(), leaseExpiryKey()},
            {lease_owner, std::to_string(lease.count()), "64"}, &encoded); !status.ok()) return status;
    if (encoded.empty()) return common::Status::Incomplete("no waiting-room ticket is claimable");
    return parseClaim(encoded, ticket, message);
}

common::Status RedisFlashSaleWaitingRoom::complete(const std::string& ticket,
    const std::string& lease_owner, bool* completed) {
    if (redis_ == nullptr || campaign_id_.empty() || ticket.empty() || lease_owner.empty() || completed == nullptr)
        return common::Status::InvalidArgument("invalid Redis waiting-room completion");
    std::int64_t result = 0;
    if (const auto status = redis_->evalInteger(kCompleteScript,
            {queueKey(), payloadKey(), leasesKey(), leaseExpiryKey(), sequenceKey()},
            {ticket, lease_owner}, &result); !status.ok()) return status;
    *completed = result == 1;
    return common::Status::Ok();
}

common::Status RedisFlashSaleWaitingRoom::release(const std::string& ticket,
    const std::string& lease_owner, bool* released) {
    if (redis_ == nullptr || campaign_id_.empty() || ticket.empty() || lease_owner.empty() || released == nullptr)
        return common::Status::InvalidArgument("invalid Redis waiting-room lease release");
    std::int64_t result = 0;
    if (const auto status = redis_->evalInteger(kReleaseScript,
            {leasesKey(), leaseExpiryKey()}, {ticket, lease_owner}, &result); !status.ok()) return status;
    *released = result == 1;
    return common::Status::Ok();
}

}  // namespace live::kv
