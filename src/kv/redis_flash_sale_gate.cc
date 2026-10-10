#include "kv/redis_flash_sale_gate.h"

#include "kv/redis_kv_store.h"

#include <cstdint>
#include <vector>

namespace live::business::promotion {
namespace {

constexpr const char* kConfigureScript =
    "if redis.call('EXISTS', KEYS[1])==1 then "
    "if redis.call('HGET', KEYS[1], 'quota')~=ARGV[1] then "
    "if redis.call('HGET', KEYS[1], 'state')~='created' or (tonumber(redis.call('HGET', KEYS[1], 'admitted')) or 0)>0 then return 0 end; "
    "redis.call('HSET', KEYS[1], 'quota', ARGV[1]); return 3 end; return 2 end; "
    "redis.call('HSET', KEYS[1], 'state', 'created', 'quota', ARGV[1], 'admitted', '0'); return 1";
constexpr const char* kSynchronizeScript =
    "if redis.call('EXISTS', KEYS[1])==0 then return 0 end; "
    "redis.call('HSET', KEYS[1], 'state', ARGV[1]); return 1";
constexpr const char* kTransitionScript =
    "local state=redis.call('HGET', KEYS[1], 'state'); "
    "if state==ARGV[2] then return 1 end; "
    "if ARGV[2]=='preheating' and (state=='running' or state=='finished') then return 1 end; "
    "if ARGV[2]=='running' and state=='finished' then return 1 end; "
    "if ARGV[2]=='finished' and state=='paused' then redis.call('HSET', KEYS[1], 'state', 'finished'); return 1 end; "
    "if state ~= ARGV[1] then return 0 end; "
    "redis.call('HSET', KEYS[1], 'state', ARGV[2]); return 1";
constexpr const char* kAcquireScript =
    "local state=redis.call('HGET', KEYS[1], 'state'); "
    "if state=='created' or state=='preheating' then return -1 end; "
    "if state=='finished' then return -2 end; if state=='paused' then return -3 end; "
    "if redis.call('SISMEMBER', KEYS[2], ARGV[1])==1 then return 2 end; "
    "local admitted=tonumber(redis.call('HGET', KEYS[1], 'admitted')) or 0; "
    "local quota=tonumber(redis.call('HGET', KEYS[1], 'quota')) or 0; "
    "if admitted>=quota then return 0 end; "
    "redis.call('SADD', KEYS[2], ARGV[1]); redis.call('HINCRBY', KEYS[1], 'admitted', 1); return 1";
constexpr const char* kRollbackScript =
    "if redis.call('SREM', KEYS[2], ARGV[1])==1 then redis.call('HINCRBY', KEYS[1], 'admitted', -1) end; return 1";

common::Status run(::live::kv::RedisKVStore* redis, const char* script,
                   const std::vector<std::string>& keys, const std::vector<std::string>& arguments,
                   std::int64_t* result) {
    if (redis == nullptr) return common::Status::FailedPrecondition("Redis admission gate is not configured");
    return redis->evalInteger(script, keys, arguments, result);
}

}  // namespace

std::string RedisFlashSaleGate::metaKey(const std::string& promotion_id) {
    return "flash_admission:{" + promotion_id + "}:meta";
}

std::string RedisFlashSaleGate::usersKey(const std::string& promotion_id) {
    return "flash_admission:{" + promotion_id + "}:users";
}

common::Status RedisFlashSaleGate::configure(const std::string& promotion_id, std::size_t quota) {
    if (promotion_id.empty() || quota == 0) return common::Status::InvalidArgument("invalid Redis admission gate");
    std::int64_t result = 0;
    const auto status = run(redis_, kConfigureScript, {metaKey(promotion_id), usersKey(promotion_id)},
                            {std::to_string(quota)}, &result);
    if (!status.ok()) return status;
    return result == 0 ? common::Status::FailedPrecondition("Redis admission quota mismatch") : common::Status::Ok();
}

common::Status RedisFlashSaleGate::synchronize(const std::string& promotion_id, AdmissionState state) {
    const char* value = "created";
    switch (state) {
        case AdmissionState::kCreated: value = "created"; break;
        case AdmissionState::kPreheating: value = "preheating"; break;
        case AdmissionState::kRunning: value = "running"; break;
        case AdmissionState::kPaused: value = "paused"; break;
        case AdmissionState::kFinished: value = "finished"; break;
    }
    std::int64_t result = 0;
    const auto status = run(redis_, kSynchronizeScript, {metaKey(promotion_id)}, {value}, &result);
    if (!status.ok()) return status;
    return result == 1 ? common::Status::Ok() : common::Status::NotFound("Redis campaign admission state is missing");
}

common::Status RedisFlashSaleGate::transition(const std::string& promotion_id, const char* expected, const char* next) {
    std::int64_t result = 0;
    const auto status = run(redis_, kTransitionScript, {metaKey(promotion_id)}, {expected, next}, &result);
    if (!status.ok()) return status;
    return result == 1 ? common::Status::Ok() : common::Status::FailedPrecondition("invalid Redis gate transition");
}

common::Status RedisFlashSaleGate::preheat(const std::string& promotion_id) {
    return transition(promotion_id, "created", "preheating");
}

common::Status RedisFlashSaleGate::start(const std::string& promotion_id) {
    return transition(promotion_id, "preheating", "running");
}

common::Status RedisFlashSaleGate::pause(const std::string& promotion_id) {
    return transition(promotion_id, "running", "paused");
}

common::Status RedisFlashSaleGate::resume(const std::string& promotion_id) {
    return transition(promotion_id, "paused", "running");
}

common::Status RedisFlashSaleGate::finish(const std::string& promotion_id) {
    return transition(promotion_id, "running", "finished");
}

common::Status RedisFlashSaleGate::tryAcquire(const std::string& promotion_id, const std::string& user_id,
                                              AdmissionDecision* decision) {
    if (user_id.empty() || decision == nullptr) return common::Status::InvalidArgument("invalid Redis admission request");
    std::int64_t result = 0;
    const auto status = run(redis_, kAcquireScript, {metaKey(promotion_id), usersKey(promotion_id)}, {user_id}, &result);
    if (!status.ok()) return status;
    decision->code = result == 1 ? AdmissionCode::kAccepted :
        (result == 2 ? AdmissionCode::kDuplicate :
         (result == 0 ? AdmissionCode::kExhausted :
          (result == -2 ? AdmissionCode::kFinished :
           (result == -3 ? AdmissionCode::kPaused : AdmissionCode::kNotStarted))));
    return common::Status::Ok();
}

common::Status RedisFlashSaleGate::rollback(const std::string& promotion_id, const std::string& user_id) {
    if (user_id.empty()) return common::Status::InvalidArgument("user id must not be empty");
    std::int64_t result = 0;
    return run(redis_, kRollbackScript, {metaKey(promotion_id), usersKey(promotion_id)}, {user_id}, &result);
}

}  // namespace live::business::promotion
