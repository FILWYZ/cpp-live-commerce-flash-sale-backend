#pragma once

#include "business/promotion/flash_sale_gate.h"

#include <cstddef>
#include <string>

namespace live::kv { class RedisKVStore; }

namespace live::business::promotion {

// Redis-backed admission gate for multi-process/multi-instance flash sales.
// It intentionally implements admission only; inventory remains an
// independent authoritative operation and must be compensated on failure.
class RedisFlashSaleGate final : public IAdmissionGate {
public:
    explicit RedisFlashSaleGate(::live::kv::RedisKVStore* redis) : redis_(redis) {}

    common::Status configure(const std::string& promotion_id, std::size_t quota) override;
    common::Status synchronize(const std::string& promotion_id, AdmissionState state) override;
    common::Status preheat(const std::string& promotion_id) override;
    common::Status start(const std::string& promotion_id) override;
    common::Status pause(const std::string& promotion_id) override;
    common::Status resume(const std::string& promotion_id) override;
    common::Status finish(const std::string& promotion_id) override;
    common::Status tryAcquire(const std::string& promotion_id, const std::string& user_id,
                              AdmissionDecision* decision) override;
    common::Status rollback(const std::string& promotion_id, const std::string& user_id) override;

private:
    static std::string metaKey(const std::string& promotion_id);
    static std::string usersKey(const std::string& promotion_id);
    common::Status transition(const std::string& promotion_id, const char* expected, const char* next);

    ::live::kv::RedisKVStore* redis_;
};

}  // namespace live::business::promotion
