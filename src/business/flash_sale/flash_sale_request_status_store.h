#pragma once

#include "common/error/status.h"

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace live::kv { class IKVStore; }

namespace live::business::flash_sale {

enum class FlashSaleRequestState { kQueued, kProcessing, kSoldOut, kRejected, kPendingPayment, kPaid, kExpired, kCancelled, kSubmitting };

struct FlashSaleRequestStatus {
    std::string request_id;
    std::string user_id;
    std::string order_id;
    FlashSaleRequestState state{FlashSaleRequestState::kQueued};
    std::string message;
    std::string promotion_id;

    FlashSaleRequestStatus() = default;
    FlashSaleRequestStatus(std::string request, std::string user, std::string order,
                           FlashSaleRequestState request_state, std::string detail,
                           std::string promotion = {})
        : request_id(std::move(request)), user_id(std::move(user)), order_id(std::move(order)),
          state(request_state), message(std::move(detail)), promotion_id(std::move(promotion)) {}
};

class IFlashSaleRequestStatusStore {
public:
    virtual ~IFlashSaleRequestStatusStore() = default;
    virtual common::Status putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) = 0;
    virtual common::Status put(const FlashSaleRequestStatus& status) = 0;
    virtual common::Status get(const std::string& request_id, FlashSaleRequestStatus* status) const = 0;
    virtual common::Status transition(const std::string& request_id, FlashSaleRequestState expected,
                                      FlashSaleRequestState next, bool* updated) = 0;
};

class InMemoryFlashSaleRequestStatusStore final : public IFlashSaleRequestStatusStore {
public:
    common::Status putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) override;
    common::Status put(const FlashSaleRequestStatus& status) override;
    common::Status get(const std::string& request_id, FlashSaleRequestStatus* status) const override;
    common::Status transition(const std::string& request_id, FlashSaleRequestState expected,
                              FlashSaleRequestState next, bool* updated) override;
private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, FlashSaleRequestStatus> statuses_;
};

class KvFlashSaleRequestStatusStore final : public IFlashSaleRequestStatusStore {
public:
    explicit KvFlashSaleRequestStatusStore(::live::kv::IKVStore* store) : store_(store) {}
    common::Status putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) override;
    common::Status put(const FlashSaleRequestStatus& status) override;
    common::Status get(const std::string& request_id, FlashSaleRequestStatus* status) const override;
    common::Status transition(const std::string& request_id, FlashSaleRequestState expected,
                              FlashSaleRequestState next, bool* updated) override;
private:
    ::live::kv::IKVStore* store_{nullptr};
};

// Writes short-lived request progress to Redis and mirrors worker outcomes to
// durable storage. Reads Redis first and falls back to the durable store after
// cache expiry or Redis failure.
class TieredFlashSaleRequestStatusStore final : public IFlashSaleRequestStatusStore {
public:
    TieredFlashSaleRequestStatusStore(IFlashSaleRequestStatusStore* primary, IFlashSaleRequestStatusStore* durable)
        : primary_(primary), durable_(durable) {}
    common::Status putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) override;
    common::Status put(const FlashSaleRequestStatus& status) override;
    common::Status get(const std::string& request_id, FlashSaleRequestStatus* status) const override;
    common::Status transition(const std::string& request_id, FlashSaleRequestState expected,
                              FlashSaleRequestState next, bool* updated) override;
private:
    IFlashSaleRequestStatusStore* primary_{nullptr};
    IFlashSaleRequestStatusStore* durable_{nullptr};
};

// Gateway-facing store: accepts writes into the fast cache only but reads
// through to durable storage when the short-lived cache misses.
class ReadThroughFlashSaleRequestStatusStore final : public IFlashSaleRequestStatusStore {
public:
    ReadThroughFlashSaleRequestStatusStore(IFlashSaleRequestStatusStore* primary, IFlashSaleRequestStatusStore* durable)
        : primary_(primary), durable_(durable) {}
    common::Status putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) override;
    common::Status put(const FlashSaleRequestStatus& status) override;
    common::Status get(const std::string& request_id, FlashSaleRequestStatus* status) const override;
    common::Status transition(const std::string& request_id, FlashSaleRequestState expected,
                              FlashSaleRequestState next, bool* updated) override;
private:
    IFlashSaleRequestStatusStore* primary_{nullptr};
    IFlashSaleRequestStatusStore* durable_{nullptr};
};

const char* flashSaleRequestStateName(FlashSaleRequestState state);
FlashSaleRequestState parseFlashSaleRequestState(const std::string& state);

}  // namespace live::business::flash_sale
