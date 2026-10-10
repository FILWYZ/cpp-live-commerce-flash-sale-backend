#pragma once

#include "common/error/status.h"
#include "business/promotion/flash_sale_gate.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

namespace live::business::promotion {

enum class PromotionState { kCreated, kPreheating, kRunning, kPaused, kFinished };
const char* promotionStateName(PromotionState state);

struct Promotion {
    std::string id;
    std::string sku_id;
    std::size_t participant_limit{0};
    PromotionState state{PromotionState::kCreated};
    std::int64_t starts_at{0};  // Unix seconds; zero means no scheduled lower bound.
    std::int64_t ends_at{0};    // Unix seconds; zero means no scheduled upper bound.
};

struct AdminAuditRecord {
    std::uint64_t id{0};
    std::string actor;
    std::string action;
    std::string resource_type;
    std::string resource_id;
    std::string previous_value;
    std::string new_value;
    std::string created_at;
};

class IPromotionRepository {
public:
    virtual ~IPromotionRepository() = default;
    virtual common::Status create(const Promotion& promotion, const std::string& actor) = 0;
    virtual common::Status update(const Promotion& previous, const Promotion& next, const std::string& actor) = 0;
    virtual common::Status list(std::vector<Promotion>* promotions) const = 0;
    virtual common::Status transition(const std::string& id, PromotionState expected,
                                      PromotionState next, const std::string& actor) = 0;
    virtual common::Status listAudit(std::size_t limit, std::vector<AdminAuditRecord>* records) const = 0;
};

class PromotionService {
public:
    explicit PromotionService(std::unique_ptr<IAdmissionGate> gate = std::make_unique<FlashSaleAdmissionGate>(),
                              IPromotionRepository* repository = nullptr)
        : gate_(std::move(gate)), repository_(repository) {}

    common::Status create(Promotion promotion, const std::string& actor = "system");
    common::Status update(Promotion promotion, const std::string& actor = "system");
    common::Status restore();
    common::Status preheat(const std::string& id);
    common::Status start(const std::string& id);
    common::Status pause(const std::string& id);
    common::Status resume(const std::string& id);
    common::Status finish(const std::string& id);
    common::Status preheat(const std::string& id, const std::string& actor);
    common::Status start(const std::string& id, const std::string& actor);
    common::Status pause(const std::string& id, const std::string& actor);
    common::Status resume(const std::string& id, const std::string& actor);
    common::Status finish(const std::string& id, const std::string& actor);
    common::Status get(const std::string& id, Promotion* promotion) const;
    common::Status list(std::vector<Promotion>* promotions) const;
    common::Status listAudit(std::size_t limit, std::vector<AdminAuditRecord>* records) const;
    IAdmissionGate* admissionGate() const { return gate_.get(); }

private:
    static bool transitionAllowed(PromotionState from, PromotionState to);
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Promotion> promotions_;
    std::unique_ptr<IAdmissionGate> gate_;
    IPromotionRepository* repository_{nullptr};
};

}  // namespace live::business::promotion
