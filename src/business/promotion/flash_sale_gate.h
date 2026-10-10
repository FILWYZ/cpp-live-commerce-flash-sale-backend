#pragma once

#include "common/error/status.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace live::business::promotion {

enum class AdmissionCode { kAccepted, kDuplicate, kNotStarted, kFinished, kExhausted, kPaused };
enum class AdmissionState { kCreated, kPreheating, kRunning, kPaused, kFinished };

struct AdmissionDecision {
    AdmissionCode code{AdmissionCode::kNotStarted};
    std::size_t admitted{0};
    std::size_t quota{0};

    bool accepted() const { return code == AdmissionCode::kAccepted || code == AdmissionCode::kDuplicate; }
};

class IAdmissionGate {
public:
    virtual ~IAdmissionGate() = default;
    virtual common::Status configure(const std::string& promotion_id, std::size_t quota) = 0;
    virtual common::Status synchronize(const std::string& promotion_id, AdmissionState state) = 0;
    virtual common::Status preheat(const std::string& promotion_id) = 0;
    virtual common::Status start(const std::string& promotion_id) = 0;
    virtual common::Status pause(const std::string& promotion_id) = 0;
    virtual common::Status resume(const std::string& promotion_id) = 0;
    virtual common::Status finish(const std::string& promotion_id) = 0;
    virtual common::Status tryAcquire(const std::string& promotion_id, const std::string& user_id,
                                      AdmissionDecision* decision) = 0;
    virtual common::Status rollback(const std::string& promotion_id, const std::string& user_id) = 0;
};

// In-process implementation used by the default build. It makes the
// promotion gate explicit and provides a deterministic fallback for tests.
class FlashSaleAdmissionGate final : public IAdmissionGate {
public:
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
    enum class State { kCreated, kPreheating, kRunning, kPaused, kFinished };
    struct Gate {
        std::size_t quota{0};
        std::size_t admitted{0};
        State state{State::kCreated};
        std::unordered_set<std::string> users;
    };

    common::Status findGate(const std::string& promotion_id, Gate** gate);
    common::Status findGate(const std::string& promotion_id, const Gate** gate) const;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Gate> gates_;
};

}  // namespace live::business::promotion
