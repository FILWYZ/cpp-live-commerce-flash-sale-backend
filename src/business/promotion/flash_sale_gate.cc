#include "business/promotion/flash_sale_gate.h"

namespace live::business::promotion {

common::Status FlashSaleAdmissionGate::configure(const std::string& promotion_id, std::size_t quota) {
    if (promotion_id.empty() || quota == 0) return common::Status::InvalidArgument("invalid admission gate");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto existing = gates_.find(promotion_id);
    if (existing != gates_.end()) {
        if (existing->second.state != State::kCreated || existing->second.admitted != 0)
            return common::Status::FailedPrecondition("only an unused created admission gate can be reconfigured");
        existing->second.quota = quota;
        return common::Status::Ok();
    }
    gates_.emplace(promotion_id, Gate{quota, 0, State::kCreated, {}});
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::synchronize(const std::string& promotion_id, AdmissionState state) {
    std::lock_guard<std::mutex> lock(mutex_);
    Gate* gate = nullptr;
    if (const auto status = findGate(promotion_id, &gate); !status.ok()) return status;
    switch (state) {
        case AdmissionState::kCreated: gate->state = State::kCreated; break;
        case AdmissionState::kPreheating: gate->state = State::kPreheating; break;
        case AdmissionState::kRunning: gate->state = State::kRunning; break;
        case AdmissionState::kPaused: gate->state = State::kPaused; break;
        case AdmissionState::kFinished: gate->state = State::kFinished; break;
    }
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::findGate(const std::string& promotion_id, Gate** gate) {
    const auto it = gates_.find(promotion_id);
    if (it == gates_.end()) return common::Status::NotFound("admission gate not found");
    *gate = &it->second;
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::findGate(const std::string& promotion_id, const Gate** gate) const {
    const auto it = gates_.find(promotion_id);
    if (it == gates_.end()) return common::Status::NotFound("admission gate not found");
    *gate = &it->second;
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::preheat(const std::string& promotion_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    Gate* gate = nullptr;
    if (const auto status = findGate(promotion_id, &gate); !status.ok()) return status;
    if (gate->state != State::kCreated) return common::Status::FailedPrecondition("invalid gate preheat transition");
    gate->state = State::kPreheating;
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::start(const std::string& promotion_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    Gate* gate = nullptr;
    if (const auto status = findGate(promotion_id, &gate); !status.ok()) return status;
    if (gate->state != State::kPreheating) return common::Status::FailedPrecondition("invalid gate start transition");
    gate->state = State::kRunning;
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::pause(const std::string& promotion_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    Gate* gate = nullptr;
    if (const auto status = findGate(promotion_id, &gate); !status.ok()) return status;
    if (gate->state != State::kRunning) return common::Status::FailedPrecondition("only running promotions can be paused");
    gate->state = State::kPaused;
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::resume(const std::string& promotion_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    Gate* gate = nullptr;
    if (const auto status = findGate(promotion_id, &gate); !status.ok()) return status;
    if (gate->state != State::kPaused) return common::Status::FailedPrecondition("only paused promotions can be resumed");
    gate->state = State::kRunning;
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::finish(const std::string& promotion_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    Gate* gate = nullptr;
    if (const auto status = findGate(promotion_id, &gate); !status.ok()) return status;
    if (gate->state != State::kRunning && gate->state != State::kPaused) return common::Status::FailedPrecondition("invalid gate finish transition");
    gate->state = State::kFinished;
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::tryAcquire(const std::string& promotion_id, const std::string& user_id,
                                                  AdmissionDecision* decision) {
    if (user_id.empty() || decision == nullptr) return common::Status::InvalidArgument("invalid admission request");
    std::lock_guard<std::mutex> lock(mutex_);
    Gate* gate = nullptr;
    if (const auto status = findGate(promotion_id, &gate); !status.ok()) return status;
    decision->quota = gate->quota;
    decision->admitted = gate->admitted;
    if (gate->state == State::kCreated || gate->state == State::kPreheating) {
        decision->code = AdmissionCode::kNotStarted;
        return common::Status::Ok();
    }
    if (gate->state == State::kFinished) {
        decision->code = AdmissionCode::kFinished;
        return common::Status::Ok();
    }
    if (gate->state == State::kPaused) {
        decision->code = AdmissionCode::kPaused;
        return common::Status::Ok();
    }
    if (gate->users.find(user_id) != gate->users.end()) {
        decision->code = AdmissionCode::kDuplicate;
        return common::Status::Ok();
    }
    if (gate->admitted >= gate->quota) {
        decision->code = AdmissionCode::kExhausted;
        return common::Status::Ok();
    }
    gate->users.insert(user_id);
    ++gate->admitted;
    decision->admitted = gate->admitted;
    decision->code = AdmissionCode::kAccepted;
    return common::Status::Ok();
}

common::Status FlashSaleAdmissionGate::rollback(const std::string& promotion_id, const std::string& user_id) {
    if (user_id.empty()) return common::Status::InvalidArgument("user id must not be empty");
    std::lock_guard<std::mutex> lock(mutex_);
    Gate* gate = nullptr;
    if (const auto status = findGate(promotion_id, &gate); !status.ok()) return status;
    if (gate->users.erase(user_id) != 0 && gate->admitted > 0) --gate->admitted;
    return common::Status::Ok();
}

}  // namespace live::business::promotion
