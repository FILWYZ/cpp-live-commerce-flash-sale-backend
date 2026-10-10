#include "business/promotion/promotion_service.h"

#include <algorithm>
#include <cctype>

namespace live::business::promotion {

namespace {
bool validIdentifier(const std::string& value) {
    if (value.empty() || value.size() > 64) return false;
    return std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '-' || c == '_' || c == '.';
    });
}

AdmissionState admissionState(PromotionState state) {
    switch (state) {
        case PromotionState::kCreated: return AdmissionState::kCreated;
        case PromotionState::kPreheating: return AdmissionState::kPreheating;
        case PromotionState::kRunning: return AdmissionState::kRunning;
        case PromotionState::kPaused: return AdmissionState::kPaused;
        case PromotionState::kFinished: return AdmissionState::kFinished;
    }
    return AdmissionState::kCreated;
}
}  // namespace

const char* promotionStateName(PromotionState state) {
    switch (state) {
        case PromotionState::kCreated: return "CREATED";
        case PromotionState::kPreheating: return "PREHEATING";
        case PromotionState::kRunning: return "RUNNING";
        case PromotionState::kPaused: return "PAUSED";
        case PromotionState::kFinished: return "FINISHED";
    }
    return "CREATED";
}

common::Status PromotionService::create(Promotion promotion, const std::string& actor) {
    if (!validIdentifier(promotion.id) || !validIdentifier(promotion.sku_id) || promotion.participant_limit == 0 ||
        promotion.starts_at < 0 || promotion.ends_at < 0 ||
        (promotion.ends_at != 0 && promotion.starts_at >= promotion.ends_at)) {
        return common::Status::InvalidArgument("invalid promotion");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (promotions_.find(promotion.id) != promotions_.end()) return common::Status::AlreadyExists("promotion exists");
    if (gate_ == nullptr) return common::Status::FailedPrecondition("admission gate is not configured");
    if (const auto status = gate_->configure(promotion.id, promotion.participant_limit); !status.ok()) return status;
    if (repository_ != nullptr) {
        if (const auto status = repository_->create(promotion, actor); !status.ok()) return status;
    }
    promotions_.emplace(promotion.id, std::move(promotion));
    return common::Status::Ok();
}

common::Status PromotionService::update(Promotion promotion, const std::string& actor) {
    if (!validIdentifier(promotion.id) || !validIdentifier(promotion.sku_id) || promotion.participant_limit == 0 || actor.empty() ||
        promotion.starts_at < 0 || promotion.ends_at < 0 ||
        (promotion.ends_at != 0 && promotion.starts_at >= promotion.ends_at))
        return common::Status::InvalidArgument("invalid promotion update");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(promotion.id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (it->second.state != PromotionState::kCreated) return common::Status::FailedPrecondition("only created promotions can be edited");
    const Promotion previous = it->second;
    if (previous.sku_id == promotion.sku_id && previous.participant_limit == promotion.participant_limit &&
        previous.starts_at == promotion.starts_at && previous.ends_at == promotion.ends_at) return common::Status::Ok();
    promotion.state = PromotionState::kCreated;
    if (const auto status = gate_->configure(promotion.id, promotion.participant_limit); !status.ok()) return status;
    if (repository_ != nullptr) if (const auto status = repository_->update(previous, promotion, actor); !status.ok()) {
        (void)gate_->configure(previous.id, previous.participant_limit);
        return status;
    }
    it->second = std::move(promotion);
    return common::Status::Ok();
}

common::Status PromotionService::restore() {
    if (repository_ == nullptr) return common::Status::Ok();
    std::vector<Promotion> persisted;
    if (const auto status = repository_->list(&persisted); !status.ok()) return status;
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto promotion : persisted) {
        const auto existing = promotions_.find(promotion.id);
        if (existing == promotions_.end()) {
            if (const auto status = gate_->configure(promotion.id, promotion.participant_limit); !status.ok()) return status;
            if (const auto status = gate_->synchronize(promotion.id, admissionState(promotion.state)); !status.ok()) return status;
            promotions_.emplace(promotion.id, std::move(promotion));
            continue;
        }
        if (existing->second.participant_limit != promotion.participant_limit) {
            if (const auto status = gate_->configure(promotion.id, promotion.participant_limit); !status.ok()) return status;
        }
        if (const auto status = gate_->synchronize(promotion.id, admissionState(promotion.state)); !status.ok()) return status;
        existing->second = std::move(promotion);
    }
    return common::Status::Ok();
}

bool PromotionService::transitionAllowed(PromotionState from, PromotionState to) {
    return (from == PromotionState::kCreated && to == PromotionState::kPreheating) ||
           (from == PromotionState::kPreheating && to == PromotionState::kRunning) ||
           (from == PromotionState::kRunning && to == PromotionState::kPaused) ||
           (from == PromotionState::kPaused && to == PromotionState::kRunning) ||
           (from == PromotionState::kRunning && to == PromotionState::kFinished) ||
           (from == PromotionState::kPaused && to == PromotionState::kFinished);
}

common::Status PromotionService::preheat(const std::string& id) {
    return preheat(id, "system");
}

common::Status PromotionService::preheat(const std::string& id, const std::string& actor) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (!transitionAllowed(it->second.state, PromotionState::kPreheating)) return common::Status::FailedPrecondition("invalid promotion transition");
    if (const auto status = gate_->preheat(id); !status.ok()) return status;
    if (repository_ != nullptr) if (const auto status = repository_->transition(id, it->second.state, PromotionState::kPreheating, actor); !status.ok()) {
        (void)gate_->synchronize(id, admissionState(it->second.state)); return status;
    }
    it->second.state = PromotionState::kPreheating;
    return common::Status::Ok();
}

common::Status PromotionService::start(const std::string& id) {
    return start(id, "system");
}

common::Status PromotionService::start(const std::string& id, const std::string& actor) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (!transitionAllowed(it->second.state, PromotionState::kRunning)) return common::Status::FailedPrecondition("invalid promotion transition");
    if (const auto status = gate_->start(id); !status.ok()) return status;
    if (repository_ != nullptr) if (const auto status = repository_->transition(id, it->second.state, PromotionState::kRunning, actor); !status.ok()) {
        (void)gate_->synchronize(id, admissionState(it->second.state)); return status;
    }
    it->second.state = PromotionState::kRunning;
    return common::Status::Ok();
}

common::Status PromotionService::finish(const std::string& id) {
    return finish(id, "system");
}

common::Status PromotionService::finish(const std::string& id, const std::string& actor) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (!transitionAllowed(it->second.state, PromotionState::kFinished)) return common::Status::FailedPrecondition("invalid promotion transition");
    if (const auto status = gate_->finish(id); !status.ok()) return status;
    if (repository_ != nullptr) if (const auto status = repository_->transition(id, it->second.state, PromotionState::kFinished, actor); !status.ok()) {
        (void)gate_->synchronize(id, admissionState(it->second.state)); return status;
    }
    it->second.state = PromotionState::kFinished;
    return common::Status::Ok();
}

common::Status PromotionService::pause(const std::string& id) {
    return pause(id, "system");
}

common::Status PromotionService::pause(const std::string& id, const std::string& actor) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (!transitionAllowed(it->second.state, PromotionState::kPaused)) return common::Status::FailedPrecondition("invalid promotion pause transition");
    if (const auto status = gate_->pause(id); !status.ok()) return status;
    if (repository_ != nullptr) if (const auto status = repository_->transition(id, it->second.state, PromotionState::kPaused, actor); !status.ok()) {
        (void)gate_->synchronize(id, admissionState(it->second.state)); return status;
    }
    it->second.state = PromotionState::kPaused;
    return common::Status::Ok();
}

common::Status PromotionService::resume(const std::string& id) {
    return resume(id, "system");
}

common::Status PromotionService::resume(const std::string& id, const std::string& actor) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    if (!transitionAllowed(it->second.state, PromotionState::kRunning)) return common::Status::FailedPrecondition("invalid promotion resume transition");
    if (const auto status = gate_->resume(id); !status.ok()) return status;
    if (repository_ != nullptr) if (const auto status = repository_->transition(id, it->second.state, PromotionState::kRunning, actor); !status.ok()) {
        (void)gate_->synchronize(id, admissionState(it->second.state)); return status;
    }
    it->second.state = PromotionState::kRunning;
    return common::Status::Ok();
}

common::Status PromotionService::get(const std::string& id, Promotion* promotion) const {
    if (promotion == nullptr) return common::Status::InvalidArgument("promotion output must not be null");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = promotions_.find(id);
    if (it == promotions_.end()) return common::Status::NotFound("promotion not found");
    *promotion = it->second;
    return common::Status::Ok();
}

common::Status PromotionService::list(std::vector<Promotion>* promotions) const {
    if (promotions == nullptr) return common::Status::InvalidArgument("promotion output must not be null");
    std::lock_guard<std::mutex> lock(mutex_);
    promotions->clear();
    promotions->reserve(promotions_.size());
    for (const auto& [id, promotion] : promotions_) {
        (void)id;
        promotions->push_back(promotion);
    }
    std::sort(promotions->begin(), promotions->end(), [](const Promotion& left, const Promotion& right) {
        return left.id < right.id;
    });
    return common::Status::Ok();
}

common::Status PromotionService::listAudit(std::size_t limit, std::vector<AdminAuditRecord>* records) const {
    if (records == nullptr || limit == 0) return common::Status::InvalidArgument("invalid audit query");
    if (repository_ == nullptr) return common::Status::FailedPrecondition("audit storage requires MySQL");
    return repository_->listAudit(limit, records);
}

}  // namespace live::business::promotion
