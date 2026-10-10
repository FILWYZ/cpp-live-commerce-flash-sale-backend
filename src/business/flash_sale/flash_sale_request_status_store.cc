#include "business/flash_sale/flash_sale_request_status_store.h"

#include "kv/kv_store.h"

#include <charconv>

namespace live::business::flash_sale {
namespace {

void appendField(std::string* out, const std::string& value) {
    out->append(std::to_string(value.size())).push_back(':');
    out->append(value);
}

bool readField(const std::string& input, std::size_t* offset, std::string* output) {
    if (offset == nullptr || output == nullptr || *offset >= input.size()) return false;
    const auto colon = input.find(':', *offset);
    if (colon == std::string::npos) return false;
    std::size_t length = 0;
    const auto parsed = std::from_chars(input.data() + *offset, input.data() + colon, length);
    if (parsed.ec != std::errc{} || parsed.ptr != input.data() + colon || length > input.size() - colon - 1) return false;
    *offset = colon + 1;
    output->assign(input.data() + *offset, length);
    *offset += length;
    return true;
}

std::string serialize(const FlashSaleRequestStatus& value) {
    std::string result = std::to_string(static_cast<int>(value.state)) + ";";
    appendField(&result, value.request_id);
    appendField(&result, value.user_id);
    appendField(&result, value.order_id);
    appendField(&result, value.message);
    appendField(&result, value.promotion_id);
    return result;
}

bool deserialize(const std::string& input, FlashSaleRequestStatus* value) {
    if (value == nullptr) return false;
    const auto semicolon = input.find(';');
    if (semicolon == std::string::npos) return false;
    int state = -1;
    const auto parsed = std::from_chars(input.data(), input.data() + semicolon, state);
    if (parsed.ec != std::errc{} || parsed.ptr != input.data() + semicolon || state < 0 || state > 9) return false;
    value->state = static_cast<FlashSaleRequestState>(state);
    std::size_t offset = semicolon + 1;
    if (!readField(input, &offset, &value->request_id) || !readField(input, &offset, &value->user_id) ||
        !readField(input, &offset, &value->order_id) || !readField(input, &offset, &value->message)) return false;
    if (offset == input.size()) return true;  // Backward compatibility with request records written before campaign IDs.
    return readField(input, &offset, &value->promotion_id) && offset == input.size();
}

bool durableTerminalState(FlashSaleRequestState state) {
    return state == FlashSaleRequestState::kSoldOut || state == FlashSaleRequestState::kRejected ||
           state == FlashSaleRequestState::kPaid || state == FlashSaleRequestState::kExpired ||
           state == FlashSaleRequestState::kCancelled;
}

}  // namespace

const char* flashSaleRequestStateName(FlashSaleRequestState state) {
    switch (state) {
        case FlashSaleRequestState::kQueued: return "QUEUED";
        case FlashSaleRequestState::kProcessing: return "PROCESSING";
        case FlashSaleRequestState::kSoldOut: return "SOLD_OUT";
        case FlashSaleRequestState::kRejected: return "REJECTED";
        case FlashSaleRequestState::kPendingPayment: return "PENDING_PAYMENT";
        case FlashSaleRequestState::kPaid: return "PAID";
        case FlashSaleRequestState::kExpired: return "EXPIRED";
        case FlashSaleRequestState::kCancelled: return "CANCELLED";
        case FlashSaleRequestState::kWaiting: return "WAITING";
        case FlashSaleRequestState::kSubmitting: return "SUBMITTING";
    }
    return "REJECTED";
}

FlashSaleRequestState parseFlashSaleRequestState(const std::string& state) {
    for (int i = 0; i <= 9; ++i) {
        auto candidate = static_cast<FlashSaleRequestState>(i);
        if (state == flashSaleRequestStateName(candidate)) return candidate;
    }
    return FlashSaleRequestState::kRejected;
}

common::Status InMemoryFlashSaleRequestStatusStore::put(const FlashSaleRequestStatus& status) {
    if (status.request_id.empty() || status.user_id.empty()) return common::Status::InvalidArgument("invalid request status");
    std::lock_guard<std::mutex> lock(mutex_);
    statuses_[status.request_id] = status;
    return common::Status::Ok();
}

common::Status InMemoryFlashSaleRequestStatusStore::putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) {
    if (inserted == nullptr || status.request_id.empty() || status.user_id.empty()) return common::Status::InvalidArgument("invalid request status claim");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto [it, created] = statuses_.emplace(status.request_id, status);
    (void)it;
    *inserted = created;
    return common::Status::Ok();
}

common::Status InMemoryFlashSaleRequestStatusStore::get(const std::string& request_id, FlashSaleRequestStatus* status) const {
    if (request_id.empty() || status == nullptr) return common::Status::InvalidArgument("invalid request status lookup");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = statuses_.find(request_id);
    if (it == statuses_.end()) return common::Status::NotFound("flash-sale request not found");
    *status = it->second;
    return common::Status::Ok();
}

common::Status InMemoryFlashSaleRequestStatusStore::transition(const std::string& request_id,
    FlashSaleRequestState expected, FlashSaleRequestState next, bool* updated) {
    if (request_id.empty() || updated == nullptr) return common::Status::InvalidArgument("invalid request status transition");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = statuses_.find(request_id);
    if (it == statuses_.end()) return common::Status::NotFound("flash-sale request not found");
    *updated = it->second.state == expected;
    if (*updated) it->second.state = next;
    return common::Status::Ok();
}

common::Status KvFlashSaleRequestStatusStore::put(const FlashSaleRequestStatus& status) {
    if (store_ == nullptr || status.request_id.empty() || status.user_id.empty()) return common::Status::FailedPrecondition("request status store is not configured");
    return store_->set("flashsale:request:" + status.request_id, serialize(status), std::chrono::hours(24));
}

common::Status KvFlashSaleRequestStatusStore::putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) {
    if (store_ == nullptr || inserted == nullptr || status.request_id.empty() || status.user_id.empty())
        return common::Status::FailedPrecondition("request status claim is not configured");
    return store_->cas("flashsale:request:" + status.request_id, std::nullopt, serialize(status), inserted,
                       std::chrono::hours(24));
}

common::Status KvFlashSaleRequestStatusStore::get(const std::string& request_id, FlashSaleRequestStatus* status) const {
    if (store_ == nullptr || request_id.empty() || status == nullptr) return common::Status::InvalidArgument("invalid request status lookup");
    std::string value;
    if (const auto result = store_->get("flashsale:request:" + request_id, &value); !result.ok()) return result;
    if (!deserialize(value, status)) return common::Status::Internal("corrupt flash-sale request status");
    return common::Status::Ok();
}

common::Status KvFlashSaleRequestStatusStore::transition(const std::string& request_id,
    FlashSaleRequestState expected, FlashSaleRequestState next, bool* updated) {
    if (store_ == nullptr || request_id.empty() || updated == nullptr)
        return common::Status::InvalidArgument("invalid Redis request status transition");
    FlashSaleRequestStatus current;
    if (const auto status = get(request_id, &current); !status.ok()) return status;
    if (current.state != expected) { *updated = false; return common::Status::Ok(); }
    const auto key = "flashsale:request:" + request_id;
    const auto previous = current;
    current.state = next;
    return store_->cas(key, serialize(previous), serialize(current), updated, std::chrono::hours(24));
}

common::Status TieredFlashSaleRequestStatusStore::put(const FlashSaleRequestStatus& status) {
    if (primary_ == nullptr && durable_ == nullptr) return common::Status::FailedPrecondition("request status stores are not configured");
    common::Status primary_status = primary_ == nullptr ? common::Status::Ok() : primary_->put(status);
    common::Status durable_status = durable_ == nullptr ? common::Status::Ok() : durable_->put(status);
    if (durable_ != nullptr) return durable_status;
    return primary_status;
}

common::Status TieredFlashSaleRequestStatusStore::putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) {
    if (inserted == nullptr) return common::Status::InvalidArgument("request status claim output must not be null");
    common::Status primary_status = primary_ == nullptr ? common::Status::FailedPrecondition("primary status store is not configured")
                                                        : primary_->putIfAbsent(status, inserted);
    if (!primary_status.ok() || !*inserted || durable_ == nullptr) return primary_status;
    bool durable_inserted = false;
    const auto durable_status = durable_->putIfAbsent(status, &durable_inserted);
    if (!durable_status.ok()) return durable_status;
    if (!durable_inserted) return common::Status::AlreadyExists("durable request status already exists");
    return common::Status::Ok();
}

common::Status TieredFlashSaleRequestStatusStore::get(const std::string& request_id,
    FlashSaleRequestStatus* status) const {
    if (status == nullptr || request_id.empty()) return common::Status::InvalidArgument("invalid request status lookup");
    common::Status primary_status = primary_ == nullptr ? common::Status::NotFound("primary status store is not configured")
                                                        : primary_->get(request_id, status);
    if (primary_status.ok() && !durableTerminalState(status->state)) return primary_status;
    if (durable_ == nullptr) return primary_status;
    const auto durable_status = durable_->get(request_id, status);
    if (durable_status.ok()) return durable_status;
    if (durable_status.code() != common::ErrorCode::kNotFound) return durable_status;
    if (primary_status.ok()) return primary_status;
    return primary_status.code() == common::ErrorCode::kNotFound ? durable_status : primary_status;
}

common::Status TieredFlashSaleRequestStatusStore::transition(const std::string& request_id,
    FlashSaleRequestState expected, FlashSaleRequestState next, bool* updated) {
    if (updated == nullptr) return common::Status::InvalidArgument("request transition output must not be null");
    bool primary_updated = false;
    const auto primary_status = primary_ == nullptr ? common::Status::FailedPrecondition("primary status store is not configured")
        : primary_->transition(request_id, expected, next, &primary_updated);
    if (!primary_status.ok() && primary_status.code() != common::ErrorCode::kNotFound) return primary_status;
    bool durable_updated = false;
    if (durable_ != nullptr) {
        const auto durable_status = durable_->transition(request_id, expected, next, &durable_updated);
        if (!durable_status.ok() && durable_status.code() != common::ErrorCode::kNotFound) return durable_status;
    }
    *updated = primary_updated || durable_updated;
    return common::Status::Ok();
}

common::Status ReadThroughFlashSaleRequestStatusStore::put(const FlashSaleRequestStatus& status) {
    if (primary_ == nullptr) return common::Status::FailedPrecondition("primary status store is not configured");
    return primary_->put(status);
}

common::Status ReadThroughFlashSaleRequestStatusStore::putIfAbsent(const FlashSaleRequestStatus& status, bool* inserted) {
    if (primary_ == nullptr) return common::Status::FailedPrecondition("primary status store is not configured");
    return primary_->putIfAbsent(status, inserted);
}

common::Status ReadThroughFlashSaleRequestStatusStore::get(const std::string& request_id,
    FlashSaleRequestStatus* status) const {
    if (status == nullptr || request_id.empty()) return common::Status::InvalidArgument("invalid request status lookup");
    common::Status primary_status = primary_ == nullptr ? common::Status::NotFound("primary status store is not configured")
                                                        : primary_->get(request_id, status);
    if (primary_status.ok() && !durableTerminalState(status->state)) return primary_status;
    if (durable_ == nullptr) return primary_status;
    const auto durable_status = durable_->get(request_id, status);
    if (durable_status.ok()) return durable_status;
    if (durable_status.code() != common::ErrorCode::kNotFound) return durable_status;
    if (primary_status.ok()) return primary_status;
    return primary_status.code() == common::ErrorCode::kNotFound ? durable_status : primary_status;
}

common::Status ReadThroughFlashSaleRequestStatusStore::transition(const std::string& request_id,
    FlashSaleRequestState expected, FlashSaleRequestState next, bool* updated) {
    if (primary_ == nullptr) return common::Status::FailedPrecondition("primary status store is not configured");
    return primary_->transition(request_id, expected, next, updated);
}

}  // namespace live::business::flash_sale
