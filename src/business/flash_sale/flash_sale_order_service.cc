#include "business/flash_sale/flash_sale_order_service.h"

#include <openssl/sha.h>

#include <array>
#include <chrono>
#include <iomanip>
#include <limits>
#include <sstream>

namespace live::business::flash_sale {

std::string FlashSaleOrderService::orderId(const FlashSaleSubmitRequest& request) {
    const std::string identity = request.promotion_id + '\0' + request.user_id;
    std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
    SHA256(reinterpret_cast<const unsigned char*>(identity.data()), identity.size(), digest.data());
    std::ostringstream result;
    result << "flash-" << std::hex << std::setfill('0');
    for (std::size_t i = 0; i < 16; ++i) result << std::setw(2) << static_cast<unsigned>(digest[i]);
    return result.str();
}

SubmitCode FlashSaleOrderService::mapGateCode(::live::business::promotion::AdmissionCode code) {
    using ::live::business::promotion::AdmissionCode;
    switch (code) {
        case AdmissionCode::kAccepted: return SubmitCode::kQueued;
        case AdmissionCode::kDuplicate: return SubmitCode::kDuplicate;
        case AdmissionCode::kFinished: return SubmitCode::kFinished;
        case AdmissionCode::kExhausted: return SubmitCode::kExhausted;
        case AdmissionCode::kNotStarted: return SubmitCode::kNotStarted;
        case AdmissionCode::kPaused: return SubmitCode::kRejected;
    }
    return SubmitCode::kRejected;
}

common::Status FlashSaleOrderService::submit(const FlashSaleSubmitRequest& request, FlashSaleSubmitResult* result) {
    if (result == nullptr) return common::Status::InvalidArgument("flash-sale result must not be null");
    *result = {};
    if (request.promotion_id.empty() || request.sku_id.empty() || request.user_id.empty() ||
        request.idempotency_key.empty() || request.quantity != 1 || request.promotion_id.size() > 64 ||
        request.sku_id.size() > 64 || request.user_id.size() > 64 || request.idempotency_key.size() > 128 ||
        request.quantity > static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max())) {
        return common::Status::InvalidArgument("invalid flash-sale order request; this campaign permits one unit per user");
    }
    if (admission_gate_ == nullptr || queue_ == nullptr) {
        return common::Status::FailedPrecondition("flash-sale order path is not configured");
    }

    bool waiting_room_claim = false;
    bool submission_claim = false;
    if (request_status_ != nullptr) {
        FlashSaleRequestStatus existing;
        const auto lookup = request_status_->get(request.idempotency_key, &existing);
        if (lookup.ok()) {
            if (existing.user_id != request.user_id) return common::Status::FailedPrecondition("idempotency key belongs to another user");
            if (existing.state == FlashSaleRequestState::kWaiting) {
                waiting_room_claim = true;
            } else if (existing.state == FlashSaleRequestState::kSubmitting) {
                submission_claim = true;
            } else {
                result->code = SubmitCode::kDuplicate;
                result->order_id = existing.order_id;
                result->request_id = existing.request_id;
                result->message = "request already exists with state " + std::string(flashSaleRequestStateName(existing.state));
                return common::Status::Ok();
            }
        }
        if (!lookup.ok() && lookup.code() != common::ErrorCode::kNotFound) return lookup;
    }

    if (promotions_ != nullptr) {
        ::live::business::promotion::Promotion promotion;
        if (const auto status = promotions_->get(request.promotion_id, &promotion); !status.ok()) return status;
        if (promotion.sku_id != request.sku_id) {
            return common::Status::InvalidArgument("SKU does not belong to the requested campaign");
        }
        const auto now = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        if (promotion.starts_at != 0 && now < promotion.starts_at) {
            result->code = SubmitCode::kNotStarted;
            result->message = "campaign has not reached its scheduled start time";
            return common::Status::Ok();
        }
        if (promotion.ends_at != 0 && now >= promotion.ends_at) {
            result->code = SubmitCode::kFinished;
            result->message = "campaign has passed its scheduled end time";
            return common::Status::Ok();
        }
    }

    if (request_status_ != nullptr && !waiting_room_claim && !submission_claim) {
        bool inserted = false;
        const auto status = request_status_->putIfAbsent({request.idempotency_key, request.user_id, orderId(request),
            FlashSaleRequestState::kSubmitting, "request submission is in progress", request.promotion_id}, &inserted);
        if (!status.ok()) return status;
        if (inserted) {
            submission_claim = true;
        } else {
            FlashSaleRequestStatus existing;
            if (const auto lookup = request_status_->get(request.idempotency_key, &existing); !lookup.ok()) return lookup;
            if (existing.user_id != request.user_id) return common::Status::FailedPrecondition("idempotency key belongs to another user");
            if (existing.state == FlashSaleRequestState::kWaiting) waiting_room_claim = true;
            else if (existing.state == FlashSaleRequestState::kSubmitting) submission_claim = true;
            else {
                result->code = SubmitCode::kDuplicate;
                result->order_id = existing.order_id;
                result->request_id = existing.request_id;
                result->message = "request already exists with state " + std::string(flashSaleRequestStateName(existing.state));
                return common::Status::Ok();
            }
        }
    }

    ::live::business::promotion::AdmissionDecision admission;
    if (const auto status = admission_gate_->tryAcquire(request.promotion_id, request.user_id, &admission); !status.ok()) {
        result->message = status.message();
        return status;
    }
    result->code = mapGateCode(admission.code);
    result->order_id = orderId(request);
    result->request_id = request.idempotency_key;
    const bool admission_owned_here = admission.code == ::live::business::promotion::AdmissionCode::kAccepted;
    if (admission.code == ::live::business::promotion::AdmissionCode::kDuplicate &&
        !submission_claim && !waiting_room_claim) {
        result->message = "user already admitted";
        if (request_status_ != nullptr) {
            bool updated = false;
            (void)request_status_->transition(request.idempotency_key, FlashSaleRequestState::kSubmitting,
                FlashSaleRequestState::kRejected, &updated);
        }
        // Do not write a terminal request state here: a parallel request with
        // the same idempotency key may have won admission and still be writing
        // QUEUED. That accepted request owns the durable status row.
        result->request_id.clear();
        return common::Status::Ok();
    }
    if (admission.code != ::live::business::promotion::AdmissionCode::kAccepted &&
        admission.code != ::live::business::promotion::AdmissionCode::kDuplicate) {
        result->message = "promotion admission rejected";
        if (request_status_ != nullptr && submission_claim) {
            const auto state = result->code == SubmitCode::kExhausted
                ? FlashSaleRequestState::kSoldOut : FlashSaleRequestState::kRejected;
            bool updated = false;
            (void)request_status_->transition(request.idempotency_key, FlashSaleRequestState::kSubmitting, state, &updated);
        }
        return common::Status::Ok();
    }


    ::live::messaging::FlashSaleOrderMessage message;
    message.promotion_id = request.promotion_id;
    message.order_id = result->order_id;
    message.idempotency_key = request.idempotency_key;
    message.user_id = request.user_id;
    message.sku_id = request.sku_id;
    message.quantity = request.quantity;
    if (const auto status = queue_->push(message); !status.ok()) {
        if (admission_owned_here) (void)admission_gate_->rollback(request.promotion_id, request.user_id);
        result->code = SubmitCode::kRejected;
        result->message = status.message();
        return status;
    }
    if (request_status_ != nullptr) {
        bool updated = false;
        const auto expected_state = waiting_room_claim ? FlashSaleRequestState::kWaiting : FlashSaleRequestState::kSubmitting;
        const auto status = request_status_->transition(request.idempotency_key, expected_state,
            FlashSaleRequestState::kQueued, &updated);
        if (!status.ok()) {
            // The request claim remains recoverable. A retry can safely send
            // the same idempotency key again if the broker already accepted it.
            result->code = SubmitCode::kRejected;
            result->message = status.message();
            return status;
        }
    }
    result->code = SubmitCode::kQueued;
    result->message = "order accepted and queued";
    return common::Status::Ok();
}

}  // namespace live::business::flash_sale
