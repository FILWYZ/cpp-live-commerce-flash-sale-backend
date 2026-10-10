#include "business/flash_sale/flash_sale_order_consumer.h"

#include "business/order/order_service.h"

#include <iostream>
#include <thread>

namespace live::business::flash_sale {

common::Status FlashSaleOrderConsumer::process(const ::live::messaging::FlashSaleOrderMessage& message) {
    if (repository_ == nullptr || inventory_ == nullptr) return common::Status::FailedPrecondition("flash-sale consumer is not configured");
    if (request_status_ != nullptr) {
        FlashSaleRequestStatus previous;
        const auto existing_status = request_status_->get(message.idempotency_key, &previous);
        if (existing_status.ok() && (previous.state == FlashSaleRequestState::kRejected ||
            previous.state == FlashSaleRequestState::kSoldOut ||
            previous.state == FlashSaleRequestState::kPaid || previous.state == FlashSaleRequestState::kExpired ||
            previous.state == FlashSaleRequestState::kCancelled)) {
            return common::Status::Ok();
        }
        if (!existing_status.ok() && existing_status.code() != common::ErrorCode::kNotFound) return existing_status;
        const auto status = request_status_->put({message.idempotency_key, message.user_id, message.order_id,
            FlashSaleRequestState::kProcessing, "request is being processed"});
        if (!status.ok()) return status;
    }
    ::live::business::inventory::InventoryReservationResult reservation;
    if (const auto reserve_status = inventory_->reserve(message.sku_id, message.quantity, message.order_id, &reservation);
        !reserve_status.ok()) return reserve_status;
    if (reservation.decision == ::live::business::inventory::InventoryDecision::kInsufficient) {
        if (admission_gate_ != nullptr) (void)admission_gate_->rollback(message.promotion_id, message.user_id);
        if (request_status_ != nullptr) {
            const auto status = request_status_->put({message.idempotency_key, message.user_id, message.order_id,
                FlashSaleRequestState::kSoldOut, "campaign inventory is exhausted"});
            if (!status.ok()) return status;
        }
        ++rejected_;
        return common::Status::Ok();
    }
    if (reservation.decision != ::live::business::inventory::InventoryDecision::kSuccess &&
        reservation.decision != ::live::business::inventory::InventoryDecision::kDuplicate) {
        return common::Status::Internal("inventory reservation failed");
    }
    ::live::business::order::Order order;
    if (const auto status = repository_->persistQueuedOrder(message, &order); !status.ok()) return status;
    if (request_status_ != nullptr) {
        FlashSaleRequestState request_state = FlashSaleRequestState::kPendingPayment;
        if (order.state == ::live::business::order::OrderState::kPaid) request_state = FlashSaleRequestState::kPaid;
        else if (order.state == ::live::business::order::OrderState::kExpired) request_state = FlashSaleRequestState::kExpired;
        else if (order.state == ::live::business::order::OrderState::kCancelled) request_state = FlashSaleRequestState::kCancelled;
        const auto status = request_status_->put({message.idempotency_key, message.user_id, message.order_id,
            request_state, request_state == FlashSaleRequestState::kPendingPayment ? "order created; payment is pending" : "order already reached a terminal state"});
        if (!status.ok()) return status;
    }
    // Keep the reservation while the order is pending payment. The paid or
    // expired transition confirms or releases it idempotently.
    ++processed_;
    return common::Status::Ok();
}

common::Status FlashSaleOrderConsumer::compensate(const ::live::messaging::FlashSaleOrderMessage& message) {
    if (repository_ != nullptr) {
        ::live::business::order::Order durable_order;
        const auto lookup = repository_->getOrder(message.order_id, &durable_order);
        if (lookup.ok()) return common::Status::FailedPrecondition("cannot release reservation for a persisted order");
        if (lookup.code() != common::ErrorCode::kNotFound) return lookup;
    }
    if (inventory_ != nullptr) {
        ::live::business::inventory::InventoryReservationResult result;
        (void)inventory_->release(message.sku_id, message.order_id, &result);
    }
    if (admission_gate_ != nullptr) (void)admission_gate_->rollback(message.promotion_id, message.user_id);
    return common::Status::Ok();
}

common::Status FlashSaleOrderConsumer::processOnce(std::chrono::milliseconds timeout) {
    if (queue_ == nullptr || max_attempts_ == 0) return common::Status::FailedPrecondition("flash-sale queue is not configured");
    ::live::messaging::FlashSaleOrderMessage message;
    const auto pop_status = queue_->pop(&message, timeout);
    if (!pop_status.ok()) return pop_status;
    common::Status last_status = common::Status::Internal("order consumer failed");
    for (std::size_t attempt = 1; attempt <= max_attempts_; ++attempt) {
        last_status = process(message);
        if (last_status.ok()) return queue_->acknowledge(message, true);
        if (attempt < max_attempts_) std::this_thread::yield();
    }
    std::clog << "flash-sale command exhausted local retries: order_id=" << message.order_id
              << " attempts=" << max_attempts_ << " reason=" << last_status.message() << '\n';
    if (dead_letter_queue_ != nullptr && dead_letter_queue_->push(message).ok()) {
        ++dead_lettered_;
        if (request_status_ != nullptr) {
            ::live::business::order::Order durable_order;
            const auto lookup = repository_ == nullptr ? common::Status::FailedPrecondition("order repository is unavailable")
                                                       : repository_->getOrder(message.order_id, &durable_order);
            if (lookup.ok()) {
                auto state = FlashSaleRequestState::kPendingPayment;
                if (durable_order.state == ::live::business::order::OrderState::kPaid) state = FlashSaleRequestState::kPaid;
                else if (durable_order.state == ::live::business::order::OrderState::kExpired) state = FlashSaleRequestState::kExpired;
                else if (durable_order.state == ::live::business::order::OrderState::kCancelled) state = FlashSaleRequestState::kCancelled;
                (void)request_status_->put({message.idempotency_key, message.user_id, message.order_id, state,
                    "message moved to DLQ after order persistence"});
            } else if (lookup.code() == common::ErrorCode::kNotFound) {
                (void)request_status_->put({message.idempotency_key, message.user_id, message.order_id,
                    FlashSaleRequestState::kRejected, "message moved to DLQ after retries"});
            }
        }
        (void)compensate(message);
        return queue_->acknowledge(message, true);
    }
    // A poison message must not keep stock reserved forever. The durable
    // order repository is idempotent, so compensation is safe to retry during
    // reconciliation if a database write happened before a later failure.
    (void)compensate(message);
    (void)queue_->acknowledge(message, false);
    return last_status;
}

common::Status FlashSaleOrderConsumer::start() {
    if (queue_ == nullptr || repository_ == nullptr || inventory_ == nullptr) return common::Status::FailedPrecondition("flash-sale consumer is not configured");
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return common::Status::AlreadyExists("flash-sale consumer is already running");
    workers_.reserve(worker_count_);
    for (std::size_t i = 0; i < worker_count_; ++i) workers_.emplace_back([this] { run(); });
    return common::Status::Ok();
}

void FlashSaleOrderConsumer::run() {
    while (running_.load()) {
        const auto status = processOnce(std::chrono::milliseconds(250));
        if (!status.ok() && !status.isIncomplete()) std::this_thread::yield();
    }
}

void FlashSaleOrderConsumer::stop() {
    if (!running_.exchange(false)) return;
    for (auto& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
    workers_.clear();
}

}  // namespace live::business::flash_sale
