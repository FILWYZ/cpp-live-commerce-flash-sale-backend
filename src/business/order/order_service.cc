#include "business/order/order_service.h"

#include "business/order/order_transaction_store.h"

#include "storage/local_engine/local_kv_engine.h"

#include <sstream>
#include <unordered_set>

namespace live::business::order {

common::Status OrderService::restore() {
    if (storage_ == nullptr) return common::Status::Ok();
    std::lock_guard<std::mutex> lock(mutex_);
    orders_.clear();
    key_to_order_.clear();
    static const std::string prefix = "orders/";
    for (const auto& [key, value] : storage_->scanPrefix(prefix)) {
        (void)key;
        Order order;
        if (!deserialize(value, &order)) return common::Status::Internal("invalid persisted order");
        orders_[order.id] = order;
        key_to_order_[order.idempotency_key] = order.id;
    }
    if (inventory_ != nullptr) {
        std::unordered_set<std::string> active_operations;
        active_operations.reserve(orders_.size());
        for (const auto& [order_id, order] : orders_) {
            (void)order;
            active_operations.insert(order_id);
        }
        if (const auto status = inventory_->releaseOrphanReservations(active_operations); !status.ok()) return status;
    }
    return common::Status::Ok();
}

common::Status OrderService::createOrderAfterReservation(const std::string& order_id, const std::string& user_id,
                                                         const std::string& sku_id, std::int64_t quantity,
                                                         const std::string& idempotency_key, Order* order) {
    if (order_id.empty() || user_id.empty() || sku_id.empty() || quantity <= 0 || idempotency_key.empty() || order == nullptr ||
        (publisher_ == nullptr && transaction_store_ == nullptr)) {
        return common::Status::InvalidArgument("invalid pre-reserved order arguments");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto existing = key_to_order_.find(idempotency_key);
    if (existing != key_to_order_.end()) {
        const auto it = orders_.find(existing->second);
        if (it == orders_.end()) return common::Status::Internal("idempotency index is inconsistent");
        if (it->second.user_id != user_id || it->second.sku_id != sku_id || it->second.quantity != quantity) {
            return common::Status::FailedPrecondition("idempotency key was reused with different order data");
        }
        *order = it->second;
        return common::Status::Ok();
    }
    if (transaction_store_ != nullptr) return common::Status::FailedPrecondition("pre-reserved orders require a flash-sale repository");
    if (orders_.find(order_id) != orders_.end()) return common::Status::AlreadyExists("order id already exists");
    Order created{order_id, idempotency_key, user_id, sku_id, quantity};
    if (const auto status = persist(created); !status.ok()) return status;
    orders_.emplace(order_id, created);
    key_to_order_.emplace(idempotency_key, order_id);
    if (const auto status = publisher_->publish(::live::messaging::Event{"", "OrderCreated", order_id, eventPayload(created), 0}); !status.ok()) {
        orders_.erase(order_id);
        key_to_order_.erase(idempotency_key);
        if (storage_ != nullptr) storage_->del("orders/" + order_id);
        return status;
    }
    *order = created;
    return common::Status::Ok();
}

common::Status OrderService::getOrder(const std::string& order_id, Order* order) const {
    if (order == nullptr) return common::Status::InvalidArgument("order output must not be null");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = orders_.find(order_id);
    if (it == orders_.end()) {
        if (transaction_store_ == nullptr) return common::Status::NotFound("order not found");
        return transaction_store_->getOrder(order_id, order);
    }
    *order = it->second;
    return common::Status::Ok();
}

common::Status OrderService::transitionOrder(const std::string& order_id, OrderState expected,
                                             OrderState next, Order* order) {
    if (order_id.empty() || order == nullptr || expected == next) {
        return common::Status::InvalidArgument("invalid order transition");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = orders_.find(order_id);
    if (it == orders_.end()) {
        if (transaction_store_ == nullptr) return common::Status::NotFound("order not found");
        return transaction_store_->transitionOrder(order_id, expected, next, order);
    }
    if (it->second.state == next) { *order = it->second; return common::Status::Ok(); }
    if (it->second.state != expected) return common::Status::Aborted("order state changed concurrently");
    it->second.state = next;
    if (const auto status = persist(it->second); !status.ok()) {
        it->second.state = expected;
        return status;
    }
    *order = it->second;
    return common::Status::Ok();
}

common::Status OrderService::confirmPayment(const std::string& order_id, const std::string& provider,
    const std::string& provider_transaction_id, std::int64_t amount_cents, Order* order) {
    if (order_id.empty() || provider.empty() || provider_transaction_id.empty() || amount_cents < 0 || order == nullptr)
        return common::Status::InvalidArgument("invalid payment confirmation");
    if (transaction_store_ != nullptr)
        return transaction_store_->confirmPayment(order_id, provider, provider_transaction_id, amount_cents, order);
    return transitionOrder(order_id, OrderState::kPendingPayment, OrderState::kPaid, order);
}

std::string OrderService::eventPayload(const Order& order) {
    std::ostringstream out;
    out << order.id << '|' << order.user_id << '|' << order.sku_id << '|' << order.quantity;
    return out.str();
}

std::string OrderService::serialize(const Order& order) {
    return order.id + '\t' + order.idempotency_key + '\t' + order.user_id + '\t' + order.sku_id + '\t' +
           std::to_string(order.quantity) + '\t' + std::to_string(static_cast<int>(order.state));
}

bool OrderService::deserialize(const std::string& value, Order* order) {
    if (order == nullptr) return false;
    std::istringstream input(value);
    int state = 0;
    return static_cast<bool>(std::getline(input, order->id, '\t')) &&
           static_cast<bool>(std::getline(input, order->idempotency_key, '\t')) &&
           static_cast<bool>(std::getline(input, order->user_id, '\t')) &&
           static_cast<bool>(std::getline(input, order->sku_id, '\t')) &&
           static_cast<bool>(input >> order->quantity) && input.get() == '\t' &&
           static_cast<bool>(input >> state) && state >= 0 && state <= 3 &&
           (order->state = static_cast<OrderState>(state), true);
}

common::Status OrderService::persist(const Order& order) {
    return storage_ == nullptr ? common::Status::Ok() : storage_->set("orders/" + order.id, serialize(order));
}

}  // namespace live::business::order
