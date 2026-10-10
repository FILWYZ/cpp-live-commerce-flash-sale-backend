#pragma once

#include "business/inventory/inventory_service.h"
#include "common/error/status.h"
#include "messaging/in_memory_broker.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace live::storage { class LocalKVEngine; }

namespace live::business::order {

class IOrderTransactionStore;

// A flash-sale order is created after the admission decision and is completed
// asynchronously by the consumer.
enum class OrderState { kPendingPayment, kPaid, kExpired, kCancelled };

struct Order {
    std::string id;
    std::string idempotency_key;
    std::string user_id;
    std::string sku_id;
    std::int64_t quantity{0};
    OrderState state{OrderState::kPendingPayment};
    std::int64_t unit_price_cents{0};
};

class OrderService {
public:
    OrderService(::live::business::inventory::InventoryService* inventory, ::live::messaging::IEventPublisher* publisher,
                 ::live::storage::LocalKVEngine* storage = nullptr,
                 IOrderTransactionStore* transaction_store = nullptr)
        : inventory_(inventory), publisher_(publisher), storage_(storage), transaction_store_(transaction_store) {}

    common::Status restore();

    // Used by the asynchronous flash-sale consumer after admission and
    // inventory reservation have already succeeded. It never reserves stock.
    common::Status createOrderAfterReservation(const std::string& order_id, const std::string& user_id,
                                               const std::string& sku_id, std::int64_t quantity,
                                               const std::string& idempotency_key, Order* order);
    common::Status getOrder(const std::string& order_id, Order* order) const;
    common::Status transitionOrder(const std::string& order_id, OrderState expected, OrderState next,
                                   Order* order);
    common::Status confirmPayment(const std::string& order_id, const std::string& provider,
                                 const std::string& provider_transaction_id, std::int64_t amount_cents,
                                 Order* order);

private:
    static std::string eventPayload(const Order& order);
    static std::string serialize(const Order& order);
    static bool deserialize(const std::string& value, Order* order);
    common::Status persist(const Order& order);

    ::live::business::inventory::InventoryService* inventory_;
    ::live::messaging::IEventPublisher* publisher_;
    ::live::storage::LocalKVEngine* storage_{nullptr};
    IOrderTransactionStore* transaction_store_{nullptr};
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Order> orders_;
    std::unordered_map<std::string, std::string> key_to_order_;
};

}  // namespace live::business::order
