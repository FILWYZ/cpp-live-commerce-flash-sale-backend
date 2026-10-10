#pragma once

#include "common/error/status.h"
#include "messaging/in_memory_broker.h"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace live::business::order { struct Order; enum class OrderState; }
namespace live::business::inventory { struct StockSnapshot; }

namespace live::business::order {

// A transaction store owns the database transaction that spans inventory,
// order and outbox rows. The LocalKV implementation keeps the existing
// zero-dependency path; the MySQL implementation provides the production-like
// path used by integration tests and multi-instance deployments.
class IOrderTransactionStore {
public:
    virtual ~IOrderTransactionStore() = default;

    virtual common::Status getOrder(const std::string& order_id, Order* order) const = 0;
    virtual common::Status transitionOrder(const std::string& order_id, OrderState expected,
                                           OrderState next, Order* order) {
        (void)order_id; (void)expected; (void)next; (void)order;
        return common::Status::FailedPrecondition("order state transition is not implemented");
    }
    virtual common::Status confirmPayment(const std::string& order_id, const std::string& provider,
                                          const std::string& provider_transaction_id,
                                          std::int64_t amount_cents, Order* order) {
        (void)order_id; (void)provider; (void)provider_transaction_id; (void)amount_cents; (void)order;
        return common::Status::FailedPrecondition("payment callback confirmation is not implemented");
    }
    virtual common::Status initializeSku(const std::string& sku_id, std::int64_t quantity) = 0;
    virtual common::Status queryStock(const std::string& sku_id,
                                      inventory::StockSnapshot* snapshot) const = 0;
    virtual common::Status listExpiredOrders(std::size_t limit, std::vector<Order>* orders) const {
        (void)limit; (void)orders;
        return common::Status::FailedPrecondition("expired order scan is not implemented");
    }
    virtual common::Status claimExpiredOrders(const std::string& owner, std::size_t limit,
                                              std::vector<Order>* orders) {
        (void)owner;
        return listExpiredOrders(limit, orders);
    }
    virtual common::Status drainOutbox(const std::function<common::Status(const messaging::Event&)>& publish,
                                       std::size_t limit = 100) = 0;
};

}  // namespace live::business::order
