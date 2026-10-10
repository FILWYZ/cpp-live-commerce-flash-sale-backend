#pragma once

#include "common/error/status.h"
#include "messaging/order_message.h"

namespace live::business::order { struct Order; enum class OrderState; }

namespace live::business::flash_sale {

class IFlashSaleOrderRepository {
public:
    virtual ~IFlashSaleOrderRepository() = default;
    // Persists a pending-payment order and its OrderCreated event. The operation is idempotent
    // on message.idempotency_key. It writes the durable inventory mirror but
    // must not repeat the Redis fast-path reservation.
    virtual common::Status persistQueuedOrder(const ::live::messaging::FlashSaleOrderMessage& message,
                                              ::live::business::order::Order* order) = 0;

    // Used before releasing a reservation after an ambiguous persistence
    // failure. A missing order is proof that compensation is safe; an
    // unsupported or failed lookup must leave the reservation for reconciliation.
    virtual common::Status getOrder(const std::string& order_id,
                                    ::live::business::order::Order* order) const {
        (void)order_id;
        (void)order;
        return common::Status::FailedPrecondition("order lookup is not implemented");
    }
    virtual common::Status transitionOrder(const std::string& order_id,
                                           ::live::business::order::OrderState expected,
                                           ::live::business::order::OrderState next,
                                           ::live::business::order::Order* order) {
        (void)order_id; (void)expected; (void)next; (void)order;
        return common::Status::FailedPrecondition("order state transition is not implemented");
    }
};

}  // namespace live::business::flash_sale
