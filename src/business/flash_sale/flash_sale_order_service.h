#pragma once

#include "business/inventory/inventory_gateway.h"
#include "business/promotion/flash_sale_gate.h"
#include "business/promotion/promotion_service.h"
#include "common/error/status.h"
#include "messaging/order_queue.h"
#include "business/flash_sale/flash_sale_request_status_store.h"

#include <cstdint>
#include <string>

namespace live::business::flash_sale {

enum class SubmitCode { kQueued, kDuplicate, kNotStarted, kFinished, kExhausted, kRejected };

struct FlashSaleSubmitRequest {
    std::string promotion_id;
    std::string sku_id;
    std::string user_id;
    std::string idempotency_key;
    std::int64_t quantity{1};
};

struct FlashSaleSubmitResult {
    SubmitCode code{SubmitCode::kRejected};
    std::string order_id;
    std::string request_id;
    std::string message;
};

class FlashSaleOrderService final {
public:
    FlashSaleOrderService(::live::business::promotion::IAdmissionGate* admission_gate,
                          ::live::messaging::IOrderQueue* queue,
                          IFlashSaleRequestStatusStore* request_status = nullptr,
                          ::live::business::promotion::PromotionService* promotions = nullptr)
        : admission_gate_(admission_gate), queue_(queue), request_status_(request_status), promotions_(promotions) {}

    // Compatibility overload for existing callers. Inventory mutation now
    // happens after command dequeue, in the consumer.
    FlashSaleOrderService(::live::business::promotion::IAdmissionGate* admission_gate,
                          ::live::business::inventory::IInventoryGateway*,
                          ::live::messaging::IOrderQueue* queue,
                          IFlashSaleRequestStatusStore* request_status = nullptr,
                          ::live::business::promotion::PromotionService* promotions = nullptr)
        : FlashSaleOrderService(admission_gate, queue, request_status, promotions) {}

    common::Status submit(const FlashSaleSubmitRequest& request, FlashSaleSubmitResult* result);

private:
    static std::string orderId(const FlashSaleSubmitRequest& request);
    static SubmitCode mapGateCode(::live::business::promotion::AdmissionCode code);

    ::live::business::promotion::IAdmissionGate* admission_gate_{nullptr};
    ::live::messaging::IOrderQueue* queue_{nullptr};
    IFlashSaleRequestStatusStore* request_status_{nullptr};
    ::live::business::promotion::PromotionService* promotions_{nullptr};
};

}  // namespace live::business::flash_sale
