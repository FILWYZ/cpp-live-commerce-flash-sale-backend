#pragma once

#include "business/flash_sale/flash_sale_order_repository.h"
#include "business/flash_sale/flash_sale_request_status_store.h"
#include "business/inventory/inventory_gateway.h"
#include "business/promotion/flash_sale_gate.h"
#include "messaging/order_queue.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>
#include <vector>

namespace live::business::flash_sale {

class FlashSaleOrderConsumer final {
public:
    FlashSaleOrderConsumer(::live::messaging::IOrderQueue* queue,
                           ::live::messaging::IOrderQueue* dead_letter_queue,
                           IFlashSaleOrderRepository* repository,
                           ::live::business::inventory::IInventoryGateway* inventory,
                           ::live::business::promotion::IAdmissionGate* admission_gate,
                           std::size_t max_attempts = 5,
                           std::size_t worker_count = 1,
                           IFlashSaleRequestStatusStore* request_status = nullptr)
        : queue_(queue), dead_letter_queue_(dead_letter_queue), repository_(repository), inventory_(inventory),
                 admission_gate_(admission_gate), max_attempts_(max_attempts),
                 worker_count_(worker_count == 0 ? 1 : worker_count), request_status_(request_status) {}
    ~FlashSaleOrderConsumer() { stop(); }

    common::Status processOnce(std::chrono::milliseconds timeout = std::chrono::milliseconds(50));
    common::Status start();
    void stop();
    std::size_t processed() const { return processed_.load(); }
    std::size_t rejected() const { return rejected_.load(); }
    std::size_t deadLettered() const { return dead_lettered_.load(); }

private:
    void run();
    common::Status process(const ::live::messaging::FlashSaleOrderMessage& message);
    common::Status compensate(const ::live::messaging::FlashSaleOrderMessage& message);

    ::live::messaging::IOrderQueue* queue_{nullptr};
    ::live::messaging::IOrderQueue* dead_letter_queue_{nullptr};
    IFlashSaleOrderRepository* repository_{nullptr};
    ::live::business::inventory::IInventoryGateway* inventory_{nullptr};
    ::live::business::promotion::IAdmissionGate* admission_gate_{nullptr};
    std::size_t max_attempts_{5};
    std::atomic<bool> running_{false};
    std::vector<std::thread> workers_;
    const std::size_t worker_count_;
    IFlashSaleRequestStatusStore* request_status_{nullptr};
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> rejected_{0};
    std::atomic<std::size_t> dead_lettered_{0};
};

}  // namespace live::business::flash_sale
