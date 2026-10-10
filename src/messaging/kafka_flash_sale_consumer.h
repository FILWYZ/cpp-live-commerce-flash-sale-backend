#pragma once

#include "business/flash_sale/flash_sale_order_repository.h"
#include "business/inventory/inventory_gateway.h"
#include "business/promotion/flash_sale_gate.h"
#include "messaging/kafka_client.h"

#include <atomic>
#include <cstddef>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace live::messaging {

// Kafka consumer-group implementation of the flash-sale consumer. The
// KafkaConsumer commits an offset only when this class returns OK, which
// gives at-least-once delivery around the database transaction.
class KafkaFlashSaleOrderConsumer final {
public:
    KafkaFlashSaleOrderConsumer(KafkaConsumer* consumer, KafkaProducer* dead_letter_producer,
                                ::live::business::flash_sale::IFlashSaleOrderRepository* repository,
                                ::live::business::inventory::IInventoryGateway* inventory,
                                ::live::business::promotion::IAdmissionGate* admission_gate,
                                std::string topic, std::string dead_letter_topic,
                                std::size_t max_attempts = 5)
        : consumer_(consumer), dead_letter_producer_(dead_letter_producer), repository_(repository),
          inventory_(inventory), admission_gate_(admission_gate), topic_(std::move(topic)),
          dead_letter_topic_(std::move(dead_letter_topic)), max_attempts_(max_attempts) {}
    ~KafkaFlashSaleOrderConsumer() { stop(); }

    common::Status start(const std::string& brokers, const std::string& group_id);
    void stop();
    std::size_t processed() const { return processed_.load(); }
    std::size_t rejected() const { return rejected_.load(); }
    std::size_t deadLettered() const { return dead_lettered_.load(); }

private:
    void run();
    common::Status handle(const Event& event);
    common::Status sendDeadLetter(const Event& event);
    common::Status compensate(const ::live::messaging::FlashSaleOrderMessage& message);

    KafkaConsumer* consumer_{nullptr};
    KafkaProducer* dead_letter_producer_{nullptr};
    ::live::business::flash_sale::IFlashSaleOrderRepository* repository_{nullptr};
    ::live::business::inventory::IInventoryGateway* inventory_{nullptr};
    ::live::business::promotion::IAdmissionGate* admission_gate_{nullptr};
    std::string topic_;
    std::string dead_letter_topic_;
    std::size_t max_attempts_{5};
    std::atomic<bool> running_{false};
    std::thread worker_;
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> rejected_{0};
    std::atomic<std::size_t> dead_lettered_{0};
    std::mutex attempts_mutex_;
    std::unordered_map<std::string, std::size_t> attempts_;
};

}  // namespace live::messaging
