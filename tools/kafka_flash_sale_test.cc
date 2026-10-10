#include "business/flash_sale/flash_sale_order_service.h"
#include "business/inventory/inventory_gateway.h"
#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"
#include "business/promotion/promotion_service.h"
#include "messaging/kafka_client.h"
#include "messaging/kafka_flash_sale_consumer.h"
#include "messaging/kafka_order_queue.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

class Repository final : public live::business::flash_sale::IFlashSaleOrderRepository {
public:
    live::common::Status persistQueuedOrder(const live::messaging::FlashSaleOrderMessage& message,
                                            live::business::order::Order* order) override {
        if (order == nullptr) return live::common::Status::InvalidArgument("order output missing");
        ++persisted;
        *order = {message.order_id, message.idempotency_key, message.user_id, message.sku_id,
                  message.quantity, live::business::order::OrderState::kPendingPayment};
        return live::common::Status::Ok();
    }
    int persisted{0};
};

class FailingRepository final : public live::business::flash_sale::IFlashSaleOrderRepository {
public:
    live::common::Status persistQueuedOrder(const live::messaging::FlashSaleOrderMessage&,
                                            live::business::order::Order*) override {
        return live::common::Status::Internal("injected MySQL failure");
    }
    live::common::Status getOrder(const std::string&,
                                  live::business::order::Order*) const override {
        return live::common::Status::NotFound("injected lookup confirms no durable order");
    }
};

std::string suffix() {
    return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

}  // namespace

int main() {
    try {
        const std::string id = suffix();
        const std::string kafka_brokers = std::getenv("ECOMMERCE_KAFKA_TEST_BROKERS") == nullptr ? "127.0.0.1:19092" : std::getenv("ECOMMERCE_KAFKA_TEST_BROKERS");
        const std::string topic = "flash-sale-test-" + id;
        const std::string dlq_topic = topic + ".DLQ";
        live::messaging::KafkaProducer producer;
        check(producer.start(kafka_brokers, "flash-sale-test-producer-" + id).ok(), "Kafka producer start");

        live::business::inventory::InventoryService inventory;
        check(inventory.addSku("kafka-sku-" + id, 2).ok(), "inventory setup");
        live::business::inventory::LocalInventoryGateway gateway(&inventory);
        live::business::promotion::PromotionService promotion;
        check(promotion.create({"kafka-promo-" + id, "kafka-sku-" + id, 2}).ok(), "promotion setup");
        check(promotion.preheat("kafka-promo-" + id).ok() && promotion.start("kafka-promo-" + id).ok(), "promotion start");

        live::messaging::KafkaOrderQueue queue(&producer, topic);
        live::messaging::KafkaConsumer kafka_consumer;
        Repository repository;
        live::messaging::KafkaFlashSaleOrderConsumer consumer(
            &kafka_consumer, &producer, &repository, &gateway, promotion.admissionGate(), topic, dlq_topic, 3);
        check(consumer.start(kafka_brokers, "flash-sale-test-consumer-" + id).ok(), "Kafka consumer start");

        live::business::flash_sale::FlashSaleOrderService service(promotion.admissionGate(), &gateway, &queue);
        live::business::flash_sale::FlashSaleSubmitResult result;
        const auto submit_status = service.submit(
            {"kafka-promo-" + id, "kafka-sku-" + id, "kafka-user-1", "kafka-idem-1", 1}, &result);
        check(submit_status.ok() && result.code == live::business::flash_sale::SubmitCode::kQueued,
              submit_status.ok() ? "Kafka flash-sale submit result" : submit_status.message().c_str());
        for (int i = 0; i < 50 && consumer.processed() == 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        check(consumer.processed() == 1 && repository.persisted == 1, "Kafka flash-sale consume");
        live::business::inventory::StockSnapshot stock;
        check(inventory.queryStock("kafka-sku-" + id, &stock).ok() && stock.available == 1 &&
              stock.reserved == 1 && stock.sold == 0,
              "Kafka flash-sale inventory");
        live::business::flash_sale::FlashSaleSubmitResult duplicate;
        check(service.submit({"kafka-promo-" + id, "kafka-sku-" + id, "kafka-user-1", "kafka-idem-2", 1}, &duplicate).ok() &&
              duplicate.code == live::business::flash_sale::SubmitCode::kDuplicate, "Kafka admission idempotency");
        consumer.stop();

        const std::string dlq_promo = "kafka-dlq-promo-" + id;
        const std::string dlq_sku = "kafka-dlq-sku-" + id;
        live::business::inventory::InventoryService dlq_inventory;
        check(dlq_inventory.addSku(dlq_sku, 1).ok(), "DLQ inventory setup");
        live::business::inventory::LocalInventoryGateway dlq_gateway(&dlq_inventory);
        live::business::promotion::PromotionService dlq_promotion;
        check(dlq_promotion.create({dlq_promo, dlq_sku, 1}).ok() && dlq_promotion.preheat(dlq_promo).ok() &&
              dlq_promotion.start(dlq_promo).ok(), "DLQ promotion setup");
        live::business::promotion::AdmissionDecision admission;
        check(dlq_promotion.admissionGate()->tryAcquire(dlq_promo, "poison-user", &admission).ok() && admission.accepted(), "DLQ admission");
        live::business::inventory::InventoryReservationResult reservation;
        check(dlq_gateway.reserve(dlq_sku, 1, "poison-order", &reservation).ok(), "DLQ reservation");
        const std::string poison_topic = "flash-poison-" + id;
        live::messaging::KafkaOrderQueue poison_queue(&producer, poison_topic);
        live::messaging::KafkaConsumer poison_consumer_client;
        FailingRepository failing_repository;
        live::messaging::KafkaFlashSaleOrderConsumer poison_consumer(
            &poison_consumer_client, &producer, &failing_repository, &dlq_gateway,
            dlq_promotion.admissionGate(), poison_topic, poison_topic + ".DLQ", 2);
        check(poison_consumer.start(kafka_brokers, "flash-poison-consumer-" + id).ok(), "DLQ consumer start");
        live::messaging::FlashSaleOrderMessage poison;
        poison.promotion_id = dlq_promo;
        poison.order_id = "poison-order";
        poison.idempotency_key = "poison-idem";
        poison.user_id = "poison-user";
        poison.sku_id = dlq_sku;
        poison.quantity = 1;
        check(poison_queue.push(poison).ok(), "poison publish");
        check(producer.flush(5000).ok(), "poison flush");
        for (int i = 0; i < 60 && poison_consumer.deadLettered() == 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        check(poison_consumer.deadLettered() == 1, "Kafka DLQ delivery");
        check(dlq_inventory.queryStock(dlq_sku, &stock).ok() && stock.available == 1 && stock.reserved == 0,
              ("Kafka DLQ compensation: available=" + std::to_string(stock.available) +
               " reserved=" + std::to_string(stock.reserved) + " sold=" + std::to_string(stock.sold)).c_str());
        poison_consumer.stop();
        producer.stop();
        std::cout << "kafka_flash_sale_test passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "kafka_flash_sale_test failed: " << error.what() << '\n';
        return 1;
    }
}
