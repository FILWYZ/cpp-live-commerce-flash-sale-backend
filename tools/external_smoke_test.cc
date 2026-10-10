#include "business/promotion/flash_sale_gate.h"
#include "kv/redis_inventory_fast_path.h"
#include "kv/redis_flash_sale_gate.h"
#include "kv/redis_kv_store.h"
#include "kv/redis_order_queue.h"
#include "messaging/kafka_client.h"
#include "messaging/order_message.h"

#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <thread>

int main() {
    const std::string redis_host = std::getenv("ECOMMERCE_REDIS_TEST_HOST") == nullptr ? "127.0.0.1" : std::getenv("ECOMMERCE_REDIS_TEST_HOST");
    const std::uint16_t redis_port = static_cast<std::uint16_t>(std::getenv("ECOMMERCE_REDIS_TEST_PORT") == nullptr ? 6379 : std::strtoul(std::getenv("ECOMMERCE_REDIS_TEST_PORT"), nullptr, 10));
    const std::string kafka_brokers = std::getenv("ECOMMERCE_KAFKA_TEST_BROKERS") == nullptr ? "127.0.0.1:19092" : std::getenv("ECOMMERCE_KAFKA_TEST_BROKERS");
    live::kv::RedisKVStore redis;
    auto status = redis.connect(redis_host, redis_port);
    if (!status.ok()) { std::cerr << "Redis connect failed: " << status.message() << '\n'; return 1; }
    status = redis.set("ecommerce:smoke", "v1", std::chrono::seconds(5));
    if (!status.ok()) { std::cerr << "Redis SET failed: " << status.message() << '\n'; return 1; }
    std::string value;
    if (!redis.get("ecommerce:smoke", &value).ok() || value != "v1") { std::cerr << "Redis GET failed\n"; return 1; }
    bool updated = false;
    if (!redis.cas("ecommerce:smoke", std::optional<std::string>("v1"), "v2", &updated).ok() || !updated) {
        std::cerr << "Redis CAS failed\n";
        return 1;
    }
    redis.del("ecommerce:smoke");

    live::business::promotion::RedisFlashSaleGate gate(&redis);
    const std::string promotion_id = "external-promo-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    status = gate.configure(promotion_id, 1);
    if (!status.ok() || !gate.preheat(promotion_id).ok() || !gate.start(promotion_id).ok()) {
        std::cerr << "Redis flash-sale gate setup failed\n";
        return 1;
    }
    live::business::promotion::AdmissionDecision decision;
    if (!gate.tryAcquire(promotion_id, "user-1", &decision).ok() || !decision.accepted()) {
        std::cerr << "Redis flash-sale gate acquire failed\n";
        return 1;
    }
    if (!gate.tryAcquire(promotion_id, "user-1", &decision).ok() ||
        decision.code != live::business::promotion::AdmissionCode::kDuplicate) {
        std::cerr << "Redis flash-sale gate idempotency failed\n";
        return 1;
    }
    if (!gate.tryAcquire(promotion_id, "user-2", &decision).ok() ||
        decision.code != live::business::promotion::AdmissionCode::kExhausted) {
        std::cerr << "Redis flash-sale gate quota failed\n";
        return 1;
    }
    gate.rollback(promotion_id, "user-1");
    gate.finish(promotion_id);

    live::business::inventory::RedisInventoryFastPath fast_inventory(&redis);
    const std::string sku_id = "external-sku-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    if (!fast_inventory.initializeSku(sku_id, 2).ok()) {
        std::cerr << "Redis inventory initialization failed\n";
        return 1;
    }
    if (!fast_inventory.initializeSku(sku_id, 2).ok() || fast_inventory.initializeSku(sku_id, 3).ok()) {
        std::cerr << "Redis inventory initialization idempotency failed\n";
        return 1;
    }
    live::business::inventory::RedisInventoryResult inventory_result;
    if (!fast_inventory.reserve(sku_id, 1, "operation-1", &inventory_result).ok() ||
        inventory_result.decision != live::business::inventory::RedisInventoryDecision::kSuccess) {
        std::cerr << "Redis inventory reserve failed\n";
        return 1;
    }
    if (!fast_inventory.reserve(sku_id, 1, "operation-1", &inventory_result).ok() ||
        inventory_result.decision != live::business::inventory::RedisInventoryDecision::kDuplicate) {
        std::cerr << "Redis inventory idempotency failed\n";
        return 1;
    }
    if (!fast_inventory.confirm(sku_id, "operation-1", &inventory_result).ok() ||
        inventory_result.decision != live::business::inventory::RedisInventoryDecision::kSuccess) {
        std::cerr << "Redis inventory confirm failed\n";
        return 1;
    }
    live::business::inventory::StockSnapshot fast_stock;
    if (!fast_inventory.queryStock(sku_id, &fast_stock).ok() || fast_stock.available != 1 || fast_stock.sold != 1) {
        std::cerr << "Redis inventory snapshot failed\n";
        return 1;
    }
    if (!fast_inventory.reserve(sku_id, 1, "operation-2", &inventory_result).ok() ||
        inventory_result.decision != live::business::inventory::RedisInventoryDecision::kSuccess ||
        !fast_inventory.release(sku_id, "operation-2", &inventory_result).ok() ||
        inventory_result.decision != live::business::inventory::RedisInventoryDecision::kSuccess ||
        !fast_inventory.rollbackRelease(sku_id, "operation-2", &inventory_result).ok() ||
        inventory_result.decision != live::business::inventory::RedisInventoryDecision::kSuccess) {
        std::cerr << "Redis inventory release compensation failed\n";
        return 1;
    }

    live::kv::RedisOrderQueue order_queue(&redis, "external-flash-orders-queue");
    live::messaging::FlashSaleOrderMessage queued_message;
    queued_message.promotion_id = promotion_id;
    queued_message.order_id = "external-order-1";
    queued_message.idempotency_key = "external-idem-1";
    queued_message.user_id = "external-user-1";
    queued_message.sku_id = sku_id;
    queued_message.quantity = 1;
    redis.del("external-flash-orders-queue");
    if (!order_queue.push(queued_message).ok()) { std::cerr << "Redis order queue push failed\n"; return 1; }
    live::messaging::FlashSaleOrderMessage popped_message;
    if (!order_queue.pop(&popped_message, std::chrono::seconds(1)).ok() ||
        popped_message.order_id != queued_message.order_id || popped_message.sku_id != queued_message.sku_id) {
        std::cerr << "Redis order queue roundtrip failed\n";
        return 1;
    }

    live::messaging::KafkaConsumer consumer;
    const std::string group = "ecommerce-smoke-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    status = consumer.start(kafka_brokers, group, {"ecommerce-smoke"});
    if (!status.ok()) { std::cerr << "Kafka consumer start failed: " << status.message() << '\n'; return 1; }
    consumer.poll(1000, [](const auto&) { return live::common::Status::Ok(); });

    live::messaging::KafkaProducer producer;
    status = producer.start(kafka_brokers, "ecommerce-smoke-producer");
    if (!status.ok()) { std::cerr << "Kafka producer start failed: " << status.message() << '\n'; return 1; }
    live::messaging::Event event{"smoke-event", "ecommerce-smoke", "key-1", "payload-1", 0};
    status = producer.publish(event);
    if (!status.ok()) { std::cerr << "Kafka publish failed: " << status.message() << '\n'; return 1; }
    status = producer.flush(5000);
    if (!status.ok()) { std::cerr << "Kafka flush failed: " << status.message() << '\n'; return 1; }

    bool received = false;
    for (int i = 0; i < 10 && !received; ++i) {
        status = consumer.poll(1000, [&](const auto& received_event) {
            received = received_event.payload == "payload-1";
            return live::common::Status::Ok();
        });
        if (!status.ok()) { std::cerr << "Kafka consume failed: " << status.message() << '\n'; return 1; }
    }
    consumer.stop();
    producer.stop();
    if (!received) { std::cerr << "Kafka event was not received\n"; return 1; }
    std::cout << "Redis and Kafka smoke test passed\n";
    return 0;
}
