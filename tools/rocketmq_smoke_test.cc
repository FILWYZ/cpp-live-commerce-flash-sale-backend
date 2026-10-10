#include "messaging/rocketmq_order_queue.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

namespace {

std::string envOr(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return value == nullptr || *value == '\0' ? fallback : value;
}

bool envFlag(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr) return false;
    const std::string normalized(value);
    return normalized == "1" || normalized == "true" || normalized == "on";
}

}  // namespace

int main() {
    const auto endpoint = envOr("ECOMMERCE_ROCKETMQ_TEST_ENDPOINT", "127.0.0.1:8081");
    const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto topic = envOr("ECOMMERCE_ROCKETMQ_TEST_TOPIC", "flashsale_smoke_test");
    const auto group = envOr("ECOMMERCE_ROCKETMQ_TEST_GROUP", "ecommerce-rocketmq-smoke-" + suffix);
    const bool publish_only = envFlag("ECOMMERCE_ROCKETMQ_PUBLISH_ONLY");

    live::messaging::RocketMqOrderQueue queue;
    std::cerr << "RocketMQ test client starting: endpoint=" << endpoint << " topic=" << topic << '\n';
    auto status = queue.start(endpoint, topic, group, !publish_only, false);
    if (!status.ok()) {
        std::cerr << "RocketMQ client start failed: " << status.message() << '\n';
        return 1;
    }

    live::messaging::FlashSaleOrderMessage sent;
    sent.promotion_id = "rocketmq-validation-campaign";
    sent.order_id = "rocketmq-validation-order-" + suffix;
    sent.idempotency_key = "rocketmq-validation-request-" + suffix;
    sent.user_id = "rocketmq-validation-user";
    sent.sku_id = envOr("ECOMMERCE_ROCKETMQ_TEST_SKU", "rocketmq-validation-sku");
    sent.quantity = 1;
    std::cerr << "RocketMQ test client publishing: order_id=" << sent.order_id << '\n';
    status = queue.push(sent);
    if (!status.ok()) {
        std::cerr << "RocketMQ publish failed: " << status.message() << '\n';
        return 1;
    }

    if (publish_only) {
        queue.stop();
        std::cout << "RocketMQ test command published: order_id=" << sent.order_id
                  << " request_id=" << sent.idempotency_key << " sku_id=" << sent.sku_id << '\n';
        return 0;
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    live::messaging::FlashSaleOrderMessage received;
    while (std::chrono::steady_clock::now() < deadline) {
        status = queue.pop(&received, std::chrono::seconds(2));
        if (status.ok()) break;
        if (!status.isIncomplete()) {
            std::cerr << "RocketMQ receive failed: " << status.message() << '\n';
            return 1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (received.order_id != sent.order_id || received.idempotency_key != sent.idempotency_key) {
        std::cerr << "RocketMQ round-trip payload mismatch\n";
        return 1;
    }
    status = queue.acknowledge(received, true);
    if (!status.ok()) {
        std::cerr << "RocketMQ acknowledge failed: " << status.message() << '\n';
        return 1;
    }
    queue.stop();
    std::cout << "RocketMQ command round-trip passed: order_id=" << received.order_id << '\n';
    return 0;
}
