#include "messaging/rabbitmq_order_queue.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

int main() {
    const auto env = [](const char* key, const char* fallback) {
        const char* value = std::getenv(key);
        return std::string(value == nullptr ? fallback : value);
    };
    const auto host = env("ECOMMERCE_RABBITMQ_HOST", "127.0.0.1");
    const auto user = env("ECOMMERCE_RABBITMQ_USER", "guest");
    const auto password = env("ECOMMERCE_RABBITMQ_PASSWORD", "guest");
    const auto vhost = env("ECOMMERCE_RABBITMQ_VHOST", "/");
    const auto exchange = env("ECOMMERCE_RABBITMQ_EXCHANGE", "flashsale.direct");
    const auto port_text = env("ECOMMERCE_RABBITMQ_PORT", "5672");
    const auto queue = env("ECOMMERCE_RABBITMQ_TEST_QUEUE", "flashsale.transport.smoke") + "-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto route = env("ECOMMERCE_RABBITMQ_TEST_ROUTING_KEY", "flashsale.transport.smoke");
    const auto port = std::stoi(port_text);

    live::messaging::RabbitMqOrderQueue transport;
    auto status = transport.start(host, port, user, password, vhost, exchange, queue, route, true);
    if (!status.ok()) {
        std::cerr << "RabbitMQ startup failed: " << status.message() << '\n';
        return 1;
    }
    live::messaging::FlashSaleOrderMessage sent;
    sent.promotion_id = "smoke-campaign";
    sent.order_id = "smoke-order-1";
    sent.idempotency_key = "smoke-request-1";
    sent.user_id = "smoke-user-1";
    sent.sku_id = "smoke-sku-1";
    sent.quantity = 1;
    status = transport.push(sent);
    if (!status.ok()) {
        std::cerr << "RabbitMQ publisher confirm failed: " << status.message() << '\n';
        return 1;
    }
    live::messaging::FlashSaleOrderMessage received;
    status = transport.pop(&received, std::chrono::seconds(5));
    if (!status.ok() || received.idempotency_key != sent.idempotency_key) {
        std::cerr << "RabbitMQ delivery failed: " << status.message() << '\n';
        return 1;
    }
    // Closing with an unacknowledged delivery models a consumer crash. RabbitMQ
    // must put the durable command back on the queue for a replacement process.
    transport.stop();
    live::messaging::RabbitMqOrderQueue replacement_consumer;
    status = replacement_consumer.start(host, port, user, password, vhost, exchange, queue, route, true);
    if (!status.ok()) {
        std::cerr << "RabbitMQ recovery consumer startup failed: " << status.message() << '\n';
        return 1;
    }
    status = replacement_consumer.pop(&received, std::chrono::seconds(5));
    if (!status.ok() || received.idempotency_key != sent.idempotency_key) {
        std::cerr << "RabbitMQ unacknowledged redelivery failed: " << status.message() << '\n';
        return 1;
    }
    status = replacement_consumer.acknowledge(received, true);
    if (!status.ok()) {
        std::cerr << "RabbitMQ manual ACK failed: " << status.message() << '\n';
        return 1;
    }
    live::messaging::Event event{"smoke-event-1", "order.events", "smoke-order-1", "{\"state\":\"created\"}"};
    status = replacement_consumer.publish(event);
    if (!status.ok()) {
        std::cerr << "RabbitMQ event publish confirm failed: " << status.message() << '\n';
        return 1;
    }
    std::cout << "RabbitMQ publish-confirm, durable command delivery, manual ACK and event publish passed\n";
    return 0;
}
