#include "messaging/rocketmq_order_queue.h"

#include "messaging/order_message.h"
#include "rocketmq/Configuration.h"
#include "rocketmq/FilterExpression.h"
#include "rocketmq/Message.h"
#include "rocketmq/Producer.h"
#include "rocketmq/SimpleConsumer.h"

#include <chrono>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <mutex>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace live::messaging {

namespace {

common::Status rocketMqTopic(const std::string& logical_topic, std::string* physical_topic) {
    if (logical_topic.empty() || physical_topic == nullptr) {
        return common::Status::InvalidArgument("RocketMQ topic is required");
    }
    physical_topic->clear();
    physical_topic->reserve(logical_topic.size());
    for (const unsigned char character : logical_topic) {
        const auto value = character == '.' ? '_' : character;
        if (!(std::isalnum(value) || value == '_' || value == '-')) {
            return common::Status::InvalidArgument("RocketMQ topic contains unsupported characters");
        }
        physical_topic->push_back(static_cast<char>(value));
    }
    return common::Status::Ok();
}

std::chrono::milliseconds rocketMqInvisibleDuration() {
    constexpr std::int64_t default_duration = 120000;
    const char* configured = std::getenv("ECOMMERCE_ROCKETMQ_INVISIBLE_MS");
    if (configured == nullptr || *configured == '\0') return std::chrono::milliseconds(default_duration);
    char* end = nullptr;
    const auto parsed = std::strtoll(configured, &end, 10);
    if (end == configured || *end != '\0' || parsed < 1000 || parsed > 3600000) {
        return std::chrono::milliseconds(default_duration);
    }
    return std::chrono::milliseconds(parsed);
}

}  // namespace

class RocketMqOrderQueue::Impl {
public:
    struct DlqDelivery {
        ROCKETMQ_NAMESPACE::MessageConstSharedPtr message;
        std::chrono::steady_clock::time_point received_at;
    };
    std::unique_ptr<ROCKETMQ_NAMESPACE::Producer> producer;
    std::unique_ptr<ROCKETMQ_NAMESPACE::SimpleConsumer> consumer;
    std::unique_ptr<ROCKETMQ_NAMESPACE::SimpleConsumer> dlq_consumer;
    std::mutex mutex;
    std::unordered_map<std::string, ROCKETMQ_NAMESPACE::MessageConstSharedPtr> deliveries;
    std::map<std::string, DlqDelivery> dlq_deliveries;
    std::string command_topic;
    std::string dlq_topic;
    std::string endpoint;
    std::string dlq_group;
    bool use_tls{true};

    void ensureDlqConsumer() {
        if (dlq_consumer != nullptr) return;
        auto configuration = ROCKETMQ_NAMESPACE::Configuration::newBuilder()
            .withEndpoints(endpoint).withSsl(use_tls).build();
        auto consumer = ROCKETMQ_NAMESPACE::SimpleConsumer::newBuilder()
            .withGroup(dlq_group)
            .withConfiguration(configuration)
            .subscribe(dlq_topic, ROCKETMQ_NAMESPACE::FilterExpression("*"))
            .withAwaitDuration(std::chrono::seconds(10)).build();
        dlq_consumer = std::make_unique<ROCKETMQ_NAMESPACE::SimpleConsumer>(std::move(consumer));
    }
};

RocketMqOrderQueue::RocketMqOrderQueue() : impl_(std::make_unique<Impl>()) {}
RocketMqOrderQueue::~RocketMqOrderQueue() { stop(); }

common::Status RocketMqOrderQueue::start(const std::string& endpoint, const std::string& command_topic,
                                         const std::string& consumer_group, bool enable_consumer,
                                         bool enable_dlq_operations) {
    if (endpoint.empty() || command_topic.empty() || consumer_group.empty()) {
        return common::Status::InvalidArgument("RocketMQ endpoint, topic and consumer group are required");
    }
    stop();
    impl_ = std::make_unique<Impl>();
    impl_->endpoint = endpoint;
    if (const auto status = rocketMqTopic(command_topic, &impl_->command_topic); !status.ok()) return status;
    const char* configured_dlq_topic = std::getenv("ECOMMERCE_ROCKETMQ_DLQ_TOPIC");
    const std::string logical_dlq_topic = configured_dlq_topic == nullptr || *configured_dlq_topic == '\0'
        ? command_topic + ".dlq" : configured_dlq_topic;
    if (const auto status = rocketMqTopic(logical_dlq_topic, &impl_->dlq_topic); !status.ok()) return status;
    bool use_tls = true;
    if (const char* tls_value = std::getenv("ECOMMERCE_ROCKETMQ_TLS"); tls_value != nullptr) {
        const std::string value(tls_value);
        use_tls = value != "0" && value != "false" && value != "off";
    }
    impl_->use_tls = use_tls;
    auto configuration = ROCKETMQ_NAMESPACE::Configuration::newBuilder()
        .withEndpoints(endpoint).withSsl(use_tls).build();
    impl_->producer = std::make_unique<ROCKETMQ_NAMESPACE::Producer>(
        ROCKETMQ_NAMESPACE::Producer::newBuilder().withConfiguration(configuration)
            .withTopics({impl_->command_topic, "order_events", "inventory_events", "notification_events"}).build());
    if (enable_consumer) {
        auto consumer = ROCKETMQ_NAMESPACE::SimpleConsumer::newBuilder().withGroup(consumer_group)
            .withConfiguration(configuration).subscribe(impl_->command_topic, ROCKETMQ_NAMESPACE::FilterExpression("*"))
            .withAwaitDuration(std::chrono::seconds(10)).build();
        impl_->consumer = std::make_unique<ROCKETMQ_NAMESPACE::SimpleConsumer>(std::move(consumer));
    }
    if (enable_dlq_operations) {
        impl_->dlq_group = consumer_group.substr(0, 38) + "-dlq-" +
            std::to_string(std::hash<std::string>{}(consumer_group));
    }
    return common::Status::Ok();
}

void RocketMqOrderQueue::stop() {
    if (impl_ == nullptr) return;
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->deliveries.clear();
    impl_->dlq_deliveries.clear();
    impl_->dlq_consumer.reset();
    impl_->consumer.reset();
    impl_->producer.reset();
}

common::Status RocketMqOrderQueue::push(const FlashSaleOrderMessage& message) {
    if (impl_ == nullptr || impl_->producer == nullptr) return common::Status::FailedPrecondition("RocketMQ producer is not started");
    if (const auto status = message.validate(); !status.ok()) return status;
    auto payload = message.serialize();
    auto request = ROCKETMQ_NAMESPACE::Message::newBuilder().withTopic(impl_->command_topic)
        .withTag("FlashSaleCommand").withKeys({message.idempotency_key}).withBody(std::move(payload)).build();
    std::error_code error;
    (void)impl_->producer->send(std::move(request), error);
    return error ? common::Status::Internal(error.message()) : common::Status::Ok();
}

common::Status RocketMqOrderQueue::pop(FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) {
    if (impl_ == nullptr || impl_->consumer == nullptr || message == nullptr || timeout.count() < 0) {
        return common::Status::FailedPrecondition("RocketMQ consumer is not ready");
    }
    std::vector<ROCKETMQ_NAMESPACE::MessageConstSharedPtr> messages;
    std::error_code error;
    (void)timeout;  // The SDK's synchronous SimpleConsumer API uses its configured long-poll duration.
    // Keep the delivery invisible while database work runs. The SDK's
    // synchronous API uses the ten-second long-poll configured at startup.
    impl_->consumer->receive(1, rocketMqInvisibleDuration(), error, messages);
    if (error) return common::Status::Internal(error.message());
    if (messages.empty() || messages.front() == nullptr) return common::Status::Incomplete("RocketMQ receive timed out");
    const auto& delivered = messages.front();
    if (const auto status = FlashSaleOrderMessage::deserialize(delivered->body(), message); !status.ok()) return status;
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->deliveries[message->order_id] = delivered;
    return common::Status::Ok();
}

common::Status RocketMqOrderQueue::acknowledge(const FlashSaleOrderMessage& message, bool processed) {
    if (impl_ == nullptr || impl_->consumer == nullptr) return common::Status::FailedPrecondition("RocketMQ consumer is not ready");
    std::lock_guard<std::mutex> lock(impl_->mutex);
    const auto it = impl_->deliveries.find(message.order_id);
    if (it == impl_->deliveries.end()) return common::Status::NotFound("RocketMQ delivery handle not found");
    if (!processed) return common::Status::Ok();  // Visibility timeout will redeliver the command.
    std::error_code error;
    impl_->consumer->ack(*it->second, error);
    if (error) return common::Status::Internal(error.message());
    impl_->deliveries.erase(it);
    return common::Status::Ok();
}

common::Status RocketMqOrderQueue::peekDlq(std::size_t limit, std::vector<RocketMqDlqMessage>* messages) {
    if (messages == nullptr || limit == 0) return common::Status::InvalidArgument("DLQ output and positive limit are required");
    if (impl_ == nullptr || impl_->dlq_topic.empty()) {
        return common::Status::FailedPrecondition("RocketMQ DLQ operations are not enabled");
    }
    limit = std::min<std::size_t>(limit, 1000);
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->ensureDlqConsumer();
    std::vector<ROCKETMQ_NAMESPACE::MessageConstSharedPtr> delivered;
    std::error_code error;
    impl_->dlq_consumer->receive(std::min<std::size_t>(limit, 32), std::chrono::seconds(30), error, delivered);
    if (error) return common::Status::Internal(error.message());
    for (const auto& message : delivered) {
        if (message == nullptr) continue;
        if (impl_->dlq_deliveries.size() >= 1000 && impl_->dlq_deliveries.find(message->id()) == impl_->dlq_deliveries.end()) {
            continue;  // Leave excess messages unacked; RocketMQ will make them visible again.
        }
        impl_->dlq_deliveries[message->id()] = {message, std::chrono::steady_clock::now()};
    }
    messages->clear();
    messages->reserve(std::min(limit, impl_->dlq_deliveries.size()));
    for (const auto& [message_id, message] : impl_->dlq_deliveries) {
        if (messages->size() >= limit) break;
        messages->push_back({message_id, message.message->body()});
    }
    return common::Status::Ok();
}

common::Status RocketMqOrderQueue::replayDlq(std::size_t limit, std::size_t* replayed) {
    if (replayed == nullptr || limit == 0) return common::Status::InvalidArgument("DLQ replay output and positive limit are required");
    *replayed = 0;
    if (impl_ == nullptr || impl_->dlq_topic.empty() || impl_->producer == nullptr) {
        return common::Status::FailedPrecondition("RocketMQ DLQ operations are not enabled");
    }
    limit = std::min<std::size_t>(limit, 1000);
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->ensureDlqConsumer();
    std::vector<ROCKETMQ_NAMESPACE::MessageConstSharedPtr> delivered;
    std::error_code error;
    impl_->dlq_consumer->receive(std::min<std::size_t>(limit, 32), std::chrono::seconds(30), error, delivered);
    if (error) return common::Status::Internal(error.message());
    for (const auto& message : delivered) {
        if (message != nullptr && (impl_->dlq_deliveries.size() < 1000 ||
            impl_->dlq_deliveries.find(message->id()) != impl_->dlq_deliveries.end())) {
            impl_->dlq_deliveries[message->id()] = {message, std::chrono::steady_clock::now()};
        }
    }

    std::size_t attempted = 0;
    for (auto it = impl_->dlq_deliveries.begin(); it != impl_->dlq_deliveries.end() && attempted < limit;) {
        const auto delivery = it->second;
        const auto message = delivery.message;
        if (message == nullptr || std::chrono::steady_clock::now() - delivery.received_at >= std::chrono::seconds(25)) {
            ++it;  // Only replay a current visibility lease; stale peek results remain untouched.
            continue;
        }
        FlashSaleOrderMessage command;
        if (!FlashSaleOrderMessage::deserialize(message->body(), &command).ok()) {
            ++it;  // Keep malformed records available for inspection/manual repair.
            continue;
        }
        auto replay = ROCKETMQ_NAMESPACE::Message::newBuilder().withTopic(impl_->command_topic)
            .withTag("FlashSaleCommand").withKeys({command.idempotency_key}).withBody(message->body()).build();
        error.clear();
        (void)impl_->producer->send(std::move(replay), error);
        if (error) return common::Status::Internal("RocketMQ DLQ replay publish failed: " + error.message());
        error.clear();
        impl_->dlq_consumer->ack(*message, error);
        if (error) return common::Status::Internal("RocketMQ DLQ replay was published but ACK failed: " + error.message());
        it = impl_->dlq_deliveries.erase(it);
        ++attempted;
        ++(*replayed);
    }
    return common::Status::Ok();
}

common::Status RocketMqOrderQueue::publish(const Event& event) {
    if (impl_ == nullptr || impl_->producer == nullptr || event.topic.empty()) {
        return common::Status::FailedPrecondition("RocketMQ producer is not ready");
    }
    std::string topic;
    if (const auto status = rocketMqTopic(event.topic, &topic); !status.ok()) return status;
    auto message = ROCKETMQ_NAMESPACE::Message::newBuilder().withTopic(std::move(topic))
        .withTag(event.topic).withKeys({event.event_id.empty() ? event.key : event.event_id})
        .withBody(event.payload).build();
    std::error_code error;
    (void)impl_->producer->send(std::move(message), error);
    return error ? common::Status::Internal(error.message()) : common::Status::Ok();
}

}  // namespace live::messaging
