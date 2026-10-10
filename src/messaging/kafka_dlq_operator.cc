#include "messaging/kafka_dlq_operator.h"

#include "messaging/kafka_client.h"

#include <atomic>
#include <chrono>

namespace live::messaging {

common::Status KafkaDlqOperator::peek(const std::string& brokers, const std::string& dlq_topic,
                                      std::size_t max_messages, std::vector<Event>* events) {
    if (brokers.empty() || dlq_topic.empty() || max_messages == 0 || events == nullptr) {
        return common::Status::InvalidArgument("invalid DLQ inspection arguments");
    }
    events->clear();
    KafkaConsumer consumer;
    static std::atomic<std::uint64_t> sequence{1};
    const std::string group = "dlq-inspect-" + std::to_string(sequence.fetch_add(1));
    if (const auto status = consumer.start(brokers, group, {dlq_topic}); !status.ok()) return status;
    auto last_message = std::chrono::steady_clock::now();
    while (events->size() < max_messages && std::chrono::steady_clock::now() - last_message < std::chrono::seconds(2)) {
        const auto status = consumer.poll(250, [&](const Event& event) {
            events->push_back(event);
            last_message = std::chrono::steady_clock::now();
            return common::Status::Ok();
        });
        if (!status.ok()) { consumer.stop(); return status; }
    }
    consumer.stop();
    return common::Status::Ok();
}

common::Status KafkaDlqOperator::replay(const std::string& brokers, const std::string& dlq_topic,
                                        const std::string& target_topic, std::size_t max_messages,
                                        std::size_t* replayed) {
    if (brokers.empty() || dlq_topic.empty() || target_topic.empty() || max_messages == 0 || replayed == nullptr) {
        return common::Status::InvalidArgument("invalid DLQ replay arguments");
    }
    *replayed = 0;
    KafkaConsumer consumer;
    KafkaProducer producer;
    static std::atomic<std::uint64_t> sequence{1};
    const std::string group = "dlq-replay-" + std::to_string(sequence.fetch_add(1));
    if (const auto status = consumer.start(brokers, group, {dlq_topic}); !status.ok()) return status;
    if (const auto status = producer.start(brokers, "dlq-replay-producer"); !status.ok()) return status;
    auto last_message = std::chrono::steady_clock::now();
    while (*replayed < max_messages && std::chrono::steady_clock::now() - last_message < std::chrono::seconds(2)) {
        const auto status = consumer.poll(250, [&](const Event& event) {
            Event replayed_event = event;
            replayed_event.topic = target_topic;
            replayed_event.event_id = "replay-" + event.event_id;
            const auto publish_status = producer.publish(replayed_event);
            if (publish_status.ok()) {
                ++(*replayed);
                last_message = std::chrono::steady_clock::now();
            }
            return publish_status;
        });
        if (!status.ok()) return status;
    }
    const auto flush_status = producer.flush(5000);
    producer.stop();
    consumer.stop();
    return flush_status;
}

}  // namespace live::messaging
