#include "messaging/rabbitmq_order_queue.h"

#include "messaging/order_message.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <sys/time.h>
#include <thread>
#include <unordered_map>
#include <utility>

namespace live::messaging {
namespace {

common::Status checkReply(amqp_connection_state_t connection, const char* operation) {
    const auto reply = amqp_get_rpc_reply(connection);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) return common::Status::Ok();
    std::string detail(operation);
    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
        detail += ": ";
        detail += amqp_error_string2(reply.library_error);
    } else if (reply.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
        detail += ": broker returned a protocol error";
    }
    return common::Status::Internal(detail);
}

common::Status openConnection(const std::string& host, int port, const std::string& username,
                              const std::string& password, const std::string& vhost,
                              amqp_connection_state_t* connection) {
    if (connection == nullptr) return common::Status::InvalidArgument("connection output is required");
    *connection = amqp_new_connection();
    if (*connection == nullptr) return common::Status::Internal("cannot allocate RabbitMQ connection");
    amqp_socket_t* socket = amqp_tcp_socket_new(*connection);
    if (socket == nullptr) return common::Status::Internal("cannot allocate RabbitMQ socket");
    const int socket_status = amqp_socket_open(socket, host.c_str(), port);
    if (socket_status != AMQP_STATUS_OK) {
        const auto detail = std::string("RabbitMQ socket open failed: ") + amqp_error_string2(socket_status);
        (void)amqp_destroy_connection(*connection);
        *connection = nullptr;
        return common::Status::Internal(detail);
    }
    const auto login = amqp_login(*connection, vhost.c_str(), 0, 131072, 0,
                                  AMQP_SASL_METHOD_PLAIN, username.c_str(), password.c_str());
    if (login.reply_type != AMQP_RESPONSE_NORMAL) {
        (void)amqp_destroy_connection(*connection);
        *connection = nullptr;
        return common::Status::Internal("RabbitMQ login failed");
    }
    return common::Status::Ok();
}

common::Status openChannel(amqp_connection_state_t connection, amqp_channel_t channel) {
    amqp_channel_open(connection, channel);
    return checkReply(connection, "RabbitMQ channel open failed");
}

void closeConnection(amqp_connection_state_t* connection) {
    if (connection == nullptr || *connection == nullptr) return;
    (void)amqp_connection_close(*connection, AMQP_REPLY_SUCCESS);
    (void)amqp_destroy_connection(*connection);
    *connection = nullptr;
}

}  // namespace

class RabbitMqOrderQueue::Impl {
public:
    amqp_connection_state_t publisher{nullptr};
    amqp_connection_state_t consumer{nullptr};
    amqp_channel_t publisher_channel{1};
    amqp_channel_t consumer_channel{1};
    std::string exchange;
    std::string queue;
    std::string routing_key;
    std::string host;
    int port{0};
    std::string username;
    std::string password;
    std::string vhost;
    bool consumer_enabled{false};
    mutable std::mutex publisher_mutex;
    std::mutex consumer_mutex;
    std::unordered_map<std::string, std::uint64_t> deliveries;
    std::unordered_map<std::string, FlashSaleOrderMessage> held_messages;
    std::atomic<bool> started{false};

    common::Status connectPublisher() {
        closeConnection(&publisher);
        auto status = openConnection(host, port, username, password, vhost, &publisher);
        if (!status.ok()) return status;
        if (!(status = openChannel(publisher, publisher_channel)).ok()) {
            closeConnection(&publisher);
            return status;
        }
        amqp_confirm_select(publisher, publisher_channel);
        if (!(status = checkReply(publisher, "RabbitMQ publisher confirms unavailable")).ok()) {
            closeConnection(&publisher);
            return status;
        }
        amqp_exchange_declare(publisher, publisher_channel, amqp_cstring_bytes(exchange.c_str()),
                              amqp_cstring_bytes("direct"), 0, 1, 0, 0, amqp_empty_table);
        if (!(status = checkReply(publisher, "RabbitMQ exchange declaration failed")).ok()) {
            closeConnection(&publisher);
            return status;
        }
        amqp_table_t queue_arguments = amqp_empty_table;
        amqp_table_entry_t dead_letter_arguments[2]{};
        if (routing_key == "flashsale.command") {
            dead_letter_arguments[0].key = amqp_cstring_bytes("x-dead-letter-exchange");
            dead_letter_arguments[0].value.kind = AMQP_FIELD_KIND_UTF8;
            dead_letter_arguments[0].value.value.bytes = amqp_cstring_bytes(exchange.c_str());
            dead_letter_arguments[1].key = amqp_cstring_bytes("x-dead-letter-routing-key");
            dead_letter_arguments[1].value.kind = AMQP_FIELD_KIND_UTF8;
            dead_letter_arguments[1].value.value.bytes = amqp_cstring_bytes("flashsale.command.dlq");
            queue_arguments.num_entries = 2;
            queue_arguments.entries = dead_letter_arguments;
        }
        amqp_queue_declare(publisher, publisher_channel, amqp_cstring_bytes(queue.c_str()),
                           0, 1, 0, 0, queue_arguments);
        if (!(status = checkReply(publisher, "RabbitMQ queue declaration failed")).ok()) {
            closeConnection(&publisher);
            return status;
        }
        amqp_queue_bind(publisher, publisher_channel, amqp_cstring_bytes(queue.c_str()),
                        amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes(routing_key.c_str()), amqp_empty_table);
        if (!(status = checkReply(publisher, "RabbitMQ queue binding failed")).ok()) {
            closeConnection(&publisher);
            return status;
        }
        return common::Status::Ok();
    }

    common::Status connectConsumer() {
        closeConnection(&consumer);
        auto status = openConnection(host, port, username, password, vhost, &consumer);
        if (!status.ok()) return status;
        if (!(status = openChannel(consumer, consumer_channel)).ok()) {
            closeConnection(&consumer);
            return status;
        }
        amqp_basic_qos(consumer, consumer_channel, 0, 16, 0);
        if (!(status = checkReply(consumer, "RabbitMQ prefetch configuration failed")).ok()) {
            closeConnection(&consumer);
            return status;
        }
        amqp_basic_consume(consumer, consumer_channel, amqp_cstring_bytes(queue.c_str()),
                           amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
        if (!(status = checkReply(consumer, "RabbitMQ command consumer setup failed")).ok()) {
            closeConnection(&consumer);
            return status;
        }
        return common::Status::Ok();
    }

    common::Status publishConfirmed(const std::string& routing, const std::string& payload,
                                    const std::string& message_id) {
        std::lock_guard<std::mutex> lock(publisher_mutex);
        if (publisher == nullptr) {
            if (const auto status = connectPublisher(); !status.ok()) return status;
        }
        amqp_basic_properties_t properties{};
        properties._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
        properties.content_type = amqp_cstring_bytes("application/octet-stream");
        properties.delivery_mode = 2;
        if (!message_id.empty()) {
            properties._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
            properties.message_id = amqp_cstring_bytes(message_id.c_str());
        }
        amqp_bytes_t body{payload.size(), const_cast<char*>(payload.data())};
        const int result = amqp_basic_publish(publisher, publisher_channel,
            amqp_cstring_bytes(exchange.c_str()), amqp_cstring_bytes(routing.c_str()), 0, 0,
            &properties, body);
        if (result != AMQP_STATUS_OK) {
            const auto status = common::Status::Internal(std::string("RabbitMQ publish failed: ") + amqp_error_string2(result));
            closeConnection(&publisher);
            return status;
        }
        timeval timeout{5, 0};
        amqp_frame_t frame{};
        const int wait_result = amqp_simple_wait_frame_noblock(publisher, &frame, &timeout);
        if (wait_result != AMQP_STATUS_OK) {
            const auto status = common::Status::Internal(std::string("RabbitMQ publisher confirm failed: ") + amqp_error_string2(wait_result));
            closeConnection(&publisher);
            return status;
        }
        if (frame.frame_type != AMQP_FRAME_METHOD || frame.channel != publisher_channel) {
            closeConnection(&publisher);
            return common::Status::Internal("RabbitMQ returned an unexpected publisher confirm frame");
        }
        if (frame.payload.method.id == AMQP_BASIC_ACK_METHOD) return common::Status::Ok();
        if (frame.payload.method.id == AMQP_BASIC_NACK_METHOD) return common::Status::Internal("RabbitMQ negatively acknowledged the message");
        return common::Status::Internal("RabbitMQ returned an unknown publisher confirm");
    }
};

RabbitMqOrderQueue::RabbitMqOrderQueue() : impl_(std::make_unique<Impl>()) {}
RabbitMqOrderQueue::~RabbitMqOrderQueue() { stop(); }

common::Status RabbitMqOrderQueue::start(const std::string& host, int port, const std::string& username,
                                         const std::string& password, const std::string& vhost,
                                         const std::string& exchange, const std::string& command_queue,
                                         const std::string& routing_key, bool enable_consumer) {
    if (host.empty() || port < 1 || port > 65535 || username.empty() || exchange.empty() ||
        command_queue.empty() || routing_key.empty()) {
        return common::Status::InvalidArgument("RabbitMQ host, credentials, exchange, queue and routing key are required");
    }
    stop();
    impl_ = std::make_unique<Impl>();
    impl_->exchange = exchange;
    impl_->queue = command_queue;
    impl_->routing_key = routing_key;
    impl_->host = host;
    impl_->port = port;
    impl_->username = username;
    impl_->password = password;
    impl_->vhost = vhost;
    auto status = impl_->connectPublisher();
    if (!status.ok()) { stop(); return status; }

    if (enable_consumer) {
        status = impl_->connectConsumer();
        if (!status.ok()) { stop(); return status; }
        impl_->consumer_enabled = true;
    }
    impl_->started.store(true);
    return common::Status::Ok();
}

void RabbitMqOrderQueue::stop() {
    if (impl_ == nullptr) return;
    impl_->started.store(false);
    std::scoped_lock lock(impl_->consumer_mutex, impl_->publisher_mutex);
    closeConnection(&impl_->consumer);
    closeConnection(&impl_->publisher);
    impl_->deliveries.clear();
    impl_->held_messages.clear();
    impl_->consumer_enabled = false;
}

common::Status RabbitMqOrderQueue::push(const FlashSaleOrderMessage& message) {
    if (const auto status = message.validate(); !status.ok()) return status;
    if (impl_ == nullptr || !impl_->started.load()) return common::Status::FailedPrecondition("RabbitMQ queue is not started");
    return impl_->publishConfirmed(impl_->routing_key, message.serialize(), message.idempotency_key);
}

common::Status RabbitMqOrderQueue::pop(FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) {
    if (impl_ == nullptr || !impl_->started.load() || !impl_->consumer_enabled || message == nullptr || timeout.count() < 0) {
        return common::Status::FailedPrecondition("RabbitMQ consumer is not ready");
    }
    std::lock_guard<std::mutex> lock(impl_->consumer_mutex);
    if (impl_->consumer == nullptr) {
        if (const auto status = impl_->connectConsumer(); !status.ok()) return status;
    }
    timeval wait{static_cast<time_t>(timeout.count() / 1000), static_cast<suseconds_t>((timeout.count() % 1000) * 1000)};
    amqp_envelope_t envelope{};
    const auto result = amqp_consume_message(impl_->consumer, &envelope, &wait, 0);
    if (result.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && result.library_error == AMQP_STATUS_TIMEOUT) {
        return common::Status::Incomplete("RabbitMQ receive timed out");
    }
    if (result.reply_type != AMQP_RESPONSE_NORMAL) {
        const auto status = checkReply(impl_->consumer, "RabbitMQ receive failed");
        closeConnection(&impl_->consumer);
        impl_->deliveries.clear();
        impl_->held_messages.clear();
        return status;
    }
    const auto* bytes = static_cast<const char*>(envelope.message.body.bytes);
    const std::string payload(bytes == nullptr ? "" : bytes, envelope.message.body.len);
    const auto delivery_tag = envelope.delivery_tag;
    amqp_destroy_envelope(&envelope);
    if (const auto status = FlashSaleOrderMessage::deserialize(payload, message); !status.ok()) {
        amqp_basic_nack(impl_->consumer, impl_->consumer_channel, delivery_tag, 0, 0);
        (void)checkReply(impl_->consumer, "RabbitMQ poison message reject failed");
        return status;
    }
    impl_->deliveries[message->order_id] = delivery_tag;
    impl_->held_messages[message->order_id] = *message;
    return common::Status::Ok();
}

common::Status RabbitMqOrderQueue::acknowledge(const FlashSaleOrderMessage& message, bool processed) {
    if (impl_ == nullptr || !impl_->consumer_enabled) return common::Status::FailedPrecondition("RabbitMQ consumer is not ready");
    std::lock_guard<std::mutex> lock(impl_->consumer_mutex);
    const auto it = impl_->deliveries.find(message.order_id);
    if (it == impl_->deliveries.end()) return common::Status::NotFound("RabbitMQ delivery handle not found");
    if (processed) amqp_basic_ack(impl_->consumer, impl_->consumer_channel, it->second, 0);
    else amqp_basic_nack(impl_->consumer, impl_->consumer_channel, it->second, 0, 1);
    if (const auto status = checkReply(impl_->consumer, "RabbitMQ acknowledgement failed"); !status.ok()) {
        closeConnection(&impl_->consumer);
        impl_->deliveries.clear();
        impl_->held_messages.clear();
        return status;
    }
    impl_->deliveries.erase(it);
    impl_->held_messages.erase(message.order_id);
    return common::Status::Ok();
}

common::Status RabbitMqOrderQueue::peekDlq(std::size_t limit, std::vector<FlashSaleOrderMessage>* messages) {
    if (messages == nullptr || limit == 0) return common::Status::InvalidArgument("DLQ output and positive limit are required");
    if (impl_ == nullptr || !impl_->consumer_enabled) return common::Status::FailedPrecondition("RabbitMQ DLQ consumer is not enabled");
    limit = std::min<std::size_t>(limit, 1000);
    while (true) {
        {
            std::lock_guard<std::mutex> lock(impl_->consumer_mutex);
            if (impl_->held_messages.size() >= limit) break;
        }
        FlashSaleOrderMessage message;
        const auto status = pop(&message, std::chrono::milliseconds(5));
        if (!status.ok()) {
            if (status.isIncomplete()) break;
            return status;
        }
    }
    std::lock_guard<std::mutex> lock(impl_->consumer_mutex);
    messages->clear();
    messages->reserve(std::min(limit, impl_->held_messages.size()));
    for (const auto& [order_id, message] : impl_->held_messages) {
        (void)order_id;
        if (messages->size() == limit) break;
        messages->push_back(message);
    }
    return common::Status::Ok();
}

common::Status RabbitMqOrderQueue::replayDlq(RabbitMqOrderQueue* destination, std::size_t limit,
                                              std::size_t* replayed) {
    if (destination == nullptr || replayed == nullptr || limit == 0) {
        return common::Status::InvalidArgument("DLQ destination and positive replay limit are required");
    }
    *replayed = 0;
    std::vector<FlashSaleOrderMessage> messages;
    if (const auto status = peekDlq(limit, &messages); !status.ok()) return status;
    for (const auto& message : messages) {
        if (const auto status = destination->push(message); !status.ok()) return status;
        if (const auto status = acknowledge(message, true); !status.ok()) return status;
        ++*replayed;
    }
    return common::Status::Ok();
}

std::size_t RabbitMqOrderQueue::pending() const {
    if (impl_ == nullptr || !impl_->started.load()) return 0;
    std::lock_guard<std::mutex> lock(impl_->publisher_mutex);
    if (impl_->publisher == nullptr && !impl_->connectPublisher().ok()) return 0;
    const auto* details = amqp_queue_declare(impl_->publisher, impl_->publisher_channel,
        amqp_cstring_bytes(impl_->queue.c_str()), 1, 0, 0, 0, amqp_empty_table);
    if (details == nullptr || amqp_get_rpc_reply(impl_->publisher).reply_type != AMQP_RESPONSE_NORMAL) return 0;
    return details->message_count;
}

common::Status RabbitMqOrderQueue::publish(const Event& event) {
    if (event.topic.empty() || event.event_id.empty()) return common::Status::InvalidArgument("outbox event is missing topic or event id");
    if (impl_ == nullptr || !impl_->started.load()) return common::Status::FailedPrecondition("RabbitMQ publisher is not started");
    return impl_->publishConfirmed(event.topic, event.payload, event.event_id);
}

}  // namespace live::messaging
