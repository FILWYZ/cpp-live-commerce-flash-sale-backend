#include "storage/mysql/mysql_transaction_store.h"

#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"

#include <chrono>
#include <algorithm>
#include <cstring>
#include <limits>
#include <atomic>
#include <vector>

namespace live::storage::mysql {
namespace {

using ::live::business::promotion::PromotionState;

PromotionState parsePromotionState(const char* value) {
    if (value == nullptr) return PromotionState::kCreated;
    if (std::strcmp(value, "PREHEATING") == 0) return PromotionState::kPreheating;
    if (std::strcmp(value, "RUNNING") == 0) return PromotionState::kRunning;
    if (std::strcmp(value, "PAUSED") == 0) return PromotionState::kPaused;
    if (std::strcmp(value, "FINISHED") == 0) return PromotionState::kFinished;
    return PromotionState::kCreated;
}


std::string mysqlError(MYSQL* connection, const char* fallback) {
    if (connection == nullptr) return fallback;
    const char* error = mysql_error(connection);
    return error == nullptr || *error == '\0' ? fallback : error;
}

::live::business::order::OrderState parseState(const char* value) {
    using ::live::business::order::OrderState;
    if (value == nullptr) return OrderState::kPendingPayment;
    if (std::strcmp(value, "PAID") == 0) return OrderState::kPaid;
    if (std::strcmp(value, "EXPIRED") == 0) return OrderState::kExpired;
    if (std::strcmp(value, "CANCELLED") == 0) return OrderState::kCancelled;
    return OrderState::kPendingPayment;
}

const char* stateName(::live::business::order::OrderState state) {
    using ::live::business::order::OrderState;
    switch (state) {
        case OrderState::kPendingPayment: return "PENDING_PAYMENT";
        case OrderState::kPaid: return "PAID";
        case OrderState::kExpired: return "EXPIRED";
        case OrderState::kCancelled: return "CANCELLED";
    }
    return "PENDING_PAYMENT";
}

common::Status readOrder(MYSQL* connection, const std::string& sql, ::live::business::order::Order* order) {
    if (connection == nullptr || order == nullptr) return common::Status::InvalidArgument("invalid MySQL order query");
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "MySQL order query failed"));
    MYSQL_RES* result = mysql_store_result(connection);
    if (result == nullptr) return common::Status::Internal(mysqlError(connection, "MySQL order result failed"));
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == nullptr) { mysql_free_result(result); return common::Status::NotFound("order not found"); }
    order->id = row[0] == nullptr ? "" : row[0]; order->user_id = row[1] == nullptr ? "" : row[1];
    order->idempotency_key = row[2] == nullptr ? "" : row[2]; order->state = parseState(row[3]);
    order->sku_id = row[4] == nullptr ? "" : row[4]; order->quantity = row[5] == nullptr ? 0 : std::stoll(row[5]);
    order->unit_price_cents = row[6] == nullptr ? 0 : std::stoll(row[6]);
    mysql_free_result(result); return common::Status::Ok();
}

}  // namespace

MySqlTransactionStore::MySqlTransactionStore() : pool_(std::make_unique<MySqlConnectionPool>()) {}
MySqlTransactionStore::~MySqlTransactionStore() { close(); }

common::Status MySqlTransactionStore::connect(const std::string& host, std::uint16_t port, const std::string& user,
                                              const std::string& password, const std::string& database) {
    return connect({host}, port, user, password, database, 4);
}
common::Status MySqlTransactionStore::connect(const std::vector<std::string>& hosts, std::uint16_t port,
                                              const std::string& user, const std::string& password,
                                              const std::string& database, std::size_t pool_size) {
    return pool_->connect(hosts, port, user, password, database, pool_size);
}
void MySqlTransactionStore::close() { if (pool_ != nullptr) pool_->close(); }
bool MySqlTransactionStore::connected() const { return pool_ != nullptr && pool_->connected(); }
common::Status MySqlTransactionStore::ping() const {
    if (pool_ == nullptr) return common::Status::FailedPrecondition("MySQL pool is not configured");
    auto lease = pool_->acquire(std::chrono::milliseconds(500));
    if (lease.get() == nullptr) return common::Status::FailedPrecondition("MySQL connection pool is unavailable");
    return execute(lease.get(), "SELECT 1");
}

void MySqlTransactionStore::recordSql(const char* operation, const common::Status& status,
                                       std::chrono::steady_clock::time_point started) const {
    if (metrics_ == nullptr || operation == nullptr) return;
    metrics_->increment(operation);
    metrics_->observe(std::string(operation) + "_duration_ms",
                      std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count());
    if (!status.ok()) metrics_->increment(std::string(operation) + "_errors");
}

common::Status MySqlTransactionStore::execute(MYSQL* connection, const std::string& sql) const {
    const auto started = std::chrono::steady_clock::now();
    if (connection == nullptr) {
        const auto status = common::Status::FailedPrecondition("MySQL connection is not available");
        recordSql("mysql_sql", status, started);
        return status;
    }
    if (mysql_query(connection, sql.c_str()) != 0) {
        const auto status = common::Status::Internal(mysqlError(connection, "MySQL query failed"));
        recordSql("mysql_sql", status, started);
        return status;
    }
    MYSQL_RES* result = mysql_store_result(connection); if (result != nullptr) mysql_free_result(result);
    const auto status = common::Status::Ok();
    recordSql("mysql_sql", status, started);
    return status;
}
common::Status MySqlTransactionStore::begin(MYSQL* connection) const { return execute(connection, "START TRANSACTION"); }
common::Status MySqlTransactionStore::rollback(MYSQL* connection) const { return execute(connection, "ROLLBACK"); }
common::Status MySqlTransactionStore::commit(MYSQL* connection) const { return execute(connection, "COMMIT"); }

std::string MySqlTransactionStore::quote(MYSQL* connection, const std::string& value) const {
    if (connection == nullptr) return "''";
    std::string escaped(value.size() * 2 + 1, '\0');
    const auto size = mysql_real_escape_string(connection, escaped.data(), value.data(), value.size());
    escaped.resize(size); return "'" + escaped + "'";
}

common::Status MySqlTransactionStore::loadOrder(MYSQL* connection, const std::string& order_id,
                                                ::live::business::order::Order* order) const {
    return readOrder(connection, "SELECT o.id,o.user_id,o.idempotency_key,o.state,oi.sku_id,oi.quantity,oi.unit_price_cents FROM orders o JOIN order_items oi ON oi.order_id=o.id WHERE o.id=" + quote(connection, order_id) + " LIMIT 1", order);
}

common::Status MySqlTransactionStore::appendOutbox(MYSQL* connection, const ::live::messaging::Event& event) const {
    if (event.event_id.empty() || event.topic.empty()) return common::Status::InvalidArgument("invalid outbox event");
    return execute(connection, "INSERT INTO outbox_events(event_id,topic,event_key,payload,published) VALUES (" + quote(connection, event.event_id) + "," + quote(connection, event.topic) + "," + quote(connection, event.key) + "," + quote(connection, event.payload) + ",FALSE) ON DUPLICATE KEY UPDATE topic=VALUES(topic),event_key=VALUES(event_key),payload=VALUES(payload)");
}

common::Status MySqlTransactionStore::persistQueuedOrder(const ::live::messaging::FlashSaleOrderMessage& message,
                                                         ::live::business::order::Order* order) {
    if (!message.validate().ok() || order == nullptr) return common::Status::InvalidArgument("invalid queued order message");
    auto lease = pool_->acquire(); MYSQL* connection = lease.get(); if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    // These point lookups intentionally probe for absent idempotency/request rows.
    // Under InnoDB's default REPEATABLE READ, locking reads can retain next-key
    // gap locks and deadlock concurrent inserts for different request ids.
    if (const auto status = execute(connection, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); !status.ok()) return status;
    if (const auto status = begin(connection); !status.ok()) return status;
    const std::string sql = "SELECT o.id,o.user_id,o.idempotency_key,o.state,oi.sku_id,oi.quantity,oi.unit_price_cents FROM orders o JOIN order_items oi ON oi.order_id=o.id WHERE o.idempotency_key=" + quote(connection, message.idempotency_key) + " FOR UPDATE";
    if (mysql_query(connection, sql.c_str()) != 0) { rollback(connection); return common::Status::Internal(mysqlError(connection, "MySQL flash-sale idempotency query failed")); }
    MYSQL_RES* result = mysql_store_result(connection); if (result == nullptr) { rollback(connection); return common::Status::Internal(mysqlError(connection, "MySQL flash-sale idempotency result failed")); }
    MYSQL_ROW existing = mysql_fetch_row(result);
    if (existing != nullptr) {
        order->id = existing[0] == nullptr ? "" : existing[0]; order->user_id = existing[1] == nullptr ? "" : existing[1]; order->idempotency_key = existing[2] == nullptr ? "" : existing[2]; order->state = parseState(existing[3]); order->sku_id = existing[4] == nullptr ? "" : existing[4]; order->quantity = existing[5] == nullptr ? 0 : std::stoll(existing[5]); order->unit_price_cents = existing[6] == nullptr ? 0 : std::stoll(existing[6]); mysql_free_result(result);
        if (order->id != message.order_id || order->user_id != message.user_id || order->sku_id != message.sku_id || order->quantity != message.quantity) { rollback(connection); return common::Status::FailedPrecondition("flash-sale message conflicts with persisted order"); }
        return commit(connection);
    }
    mysql_free_result(result);
    const auto request_state_sql = "SELECT user_id,state FROM flash_sale_requests WHERE request_id=" +
        quote(connection, message.idempotency_key) + " FOR UPDATE";
    if (mysql_query(connection, request_state_sql.c_str()) != 0) {
        const auto status = common::Status::Internal(mysqlError(connection, "MySQL flash-sale request state query failed"));
        rollback(connection);
        return status;
    }
    MYSQL_RES* request_state_result = mysql_store_result(connection);
    if (request_state_result == nullptr) {
        const auto status = common::Status::Internal(mysqlError(connection, "MySQL flash-sale request state result failed"));
        rollback(connection);
        return status;
    }
    MYSQL_ROW request_state_row = mysql_fetch_row(request_state_result);
    if (request_state_row != nullptr) {
        const std::string request_user = request_state_row[0] == nullptr ? "" : request_state_row[0];
        const auto request_state = ::live::business::flash_sale::parseFlashSaleRequestState(
            request_state_row[1] == nullptr ? "" : request_state_row[1]);
        mysql_free_result(request_state_result);
        if (request_user != message.user_id) {
            rollback(connection);
            return common::Status::FailedPrecondition("request id belongs to another user");
        }
        using ::live::business::flash_sale::FlashSaleRequestState;
        if (request_state == FlashSaleRequestState::kRejected || request_state == FlashSaleRequestState::kSoldOut ||
            request_state == FlashSaleRequestState::kPaid || request_state == FlashSaleRequestState::kExpired ||
            request_state == FlashSaleRequestState::kCancelled) {
            rollback(connection);
            return common::Status::FailedPrecondition("terminal flash-sale request cannot create an order");
        }
    } else {
        mysql_free_result(request_state_result);
    }
    if (const auto status = execute(connection, "UPDATE inventory SET available=available-" + std::to_string(message.quantity) + ",reserved=reserved+" + std::to_string(message.quantity) + ",version=version+1 WHERE sku_id=" + quote(connection, message.sku_id) + " AND available>=" + std::to_string(message.quantity)); !status.ok()) { rollback(connection); return status; }
    if (mysql_affected_rows(connection) != 1) { rollback(connection); return common::Status::ResourceExhausted("insufficient or missing MySQL inventory for flash-sale order"); }
    if (const auto status = execute(connection, "INSERT INTO orders(id,user_id,campaign_id,idempotency_key,state) VALUES (" + quote(connection, message.order_id) + "," + quote(connection, message.user_id) + "," + quote(connection, message.promotion_id) + "," + quote(connection, message.idempotency_key) + ",'PENDING_PAYMENT')"); !status.ok()) { rollback(connection); return status; }
    std::int64_t unit_price_cents = 0;
    if (mysql_query(connection, ("SELECT price_cents FROM products WHERE id=" + quote(connection, message.sku_id) + " LIMIT 1").c_str()) == 0) {
        MYSQL_RES* product_result = mysql_store_result(connection);
        if (product_result != nullptr) {
            MYSQL_ROW product = mysql_fetch_row(product_result);
            if (product != nullptr && product[0] != nullptr) unit_price_cents = std::stoll(product[0]);
            mysql_free_result(product_result);
        }
    }
    if (const auto status = execute(connection, "INSERT INTO order_items(order_id,sku_id,quantity,unit_price_cents) VALUES (" + quote(connection, message.order_id) + "," + quote(connection, message.sku_id) + "," + std::to_string(message.quantity) + "," + std::to_string(unit_price_cents) + ")"); !status.ok()) { rollback(connection); return status; }
    const auto request_insert = "INSERT INTO flash_sale_requests(request_id,user_id,order_id,state,message) VALUES (" +
        quote(connection, message.idempotency_key) + "," + quote(connection, message.user_id) + "," + quote(connection, message.order_id) +
        ",'PENDING_PAYMENT','order created; payment is pending') ON DUPLICATE KEY UPDATE "
        "order_id=IF(user_id=VALUES(user_id),VALUES(order_id),order_id),"
        "state=IF(user_id=VALUES(user_id) AND state NOT IN ('SOLD_OUT','REJECTED','PAID','EXPIRED','CANCELLED'),VALUES(state),state),"
        "message=IF(user_id=VALUES(user_id),VALUES(message),message)";
    if (const auto status = execute(connection, request_insert); !status.ok()) { rollback(connection); return status; }
    const auto request_owner_query = "SELECT user_id FROM flash_sale_requests WHERE request_id=" + quote(connection, message.idempotency_key) + " FOR UPDATE";
    if (mysql_query(connection, request_owner_query.c_str()) != 0) { const auto status = common::Status::Internal(mysqlError(connection, "flash-sale request owner lookup failed")); rollback(connection); return status; }
    MYSQL_RES* request_owner_result = mysql_store_result(connection);
    if (request_owner_result == nullptr) { const auto status = common::Status::Internal(mysqlError(connection, "flash-sale request owner result failed")); rollback(connection); return status; }
    MYSQL_ROW request_owner = mysql_fetch_row(request_owner_result);
    const bool same_owner = request_owner != nullptr && request_owner[0] != nullptr && message.user_id == request_owner[0];
    mysql_free_result(request_owner_result);
    if (!same_owner) { rollback(connection); return common::Status::FailedPrecondition("request id belongs to another user"); }
    ::live::messaging::Event event{"flash-order-created-" + message.order_id, "order.events", message.order_id,
        "OrderCreated|" + message.order_id + "|" + message.user_id + "|" + message.sku_id + "|" + std::to_string(message.quantity), 0};
    if (const auto status = appendOutbox(connection, event); !status.ok()) { rollback(connection); return status; }
    if (const auto status = commit(connection); !status.ok()) return status;
    *order = {message.order_id, message.idempotency_key, message.user_id, message.sku_id, message.quantity, ::live::business::order::OrderState::kPendingPayment}; return common::Status::Ok();
}

common::Status MySqlTransactionStore::getOrder(const std::string& order_id, ::live::business::order::Order* order) const {
    auto lease = pool_->acquire(); return loadOrder(lease.get(), order_id, order);
}

common::Status MySqlTransactionStore::put(const ::live::business::flash_sale::FlashSaleRequestStatus& request) {
    if (request.request_id.empty() || request.user_id.empty()) return common::Status::InvalidArgument("invalid flash-sale request status");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    const auto state = ::live::business::flash_sale::flashSaleRequestStateName(request.state);
    const auto sql = "INSERT INTO flash_sale_requests(request_id,user_id,order_id,state,message) VALUES (" +
        quote(connection, request.request_id) + "," + quote(connection, request.user_id) + "," + quote(connection, request.order_id) +
        "," + quote(connection, state) + "," + quote(connection, request.message.substr(0, 500)) +
        ") ON DUPLICATE KEY UPDATE user_id=IF(user_id=VALUES(user_id),user_id,user_id),order_id=VALUES(order_id)," +
        "message=VALUES(message),state=IF(state IN ('SOLD_OUT','REJECTED','PAID','EXPIRED','CANCELLED'),state,VALUES(state))";
    return execute(connection, sql);
}

common::Status MySqlTransactionStore::putIfAbsent(
    const ::live::business::flash_sale::FlashSaleRequestStatus& request, bool* inserted) {
    if (inserted == nullptr || request.request_id.empty() || request.user_id.empty())
        return common::Status::InvalidArgument("invalid flash-sale request status claim");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto state = ::live::business::flash_sale::flashSaleRequestStateName(request.state);
    const auto sql = "INSERT IGNORE INTO flash_sale_requests(request_id,user_id,order_id,state,message) VALUES (" +
        quote(connection, request.request_id) + "," + quote(connection, request.user_id) + "," + quote(connection, request.order_id) +
        "," + quote(connection, state) + "," + quote(connection, request.message.substr(0, 500)) + ")";
    if (const auto status = execute(connection, sql); !status.ok()) return status;
    *inserted = mysql_affected_rows(connection) == 1;
    return common::Status::Ok();
}

common::Status MySqlTransactionStore::get(const std::string& request_id,
    ::live::business::flash_sale::FlashSaleRequestStatus* status) const {
    if (request_id.empty() || status == nullptr) return common::Status::InvalidArgument("invalid request status lookup");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto sql = "SELECT request_id,user_id,order_id,state,message FROM flash_sale_requests WHERE request_id=" + quote(connection, request_id);
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "MySQL request status query failed"));
    MYSQL_RES* result = mysql_store_result(connection);
    if (result == nullptr) return common::Status::Internal(mysqlError(connection, "MySQL request status result failed"));
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == nullptr) { mysql_free_result(result); return common::Status::NotFound("flash-sale request not found"); }
    status->request_id = row[0] == nullptr ? "" : row[0];
    status->user_id = row[1] == nullptr ? "" : row[1];
    status->order_id = row[2] == nullptr ? "" : row[2];
    status->state = ::live::business::flash_sale::parseFlashSaleRequestState(row[3] == nullptr ? "" : row[3]);
    status->message = row[4] == nullptr ? "" : row[4];
    mysql_free_result(result);
    return common::Status::Ok();
}

common::Status MySqlTransactionStore::transition(const std::string& request_id,
    ::live::business::flash_sale::FlashSaleRequestState expected,
    ::live::business::flash_sale::FlashSaleRequestState next, bool* updated) {
    if (request_id.empty() || updated == nullptr)
        return common::Status::InvalidArgument("invalid request state transition");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto sql = "UPDATE flash_sale_requests SET state=" +
        quote(connection, ::live::business::flash_sale::flashSaleRequestStateName(next)) +
        " WHERE request_id=" + quote(connection, request_id) + " AND state=" +
        quote(connection, ::live::business::flash_sale::flashSaleRequestStateName(expected));
    if (const auto status = execute(connection, sql); !status.ok()) return status;
    *updated = mysql_affected_rows(connection) == 1;
    return common::Status::Ok();
}

common::Status MySqlTransactionStore::transitionOrder(const std::string& order_id,
    ::live::business::order::OrderState expected, ::live::business::order::OrderState next,
    ::live::business::order::Order* order) {
    using ::live::business::order::OrderState;
    if (order_id.empty() || order == nullptr || expected != OrderState::kPendingPayment ||
        (next != OrderState::kPaid && next != OrderState::kExpired && next != OrderState::kCancelled)) {
        return common::Status::InvalidArgument("invalid order state transition");
    }
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    if (const auto status = begin(connection); !status.ok()) return status;
    const std::string update = "UPDATE orders SET state=" + quote(connection, stateName(next)) +
        ",expiry_claimed_by=NULL,expiry_lease_until=NULL" +
        " WHERE id=" + quote(connection, order_id) + " AND state='PENDING_PAYMENT'";
    if (const auto status = execute(connection, update); !status.ok()) { rollback(connection); return status; }
    if (mysql_affected_rows(connection) == 0) {
        ::live::business::order::Order current;
        const auto found = loadOrder(connection, order_id, &current);
        if (!found.ok()) { rollback(connection); return found; }
        if (current.state == next) { *order = current; return commit(connection); }
        rollback(connection);
        return common::Status::Aborted("order state changed concurrently");
    }
    ::live::business::order::Order current;
    if (const auto status = loadOrder(connection, order_id, &current); !status.ok()) { rollback(connection); return status; }
    const std::string inventory_transition = next == OrderState::kPaid
        ? "UPDATE inventory SET reserved=reserved-" + std::to_string(current.quantity) + ",sold=sold+" +
          std::to_string(current.quantity) + ",version=version+1 WHERE sku_id=" + quote(connection, current.sku_id) +
          " AND reserved>=" + std::to_string(current.quantity)
        : "UPDATE inventory SET available=available+" + std::to_string(current.quantity) + ",reserved=reserved-" +
          std::to_string(current.quantity) + ",version=version+1 WHERE sku_id=" + quote(connection, current.sku_id) +
          " AND reserved>=" + std::to_string(current.quantity);
    if (const auto status = execute(connection, inventory_transition); !status.ok()) { rollback(connection); return status; }
    if (mysql_affected_rows(connection) != 1) { rollback(connection); return common::Status::FailedPrecondition("durable inventory reservation is inconsistent"); }
    if (next == OrderState::kPaid) {
        const std::string payment_id = "payment-" + order_id;
        if (current.unit_price_cents < 0 || (current.quantity > 0 &&
            current.unit_price_cents > std::numeric_limits<std::int64_t>::max() / current.quantity)) {
            rollback(connection);
            return common::Status::InvalidArgument("order amount overflows payment ledger");
        }
        const auto amount_cents = current.unit_price_cents * current.quantity;
        const std::string insert_payment = "INSERT INTO payments(id,order_id,provider,provider_transaction_id,amount_cents,state) VALUES (" +
            quote(connection, payment_id) + "," + quote(connection, order_id) + ",'mock'," + quote(connection, "mock-tx-" + order_id) +
            "," + std::to_string(amount_cents) + ",'SUCCEEDED') ON DUPLICATE KEY UPDATE state='SUCCEEDED'";
        if (const auto status = execute(connection, insert_payment); !status.ok()) { rollback(connection); return status; }
    }
    ::live::messaging::Event event;
    event.event_id = std::string(next == OrderState::kPaid ? "stock-confirm-" : "stock-release-") + order_id;
    event.topic = "inventory.events";
    event.key = order_id;
    event.payload = std::string(next == OrderState::kPaid ? "StockConfirm|" : "StockRelease|") + order_id + "|" +
                    current.sku_id + "|" + std::to_string(current.quantity);
    if (const auto status = appendOutbox(connection, event); !status.ok()) { rollback(connection); return status; }
    const auto request_state = next == OrderState::kPaid ? "PAID" :
        (next == OrderState::kExpired ? "EXPIRED" : "CANCELLED");
    const auto update_request = "UPDATE flash_sale_requests SET state=" + quote(connection, request_state) +
        ",message=" + quote(connection, next == OrderState::kPaid ? "mock payment succeeded" :
            (next == OrderState::kExpired ? "payment window expired" : "order cancelled")) +
        " WHERE request_id=" + quote(connection, current.idempotency_key) + " AND user_id=" + quote(connection, current.user_id);
    if (const auto status = execute(connection, update_request); !status.ok()) { rollback(connection); return status; }
    if (const auto status = commit(connection); !status.ok()) return status;
    current.state = next;
    *order = current;
    return common::Status::Ok();
}
common::Status MySqlTransactionStore::confirmPayment(const std::string& order_id, const std::string& provider,
    const std::string& provider_transaction_id, std::int64_t amount_cents,
    ::live::business::order::Order* order) {
    using ::live::business::order::OrderState;
    if (order_id.empty() || provider.empty() || provider.size() > 32 || provider_transaction_id.empty() ||
        provider_transaction_id.size() > 128 || amount_cents < 0 || order == nullptr)
        return common::Status::InvalidArgument("invalid payment confirmation");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    if (const auto status = begin(connection); !status.ok()) return status;
    ::live::business::order::Order current;
    if (const auto status = loadOrder(connection, order_id, &current); !status.ok()) { rollback(connection); return status; }
    if (current.state == OrderState::kPaid) {
        const auto query = "SELECT provider,provider_transaction_id,amount_cents FROM payments WHERE order_id=" + quote(connection, order_id) +
            " AND state='SUCCEEDED'";
        if (mysql_query(connection, query.c_str()) != 0) { const auto status = common::Status::Internal(mysqlError(connection, "payment idempotency lookup failed")); rollback(connection); return status; }
        MYSQL_RES* result = mysql_store_result(connection);
        if (result == nullptr) { const auto status = common::Status::Internal(mysqlError(connection, "payment result failed")); rollback(connection); return status; }
        MYSQL_ROW row = mysql_fetch_row(result);
        const bool duplicate = row != nullptr && row[0] != nullptr && row[1] != nullptr && row[2] != nullptr &&
            provider == row[0] && provider_transaction_id == row[1] && amount_cents == std::stoll(row[2]);
        mysql_free_result(result);
        if (duplicate) { *order = current; return commit(connection); }
        rollback(connection);
        return common::Status::Aborted("paid order was confirmed by a different payment transaction");
    }
    if (current.state != OrderState::kPendingPayment) { rollback(connection); return common::Status::Aborted("order is not pending payment"); }
    if (current.unit_price_cents < 0 || (current.quantity > 0 &&
        current.unit_price_cents > std::numeric_limits<std::int64_t>::max() / current.quantity)) {
        rollback(connection);
        return common::Status::InvalidArgument("order amount overflows payment ledger");
    }
    const auto expected_amount = current.unit_price_cents * current.quantity;
    if (amount_cents != expected_amount) { rollback(connection); return common::Status::FailedPrecondition("payment amount does not match order total"); }
    const auto update = "UPDATE orders SET state='PAID',expiry_claimed_by=NULL,expiry_lease_until=NULL WHERE id=" +
        quote(connection, order_id) + " AND state='PENDING_PAYMENT'";
    if (const auto status = execute(connection, update); !status.ok()) { rollback(connection); return status; }
    if (mysql_affected_rows(connection) != 1) { rollback(connection); return common::Status::Aborted("order state changed concurrently"); }
    const auto stock = "UPDATE inventory SET reserved=reserved-" + std::to_string(current.quantity) + ",sold=sold+" +
        std::to_string(current.quantity) + ",version=version+1 WHERE sku_id=" + quote(connection, current.sku_id) +
        " AND reserved>=" + std::to_string(current.quantity);
    if (const auto status = execute(connection, stock); !status.ok()) { rollback(connection); return status; }
    if (mysql_affected_rows(connection) != 1) { rollback(connection); return common::Status::FailedPrecondition("durable inventory reservation is inconsistent"); }
    const auto payment = "INSERT INTO payments(id,order_id,provider,provider_transaction_id,amount_cents,state) VALUES (" +
        quote(connection, "payment-" + order_id) + "," + quote(connection, order_id) + "," + quote(connection, provider) + "," +
        quote(connection, provider_transaction_id) + "," + std::to_string(amount_cents) + ",'SUCCEEDED')";
    if (const auto status = execute(connection, payment); !status.ok()) { rollback(connection); return status; }
    ::live::messaging::Event event{"stock-confirm-" + order_id, "inventory.events", order_id,
        "StockConfirm|" + order_id + "|" + current.sku_id + "|" + std::to_string(current.quantity), 0};
    if (const auto status = appendOutbox(connection, event); !status.ok()) { rollback(connection); return status; }
    const auto request = "UPDATE flash_sale_requests SET state='PAID',message='payment callback confirmed' WHERE request_id=" +
        quote(connection, current.idempotency_key) + " AND user_id=" + quote(connection, current.user_id);
    if (const auto status = execute(connection, request); !status.ok()) { rollback(connection); return status; }
    if (const auto status = commit(connection); !status.ok()) return status;
    current.state = OrderState::kPaid;
    *order = current;
    return common::Status::Ok();
}

common::Status MySqlTransactionStore::initializeSku(const std::string& sku_id, std::int64_t quantity) {
    if (sku_id.empty() || quantity <= 0) return common::Status::InvalidArgument("invalid MySQL SKU");
    auto lease = pool_->acquire(); MYSQL* connection = lease.get(); return execute(connection, "INSERT INTO inventory(sku_id,available,reserved,sold,version) VALUES (" + quote(connection, sku_id) + "," + std::to_string(quantity) + ",0,0,0) ON DUPLICATE KEY UPDATE sku_id=VALUES(sku_id)");
}
common::Status MySqlTransactionStore::initializeProduct(const std::string& sku_id, const std::string& name,
                                                         std::int64_t price_cents) {
    if (sku_id.empty() || name.empty() || price_cents < 0) return common::Status::InvalidArgument("invalid MySQL product");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    return execute(connection, "INSERT INTO products(id,name,price_cents,on_sale) VALUES (" + quote(connection, sku_id) + "," +
        quote(connection, name) + "," + std::to_string(price_cents) + ",TRUE) ON DUPLICATE KEY UPDATE name=VALUES(name),price_cents=VALUES(price_cents),on_sale=TRUE");
}
common::Status MySqlTransactionStore::queryStock(const std::string& sku_id, ::live::business::inventory::StockSnapshot* snapshot) const {
    if (sku_id.empty() || snapshot == nullptr) return common::Status::InvalidArgument("invalid MySQL stock query");
    auto lease = pool_->acquire(); MYSQL* connection = lease.get(); if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto sql = "SELECT available,reserved,sold FROM inventory WHERE sku_id=" + quote(connection, sku_id);
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "MySQL stock query failed"));
    MYSQL_RES* result = mysql_store_result(connection); if (result == nullptr) return common::Status::Internal(mysqlError(connection, "MySQL stock result failed"));
    MYSQL_ROW row = mysql_fetch_row(result); if (row == nullptr) { mysql_free_result(result); return common::Status::NotFound("sku not found"); }
    snapshot->available = row[0] == nullptr ? 0 : std::stoll(row[0]); snapshot->reserved = row[1] == nullptr ? 0 : std::stoll(row[1]); snapshot->sold = row[2] == nullptr ? 0 : std::stoll(row[2]); mysql_free_result(result); return common::Status::Ok();
}

common::Status MySqlTransactionStore::listExpiredOrders(std::size_t limit,
    std::vector<::live::business::order::Order>* orders) const {
    if (orders == nullptr || limit == 0 || limit > 1000) return common::Status::InvalidArgument("invalid expired order scan");
    orders->clear();
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto sql = "SELECT o.id,o.user_id,o.idempotency_key,o.state,oi.sku_id,oi.quantity,oi.unit_price_cents FROM orders o JOIN order_items oi ON oi.order_id=o.id WHERE o.state='PENDING_PAYMENT' AND o.expires_at<=CURRENT_TIMESTAMP ORDER BY o.expires_at,o.id LIMIT " + std::to_string(limit);
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "expired order scan failed"));
    MYSQL_RES* result = mysql_store_result(connection);
    if (result == nullptr) return common::Status::Internal(mysqlError(connection, "expired order result failed"));
    MYSQL_ROW row = nullptr;
    while ((row = mysql_fetch_row(result)) != nullptr) {
        ::live::business::order::Order order;
        order.id = row[0] == nullptr ? "" : row[0];
        order.user_id = row[1] == nullptr ? "" : row[1];
        order.idempotency_key = row[2] == nullptr ? "" : row[2];
        order.state = parseState(row[3]);
        order.sku_id = row[4] == nullptr ? "" : row[4];
        order.quantity = row[5] == nullptr ? 0 : std::stoll(row[5]);
        order.unit_price_cents = row[6] == nullptr ? 0 : std::stoll(row[6]);
        orders->push_back(std::move(order));
    }
    mysql_free_result(result);
    return common::Status::Ok();
}
common::Status MySqlTransactionStore::claimExpiredOrders(const std::string& owner, std::size_t limit,
    std::vector<::live::business::order::Order>* orders) {
    if (owner.empty() || orders == nullptr || limit == 0 || limit > 1000)
        return common::Status::InvalidArgument("invalid expired order claim");
    orders->clear();
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    if (const auto status = begin(connection); !status.ok()) return status;
    const auto sql = "SELECT o.id,o.user_id,o.idempotency_key,o.state,oi.sku_id,oi.quantity,oi.unit_price_cents "
        "FROM orders o JOIN order_items oi ON oi.order_id=o.id WHERE o.state='PENDING_PAYMENT' "
        "AND o.expires_at<=CURRENT_TIMESTAMP AND (o.expiry_lease_until IS NULL OR o.expiry_lease_until<CURRENT_TIMESTAMP) "
        "ORDER BY o.expires_at,o.id LIMIT " + std::to_string(limit) + " FOR UPDATE SKIP LOCKED";
    if (mysql_query(connection, sql.c_str()) != 0) {
        const auto status = common::Status::Internal(mysqlError(connection, "expired order claim query failed"));
        rollback(connection);
        return status;
    }
    MYSQL_RES* result = mysql_store_result(connection);
    if (result == nullptr) {
        const auto status = common::Status::Internal(mysqlError(connection, "expired order claim result failed"));
        rollback(connection);
        return status;
    }
    MYSQL_ROW row = nullptr;
    while ((row = mysql_fetch_row(result)) != nullptr) {
        ::live::business::order::Order order;
        order.id = row[0] == nullptr ? "" : row[0];
        order.user_id = row[1] == nullptr ? "" : row[1];
        order.idempotency_key = row[2] == nullptr ? "" : row[2];
        order.state = parseState(row[3]);
        order.sku_id = row[4] == nullptr ? "" : row[4];
        order.quantity = row[5] == nullptr ? 0 : std::stoll(row[5]);
        order.unit_price_cents = row[6] == nullptr ? 0 : std::stoll(row[6]);
        orders->push_back(std::move(order));
    }
    mysql_free_result(result);
    for (const auto& order : *orders) {
        const auto claim = "UPDATE orders SET expiry_claimed_by=" + quote(connection, owner.substr(0, 128)) +
            ",expiry_lease_until=DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 30 SECOND) WHERE id=" + quote(connection, order.id) +
            " AND state='PENDING_PAYMENT'";
        if (const auto status = execute(connection, claim); !status.ok()) { rollback(connection); orders->clear(); return status; }
    }
    if (const auto status = commit(connection); !status.ok()) { orders->clear(); return status; }
    return common::Status::Ok();
}

common::Status MySqlTransactionStore::drainOutbox(const std::function<common::Status(const ::live::messaging::Event&)>& publish, std::size_t limit) {
    if (!publish || limit == 0 || limit > 1000) return common::Status::InvalidArgument("invalid MySQL outbox drain arguments");
    static std::atomic<std::uint64_t> sequence{0};
    const std::string owner = "outbox-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
                              "-" + std::to_string(sequence.fetch_add(1));
    std::vector<::live::messaging::Event> events;
    {
        auto lease = pool_->acquire();
        MYSQL* connection = lease.get();
        if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
        if (const auto status = begin(connection); !status.ok()) return status;
        const auto sql = "SELECT event_id,topic,event_key,payload FROM outbox_events WHERE published=FALSE AND (lease_until IS NULL OR lease_until<CURRENT_TIMESTAMP) ORDER BY created_at,event_id LIMIT " + std::to_string(limit) + " FOR UPDATE SKIP LOCKED";
        if (mysql_query(connection, sql.c_str()) != 0) { rollback(connection); return common::Status::Internal(mysqlError(connection, "MySQL outbox claim query failed")); }
        MYSQL_RES* result = mysql_store_result(connection);
        if (result == nullptr) { rollback(connection); return common::Status::Internal(mysqlError(connection, "MySQL outbox result failed")); }
        MYSQL_ROW row = nullptr;
        while ((row = mysql_fetch_row(result)) != nullptr) {
            ::live::messaging::Event event;
            event.event_id = row[0] == nullptr ? "" : row[0]; event.topic = row[1] == nullptr ? "" : row[1];
            event.key = row[2] == nullptr ? "" : row[2]; event.payload = row[3] == nullptr ? "" : row[3];
            events.push_back(std::move(event));
        }
        mysql_free_result(result);
        for (const auto& event : events) {
            const auto claim = "UPDATE outbox_events SET claimed_by=" + quote(connection, owner) +
                ",lease_until=DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 30 SECOND),attempts=attempts+1 WHERE event_id=" +
                quote(connection, event.event_id) + " AND published=FALSE";
            if (const auto status = execute(connection, claim); !status.ok()) { rollback(connection); return status; }
        }
        if (const auto status = commit(connection); !status.ok()) return status;
    }
    for (const auto& event : events) {
        const auto status = publish(event);
        auto update_lease = pool_->acquire();
        MYSQL* connection = update_lease.get();
        if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
        if (status.ok()) {
            const auto update = "UPDATE outbox_events SET published=TRUE,claimed_by=NULL,lease_until=NULL,last_error=NULL WHERE event_id=" +
                quote(connection, event.event_id) + " AND claimed_by=" + quote(connection, owner);
            if (const auto mark = execute(connection, update); !mark.ok()) return mark;
        } else {
            const auto update = "UPDATE outbox_events SET claimed_by=NULL,lease_until=DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 5 SECOND),last_error=" +
                quote(connection, status.message().substr(0, 1000)) + " WHERE event_id=" + quote(connection, event.event_id) +
                " AND claimed_by=" + quote(connection, owner);
            if (const auto release = execute(connection, update); !release.ok()) return release;
            return status;
        }
    }
    return common::Status::Ok();
}

common::Status MySqlTransactionStore::create(const ::live::business::promotion::Promotion& promotion, const std::string& actor) {
    if (promotion.id.empty() || promotion.sku_id.empty() || promotion.participant_limit == 0 || actor.empty() ||
        promotion.participant_limit > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        promotion.starts_at < 0 || promotion.ends_at < 0 ||
        (promotion.ends_at != 0 && promotion.starts_at >= promotion.ends_at))
        return common::Status::InvalidArgument("invalid campaign");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    if (const auto status = begin(connection); !status.ok()) return status;
    const auto stock_sql = "SELECT available FROM inventory WHERE sku_id=" + quote(connection, promotion.sku_id) + " FOR UPDATE";
    if (mysql_query(connection, stock_sql.c_str()) != 0) { const auto status = common::Status::Internal(mysqlError(connection, "campaign stock lock failed")); rollback(connection); return status; }
    MYSQL_RES* stock_result = mysql_store_result(connection);
    if (stock_result == nullptr) { const auto status = common::Status::Internal(mysqlError(connection, "campaign stock result failed")); rollback(connection); return status; }
    MYSQL_ROW stock_row = mysql_fetch_row(stock_result);
    if (stock_row == nullptr || stock_row[0] == nullptr) { mysql_free_result(stock_result); rollback(connection); return common::Status::NotFound("campaign SKU inventory not found"); }
    const auto available_stock = std::stoll(stock_row[0]);
    mysql_free_result(stock_result);
    const auto allocation_sql = "SELECT COALESCE(SUM(GREATEST(c.participant_limit-(SELECT COUNT(*) FROM orders o WHERE o.campaign_id=c.id AND o.state IN ('PENDING_PAYMENT','PAID')),0)),0) "
        "FROM campaigns c "
        "WHERE c.sku_id=" + quote(connection, promotion.sku_id) +
        " AND c.state<>'FINISHED' AND (c.ends_at=0 OR c.ends_at>UNIX_TIMESTAMP())";
    if (mysql_query(connection, allocation_sql.c_str()) != 0) { const auto status = common::Status::Internal(mysqlError(connection, "campaign allocation query failed")); rollback(connection); return status; }
    MYSQL_RES* allocation_result = mysql_store_result(connection);
    if (allocation_result == nullptr) { const auto status = common::Status::Internal(mysqlError(connection, "campaign allocation result failed")); rollback(connection); return status; }
    MYSQL_ROW allocation_row = mysql_fetch_row(allocation_result);
    const auto allocated_stock = allocation_row == nullptr || allocation_row[0] == nullptr ? 0 : std::stoll(allocation_row[0]);
    mysql_free_result(allocation_result);
    const auto available_unsigned = available_stock < 0 ? 0U : static_cast<std::uint64_t>(available_stock);
    const auto allocated_unsigned = allocated_stock < 0 ? std::numeric_limits<std::uint64_t>::max() :
        static_cast<std::uint64_t>(allocated_stock);
    if (available_stock < 0 || allocated_stock < 0 || allocated_unsigned > available_unsigned ||
        promotion.participant_limit > available_unsigned - allocated_unsigned) {
        rollback(connection);
        return common::Status::ResourceExhausted("campaign quota exceeds unallocated SKU stock");
    }
    const auto sql = "INSERT INTO campaigns(id,sku_id,participant_limit,state,starts_at,ends_at) VALUES (" + quote(connection, promotion.id) + "," +
        quote(connection, promotion.sku_id) + "," + std::to_string(promotion.participant_limit) + "," +
        quote(connection, ::live::business::promotion::promotionStateName(promotion.state)) + "," +
        std::to_string(promotion.starts_at) + "," + std::to_string(promotion.ends_at) + ")";
    if (const auto status = execute(connection, sql); !status.ok()) {
        constexpr unsigned int kDuplicateEntryError = 1062;
        const auto create_status = mysql_errno(connection) == kDuplicateEntryError
            ? common::Status::AlreadyExists("campaign already exists") : status;
        rollback(connection);
        return create_status;
    }
    const auto audit = "INSERT INTO admin_audit_logs(actor,action,resource_type,resource_id,previous_value,new_value) VALUES (" + quote(connection, actor.substr(0, 128)) + ",'CAMPAIGN_CREATED','campaign'," +
        quote(connection, promotion.id) + ",''," + quote(connection, std::string(promotion.sku_id) + ":" + std::to_string(promotion.participant_limit) + ":" +
            std::to_string(promotion.starts_at) + ":" + std::to_string(promotion.ends_at)) + ")";
    if (const auto status = execute(connection, audit); !status.ok()) { rollback(connection); return status; }
    return commit(connection);
}

common::Status MySqlTransactionStore::update(const ::live::business::promotion::Promotion& previous,
    const ::live::business::promotion::Promotion& next, const std::string& actor) {
    using ::live::business::promotion::PromotionState;
    if (previous.id.empty() || previous.id != next.id || previous.state != PromotionState::kCreated ||
        next.state != PromotionState::kCreated || next.sku_id.empty() || next.participant_limit == 0 || actor.empty() ||
        next.participant_limit > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        next.starts_at < 0 || next.ends_at < 0 || (next.ends_at != 0 && next.starts_at >= next.ends_at))
        return common::Status::InvalidArgument("invalid campaign update");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    if (const auto status = begin(connection); !status.ok()) return status;
    const auto stock_sql = "SELECT available FROM inventory WHERE sku_id=" + quote(connection, next.sku_id) + " FOR UPDATE";
    if (mysql_query(connection, stock_sql.c_str()) != 0) { const auto status = common::Status::Internal(mysqlError(connection, "campaign stock lock failed")); rollback(connection); return status; }
    MYSQL_RES* stock_result = mysql_store_result(connection);
    if (stock_result == nullptr) { const auto status = common::Status::Internal(mysqlError(connection, "campaign stock result failed")); rollback(connection); return status; }
    MYSQL_ROW stock_row = mysql_fetch_row(stock_result);
    if (stock_row == nullptr || stock_row[0] == nullptr) { mysql_free_result(stock_result); rollback(connection); return common::Status::NotFound("campaign SKU inventory not found"); }
    const auto available_stock = std::stoll(stock_row[0]);
    mysql_free_result(stock_result);
    const auto allocation_sql = "SELECT COALESCE(SUM(GREATEST(c.participant_limit-(SELECT COUNT(*) FROM orders o WHERE o.campaign_id=c.id AND o.state IN ('PENDING_PAYMENT','PAID')),0)),0) "
        "FROM campaigns c "
        "WHERE c.sku_id=" + quote(connection, next.sku_id) + " AND c.state<>'FINISHED' "
        "AND (c.ends_at=0 OR c.ends_at>UNIX_TIMESTAMP()) AND c.id<>" + quote(connection, next.id);
    if (mysql_query(connection, allocation_sql.c_str()) != 0) { const auto status = common::Status::Internal(mysqlError(connection, "campaign allocation query failed")); rollback(connection); return status; }
    MYSQL_RES* allocation_result = mysql_store_result(connection);
    if (allocation_result == nullptr) { const auto status = common::Status::Internal(mysqlError(connection, "campaign allocation result failed")); rollback(connection); return status; }
    MYSQL_ROW allocation_row = mysql_fetch_row(allocation_result);
    const auto allocated_stock = allocation_row == nullptr || allocation_row[0] == nullptr ? 0 : std::stoll(allocation_row[0]);
    mysql_free_result(allocation_result);
    const auto available_unsigned = available_stock < 0 ? 0U : static_cast<std::uint64_t>(available_stock);
    const auto allocated_unsigned = allocated_stock < 0 ? std::numeric_limits<std::uint64_t>::max() :
        static_cast<std::uint64_t>(allocated_stock);
    if (available_stock < 0 || allocated_stock < 0 || allocated_unsigned > available_unsigned ||
        next.participant_limit > available_unsigned - allocated_unsigned) {
        rollback(connection);
        return common::Status::ResourceExhausted("campaign quota exceeds unallocated SKU stock");
    }
    const auto sql = "UPDATE campaigns SET sku_id=" + quote(connection, next.sku_id) + ",participant_limit=" +
        std::to_string(next.participant_limit) + ",starts_at=" + std::to_string(next.starts_at) + ",ends_at=" +
        std::to_string(next.ends_at) + " WHERE id=" + quote(connection, previous.id) +
        " AND state='CREATED' AND sku_id=" + quote(connection, previous.sku_id) +
        " AND participant_limit=" + std::to_string(previous.participant_limit) + " AND starts_at=" +
        std::to_string(previous.starts_at) + " AND ends_at=" + std::to_string(previous.ends_at);
    if (const auto status = execute(connection, sql); !status.ok()) { rollback(connection); return status; }
    if (mysql_affected_rows(connection) != 1) {
        rollback(connection);
        return common::Status::Aborted("campaign changed concurrently or is no longer editable");
    }
    const auto before = previous.sku_id + ":" + std::to_string(previous.participant_limit) + ":" +
        std::to_string(previous.starts_at) + ":" + std::to_string(previous.ends_at);
    const auto after = next.sku_id + ":" + std::to_string(next.participant_limit) + ":" +
        std::to_string(next.starts_at) + ":" + std::to_string(next.ends_at);
    const auto audit = "INSERT INTO admin_audit_logs(actor,action,resource_type,resource_id,previous_value,new_value) VALUES (" +
        quote(connection, actor.substr(0, 128)) + ",'CAMPAIGN_UPDATED','campaign'," + quote(connection, previous.id) + "," +
        quote(connection, before) + "," + quote(connection, after) + ")";
    if (const auto status = execute(connection, audit); !status.ok()) { rollback(connection); return status; }
    return commit(connection);
}

common::Status MySqlTransactionStore::list(std::vector<::live::business::promotion::Promotion>* promotions) const {
    if (promotions == nullptr) return common::Status::InvalidArgument("campaign output must not be null");
    promotions->clear();
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    if (mysql_query(connection, "SELECT id,sku_id,participant_limit,state,starts_at,ends_at FROM campaigns ORDER BY created_at,id") != 0)
        return common::Status::Internal(mysqlError(connection, "MySQL campaign query failed"));
    MYSQL_RES* result = mysql_store_result(connection);
    if (result == nullptr) return common::Status::Internal(mysqlError(connection, "MySQL campaign result failed"));
    MYSQL_ROW row = nullptr;
    while ((row = mysql_fetch_row(result)) != nullptr) {
        ::live::business::promotion::Promotion promotion;
        promotion.id = row[0] == nullptr ? "" : row[0];
        promotion.sku_id = row[1] == nullptr ? "" : row[1];
        promotion.participant_limit = row[2] == nullptr ? 0 : static_cast<std::size_t>(std::stoull(row[2]));
        promotion.state = parsePromotionState(row[3]);
        promotion.starts_at = row[4] == nullptr ? 0 : std::stoll(row[4]);
        promotion.ends_at = row[5] == nullptr ? 0 : std::stoll(row[5]);
        promotions->push_back(std::move(promotion));
    }
    mysql_free_result(result);
    return common::Status::Ok();
}

common::Status MySqlTransactionStore::transition(const std::string& id,
    ::live::business::promotion::PromotionState expected, ::live::business::promotion::PromotionState next,
    const std::string& actor) {
    if (id.empty() || actor.empty()) return common::Status::InvalidArgument("campaign id and audit actor are required");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    if (const auto status = begin(connection); !status.ok()) return status;
    const auto sql = "UPDATE campaigns SET state=" + quote(connection, ::live::business::promotion::promotionStateName(next)) +
        " WHERE id=" + quote(connection, id) + " AND state=" + quote(connection, ::live::business::promotion::promotionStateName(expected));
    if (const auto status = execute(connection, sql); !status.ok()) { rollback(connection); return status; }
    if (mysql_affected_rows(connection) != 1) {
        rollback(connection);
        return common::Status::Aborted("campaign state changed concurrently or campaign is missing");
    }
    const auto audit = "INSERT INTO admin_audit_logs(actor,action,resource_type,resource_id,previous_value,new_value) VALUES (" +
        quote(connection, actor.substr(0, 128)) + ",'CAMPAIGN_STATE_CHANGED','campaign'," + quote(connection, id) + "," +
        quote(connection, ::live::business::promotion::promotionStateName(expected)) + "," +
        quote(connection, ::live::business::promotion::promotionStateName(next)) + ")";
    if (const auto status = execute(connection, audit); !status.ok()) { rollback(connection); return status; }
    return commit(connection);
}

common::Status MySqlTransactionStore::listAudit(std::size_t limit,
    std::vector<::live::business::promotion::AdminAuditRecord>* records) const {
    if (records == nullptr || limit == 0) return common::Status::InvalidArgument("invalid admin audit query");
    records->clear();
    limit = std::min<std::size_t>(limit, 1000);
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto sql = "SELECT id,actor,action,resource_type,resource_id,previous_value,new_value,"
        "DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s') FROM admin_audit_logs ORDER BY id DESC LIMIT " + std::to_string(limit);
    if (mysql_query(connection, sql.c_str()) != 0) return common::Status::Internal(mysqlError(connection, "MySQL admin audit query failed"));
    MYSQL_RES* result = mysql_store_result(connection);
    if (result == nullptr) return common::Status::Internal(mysqlError(connection, "MySQL admin audit result failed"));
    MYSQL_ROW row = nullptr;
    while ((row = mysql_fetch_row(result)) != nullptr) {
        ::live::business::promotion::AdminAuditRecord record;
        record.id = row[0] == nullptr ? 0 : static_cast<std::uint64_t>(std::stoull(row[0]));
        record.actor = row[1] == nullptr ? "" : row[1];
        record.action = row[2] == nullptr ? "" : row[2];
        record.resource_type = row[3] == nullptr ? "" : row[3];
        record.resource_id = row[4] == nullptr ? "" : row[4];
        record.previous_value = row[5] == nullptr ? "" : row[5];
        record.new_value = row[6] == nullptr ? "" : row[6];
        record.created_at = row[7] == nullptr ? "" : row[7];
        records->push_back(std::move(record));
    }
    mysql_free_result(result);
    return common::Status::Ok();
}

common::Status MySqlTransactionStore::recordAudit(const std::string& actor, const std::string& action,
    const std::string& resource_type, const std::string& resource_id,
    const std::string& previous_value, const std::string& new_value) {
    if (actor.empty() || action.empty() || resource_type.empty() || resource_id.empty())
        return common::Status::InvalidArgument("invalid admin audit record");
    auto lease = pool_->acquire();
    MYSQL* connection = lease.get();
    if (connection == nullptr) return common::Status::FailedPrecondition("MySQL is not connected");
    const auto sql = "INSERT INTO admin_audit_logs(actor,action,resource_type,resource_id,previous_value,new_value) VALUES (" +
        quote(connection, actor.substr(0, 128)) + "," + quote(connection, action.substr(0, 64)) + "," +
        quote(connection, resource_type.substr(0, 64)) + "," + quote(connection, resource_id.substr(0, 128)) + "," +
        quote(connection, previous_value.substr(0, 512)) + "," + quote(connection, new_value.substr(0, 512)) + ")";
    return execute(connection, sql);
}

}  // namespace live::storage::mysql
