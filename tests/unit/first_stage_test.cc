#include "business/inventory/inventory_service.h"
#include "business/flash_sale/flash_sale_request_status_store.h"
#include "business/promotion/flash_sale_gate.h"
#include "business/user/user_service.h"
#include "cache/sharded_cache.h"
#include "common/security/password_hasher.h"
#include "common/security/webhook_signature.h"
#include "gateway/http/http_parser.h"
#include "gateway/router.h"
#include "kv/kv_store.h"
#include "messaging/async_outbox_dispatcher.h"
#include "messaging/in_memory_broker.h"
#include "messaging/outbox.h"
#include "network/buffer.h"
#include "network/reactor/event_loop.h"
#include "observability/metrics/metrics_registry.h"
#include "storage/local_engine/local_kv_engine.h"
#include "traffic/token_bucket.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void check(bool condition, const char* expression) {
    if (!condition) throw std::runtime_error(expression);
}

void testHttpAndRouter() {
    live::gateway::http::HttpRequest request;
    const std::string raw =
        "POST /flash-sale/orders HTTP/1.1\r\nHost: example.test\r\n"
        "Content-Length: 7\r\n\r\npayload";
    check(live::gateway::http::HttpRequestParser::parse(raw, &request).ok(), "HTTP parser");
    check(request.method == "POST" && request.target == "/flash-sale/orders", "HTTP request line");
    check(request.body == "payload", "HTTP request body");
    check(live::gateway::http::HttpRequestParser::parse(
        "GET /health HTTP/1.1\r\nHost: test\r\n", &request).isIncomplete(), "HTTP incomplete request");

    live::gateway::Router router;
    router.addRoute("GET", "/health", [](const auto&) {
        return live::gateway::http::HttpResponse{200, "OK", {}, "ok"};
    });
    request.method = "GET";
    request.target = "/health";
    check(router.dispatch(request).body == "ok", "router dispatch");
    request.target = "/missing";
    check(router.dispatch(request).status_code == 404, "router missing route");
}

void testEventLoopWakeup() {
    std::promise<live::network::EventLoop*> loop_promise;
    std::promise<void> callback_promise;
    auto callback_future = callback_promise.get_future();
    std::thread loop_thread([&] {
        live::network::EventLoop loop;
        loop_promise.set_value(&loop);
        loop.loop();
    });
    live::network::EventLoop* loop = loop_promise.get_future().get();
    loop->runInLoop([&] {
        callback_promise.set_value();
        loop->quit();
    });
    callback_future.wait();
    loop_thread.join();
}

void testInventoryNoOversell() {
    live::business::inventory::InventoryService inventory;
    check(inventory.addSku("sku-1", 100).ok(), "inventory setup");
    std::atomic<int> success{0};
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 32; ++worker) {
        workers.emplace_back([&, worker] {
            for (int i = worker; i < 10000; i += 32) {
                if (inventory.reserveStock("sku-1", 1, "operation-" + std::to_string(i)).ok()) {
                    success.fetch_add(1);
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    live::business::inventory::StockSnapshot snapshot;
    check(inventory.queryStock("sku-1", &snapshot).ok(), "inventory query");
    check(success.load() == 100 && snapshot.available == 0 && snapshot.reserved == 100,
          "inventory no oversell");
    check(inventory.validateInvariants().ok(), "inventory invariants");
}

void testCacheAndKv() {
    live::cache::ShardedCache cache(4, 32);
    std::atomic<int> loads{0};
    std::vector<std::thread> workers;
    for (int i = 0; i < 16; ++i) {
        workers.emplace_back([&] {
            std::string value;
            const auto status = cache.getOrLoad("hot-key", [&](std::string* loaded) {
                loads.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                *loaded = "hot-value";
                return live::common::Status::Ok();
            }, &value, std::chrono::seconds(1));
            check(status.ok() && value == "hot-value", "cache singleflight");
        });
    }
    for (auto& worker : workers) worker.join();
    check(loads.load() == 1, "singleflight loader count");

    auto store = std::make_shared<live::kv::InMemoryKVStore>();
    live::kv::KVClient client(store);
    bool updated = false;
    check(client.cas("version", std::nullopt, "1", &updated).ok() && updated, "KV CAS create");
    check(client.cas("version", std::string("0"), "2", &updated).ok() && !updated, "KV CAS conflict");
    check(client.set("temporary", "value", std::chrono::milliseconds(5)).ok(), "KV TTL set");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::string value;
    check(client.get("temporary", &value).code() == live::common::ErrorCode::kNotFound, "KV TTL expiry");
    live::business::flash_sale::KvFlashSaleRequestStatusStore waiting_status_store(store.get());
    check(waiting_status_store.put({"waiting-request", "waiting-user", "",
              live::business::flash_sale::FlashSaleRequestState::kWaiting,
              "waiting for admission", "promo-waiting"}).ok(), "waiting-room status write");
    live::business::flash_sale::FlashSaleRequestStatus waiting_status;
    check(waiting_status_store.get("waiting-request", &waiting_status).ok() &&
              waiting_status.state == live::business::flash_sale::FlashSaleRequestState::kWaiting &&
              waiting_status.promotion_id == "promo-waiting", "waiting-room status persistence");
    bool waiting_transitioned = false;
    check(waiting_status_store.transition("waiting-request",
              live::business::flash_sale::FlashSaleRequestState::kWaiting,
              live::business::flash_sale::FlashSaleRequestState::kSubmitting, &waiting_transitioned).ok() &&
              waiting_transitioned, "request status compare-and-set transition");
    check(waiting_status_store.transition("waiting-request",
              live::business::flash_sale::FlashSaleRequestState::kWaiting,
              live::business::flash_sale::FlashSaleRequestState::kQueued, &waiting_transitioned).ok() &&
              !waiting_transitioned, "stale request status transition cannot regress state");
}

void testFlashSaleAdmission() {
    live::business::promotion::FlashSaleAdmissionGate gate;
    check(gate.configure("promo-1", 2).ok(), "admission configure");
    live::business::promotion::AdmissionDecision decision;
    check(gate.tryAcquire("promo-1", "user-0", &decision).ok() &&
              decision.code == live::business::promotion::AdmissionCode::kNotStarted,
          "admission before start");
    check(gate.preheat("promo-1").ok() && gate.start("promo-1").ok(), "admission lifecycle");

    std::atomic<int> accepted{0};
    std::vector<std::thread> workers;
    for (int i = 0; i < 16; ++i) {
        workers.emplace_back([&, i] {
            live::business::promotion::AdmissionDecision current;
            if (gate.tryAcquire("promo-1", "user-" + std::to_string(i), &current).ok() && current.accepted()) {
                accepted.fetch_add(1);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    check(accepted.load() == 2, "admission quota");
}

void testUserRecovery() {
    std::string encoded;
    bool matches = false;
    check(live::common::security::PasswordHasher::hash("secret", &encoded).ok(), "password hash");
    check(live::common::security::PasswordHasher::verify("secret", encoded, &matches).ok() && matches, "password verify");
    check(live::common::security::PasswordHasher::verify("wrong", encoded, &matches).ok() && !matches, "password mismatch");

    const std::string snapshot_path = "/tmp/ecommerce-user-test.snapshot";
    const std::string wal_path = "/tmp/ecommerce-user-test.wal";
    std::remove(snapshot_path.c_str());
    std::remove(wal_path.c_str());
    {
        live::storage::LocalKVEngine storage;
        check(storage.open(snapshot_path, wal_path).ok(), "user storage open");
        live::business::user::UserService users(&storage);
        live::business::user::User user;
        check(users.registerUser("persistent-user", "secret", live::business::user::UserRole::kCustomer, &user).ok(), "user register");
    }
    {
        live::storage::LocalKVEngine storage;
        check(storage.open(snapshot_path, wal_path).ok(), "user recovery open");
        live::business::user::UserService users(&storage);
        check(users.restore().ok(), "user restore");
        live::business::user::Session session;
        check(users.login("persistent-user", "secret", &session).ok(), "recovered login");
    }
    std::remove(snapshot_path.c_str());
    std::remove(wal_path.c_str());
}

void testStorageAndOutbox() {
    const std::string snapshot_path = "/tmp/ecommerce-storage-test.snapshot";
    const std::string wal_path = "/tmp/ecommerce-storage-test.wal";
    std::remove(snapshot_path.c_str());
    std::remove(wal_path.c_str());
    {
        live::storage::LocalKVEngine engine;
        check(engine.open(snapshot_path, wal_path).ok(), "storage open");
        check(engine.set("inventory/stock/sku-1", "98|2|0").ok(), "storage set");
        check(engine.snapshot().ok(), "storage snapshot");
        check(engine.set("inventory/reservation/order-1", "sku-1|2|0").ok(), "storage reservation");
    }
    {
        live::storage::LocalKVEngine recovered;
        check(recovered.open(snapshot_path, wal_path).ok(), "storage recovery");
        std::string value;
        check(recovered.get("inventory/stock/sku-1", &value).ok() && value == "98|2|0", "storage snapshot value");
        check(recovered.get("inventory/reservation/order-1", &value).ok(), "storage WAL value");
    }
    std::remove(snapshot_path.c_str());
    std::remove(wal_path.c_str());

    const std::string outbox_path = "/tmp/ecommerce-outbox-test.bin";
    std::remove(outbox_path.c_str());
    live::messaging::FileOutbox outbox;
    check(outbox.open(outbox_path).ok(), "outbox open");
    check(outbox.append({"event-1", "OrderCreated", "order-1", "payload", 1}).ok(), "outbox append");
    live::messaging::FileOutbox recovered;
    check(recovered.open(outbox_path).ok() && recovered.size() == 1, "outbox recovery");
    check(recovered.drain([](const auto&) { return live::common::Status::Ok(); }).ok() && recovered.size() == 0,
          "outbox drain");
    std::remove(outbox_path.c_str());
}

void testOutboxDispatcherMetricsAndRateLimit() {
    const std::string path = "/tmp/ecommerce-async-outbox-test.bin";
    std::remove(path.c_str());
    live::messaging::FileOutbox outbox;
    check(outbox.open(path).ok(), "async outbox open");
    live::messaging::InMemoryBroker broker;
    live::messaging::AsyncOutboxDispatcher dispatcher(
        &outbox, [&broker](const live::messaging::Event& event) { return broker.publish(event); },
        std::chrono::seconds(1));
    check(outbox.append({"async-event-1", "OrderCreated", "order-1", "payload", 1}).ok(), "async append");
    check(dispatcher.start().ok(), "async dispatcher start");
    dispatcher.wake();
    for (int i = 0; i < 50 && outbox.size() != 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(2));
    check(outbox.size() == 0 && broker.pending("OrderCreated") == 1, "async delivery");
    dispatcher.stop();
    std::remove(path.c_str());

    live::observability::MetricsRegistry metrics;
    metrics.increment("http_requests", 3);
    metrics.observe("http_request_duration_ms", 1.5);
    check(metrics.counter("http_requests") == 3, "metrics counter");
    check(metrics.renderPrometheus().find("http_requests_total 3") != std::string::npos, "metrics output");

    live::traffic::TokenBucket bucket(1.0, 1.0);
    check(bucket.tryAcquire() && !bucket.tryAcquire(), "rate limit");
}

}  // namespace

int main() {
    check(live::common::security::verifyTimestampedHmacSha256(
        "secret", "123", "test-body", "f4488e93d9b27a0bf77b40edf431fb37fc8874453a5e791e471158cf942de9e0"),
        "valid timestamped webhook signature");
    check(live::common::security::verifyTimestampedHmacSha256(
        "secret", "123", "test-body", "sha256=F4488E93D9B27A0BF77B40EDF431FB37FC8874453A5E791E471158CF942DE9E0"),
        "prefixed uppercase webhook signature");
    check(!live::common::security::verifyTimestampedHmacSha256(
        "secret", "123", "changed-body", "f4488e93d9b27a0bf77b40edf431fb37fc8874453a5e791e471158cf942de9e0"),
        "changed webhook body rejected");
    try {
        testHttpAndRouter();
        testEventLoopWakeup();
        testInventoryNoOversell();
        testCacheAndKv();
        testFlashSaleAdmission();
        testUserRecovery();
        testStorageAndOutbox();
        testOutboxDispatcherMetricsAndRateLimit();
    } catch (const std::exception& error) {
        std::cerr << "first_stage_test failed: " << error.what() << '\n';
        return 1;
    }
    std::cout << "first_stage_test passed\n";
    return 0;
}
