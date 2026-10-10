#include "kv/redis_inventory_fast_path.h"
#include "kv/redis_kv_store.h"
#include "kv/redis_order_queue.h"
#include "kv/redis_flash_sale_waiting_room.h"
#include "messaging/order_message.h"
#include "traffic/redis_token_bucket.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main() {
    const std::string redis_host = std::getenv("ECOMMERCE_REDIS_TEST_HOST") == nullptr ? "127.0.0.1" : std::getenv("ECOMMERCE_REDIS_TEST_HOST");
    const std::uint16_t redis_port = static_cast<std::uint16_t>(std::getenv("ECOMMERCE_REDIS_TEST_PORT") == nullptr ? 6379 : std::strtoul(std::getenv("ECOMMERCE_REDIS_TEST_PORT"), nullptr, 10));
    const std::string sku = "redis-concurrency-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    live::kv::RedisKVStore setup;
    if (!setup.connect(redis_host, redis_port).ok()) return 2;
    live::business::inventory::RedisInventoryFastPath setup_inventory(&setup);
    if (!setup_inventory.initializeSku(sku, 100).ok()) return 1;

    std::atomic<int> successful{0};
    std::atomic<bool> failed{false};
    constexpr int kRequests = 1000;
    constexpr int kWorkers = 64;
    std::vector<std::thread> workers;
    for (int worker = 0; worker < kWorkers; ++worker) {
        workers.emplace_back([&, worker] {
            live::kv::RedisKVStore redis;
            if (!redis.connect(redis_host, redis_port).ok()) { failed = true; return; }
            live::business::inventory::RedisInventoryFastPath inventory(&redis);
            for (int request = worker; request < kRequests; request += kWorkers) {
                live::business::inventory::RedisInventoryResult result;
                const auto status = inventory.reserve(sku, 1, "operation-" + std::to_string(request), &result);
                if (!status.ok()) { failed = true; return; }
                if (result.decision == live::business::inventory::RedisInventoryDecision::kSuccess) ++successful;
            }
        });
    }
    for (auto& worker : workers) worker.join();
    live::business::inventory::StockSnapshot stock;
    if (failed.load() || successful.load() != 100 || !setup_inventory.queryStock(sku, &stock).ok() ||
        stock.available != 0 || stock.reserved != 100 || stock.sold != 0) {
        std::cerr << "Redis concurrency invariant failed: success=" << successful.load()
                  << " available=" << stock.available << " reserved=" << stock.reserved << '\n';
        return 1;
    }
    const std::string limiter_key = "redis-limiter-concurrency:" + sku;
    std::atomic<int> admitted{0};
    failed = false;
    workers.clear();
    for (int worker = 0; worker < kWorkers; ++worker) {
        workers.emplace_back([&, worker] {
            live::kv::RedisKVStore redis;
            if (!redis.connect(redis_host, redis_port).ok()) { failed = true; return; }
            live::traffic::RedisTokenBucket limiter(&redis, limiter_key, 0.001, 100.0);
            for (int request = worker; request < kRequests; request += kWorkers) {
                bool allowed = false;
                if (!limiter.tryAcquire(&allowed).ok()) { failed = true; return; }
                if (allowed) ++admitted;
            }
        });
    }
    for (auto& worker : workers) worker.join();
    if (failed.load() || admitted.load() != 100) {
        std::cerr << "Redis distributed limiter invariant failed: admitted=" << admitted.load() << '\n';
        return 1;
    }
    admitted = 0;
    failed = false;
    workers.clear();
    for (int worker = 0; worker < kWorkers; ++worker) {
        workers.emplace_back([&, worker] {
            live::kv::RedisKVStore redis;
            if (!redis.connect(redis_host, redis_port).ok()) { failed = true; return; }
            live::traffic::RedisTokenBucket limiter(&redis, limiter_key + ":user", 0.001, 5.0);
            for (int request = worker; request < kRequests; request += kWorkers) {
                bool allowed = false;
                if (!limiter.tryAcquire(&allowed).ok()) { failed = true; return; }
                if (allowed) ++admitted;
            }
        });
    }
    for (auto& worker : workers) worker.join();
    if (failed.load() || admitted.load() != 5) {
        std::cerr << "Redis per-user limiter invariant failed: admitted=" << admitted.load() << '\n';
        return 1;
    }
    const std::string replay_tag = "redis-replay-{" + sku + "}";
    live::kv::RedisOrderQueue dlq(&setup, replay_tag + ":dlq", 16);
    live::kv::RedisOrderQueue target(&setup, replay_tag + ":orders", 1);
    live::messaging::FlashSaleOrderMessage replay_message;
    replay_message.promotion_id = "replay-promo";
    replay_message.order_id = "replay-order-1";
    replay_message.idempotency_key = "replay-request-1";
    replay_message.user_id = "replay-user";
    replay_message.sku_id = sku;
    replay_message.quantity = 1;
    if (!dlq.push(replay_message).ok()) return 1;
    replay_message.order_id = "replay-order-2";
    replay_message.idempotency_key = "replay-request-2";
    if (!dlq.push(replay_message).ok()) return 1;
    std::size_t replayed = 0;
    const auto replay_status = dlq.replayTo(&target, 2, &replayed);
    if (replay_status.code() != live::common::ErrorCode::kResourceExhausted || replayed != 1 ||
        target.pending() != 1 || dlq.pending() != 1) {
        std::cerr << "Redis bounded DLQ replay invariant failed: replayed=" << replayed << '\n';
        return 1;
    }
    std::vector<std::string> inspected;
    if (!dlq.peekRaw(10, &inspected).ok() || inspected.size() != 1 ||
        !setup.listPushBounded(replay_tag + ":poison", "malformed", 16).ok()) return 1;
    live::kv::RedisOrderQueue poison_dlq(&setup, replay_tag + ":poison", 16);
    live::kv::RedisOrderQueue poison_target(&setup, replay_tag + ":poison_target", 16);
    std::size_t poison_replayed = 0;
    if (poison_dlq.replayTo(&poison_target, 1, &poison_replayed).ok() || poison_replayed != 0 || poison_dlq.pending() != 1) return 1;
    const std::string waiting_campaign = "waiting-" + sku;
    live::kv::RedisFlashSaleWaitingRoom waiting_room(&setup, waiting_campaign, 2);
    live::messaging::FlashSaleOrderMessage waiting_message;
    waiting_message.promotion_id = waiting_campaign;
    waiting_message.order_id = "waiting-order-1";
    waiting_message.idempotency_key = "waiting-request-1";
    waiting_message.user_id = "waiting-user-1";
    waiting_message.sku_id = sku;
    waiting_message.quantity = 1;
    std::size_t position = 0;
    if (!waiting_room.enqueue(waiting_message, &position).ok() || position != 1 ||
        !waiting_room.enqueue(waiting_message, &position).ok() || position != 1) return 1;
    waiting_message.order_id = "waiting-order-2";
    waiting_message.idempotency_key = "waiting-request-2";
    waiting_message.user_id = "waiting-user-2";
    if (!waiting_room.enqueue(waiting_message, &position).ok() || position != 2) return 1;
    waiting_message.order_id = "waiting-order-3";
    waiting_message.idempotency_key = "waiting-request-3";
    if (waiting_room.enqueue(waiting_message, &position).code() != live::common::ErrorCode::kResourceExhausted) return 1;
    std::string claimed_ticket;
    live::messaging::FlashSaleOrderMessage claimed_message;
    if (!waiting_room.claim("worker-a", std::chrono::milliseconds(500), &claimed_ticket, &claimed_message).ok() ||
        claimed_ticket != "waiting-request-1" || claimed_message.user_id != "waiting-user-1") return 1;
    bool completed = false;
    if (!waiting_room.complete(claimed_ticket, "wrong-owner", &completed).ok() || completed) return 1;
    if (!waiting_room.complete(claimed_ticket, "worker-a", &completed).ok() || !completed ||
        !waiting_room.position("waiting-request-2", &position).ok() || position != 1) return 1;
    if (!waiting_room.claim("worker-b", std::chrono::milliseconds(25), &claimed_ticket, &claimed_message).ok() ||
        claimed_ticket != "waiting-request-2") return 1;
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    if (!waiting_room.claim("worker-c", std::chrono::milliseconds(500), &claimed_ticket, &claimed_message).ok() ||
        claimed_ticket != "waiting-request-2") return 1;
    if (!waiting_room.complete(claimed_ticket, "worker-b", &completed).ok() || completed ||
        !waiting_room.complete(claimed_ticket, "worker-c", &completed).ok() || !completed) return 1;
    std::cout << "redis_concurrency_test passed: reservations, shared rate budgets, waiting-room lease recovery, and bounded DLQ replay held\n";
    return 0;
}
