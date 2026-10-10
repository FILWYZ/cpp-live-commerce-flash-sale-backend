#include "kv/redis_inventory_fast_path.h"
#include "kv/redis_kv_store.h"
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
    std::cout << "redis_concurrency_test passed: Lua inventory reservation and shared token-bucket budgets held\n";
    return 0;
}
