#pragma once

#include "kv/kv_store.h"

#include <cstdint>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

struct redisContext;

namespace live::kv {

class RedisKVStore final : public IKVStore {
public:
    RedisKVStore() = default;
    ~RedisKVStore() override;

    common::Status connect(const std::string& host, std::uint16_t port,
                           std::chrono::milliseconds timeout = std::chrono::seconds(2),
                           std::size_t pool_size = 8);
    void close();
    bool connected() const;
    common::Status ping();

    common::Status get(const std::string& key, std::string* value) override;
    common::Status set(const std::string& key, const std::string& value,
                       std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) override;
    common::Status del(const std::string& key) override;
    common::Status cas(const std::string& key, const std::optional<std::string>& expected,
                       const std::string& value, bool* updated,
                       std::chrono::milliseconds ttl = std::chrono::milliseconds::zero()) override;
    common::Status expire(const std::string& key, std::chrono::milliseconds ttl) override;

    // Executes an integer-returning Lua script. The caller must ensure that
    // all keys belong to the same Redis hash slot when Redis Cluster is used.
    common::Status evalInteger(const std::string& script, const std::vector<std::string>& keys,
                               const std::vector<std::string>& arguments, std::int64_t* result);
    // Executes a Lua script returning a bulk string. A nil reply is returned as
    // an empty string so scripts can express an empty queue without an error.
    common::Status evalString(const std::string& script, const std::vector<std::string>& keys,
                              const std::vector<std::string>& arguments, std::string* result);
    common::Status hashGet(const std::string& key, const std::string& field, std::string* value);
    common::Status hashGetMany(const std::string& key, const std::vector<std::string>& fields,
                               std::vector<std::string>* values);
    common::Status hashSet(const std::string& key, const std::string& field, const std::string& value);
    common::Status listPush(const std::string& key, const std::string& value);
    common::Status listPushBounded(const std::string& key, const std::string& value, std::size_t max_length);
    common::Status listBlockingPop(const std::string& key, std::chrono::milliseconds timeout,
                                   std::string* value);
    common::Status listLength(const std::string& key, std::size_t* length);
    common::Status listRange(const std::string& key, std::int64_t start, std::int64_t stop,
                            std::vector<std::string>* values);
    // Atomically moves the oldest list item to the head of another list unless
    // the destination's bounded capacity has been reached. Keys must share a
    // Redis Cluster hash slot.
    common::Status listMoveBounded(const std::string& source, const std::string& destination,
                                   std::size_t max_destination_length, const std::string& expected_tail,
                                   std::int64_t* result);

private:
    struct Connection;
    std::shared_ptr<Connection> acquire(std::chrono::milliseconds timeout = std::chrono::seconds(3));
    void release(std::shared_ptr<Connection> connection);

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<std::shared_ptr<Connection>> connections_;
    std::deque<std::shared_ptr<Connection>> idle_;
    std::size_t in_use_{0};
    bool closing_{true};
};

}  // namespace live::kv
