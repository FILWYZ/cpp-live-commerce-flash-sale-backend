#include "kv/redis_kv_store.h"

#include <hiredis/hiredis.h>

#include <algorithm>
#include <cstring>
#include <string_view>

namespace live::kv {
namespace {

common::Status redisError(redisContext* context) {
    return common::Status::Internal(context == nullptr ? "Redis is not connected" : context->errstr);
}

}  // namespace

struct RedisKVStore::Connection {
    redisContext* context{nullptr};
    std::unordered_map<std::string, std::string> script_shas;
};

RedisKVStore::~RedisKVStore() { close(); }

common::Status RedisKVStore::connect(const std::string& host, std::uint16_t port,
                                     std::chrono::milliseconds timeout, std::size_t pool_size) {
    if (host.empty() || pool_size == 0) return common::Status::InvalidArgument("invalid Redis connection settings");
    close();
    std::vector<std::shared_ptr<Connection>> connections;
    connections.reserve(pool_size);
    for (std::size_t i = 0; i < pool_size; ++i) {
        timeval tv{};
        tv.tv_sec = static_cast<long>(timeout.count() / 1000);
        tv.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);
        auto connection = std::make_shared<Connection>();
        connection->context = redisConnectWithTimeout(host.c_str(), port, tv);
        if (connection->context == nullptr || connection->context->err != 0) {
            const auto status = redisError(connection->context);
            for (const auto& item : connections) {
                if (item->context != nullptr) redisFree(item->context);
            }
            if (connection->context != nullptr) redisFree(connection->context);
            return status;
        }
        connections.push_back(std::move(connection));
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connections_ = connections;
        for (const auto& connection : connections_) idle_.push_back(connection);
        host_ = host;
        port_ = port;
        timeout_ = timeout;
        closing_ = false;
    }
    condition_.notify_all();
    return common::Status::Ok();
}

void RedisKVStore::close() {
    std::vector<std::shared_ptr<Connection>> connections;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        closing_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this] { return in_use_ == 0; });
        connections.swap(connections_);
        idle_.clear();
    }
    for (const auto& connection : connections) {
        if (connection != nullptr && connection->context != nullptr) {
            redisFree(connection->context);
            connection->context = nullptr;
        }
    }
}

bool RedisKVStore::connected() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !closing_ && !connections_.empty();
}

common::Status RedisKVStore::ping() {
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(connection->context, "PING"));
    if (reply == nullptr) {
        const auto status = redisError(connection->context);
        release(std::move(connection));
        return status;
    }
    const bool ok = reply->type == REDIS_REPLY_STATUS && std::string(reply->str, reply->len) == "PONG";
    freeReplyObject(reply);
    release(std::move(connection));
    return ok ? common::Status::Ok() : common::Status::Internal("unexpected Redis PING response");
}

std::shared_ptr<RedisKVStore::Connection> RedisKVStore::acquire(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!condition_.wait_for(lock, timeout, [this] { return closing_ || !idle_.empty(); })) return {};
    if (closing_ || idle_.empty()) return {};
    auto connection = idle_.front();
    idle_.pop_front();
    ++in_use_;
    const auto host = host_;
    const auto port = port_;
    const auto connect_timeout = timeout_;
    lock.unlock();

    if (connection->context == nullptr || connection->context->err != 0) {
        if (connection->context != nullptr) redisFree(connection->context);
        connection->context = nullptr;
        connection->script_shas.clear();
        timeval tv{};
        tv.tv_sec = static_cast<long>(connect_timeout.count() / 1000);
        tv.tv_usec = static_cast<long>((connect_timeout.count() % 1000) * 1000);
        connection->context = redisConnectWithTimeout(host.c_str(), port, tv);
        if (connection->context == nullptr || connection->context->err != 0) {
            if (connection->context != nullptr) redisFree(connection->context);
            connection->context = nullptr;
            lock.lock();
            --in_use_;
            idle_.push_back(std::move(connection));
            condition_.notify_one();
            return {};
        }
    }
    return connection;
}

void RedisKVStore::release(std::shared_ptr<Connection> connection) {
    if (connection == nullptr) return;
    if (connection->context != nullptr && connection->context->err != 0) {
        redisFree(connection->context);
        connection->context = nullptr;
        connection->script_shas.clear();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (in_use_ > 0) --in_use_;
    if (!closing_) idle_.push_back(std::move(connection));
    condition_.notify_one();
}

common::Status RedisKVStore::get(const std::string& key, std::string* value) {
    if (key.empty() || value == nullptr) return common::Status::InvalidArgument("invalid Redis GET");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(connection->context, "GET %b", key.data(), key.size()));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    common::Status status = common::Status::Ok();
    if (reply->type == REDIS_REPLY_NIL) status = common::Status::NotFound("key not found");
    else if (reply->type != REDIS_REPLY_STRING) status = common::Status::Internal("unexpected Redis GET reply");
    else *value = std::string(reply->str, reply->len);
    freeReplyObject(reply);
    release(std::move(connection));
    return status;
}

common::Status RedisKVStore::set(const std::string& key, const std::string& value, std::chrono::milliseconds ttl) {
    if (key.empty()) return common::Status::InvalidArgument("invalid Redis SET");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    redisReply* reply = nullptr;
    if (ttl.count() > 0) {
        reply = static_cast<redisReply*>(redisCommand(
            connection->context, "PSETEX %b %lld %b", key.data(), key.size(),
            static_cast<long long>(ttl.count()), value.data(), value.size()));
    } else {
        reply = static_cast<redisReply*>(redisCommand(
            connection->context, "SET %b %b", key.data(), key.size(), value.data(), value.size()));
    }
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    const bool ok = reply->type == REDIS_REPLY_STATUS && std::string(reply->str, reply->len) == "OK";
    freeReplyObject(reply); release(std::move(connection));
    return ok ? common::Status::Ok() : common::Status::Internal("Redis SET failed");
}

common::Status RedisKVStore::del(const std::string& key) {
    if (key.empty()) return common::Status::InvalidArgument("invalid Redis DEL");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(connection->context, "DEL %b", key.data(), key.size()));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    const bool ok = reply->type == REDIS_REPLY_INTEGER;
    freeReplyObject(reply); release(std::move(connection));
    return ok ? common::Status::Ok() : common::Status::Internal("Redis DEL failed");
}

common::Status RedisKVStore::cas(const std::string& key, const std::optional<std::string>& expected,
                                 const std::string& value, bool* updated, std::chrono::milliseconds ttl) {
    if (key.empty() || updated == nullptr) return common::Status::InvalidArgument("invalid Redis CAS");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    static const char* script =
        "local c=redis.call('get',KEYS[1]); "
        "if ARGV[1]=='__MISSING__' then if c then return 0 end "
        "elseif not c or c~=ARGV[1] then return 0 end; "
        "if tonumber(ARGV[2])>0 then redis.call('psetex',KEYS[1],ARGV[2],ARGV[3]) "
        "else redis.call('set',KEYS[1],ARGV[3]) end; return 1";
    const std::string expected_value = expected.has_value() ? *expected : "__MISSING__";
    const std::string ttl_value = std::to_string(std::max<std::int64_t>(0, ttl.count()));
    const char* argv[] = {"EVAL", script, "1", key.data(), expected_value.data(), ttl_value.data(), value.data()};
    std::size_t lengths[] = {4, std::strlen(script), 1, key.size(), expected_value.size(), ttl_value.size(), value.size()};
    auto* reply = static_cast<redisReply*>(redisCommandArgv(connection->context, 7, argv, lengths));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    *updated = reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
    freeReplyObject(reply); release(std::move(connection));
    return common::Status::Ok();
}

common::Status RedisKVStore::expire(const std::string& key, std::chrono::milliseconds ttl) {
    if (key.empty() || ttl.count() <= 0) return common::Status::InvalidArgument("invalid Redis EXPIRE");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(
        connection->context, "PEXPIRE %b %lld", key.data(), key.size(),
        static_cast<long long>(ttl.count())));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    const bool ok = reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
    freeReplyObject(reply); release(std::move(connection));
    return ok ? common::Status::Ok() : common::Status::NotFound("key not found");
}

common::Status RedisKVStore::evalInteger(const std::string& script, const std::vector<std::string>& keys,
                                          const std::vector<std::string>& arguments, std::int64_t* result) {
    if (script.empty() || result == nullptr) return common::Status::InvalidArgument("invalid Redis script arguments");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto script_it = connection->script_shas.find(script);
        if (script_it == connection->script_shas.end()) {
            auto* load_reply = static_cast<redisReply*>(redisCommand(connection->context, "SCRIPT LOAD %b", script.data(), script.size()));
            if (load_reply == nullptr) {
                const auto status = redisError(connection->context);
                release(std::move(connection));
                return status;
            }
            if (load_reply->type != REDIS_REPLY_STRING || load_reply->str == nullptr) {
                const std::string message = load_reply->str == nullptr ? "Redis SCRIPT LOAD failed" : load_reply->str;
                freeReplyObject(load_reply); release(std::move(connection)); return common::Status::Internal(message);
            }
            script_it = connection->script_shas.emplace(script, std::string(load_reply->str, load_reply->len)).first;
            freeReplyObject(load_reply);
        }
        std::vector<std::string> storage{"EVALSHA", script_it->second, std::to_string(keys.size())};
        storage.insert(storage.end(), keys.begin(), keys.end());
        storage.insert(storage.end(), arguments.begin(), arguments.end());
        std::vector<const char*> argv; std::vector<std::size_t> lengths;
        for (const auto& item : storage) { argv.push_back(item.data()); lengths.push_back(item.size()); }
        auto* reply = static_cast<redisReply*>(redisCommandArgv(
            connection->context, static_cast<int>(argv.size()), argv.data(), lengths.data()));
        if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
        if (reply->type == REDIS_REPLY_ERROR && reply->str != nullptr &&
            std::string_view(reply->str, reply->len).find("NOSCRIPT") != std::string_view::npos) {
            connection->script_shas.erase(script); freeReplyObject(reply); continue;
        }
        if (reply->type != REDIS_REPLY_INTEGER) {
            const std::string message = reply->str == nullptr ? "Redis script did not return integer" : reply->str;
            freeReplyObject(reply); release(std::move(connection)); return common::Status::Internal(message);
        }
        *result = reply->integer;
        freeReplyObject(reply); release(std::move(connection)); return common::Status::Ok();
    }
    release(std::move(connection));
    return common::Status::Internal("Redis script disappeared while executing");
}

common::Status RedisKVStore::evalString(const std::string& script, const std::vector<std::string>& keys,
                                         const std::vector<std::string>& arguments, std::string* result) {
    if (script.empty() || result == nullptr) return common::Status::InvalidArgument("invalid Redis script arguments");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto script_it = connection->script_shas.find(script);
        if (script_it == connection->script_shas.end()) {
            auto* load_reply = static_cast<redisReply*>(redisCommand(connection->context, "SCRIPT LOAD %b", script.data(), script.size()));
            if (load_reply == nullptr) {
                const auto status = redisError(connection->context);
                release(std::move(connection));
                return status;
            }
            if (load_reply->type != REDIS_REPLY_STRING || load_reply->str == nullptr) {
                const std::string message = load_reply->str == nullptr ? "Redis SCRIPT LOAD failed" : load_reply->str;
                freeReplyObject(load_reply); release(std::move(connection)); return common::Status::Internal(message);
            }
            script_it = connection->script_shas.emplace(script, std::string(load_reply->str, load_reply->len)).first;
            freeReplyObject(load_reply);
        }
        std::vector<std::string> storage{"EVALSHA", script_it->second, std::to_string(keys.size())};
        storage.insert(storage.end(), keys.begin(), keys.end());
        storage.insert(storage.end(), arguments.begin(), arguments.end());
        std::vector<const char*> argv; std::vector<std::size_t> lengths;
        for (const auto& item : storage) { argv.push_back(item.data()); lengths.push_back(item.size()); }
        auto* reply = static_cast<redisReply*>(redisCommandArgv(
            connection->context, static_cast<int>(argv.size()), argv.data(), lengths.data()));
        if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
        if (reply->type == REDIS_REPLY_ERROR && reply->str != nullptr &&
            std::string_view(reply->str, reply->len).find("NOSCRIPT") != std::string_view::npos) {
            connection->script_shas.erase(script); freeReplyObject(reply); continue;
        }
        if (reply->type == REDIS_REPLY_NIL) {
            result->clear();
        } else if (reply->type == REDIS_REPLY_STRING) {
            result->assign(reply->str, reply->len);
        } else {
            const std::string message = reply->str == nullptr ? "Redis script did not return string" : reply->str;
            freeReplyObject(reply); release(std::move(connection)); return common::Status::Internal(message);
        }
        freeReplyObject(reply); release(std::move(connection)); return common::Status::Ok();
    }
    release(std::move(connection));
    return common::Status::Internal("Redis script disappeared while executing");
}

common::Status RedisKVStore::hashGet(const std::string& key, const std::string& field, std::string* value) {
    if (key.empty() || field.empty() || value == nullptr) return common::Status::InvalidArgument("invalid Redis HGET");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(
        connection->context, "HGET %b %b", key.data(), key.size(), field.data(), field.size()));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    common::Status status = common::Status::Ok();
    if (reply->type == REDIS_REPLY_NIL) status = common::Status::NotFound("hash field not found");
    else if (reply->type != REDIS_REPLY_STRING) status = common::Status::Internal("unexpected Redis HGET reply");
    else *value = std::string(reply->str, reply->len);
    freeReplyObject(reply); release(std::move(connection)); return status;
}

common::Status RedisKVStore::hashGetMany(const std::string& key, const std::vector<std::string>& fields,
                                          std::vector<std::string>* values) {
    if (key.empty() || fields.empty() || values == nullptr) return common::Status::InvalidArgument("invalid Redis HMGET");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    std::vector<std::string> storage{"HMGET", key};
    storage.insert(storage.end(), fields.begin(), fields.end());
    std::vector<const char*> argv; std::vector<std::size_t> lengths;
    for (const auto& item : storage) { argv.push_back(item.data()); lengths.push_back(item.size()); }
    auto* reply = static_cast<redisReply*>(redisCommandArgv(
        connection->context, static_cast<int>(argv.size()), argv.data(), lengths.data()));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != fields.size()) {
        freeReplyObject(reply); release(std::move(connection)); return common::Status::Internal("unexpected Redis HMGET reply");
    }
    values->clear(); values->reserve(fields.size());
    for (std::size_t i = 0; i < fields.size(); ++i) {
        if (reply->element[i]->type == REDIS_REPLY_NIL) {
            freeReplyObject(reply);
            release(std::move(connection));
            return common::Status::NotFound("hash field not found");
        }
        if (reply->element[i]->type != REDIS_REPLY_STRING) {
            freeReplyObject(reply);
            release(std::move(connection));
            return common::Status::Internal("unexpected Redis HMGET field");
        }
        values->emplace_back(reply->element[i]->str, reply->element[i]->len);
    }
    freeReplyObject(reply); release(std::move(connection)); return common::Status::Ok();
}

common::Status RedisKVStore::hashSet(const std::string& key, const std::string& field, const std::string& value) {
    if (key.empty() || field.empty()) return common::Status::InvalidArgument("invalid Redis HSET");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(
        connection->context, "HSET %b %b %b", key.data(), key.size(), field.data(), field.size(),
        value.data(), value.size()));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    const bool ok = reply->type == REDIS_REPLY_INTEGER;
    freeReplyObject(reply);
    release(std::move(connection));
    return ok ? common::Status::Ok() : common::Status::Internal("Redis HSET failed");
}

common::Status RedisKVStore::listPush(const std::string& key, const std::string& value) {
    if (key.empty() || value.empty()) return common::Status::InvalidArgument("invalid Redis LPUSH");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(
        connection->context, "LPUSH %b %b", key.data(), key.size(), value.data(), value.size()));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    const bool ok = reply->type == REDIS_REPLY_INTEGER;
    freeReplyObject(reply);
    release(std::move(connection));
    return ok ? common::Status::Ok() : common::Status::Internal("Redis LPUSH failed");
}

common::Status RedisKVStore::listPushBounded(const std::string& key, const std::string& value, std::size_t max_length) {
    if (key.empty() || value.empty() || max_length == 0) return common::Status::InvalidArgument("invalid bounded Redis LPUSH");
    static const std::string script =
        "local n=redis.call('LLEN',KEYS[1]); if n>=tonumber(ARGV[2]) then return 0 end; "
        "redis.call('LPUSH',KEYS[1],ARGV[1]); return 1";
    std::int64_t pushed = 0;
    if (const auto status = evalInteger(script, {key}, {value, std::to_string(max_length)}, &pushed); !status.ok()) return status;
    return pushed == 1 ? common::Status::Ok() : common::Status::ResourceExhausted("Redis flash-sale queue is full");
}

common::Status RedisKVStore::listBlockingPop(const std::string& key, std::chrono::milliseconds timeout, std::string* value) {
    if (key.empty() || value == nullptr || timeout.count() < 0) return common::Status::InvalidArgument("invalid Redis BRPOP");
    auto connection = acquire(timeout + std::chrono::seconds(1));
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    const long long seconds = timeout.count() == 0 ? 0 : std::max<long long>(1, (timeout.count() + 999) / 1000);
    auto* reply = static_cast<redisReply*>(redisCommand(connection->context, "BRPOP %b %lld", key.data(), key.size(), seconds));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    common::Status status = common::Status::Ok();
    if (reply->type == REDIS_REPLY_NIL) status = common::Status::Incomplete("Redis BRPOP timed out");
    else if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 ||
             reply->element[1]->type != REDIS_REPLY_STRING) {
        status = common::Status::Internal("unexpected Redis BRPOP reply");
    }
    else *value = std::string(reply->element[1]->str, reply->element[1]->len);
    freeReplyObject(reply); release(std::move(connection)); return status;
}

common::Status RedisKVStore::listLength(const std::string& key, std::size_t* length) {
    if (key.empty() || length == nullptr) return common::Status::InvalidArgument("invalid Redis LLEN");
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(connection->context, "LLEN %b", key.data(), key.size()));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    if (reply->type != REDIS_REPLY_INTEGER || reply->integer < 0) {
        freeReplyObject(reply);
        release(std::move(connection));
        return common::Status::Internal("unexpected Redis LLEN reply");
    }
    *length = static_cast<std::size_t>(reply->integer);
    freeReplyObject(reply); release(std::move(connection)); return common::Status::Ok();
}

common::Status RedisKVStore::listRange(const std::string& key, std::int64_t start, std::int64_t stop,
                                      std::vector<std::string>* values) {
    if (key.empty() || values == nullptr || start < 0 || stop < start)
        return common::Status::InvalidArgument("invalid Redis LRANGE");
    values->clear();
    auto connection = acquire();
    if (connection == nullptr) return common::Status::FailedPrecondition("Redis is not connected");
    auto* reply = static_cast<redisReply*>(redisCommand(connection->context, "LRANGE %b %lld %lld",
        key.data(), key.size(), static_cast<long long>(start), static_cast<long long>(stop)));
    if (reply == nullptr) { const auto status = redisError(connection->context); release(std::move(connection)); return status; }
    if (reply->type != REDIS_REPLY_ARRAY) {
        freeReplyObject(reply);
        release(std::move(connection));
        return common::Status::Internal("unexpected Redis LRANGE reply");
    }
    values->reserve(reply->elements);
    for (std::size_t i = 0; i < reply->elements; ++i) {
        const auto* element = reply->element[i];
        if (element == nullptr || element->type != REDIS_REPLY_STRING) {
            values->clear();
            freeReplyObject(reply);
            release(std::move(connection));
            return common::Status::Internal("unexpected Redis LRANGE item");
        }
        values->emplace_back(element->str, element->len);
    }
    freeReplyObject(reply);
    release(std::move(connection));
    return common::Status::Ok();
}

common::Status RedisKVStore::listMoveBounded(const std::string& source, const std::string& destination,
    std::size_t max_destination_length, const std::string& expected_tail, std::int64_t* result) {
    if (source.empty() || destination.empty() || source == destination || max_destination_length == 0 ||
        expected_tail.empty() || result == nullptr)
        return common::Status::InvalidArgument("invalid bounded Redis list move");
    static const std::string script =
        "if redis.call('LLEN',KEYS[2])>=tonumber(ARGV[1]) then return -1 end; "
        "local v=redis.call('LINDEX',KEYS[1],-1); if not v then return 0 end; "
        "if v~=ARGV[2] then return -2 end; redis.call('RPOPLPUSH',KEYS[1],KEYS[2]); return 1";
    return evalInteger(script, {source, destination}, {std::to_string(max_destination_length), expected_tail}, result);
}

}  // namespace live::kv
