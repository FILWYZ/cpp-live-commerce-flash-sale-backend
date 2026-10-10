#include "storage/mysql/mysql_connection_pool.h"

#include <algorithm>

namespace live::storage::mysql {
namespace {

common::Status connectOne(const std::string& host, std::uint16_t port,
                          const std::string& user, const std::string& password,
                          const std::string& database, MYSQL** output) {
    if (output == nullptr) return common::Status::InvalidArgument("MySQL connection output is required");
    *output = nullptr;
    MYSQL* connection = mysql_init(nullptr);
    if (connection == nullptr) return common::Status::Internal("mysql_init failed");
    unsigned int timeout = 3;
    mysql_options(connection, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    bool reconnect = true;
    mysql_options(connection, MYSQL_OPT_RECONNECT, &reconnect);
    if (mysql_real_connect(connection, host.c_str(), user.c_str(), password.c_str(), database.c_str(),
                           port, nullptr, 0) == nullptr) {
        const std::string message = mysql_error(connection);
        mysql_close(connection);
        return common::Status::Internal(message.empty() ? "MySQL connection failed" : message);
    }
    mysql_autocommit(connection, 1);
    *output = connection;
    return common::Status::Ok();
}

}  // namespace

MySqlConnectionPool::Lease::Lease(Lease&& other) noexcept
    : pool_(other.pool_), connection_(other.connection_) {
    other.pool_ = nullptr;
    other.connection_ = nullptr;
}

MySqlConnectionPool::Lease& MySqlConnectionPool::Lease::operator=(Lease&& other) noexcept {
    if (this == &other) return *this;
    if (pool_ != nullptr && connection_ != nullptr) pool_->release(connection_);
    pool_ = other.pool_;
    connection_ = other.connection_;
    other.pool_ = nullptr;
    other.connection_ = nullptr;
    return *this;
}

MySqlConnectionPool::Lease::~Lease() {
    if (pool_ != nullptr && connection_ != nullptr) pool_->release(connection_);
}

MySqlConnectionPool::~MySqlConnectionPool() { close(); }

common::Status MySqlConnectionPool::connect(const std::vector<std::string>& hosts, std::uint16_t port,
                                            const std::string& user, const std::string& password,
                                            const std::string& database, std::size_t pool_size) {
    if (hosts.empty() || pool_size == 0) return common::Status::InvalidArgument("MySQL hosts and pool size are required");
    close();
    std::string last_error;
    for (const auto& host : hosts) {
        std::vector<MYSQL*> connections;
        bool failed = false;
        for (std::size_t i = 0; i < pool_size; ++i) {
            MYSQL* connection = nullptr;
            const auto status = connectOne(host, port, user, password, database, &connection);
            if (!status.ok()) {
                last_error = status.message();
                failed = true;
                break;
            }
            connections.push_back(connection);
        }
        if (failed) {
            for (MYSQL* connection : connections) mysql_close(connection);
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            all_ = connections;
            idle_ = connections;
            active_endpoint_ = host;
            closing_ = false;
        }
        condition_.notify_all();
        return common::Status::Ok();
    }
    return common::Status::Internal(last_error.empty() ? "MySQL connection failed" : last_error);
}

void MySqlConnectionPool::close() {
    std::vector<MYSQL*> connections;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        closing_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this] { return in_use_ == 0; });
        connections.swap(all_);
        idle_.clear();
        active_endpoint_.clear();
    }
    condition_.notify_all();
    for (MYSQL* connection : connections) mysql_close(connection);
}

MySqlConnectionPool::Lease MySqlConnectionPool::acquire(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!condition_.wait_for(lock, timeout, [this] { return closing_ || !idle_.empty(); })) return {};
    if (closing_ || idle_.empty()) return {};
    MYSQL* connection = idle_.back();
    idle_.pop_back();
    ++in_use_;
    return Lease(this, connection);
}

bool MySqlConnectionPool::connected() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !closing_ && !all_.empty();
}

std::string MySqlConnectionPool::activeEndpoint() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_endpoint_;
}

std::size_t MySqlConnectionPool::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return all_.size();
}

void MySqlConnectionPool::release(MYSQL* connection) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (in_use_ > 0) --in_use_;
    if (closing_) {
        condition_.notify_all();
        return;
    }
    if (std::find(all_.begin(), all_.end(), connection) == all_.end()) {
        mysql_close(connection);
        return;
    }
    idle_.push_back(connection);
    condition_.notify_one();
}

}  // namespace live::storage::mysql
