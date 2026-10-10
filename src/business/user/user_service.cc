#include "business/user/user_service.h"

#include "common/security/password_hasher.h"
#include "kv/kv_store.h"
#include "storage/local_engine/local_kv_engine.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <functional>
#include <iomanip>
#include <openssl/rand.h>
#include <sstream>

namespace live::business::user {
namespace {

std::uint64_t unixSeconds() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

bool generateToken(std::string* token) {
    if (token == nullptr) return false;
    std::array<unsigned char, 32> bytes{};
    if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) return false;
    static constexpr char digits[] = "0123456789abcdef";
    token->clear();
    token->reserve(bytes.size() * 2);
    for (const auto byte : bytes) {
        token->push_back(digits[(byte >> 4U) & 0x0fU]);
        token->push_back(digits[byte & 0x0fU]);
    }
    return true;
}

}  // namespace

common::Status UserService::registerUser(std::string username, std::string password, UserRole role, User* user) {
    if (username.empty() || username.size() > 128 || password.empty() || password.size() > 1024 ||
        username.find_first_of("\t\r\n") != std::string::npos || user == nullptr) {
        return common::Status::InvalidArgument("username, password and output are required");
    }
    std::string password_hash;
    if (const auto status = ::live::common::security::PasswordHasher::hash(password, &password_hash); !status.ok()) {
        return status;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (id_by_username_.find(username) != id_by_username_.end()) {
        return common::Status::AlreadyExists("username already exists");
    }
    if (session_store_ != nullptr) {
        std::string existing_id;
        if (session_store_->get("user:username:" + username, &existing_id).ok()) {
            return common::Status::AlreadyExists("username already exists");
        }
    }
    std::string user_id = "user-" + std::to_string(next_user_id_++);
    if (session_store_ != nullptr) {
        std::string random_id;
        if (!generateToken(&random_id)) return common::Status::Internal("failed to generate user id");
        user_id = "user-" + random_id.substr(0, 24);
    }
    User created{std::move(user_id), std::move(username),
                std::move(password_hash), role, true};
    if (session_store_ != nullptr) {
        bool claimed = false;
        const auto claim_status = session_store_->cas("user:username:" + created.username, std::nullopt,
                                                     created.id, &claimed);
        if (!claim_status.ok()) return claim_status;
        if (!claimed) return common::Status::AlreadyExists("username already exists");
        if (const auto shared_status = session_store_->set("user:" + created.id, serialize(created)); !shared_status.ok()) {
            (void)session_store_->del("user:username:" + created.username);
            return shared_status;
        }
    }
    if (const auto status = persist(created); !status.ok()) {
        if (session_store_ != nullptr) {
            (void)session_store_->del("user:username:" + created.username);
            (void)session_store_->del("user:" + created.id);
        } else {
            --next_user_id_;
        }
        return status;
    }
    id_by_username_[created.username] = created.id;
    users_by_id_[created.id] = created;
    *user = created;
    return common::Status::Ok();
}

common::Status UserService::login(const std::string& username, const std::string& password, Session* session) {
    if (username.empty() || password.empty() || session == nullptr) {
        return common::Status::InvalidArgument("username, password and output are required");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto id_it = id_by_username_.find(username);
    if (id_it == id_by_username_.end() && session_store_ != nullptr) {
        std::string shared_id;
        std::string shared_value;
        if (session_store_->get("user:username:" + username, &shared_id).ok() &&
            session_store_->get("user:" + shared_id, &shared_value).ok()) {
            User shared_user;
            if (deserialize(shared_value, &shared_user) && shared_user.id == shared_id) {
                users_by_id_[shared_user.id] = shared_user;
                id_by_username_[shared_user.username] = shared_user.id;
                id_it = id_by_username_.find(username);
            }
        }
    }
    if (id_it == id_by_username_.end()) return common::Status::Unauthenticated("invalid credentials");
    const auto user_it = users_by_id_.find(id_it->second);
    bool matches = false;
    if (user_it == users_by_id_.end() || !user_it->second.enabled ||
        !::live::common::security::PasswordHasher::verify(password, user_it->second.password_hash, &matches).ok() || !matches) {
        return common::Status::Unauthenticated("invalid credentials");
    }
    do {
        if (!generateToken(&session->token)) return common::Status::Internal("failed to generate session token");
    } while (user_id_by_token_.find(session->token) != user_id_by_token_.end());
    session->user = user_it->second;
    session->expires_at_unix_seconds = unixSeconds() + static_cast<std::uint64_t>(session_ttl_.count());
    if (session_store_ != nullptr) {
        if (const auto status = session_store_->set("session:" + session->token, session->user.id, session_ttl_); !status.ok()) {
            return status;
        }
    }
    user_id_by_token_[session->token] = session->user.id;
    token_expiry_by_token_[session->token] = session->expires_at_unix_seconds;
    return common::Status::Ok();
}

common::Status UserService::authenticate(const std::string& token, User* user) const {
    if (token.empty() || user == nullptr) return common::Status::InvalidArgument("token and output are required");
    std::lock_guard<std::mutex> lock(mutex_);
    std::string user_id;
    if (session_store_ != nullptr) {
        const auto status = session_store_->get("session:" + token, &user_id);
        if (!status.ok()) {
            if (status.code() == common::ErrorCode::kNotFound) {
                return common::Status::Unauthenticated("invalid or expired session");
            }
            return status;
        }
    } else {
        const auto token_it = user_id_by_token_.find(token);
        if (token_it == user_id_by_token_.end()) return common::Status::Unauthenticated("invalid session");
        const auto expiry_it = token_expiry_by_token_.find(token);
        if (expiry_it == token_expiry_by_token_.end() || expiry_it->second <= unixSeconds()) {
            user_id_by_token_.erase(token_it);
            token_expiry_by_token_.erase(token);
            return common::Status::Unauthenticated("session expired");
        }
        user_id = token_it->second;
    }
    auto user_it = users_by_id_.find(user_id);
    if (user_it == users_by_id_.end() && session_store_ != nullptr) {
        std::string shared_value;
        User shared_user;
        const auto shared_status = session_store_->get("user:" + user_id, &shared_value);
        if (!shared_status.ok() && shared_status.code() != common::ErrorCode::kNotFound) return shared_status;
        if (shared_status.ok() && deserialize(shared_value, &shared_user) && shared_user.id == user_id) {
            users_by_id_[shared_user.id] = shared_user;
            id_by_username_[shared_user.username] = shared_user.id;
            user_it = users_by_id_.find(user_id);
        }
    }
    if (user_it == users_by_id_.end() || !user_it->second.enabled) return common::Status::Unauthenticated("user disabled");
    *user = user_it->second;
    return common::Status::Ok();
}

common::Status UserService::logout(const std::string& token) {
    if (token.empty()) return common::Status::InvalidArgument("token must not be empty");
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_store_ != nullptr) {
        const auto status = session_store_->del("session:" + token);
        if (!status.ok()) return status;
    }
    const auto erased = user_id_by_token_.erase(token);
    token_expiry_by_token_.erase(token);
    return session_store_ != nullptr || erased != 0 ? common::Status::Ok() : common::Status::NotFound("session not found");
}

common::Status UserService::disable(const std::string& user_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = users_by_id_.find(user_id);
    if (it == users_by_id_.end()) return common::Status::NotFound("user not found");
    const bool previous_enabled = it->second.enabled;
    it->second.enabled = false;
    const auto status = persist(it->second);
    if (!status.ok()) it->second.enabled = previous_enabled;
    if (status.ok()) {
        for (auto token_it = user_id_by_token_.begin(); token_it != user_id_by_token_.end();) {
            if (token_it->second == user_id) {
                token_expiry_by_token_.erase(token_it->first);
                token_it = user_id_by_token_.erase(token_it);
            } else {
                ++token_it;
            }
        }
    }
    return status;
}

common::Status UserService::restore() {
    if (storage_ == nullptr) return common::Status::Ok();
    std::lock_guard<std::mutex> lock(mutex_);
    users_by_id_.clear();
    id_by_username_.clear();
    user_id_by_token_.clear();
    token_expiry_by_token_.clear();
    next_user_id_ = 1;
    for (const auto& [key, value] : storage_->scanPrefix("users/")) {
        User user;
        if (!deserialize(value, &user) || key != "users/" + user.id) {
            return common::Status::Internal("invalid persisted user");
        }
        if (users_by_id_.find(user.id) != users_by_id_.end() || id_by_username_.find(user.username) != id_by_username_.end()) {
            return common::Status::Internal("duplicate persisted user");
        }
        users_by_id_[user.id] = user;
        id_by_username_[user.username] = user.id;
        const auto delimiter = user.id.rfind('-');
        if (delimiter != std::string::npos) {
            const std::string suffix = user.id.substr(delimiter + 1);
            const bool numeric_suffix = !suffix.empty() && std::all_of(suffix.begin(), suffix.end(),
                [](unsigned char value) { return std::isdigit(value) != 0; });
            if (numeric_suffix) {
                try {
                    next_user_id_ = std::max(next_user_id_,
                        static_cast<std::uint64_t>(std::stoull(suffix)) + 1);
                } catch (...) {
                    return common::Status::Internal("invalid persisted numeric user id");
                }
            }
        }
    }
    return common::Status::Ok();
}

std::string UserService::serialize(const User& user) {
    return user.id + '\t' + user.username + '\t' + user.password_hash + '\t' +
           std::to_string(static_cast<int>(user.role)) + '\t' + (user.enabled ? "1" : "0");
}

bool UserService::deserialize(const std::string& value, User* user) {
    if (user == nullptr) return false;
    std::istringstream input(value);
    int role = -1;
    int enabled = 0;
    return static_cast<bool>(std::getline(input, user->id, '\t')) &&
           static_cast<bool>(std::getline(input, user->username, '\t')) &&
           static_cast<bool>(std::getline(input, user->password_hash, '\t')) &&
           static_cast<bool>(input >> role) && input.get() == '\t' &&
           static_cast<bool>(input >> enabled) && role >= 0 && role <= 2 &&
           (user->role = static_cast<UserRole>(role), user->enabled = enabled != 0, true);
}

common::Status UserService::persist(const User& user) {
    if (session_store_ != nullptr) {
        if (const auto status = session_store_->set("user:username:" + user.username, user.id); !status.ok()) return status;
        if (const auto status = session_store_->set("user:" + user.id, serialize(user)); !status.ok()) return status;
    }
    return storage_ == nullptr ? common::Status::Ok() : storage_->set("users/" + user.id, serialize(user));
}

}  // namespace live::business::user
