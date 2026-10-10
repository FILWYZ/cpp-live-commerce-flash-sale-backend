#include "business/inventory/inventory_service.h"
#include "business/inventory/inventory_gateway.h"
#include "business/inventory/inventory_reconciler.h"
#include "business/order/order_service.h"
#include "business/product/product_service.h"
#include "business/promotion/promotion_service.h"
#include "business/flash_sale/flash_sale_order_service.h"
#include "business/flash_sale/flash_sale_order_consumer.h"
#include "business/flash_sale/local_flash_sale_order_repository.h"
#include "business/user/user_service.h"
#include "common/security/webhook_signature.h"
#include "cache/sharded_cache.h"
#include "gateway/http/http_server.h"
#include "messaging/async_outbox_dispatcher.h"
#include "messaging/in_memory_broker.h"
#include "messaging/order_queue.h"
#include "messaging/outbox.h"
#include "network/reactor/event_loop.h"
#include "observability/metrics/metrics_registry.h"
#include "observability/logging/structured_logger.h"
#include "storage/local_engine/local_kv_engine.h"
#include "storage/local_engine/snapshot_worker.h"
#include "traffic/token_bucket.h"
#ifdef ECOMMERCE_HAS_REDIS
#include "kv/redis_kv_store.h"
#include "kv/redis_flash_sale_gate.h"
#include "kv/redis_flash_sale_waiting_room.h"
#include "kv/redis_inventory_gateway.h"
#include "kv/redis_order_queue.h"
#include "traffic/redis_token_bucket.h"
#endif
#ifdef ECOMMERCE_HAS_KAFKA
#include "messaging/kafka_client.h"
#include "messaging/kafka_dlq_operator.h"
#endif
#ifdef ECOMMERCE_HAS_ROCKETMQ
#include "messaging/rocketmq_order_queue.h"
#endif
#ifdef ECOMMERCE_HAS_MYSQL
#include "storage/mysql/mysql_transaction_store.h"
#endif

#include <cstdint>
#include <charconv>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <csignal>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <arpa/inet.h>


namespace {

volatile std::sig_atomic_t shutdown_requested = 0;

struct AdminCredential {
    std::string actor;
    std::string role;
    std::string token;
};

std::vector<AdminCredential> loadAdminCredentials() {
    std::vector<AdminCredential> credentials;
    if (const char* token = std::getenv("ECOMMERCE_ADMIN_TOKEN"); token != nullptr && *token != '\0') {
        credentials.push_back({"local-admin", "admin", token});
    }
    if (const char* configured = std::getenv("ECOMMERCE_ADMIN_CREDENTIALS"); configured != nullptr) {
        std::string entries(configured);
        std::size_t start = 0;
        while (start < entries.size()) {
            const auto end = entries.find(';', start);
            const auto record = entries.substr(start, end == std::string::npos ? std::string::npos : end - start);
            const auto first = record.find('=');
            const auto second = first == std::string::npos ? std::string::npos : record.find('=', first + 1);
            if (first != std::string::npos && second != std::string::npos) {
                AdminCredential credential{record.substr(0, first), record.substr(first + 1, second - first - 1), record.substr(second + 1)};
                if (!credential.actor.empty() && !credential.token.empty() &&
                    (credential.role == "viewer" || credential.role == "operator" || credential.role == "admin")) {
                    credentials.push_back(std::move(credential));
                }
            }
            if (end == std::string::npos) break;
            start = end + 1;
        }
    }
    return credentials;
}

int authorizeAdmin(const live::gateway::http::HttpRequest& request,
                   const std::vector<AdminCredential>& credentials,
                   const std::string& required_role, std::string* actor) {
    if (credentials.empty()) return 503;
    const auto token = request.headers.find("x-admin-token");
    if (token == request.headers.end()) return 401;
    const auto rank = [](const std::string& role) {
        return role == "admin" ? 3 : (role == "operator" ? 2 : (role == "viewer" ? 1 : 0));
    };
    for (const auto& credential : credentials) {
        const auto& expected = credential.token;
        const auto& provided = token->second;
        std::size_t difference = expected.size() ^ provided.size();
        const auto count = std::max(expected.size(), provided.size());
        for (std::size_t i = 0; i < count; ++i) {
            const unsigned char left = i < expected.size() ? static_cast<unsigned char>(expected[i]) : 0;
            const unsigned char right = i < provided.size() ? static_cast<unsigned char>(provided[i]) : 0;
            difference |= static_cast<std::size_t>(left ^ right);
        }
        if (difference != 0) continue;
        if (rank(credential.role) < rank(required_role)) return 403;
        if (actor != nullptr) *actor = credential.actor;
        return 0;
    }
    return 401;
}

void requestShutdown(int) {
    shutdown_requested = 1;
}

std::string jsonEscape(const std::string& value) {
    std::string output;
    for (const char c : value) {
        switch (c) {
            case '"': output += "\\\""; break;
            case '\\': output += "\\\\"; break;
            case '\b': output += "\\b"; break;
            case '\f': output += "\\f"; break;
            case '\n': output += "\\n"; break;
            case '\r': output += "\\r"; break;
            case '\t': output += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    const char* digits = "0123456789abcdef";
                    output += "\\u00";
                    output.push_back(digits[(static_cast<unsigned char>(c) >> 4) & 0x0f]);
                    output.push_back(digits[static_cast<unsigned char>(c) & 0x0f]);
                } else {
                    output.push_back(c);
                }
        }
    }
    return output;
}

int hexValue(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

bool percentDecode(std::string_view encoded, std::string* decoded) {
    if (decoded == nullptr) return false;
    decoded->clear();
    decoded->reserve(encoded.size());
    for (std::size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '+') {
            decoded->push_back(' ');
        } else if (encoded[i] == '%') {
            if (i + 2 >= encoded.size()) return false;
            const int high = hexValue(encoded[i + 1]);
            const int low = hexValue(encoded[i + 2]);
            if (high < 0 || low < 0) return false;
            decoded->push_back(static_cast<char>((high << 4) | low));
            i += 2;
        } else {
            decoded->push_back(encoded[i]);
        }
    }
    return true;
}

std::string formValue(const std::string& body, const std::string& key) {
    std::size_t begin = 0;
    while (begin <= body.size()) {
        const auto end = body.find('&', begin);
        const auto part_end = end == std::string::npos ? body.size() : end;
        const auto equal = body.find('=', begin);
        if (equal != std::string::npos && equal < part_end) {
            std::string decoded_key;
            std::string decoded_value;
            if (percentDecode(std::string_view(body).substr(begin, equal - begin), &decoded_key) &&
                decoded_key == key && percentDecode(std::string_view(body).substr(equal + 1, part_end - equal - 1), &decoded_value)) {
                return decoded_value;
            }
        }
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    return {};
}

std::string queryValue(const std::string& target, const std::string& key) {
    const auto query = target.find('?');
    if (query == std::string::npos) return {};
    return formValue(target.substr(query + 1), key);
}

live::gateway::http::HttpResponse jsonResponse(int status, std::string reason, std::string body) {
    return {status, std::move(reason), {{"Content-Type", "application/json"}}, std::move(body)};
}

bool parseInt64(const std::string& text, std::int64_t* value) {
    if (value == nullptr || text.empty()) return false;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), *value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

std::size_t envSize(const char* name, std::size_t fallback, std::size_t maximum) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return fallback;
    std::uint64_t parsed = 0;
    const auto result = std::from_chars(value, value + std::char_traits<char>::length(value), parsed);
    if (result.ec != std::errc{} || parsed == 0 || parsed > maximum) return fallback;
    return static_cast<std::size_t>(parsed);
}

double envDouble(const char* name, double fallback, double maximum) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return fallback;
    char* end = nullptr;
    const double parsed = std::strtod(value, &end);
    if (end == value || *end != '\0' || !std::isfinite(parsed) || parsed <= 0.0 || parsed > maximum) return fallback;
    return parsed;
}

std::vector<std::string> splitCsv(const char* value) {
    std::vector<std::string> result;
    if (value == nullptr) return result;
    std::string current;
    std::istringstream input(value);
    while (std::getline(input, current, ',')) if (!current.empty()) result.push_back(current);
    return result;
}

bool validClientAddress(const std::string& value) {
    in_addr ipv4{};
    in6_addr ipv6{};
    return ::inet_pton(AF_INET, value.c_str(), &ipv4) == 1 || ::inet_pton(AF_INET6, value.c_str(), &ipv6) == 1;
}

bool validIdentifier(const std::string& value) {
    if (value.empty() || value.size() > 64) return false;
    return std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '-' || c == '_' || c == '.';
    });
}

std::string resolveClientAddress(const live::gateway::http::HttpRequest& request,
                                 const std::vector<std::string>& trusted_proxy_ips) {
    if (std::find(trusted_proxy_ips.begin(), trusted_proxy_ips.end(), request.client_ip) == trusted_proxy_ips.end())
        return request.client_ip;
    const auto forwarded = request.headers.find("x-forwarded-for");
    if (forwarded == request.headers.end()) return request.client_ip;
    const auto comma = forwarded->second.find(',');
    std::string candidate = forwarded->second.substr(0, comma);
    const auto first = candidate.find_first_not_of(" \t");
    const auto last = candidate.find_last_not_of(" \t");
    if (first == std::string::npos) return request.client_ip;
    candidate = candidate.substr(first, last - first + 1);
    return validClientAddress(candidate) ? candidate : request.client_ip;
}

live::common::Status authenticateRequest(const live::gateway::http::HttpRequest& request,
                                         live::business::user::UserService* users,
                                         live::business::user::User* user) {
    if (users == nullptr || user == nullptr) return live::common::Status::Internal("authentication is unavailable");
    const auto authorization = request.headers.find("authorization");
    if (authorization == request.headers.end() || authorization->second.size() <= 7) {
        return live::common::Status::Unauthenticated("Bearer token is required");
    }
    const std::string scheme = authorization->second.substr(0, 6);
    const std::string normalized_scheme = [&] {
        std::string value = scheme;
        for (char& c : value) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return value;
    }();
    if (normalized_scheme != "bearer" || authorization->second[6] != ' ') {
        return live::common::Status::Unauthenticated("Bearer token is required");
    }
    return users->authenticate(authorization->second.substr(7), user);
}

live::gateway::http::HttpResponse unauthorizedResponse(const live::common::Status& status) {
    return jsonResponse(401, "Unauthorized", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
}

live::gateway::http::HttpResponse forbiddenResponse(const std::string& message) {
    return jsonResponse(403, "Forbidden", "{\"error\":\"" + jsonEscape(message) + "\"}");
}

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, requestShutdown);
    std::signal(SIGTERM, requestShutdown);
#ifdef SIGPIPE
    std::signal(SIGPIPE, SIG_IGN);
#endif
    std::uint16_t port = 8080;
    if (argc > 1) port = static_cast<std::uint16_t>(std::strtoul(argv[1], nullptr, 10));
    const char* configured_process_role = std::getenv("ECOMMERCE_PROCESS_ROLE");
    const std::string process_role = configured_process_role == nullptr ? "all" : configured_process_role;
    if (process_role != "all" && process_role != "gateway" && process_role != "worker") {
        std::cerr << "ECOMMERCE_PROCESS_ROLE must be all, gateway or worker\n";
        return 1;
    }
    const bool run_gateway = process_role != "worker";
    const bool run_background_workers = process_role != "gateway";
    const auto demo_stock = static_cast<std::int64_t>(envSize("ECOMMERCE_DEMO_STOCK", 100, 1000000000));
    const auto demo_campaign_limit = envSize("ECOMMERCE_DEMO_CAMPAIGN_LIMIT", 100, 1000000000);

    live::network::EventLoop loop;
    live::storage::LocalKVEngine domain_storage;
    const char* snapshot_path = std::getenv("ECOMMERCE_STORAGE_SNAPSHOT");
    const char* wal_path = std::getenv("ECOMMERCE_STORAGE_WAL");
    const auto storage_status = domain_storage.open(
        snapshot_path == nullptr ? "/tmp/ecommerce-domain.snapshot" : snapshot_path,
        wal_path == nullptr ? "/tmp/ecommerce-domain.wal" : wal_path);
    if (!storage_status.ok()) {
        std::cerr << "failed to open domain storage: " << storage_status.message() << '\n';
        return 1;
    }
    live::storage::SnapshotWorker snapshot_worker(
        &domain_storage, std::chrono::seconds(envSize("ECOMMERCE_SNAPSHOT_INTERVAL_SECONDS", 30, 3600)));
    if (run_gateway && !snapshot_worker.start().ok()) {
        std::cerr << "failed to start snapshot worker\n";
        return 1;
    }
    live::cache::ShardedCache cache(16, 1024);
#ifdef ECOMMERCE_HAS_REDIS
    std::unique_ptr<live::kv::RedisKVStore> redis;
    const char* redis_host = std::getenv("ECOMMERCE_REDIS_HOST");
    if (redis_host != nullptr) {
        redis = std::make_unique<live::kv::RedisKVStore>();
        const auto redis_status = redis->connect(redis_host, 6379, std::chrono::seconds(2),
                                                 envSize("ECOMMERCE_REDIS_POOL_SIZE", 8, 128));
        if (!redis_status.ok()) {
            std::cerr << "Redis configured but unavailable: " << redis_status.message() << '\n';
            return 1;
        }
    }
#endif
    live::kv::IKVStore* shared_store = nullptr;
#ifdef ECOMMERCE_HAS_REDIS
    shared_store = redis.get();
#endif
    live::business::product::ProductService products(&cache,
        shared_store
        , &domain_storage
    );
    if (!products.restore().ok()) {
        std::cerr << "failed to restore products\n";
        return 1;
    }
    live::business::inventory::InventoryService inventory(&domain_storage);
    if (!inventory.restore().ok()) {
        std::cerr << "failed to restore inventory\n";
        return 1;
    }
    if (const auto invariant_status = inventory.validateInvariants(); !invariant_status.ok()) {
        std::cerr << "inventory invariant check failed: " << invariant_status.message() << '\n';
        return 1;
    }
    live::messaging::InMemoryBroker broker;
#ifdef ECOMMERCE_HAS_KAFKA
    std::unique_ptr<live::messaging::KafkaProducer> kafka;
    const char* kafka_brokers = std::getenv("ECOMMERCE_KAFKA_BROKERS");
    if (kafka_brokers != nullptr && *kafka_brokers != '\0') {
        kafka = std::make_unique<live::messaging::KafkaProducer>();
        const auto kafka_status = kafka->start(kafka_brokers, "ecommerce-api");
        if (!kafka_status.ok()) {
            std::cerr << "Kafka configured but unavailable: " << kafka_status.message() << '\n';
            return 1;
        }
    }
#endif
#ifdef ECOMMERCE_HAS_MYSQL
    std::unique_ptr<live::storage::mysql::MySqlTransactionStore> mysql_store;
    const char* mysql_host = std::getenv("ECOMMERCE_MYSQL_HOST");
    const char* mysql_hosts_env = std::getenv("ECOMMERCE_MYSQL_HOSTS");
    const auto mysql_hosts = mysql_hosts_env != nullptr && *mysql_hosts_env != '\0'
        ? splitCsv(mysql_hosts_env) : splitCsv(mysql_host);
    if (!mysql_hosts.empty()) {
        mysql_store = std::make_unique<live::storage::mysql::MySqlTransactionStore>();
        const auto mysql_status = mysql_store->connect(
            mysql_hosts,
            static_cast<std::uint16_t>(envSize("ECOMMERCE_MYSQL_PORT", 3306, 65535)),
            std::getenv("ECOMMERCE_MYSQL_USER") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_USER"),
            std::getenv("ECOMMERCE_MYSQL_PASSWORD") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_PASSWORD"),
            std::getenv("ECOMMERCE_MYSQL_DATABASE") == nullptr ? "ecommerce" : std::getenv("ECOMMERCE_MYSQL_DATABASE"),
            envSize("ECOMMERCE_MYSQL_POOL_SIZE", 4, 64));
        if (!mysql_status.ok()) {
            std::cerr << "MySQL configured but unavailable: " << mysql_status.message() << '\n';
            return 1;
        }
        if (const auto sku_status = mysql_store->initializeSku("sku-1", demo_stock); !sku_status.ok()) {
            std::cerr << "failed to initialize MySQL inventory: " << sku_status.message() << '\n';
            return 1;
        }
        if (const auto product_status = mysql_store->initializeProduct("sku-1", "C++ 高并发实战课程", 19900); !product_status.ok()) {
            std::cerr << "failed to initialize MySQL product: " << product_status.message() << '\n';
            return 1;
        }
    }
#endif
    live::messaging::FileOutbox outbox;
    const char* configured_outbox = std::getenv("ECOMMERCE_OUTBOX_PATH");
    if (!outbox.open(configured_outbox == nullptr ? "/tmp/ecommerce-outbox.bin" : configured_outbox).ok()) {
        std::cerr << "failed to open durable outbox\n";
        return 1;
    }
    live::messaging::OutboxPublisher outbox_publisher(&outbox);
    live::business::order::OrderService orders(&inventory, &outbox_publisher, &domain_storage,
#ifdef ECOMMERCE_HAS_MYSQL
        mysql_store.get()
#else
        nullptr
#endif
    );
    if (!orders.restore().ok()) {
        std::cerr << "failed to restore orders\n";
        return 1;
    }
#ifdef ECOMMERCE_HAS_REDIS
    live::business::user::UserService users(&domain_storage, redis.get());
#else
    live::business::user::UserService users(&domain_storage);
#endif
    if (!users.restore().ok()) {
        std::cerr << "failed to restore users\n";
        return 1;
    }
    std::unique_ptr<live::business::promotion::IAdmissionGate> promotion_gate;
    const char* configured_inventory_mode = std::getenv("ECOMMERCE_INVENTORY_MODE");
    const std::string inventory_mode = configured_inventory_mode == nullptr
#ifdef ECOMMERCE_HAS_REDIS
        ? (redis != nullptr ? "redis" : "local")
#else
        ? "local"
#endif
        : configured_inventory_mode;
    const bool use_redis_flash_sale = inventory_mode == "redis";
#ifdef ECOMMERCE_HAS_REDIS
    if (use_redis_flash_sale && redis != nullptr) promotion_gate = std::make_unique<live::business::promotion::RedisFlashSaleGate>(redis.get());
#endif
    if (use_redis_flash_sale && promotion_gate == nullptr) {
        std::cerr << "ECOMMERCE_INVENTORY_MODE=redis requires a configured Redis connection\n";
        return 1;
    }
    if (promotion_gate == nullptr) promotion_gate = std::make_unique<live::business::promotion::FlashSaleAdmissionGate>();
    live::business::promotion::IPromotionRepository* promotion_repository = nullptr;
#ifdef ECOMMERCE_HAS_MYSQL
    promotion_repository = mysql_store.get();
#endif
    live::business::promotion::PromotionService promotions(std::move(promotion_gate), promotion_repository);
    if (const auto restore_status = promotions.restore(); !restore_status.ok()) {
        std::cerr << "failed to restore promotions: " << restore_status.message() << '\n';
        return 1;
    }
    live::observability::MetricsRegistry metrics;
#ifdef ECOMMERCE_HAS_MYSQL
    if (mysql_store != nullptr) mysql_store->setMetrics(&metrics);
#endif
    const auto http_requests_metric = metrics.counterHandle("http_requests");
    const auto http_response_2xx_metric = metrics.counterHandle("http_responses_2xx");
    const auto http_response_3xx_metric = metrics.counterHandle("http_responses_3xx");
    const auto http_response_4xx_metric = metrics.counterHandle("http_responses_4xx");
    const auto http_response_5xx_metric = metrics.counterHandle("http_responses_5xx");
    const auto http_duration_metric = metrics.histogramHandle("http_request_duration_ms");
    const auto health_requests_metric = metrics.counterHandle("http_health_requests");
    const auto product_queries_metric = metrics.counterHandle("product_query_requests");
    const auto active_connections_metric = metrics.gaugeHandle("http_active_connections");
    const auto http_worker_queue_metric = metrics.gaugeHandle("http_worker_queue_depth");
    const auto flash_queue_depth_metric = metrics.gaugeHandle("flash_sale_queue_depth");
    const auto flash_queue_depth_known_metric = metrics.gaugeHandle("flash_sale_queue_depth_known");
    const auto flash_processed_metric = metrics.gaugeHandle("flash_sale_consumer_processed");
    const auto flash_rejected_metric = metrics.gaugeHandle("flash_sale_consumer_rejected");
    const auto flash_dead_lettered_metric = metrics.gaugeHandle("flash_sale_consumer_dead_lettered");
    live::observability::StructuredLogger request_logger(&std::clog);
    const double flash_sale_rate = envDouble("ECOMMERCE_FLASH_SALE_RATE_PER_SECOND", 10000.0, 1000000000.0);
    const double flash_sale_burst = envDouble("ECOMMERCE_FLASH_SALE_BURST", 2000.0, 1000000000.0);
    const double flash_sale_user_rate = envDouble("ECOMMERCE_FLASH_SALE_USER_RATE_PER_SECOND", 2.0, 1000000.0);
    const double flash_sale_user_burst = envDouble("ECOMMERCE_FLASH_SALE_USER_BURST", 5.0, 1000000.0);
    const double flash_sale_campaign_rate = envDouble("ECOMMERCE_FLASH_SALE_CAMPAIGN_RATE_PER_SECOND", 10000.0, 1000000000.0);
    const double flash_sale_campaign_burst = envDouble("ECOMMERCE_FLASH_SALE_CAMPAIGN_BURST", 2000.0, 1000000000.0);
    const double flash_sale_ip_rate = envDouble("ECOMMERCE_FLASH_SALE_IP_RATE_PER_SECOND", 200.0, 1000000000.0);
    const double flash_sale_ip_burst = envDouble("ECOMMERCE_FLASH_SALE_IP_BURST", 500.0, 1000000000.0);
    const std::size_t flash_sale_waiting_capacity = envSize("ECOMMERCE_FLASH_SALE_WAITING_CAPACITY", 100000, 10000000);
    const char* waiting_room_flag = std::getenv("ECOMMERCE_FLASH_SALE_WAITING_ROOM");
    const bool waiting_room_enabled = waiting_room_flag == nullptr ||
        (std::string_view(waiting_room_flag) != "0" && std::string_view(waiting_room_flag) != "false");
#ifndef ECOMMERCE_HAS_REDIS
    (void)flash_sale_user_rate;
    (void)flash_sale_user_burst;
    (void)flash_sale_campaign_rate;
    (void)flash_sale_campaign_burst;
    (void)flash_sale_ip_rate;
    (void)flash_sale_ip_burst;
    (void)flash_sale_waiting_capacity;
    (void)waiting_room_enabled;
#endif
    const auto trusted_proxy_ips = splitCsv(std::getenv("ECOMMERCE_TRUSTED_PROXY_IPS"));
    live::traffic::TokenBucket flash_sale_limiter(flash_sale_rate, flash_sale_burst);
#ifdef ECOMMERCE_HAS_REDIS
    std::unique_ptr<live::traffic::RedisTokenBucket> shared_flash_sale_limiter;
    if (redis != nullptr) shared_flash_sale_limiter = std::make_unique<live::traffic::RedisTokenBucket>(
        redis.get(), "flashsale:{global}:entry_rate", flash_sale_rate, flash_sale_burst);
#endif
    const auto admin_credentials = loadAdminCredentials();
    std::mutex campaign_allocation_mutex;
    const auto adminAuthResponse = [](int code) {
        if (code == 503) return jsonResponse(503, "Service Unavailable", "{\"error\":\"admin credentials are not configured\"}");
        if (code == 403) return jsonResponse(403, "Forbidden", "{\"error\":\"admin role is insufficient\"}");
        return jsonResponse(401, "Unauthorized", "{\"error\":\"invalid admin credentials\"}");
    };

    if (run_gateway) {
        live::business::product::Product existing_product;
        if (products.getProduct("sku-1", &existing_product).code() == live::common::ErrorCode::kNotFound) {
            products.addProduct({"sku-1", "C++ 高并发实战课程", 19900, true});
        }
        live::business::inventory::StockSnapshot existing_stock;
        if (inventory.queryStock("sku-1", &existing_stock).code() == live::common::ErrorCode::kNotFound) inventory.addSku("sku-1", 100);
        live::business::promotion::Promotion default_promotion;
        const auto default_promotion_status = promotions.get("promo-1", &default_promotion);
        if (default_promotion_status.code() == live::common::ErrorCode::kNotFound) {
            const auto create_status = promotions.create({"promo-1", "sku-1", demo_campaign_limit});
            if (create_status.ok()) {
                if (!promotions.preheat("promo-1").ok() || !promotions.start("promo-1").ok()) {
                    std::cerr << "failed to initialize default promotion\n";
                    return 1;
                }
            } else if (create_status.code() == live::common::ErrorCode::kAlreadyExists) {
                // Another gateway may win the shared-DB seed race after this instance restored.
                if (!promotions.restore().ok() || !promotions.get("promo-1", &default_promotion).ok()) {
                    std::cerr << "failed to reload concurrently initialized promotion\n";
                    return 1;
                }
            } else {
                std::cerr << "failed to initialize default promotion: " << create_status.message() << '\n';
                return 1;
            }
        } else if (!default_promotion_status.ok()) {
            std::cerr << "failed to load default promotion: " << default_promotion_status.message() << '\n';
            return 1;
        }
    }

    std::atomic<bool> campaign_refresh_stop{false};
    std::thread campaign_refresh_thread;

    std::unique_ptr<live::business::inventory::IInventoryGateway> flash_inventory_gateway;
    std::unique_ptr<live::messaging::IOrderQueue> flash_order_queue;
    std::unique_ptr<live::messaging::IOrderQueue> flash_dead_letter_queue;
#ifdef ECOMMERCE_HAS_ROCKETMQ
    live::messaging::RocketMqOrderQueue* rocketmq_publisher = nullptr;
#endif
    const char* configured_flash_queue = std::getenv("ECOMMERCE_FLASH_SALE_QUEUE");
    const std::size_t flash_consumer_workers = envSize("ECOMMERCE_FLASH_CONSUMER_WORKERS", 4, 64);
    const std::string flash_queue_mode = configured_flash_queue == nullptr
        ? (use_redis_flash_sale ? "redis" : "local")
        : configured_flash_queue;
    const bool use_redis_flash_queue = flash_queue_mode == "redis";
    std::size_t effective_flash_consumer_workers = flash_consumer_workers;
    if (flash_queue_mode != "redis" && flash_queue_mode != "local" && flash_queue_mode != "rocketmq") {
        std::cerr << "ECOMMERCE_FLASH_SALE_QUEUE must be redis, rocketmq or local\n";
        return 1;
    }
    if (process_role != "all") {
        if (flash_queue_mode == "local") {
            std::cerr << "split gateway/worker roles require a shared Redis or RocketMQ command queue\n";
            return 1;
        }
#ifndef ECOMMERCE_HAS_REDIS
        std::cerr << "split gateway/worker roles require the Redis adapter for shared request state and admission\n";
        return 1;
#else
        if (redis == nullptr) {
            std::cerr << "split gateway/worker roles require a configured Redis endpoint\n";
            return 1;
        }
#ifndef ECOMMERCE_HAS_MYSQL
        std::cerr << "split gateway/worker roles require the MySQL adapter for shared order persistence\n";
        return 1;
#else
        if (mysql_store == nullptr) {
            std::cerr << "split gateway/worker roles require a configured MySQL endpoint\n";
            return 1;
        }
#endif
#endif
    }
    if (flash_queue_mode == "rocketmq") {
        effective_flash_consumer_workers = 1;  // SimpleConsumer polling is serialized per client instance.
#ifndef ECOMMERCE_HAS_ROCKETMQ
        std::cerr << "ECOMMERCE_FLASH_SALE_QUEUE=rocketmq requires the Apache RocketMQ C++ SDK at build time\n";
        return 1;
#endif
    }
    if (use_redis_flash_sale) {
#ifdef ECOMMERCE_HAS_REDIS
        auto redis_inventory = std::make_unique<live::business::inventory::RedisInventoryGateway>(redis.get());
        if (const auto status = redis_inventory->initializeSku("sku-1", demo_stock); !status.ok()) {
            std::cerr << "failed to initialize Redis flash-sale inventory: " << status.message() << '\n';
            return 1;
        }
        flash_inventory_gateway = std::move(redis_inventory);
 #else
        std::cerr << "Redis inventory mode requires the hiredis adapter at build time\n";
        return 1;
 #endif
    } else {
        flash_inventory_gateway = std::make_unique<live::business::inventory::LocalInventoryGateway>(&inventory);
    }
    if (use_redis_flash_queue) {
#ifdef ECOMMERCE_HAS_REDIS
        if (!use_redis_flash_sale || redis == nullptr) {
            std::cerr << "Redis queue requires Redis inventory mode and connection\n";
            return 1;
        }
        auto redis_order_queue = std::make_unique<live::kv::RedisOrderQueue>(redis.get(), "flash_orders:{promo-1}");
        if (run_background_workers) {
            if (const auto status = redis_order_queue->connectBlockingConsumer(
                    redis_host, 6379, std::chrono::seconds(2), effective_flash_consumer_workers); !status.ok()) {
                std::cerr << "failed to create dedicated Redis queue consumer connection: " << status.message() << '\n';
                return 1;
            }
        }
        flash_order_queue = std::move(redis_order_queue);
        flash_dead_letter_queue = std::make_unique<live::kv::RedisOrderQueue>(redis.get(), "flash_orders_dlq:{promo-1}");
 #else
        std::cerr << "Redis queue requires the hiredis adapter at build time\n";
        return 1;
 #endif
    } else if (flash_queue_mode == "rocketmq") {
#ifdef ECOMMERCE_HAS_ROCKETMQ
        const char* endpoint = std::getenv("ECOMMERCE_ROCKETMQ_ENDPOINT");
        const char* command_topic = std::getenv("ECOMMERCE_ROCKETMQ_COMMAND_TOPIC");
        const char* consumer_group = std::getenv("ECOMMERCE_ROCKETMQ_CONSUMER_GROUP");
        auto rocketmq_queue = std::make_unique<live::messaging::RocketMqOrderQueue>();
        const auto status = rocketmq_queue->start(endpoint == nullptr ? "" : endpoint,
            command_topic == nullptr ? "flashsale.command" : command_topic,
            consumer_group == nullptr ? "flashsale-command-consumers" : consumer_group,
            run_background_workers, run_gateway);
        if (!status.ok()) {
            std::cerr << "failed to start RocketMQ command transport: " << status.message() << '\n';
            return 1;
        }
        rocketmq_publisher = rocketmq_queue.get();
        flash_order_queue = std::move(rocketmq_queue);
        auto rocketmq_dead_letter_queue = std::make_unique<live::messaging::RocketMqOrderQueue>();
        const char* dlq_topic = std::getenv("ECOMMERCE_ROCKETMQ_DLQ_TOPIC");
        const std::string resolved_dlq_topic = dlq_topic == nullptr || *dlq_topic == '\0'
            ? std::string(command_topic == nullptr ? "flashsale.command" : command_topic) + ".dlq"
            : dlq_topic;
        const std::string dlq_group = std::string(consumer_group == nullptr
            ? "flashsale-command-consumers" : consumer_group) + "-app-dlq-writer";
        const auto dlq_status = rocketmq_dead_letter_queue->start(endpoint == nullptr ? "" : endpoint,
            resolved_dlq_topic, dlq_group, false, false);
        if (!dlq_status.ok()) {
            std::cerr << "failed to start RocketMQ application DLQ producer: " << dlq_status.message() << '\n';
            return 1;
        }
        flash_dead_letter_queue = std::move(rocketmq_dead_letter_queue);
#endif
    } else {
        flash_order_queue = std::make_unique<live::messaging::InMemoryOrderQueue>();
        flash_dead_letter_queue = std::make_unique<live::messaging::InMemoryOrderQueue>();
    }
    live::business::flash_sale::LocalFlashSaleOrderRepository local_flash_repo(&orders);
    live::business::flash_sale::IFlashSaleOrderRepository* flash_repo = &local_flash_repo;
#ifdef ECOMMERCE_HAS_MYSQL
    if (mysql_store != nullptr) flash_repo = mysql_store.get();
#endif
    std::unique_ptr<live::business::flash_sale::IFlashSaleRequestStatusStore> owned_flash_status_primary;
    std::unique_ptr<live::business::flash_sale::IFlashSaleRequestStatusStore> owned_flash_status_tiered;
    std::unique_ptr<live::business::flash_sale::IFlashSaleRequestStatusStore> owned_flash_status_reader;
    live::business::flash_sale::IFlashSaleRequestStatusStore* flash_request_status_store = nullptr;
    live::business::flash_sale::IFlashSaleRequestStatusStore* flash_ingress_status_store = nullptr;
#ifdef ECOMMERCE_HAS_MYSQL
    if (mysql_store != nullptr) {
#ifdef ECOMMERCE_HAS_REDIS
        if (redis != nullptr) {
            owned_flash_status_primary = std::make_unique<live::business::flash_sale::KvFlashSaleRequestStatusStore>(redis.get());
            owned_flash_status_reader = std::make_unique<live::business::flash_sale::ReadThroughFlashSaleRequestStatusStore>(
                owned_flash_status_primary.get(), mysql_store.get());
            flash_ingress_status_store = owned_flash_status_reader.get();
            owned_flash_status_tiered = std::make_unique<live::business::flash_sale::TieredFlashSaleRequestStatusStore>(
                owned_flash_status_primary.get(), mysql_store.get());
            flash_request_status_store = owned_flash_status_tiered.get();
        } else
#endif
        {
            flash_request_status_store = mysql_store.get();
            flash_ingress_status_store = mysql_store.get();
        }
    }
#endif
#ifdef ECOMMERCE_HAS_REDIS
    if (flash_request_status_store == nullptr && redis != nullptr) {
        owned_flash_status_primary = std::make_unique<live::business::flash_sale::KvFlashSaleRequestStatusStore>(redis.get());
        flash_request_status_store = owned_flash_status_primary.get();
        flash_ingress_status_store = owned_flash_status_primary.get();
    }
#endif
    if (flash_request_status_store == nullptr) {
        owned_flash_status_primary = std::make_unique<live::business::flash_sale::InMemoryFlashSaleRequestStatusStore>();
        flash_request_status_store = owned_flash_status_primary.get();
        flash_ingress_status_store = owned_flash_status_primary.get();
    }
    std::unique_ptr<live::business::flash_sale::FlashSaleOrderConsumer> flash_consumer;
    flash_consumer = std::make_unique<live::business::flash_sale::FlashSaleOrderConsumer>(
        flash_order_queue.get(), flash_dead_letter_queue.get(), flash_repo, flash_inventory_gateway.get(),
        promotions.admissionGate(), 5, effective_flash_consumer_workers, flash_request_status_store);
    if (run_background_workers && !flash_consumer->start().ok()) {
        std::cerr << "failed to start flash-sale consumer\n";
        return 1;
    }
    auto flash_order_service = std::make_unique<live::business::flash_sale::FlashSaleOrderService>(
        promotions.admissionGate(), flash_order_queue.get(), flash_ingress_status_store, &promotions);

    std::atomic<bool> waiting_room_stop{false};
    std::vector<std::thread> waiting_room_workers;
#ifdef ECOMMERCE_HAS_REDIS
    if (run_background_workers && redis != nullptr && waiting_room_enabled && flash_queue_mode != "local") {
        const auto worker_count = envSize("ECOMMERCE_FLASH_SALE_WAITING_WORKERS", 4, 64);
        waiting_room_workers.reserve(worker_count);
        const auto worker_epoch = std::chrono::steady_clock::now().time_since_epoch().count();
        for (std::size_t worker_index = 0; worker_index < worker_count; ++worker_index) {
            waiting_room_workers.emplace_back([&, worker_index] {
                const std::string lease_owner = "waiting-" + std::to_string(worker_epoch) + "-" + std::to_string(worker_index);
                std::vector<live::business::promotion::Promotion> campaigns;
                auto next_campaign_refresh = std::chrono::steady_clock::time_point::min();
                while (!waiting_room_stop.load(std::memory_order_relaxed)) {
                    const auto now = std::chrono::steady_clock::now();
                    if (now >= next_campaign_refresh) {
                        const auto list_status = promotions.list(&campaigns);
                        next_campaign_refresh = now + std::chrono::seconds(2);
                        if (!list_status.ok()) {
                            std::clog << "waiting-room campaign scan failed: " << list_status.message() << '\n';
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                            continue;
                        }
                    }
                    bool made_progress = false;
                    for (const auto& campaign : campaigns) {
                        if (waiting_room_stop.load(std::memory_order_relaxed)) break;
                        live::kv::RedisFlashSaleWaitingRoom room(redis.get(), campaign.id, flash_sale_waiting_capacity);
                        std::string ticket;
                        live::messaging::FlashSaleOrderMessage message;
                        const auto claim_status = room.claim(lease_owner, std::chrono::seconds(30), &ticket, &message);
                        if (!claim_status.ok()) {
                            if (!claim_status.isIncomplete()) std::clog << "waiting-room claim failed: " << claim_status.message() << '\n';
                            continue;
                        }
                        bool allowed = false;
                        live::traffic::RedisTokenBucket global_limiter(redis.get(), "flashsale:{global}:entry_rate",
                            flash_sale_rate, flash_sale_burst);
                        auto gate_status = global_limiter.tryAcquire(&allowed);
                        if (gate_status.ok() && allowed) {
                            live::traffic::RedisTokenBucket campaign_limiter(redis.get(),
                                "flashsale:{" + campaign.id + "}:entry_rate", flash_sale_campaign_rate,
                                flash_sale_campaign_burst);
                            gate_status = campaign_limiter.tryAcquire(&allowed);
                        }
                        if (!gate_status.ok() || !allowed) {
                            bool released = false;
                            (void)room.release(ticket, lease_owner, &released);
                            if (!gate_status.ok()) std::clog << "waiting-room rate gate failed: " << gate_status.message() << '\n';
                            continue;
                        }
                        live::business::flash_sale::FlashSaleSubmitRequest submit;
                        submit.promotion_id = message.promotion_id;
                        submit.sku_id = message.sku_id;
                        submit.user_id = message.user_id;
                        submit.idempotency_key = message.idempotency_key;
                        submit.quantity = message.quantity;
                        live::business::flash_sale::FlashSaleSubmitResult result;
                        const auto submit_status = flash_order_service->submit(submit, &result);
                        if (!submit_status.ok()) {
                            live::business::flash_sale::FlashSaleRequestStatus request_status;
                            const bool permanent_error = submit_status.code() == live::common::ErrorCode::kInvalidArgument ||
                                submit_status.code() == live::common::ErrorCode::kNotFound ||
                                submit_status.code() == live::common::ErrorCode::kFailedPrecondition;
                            const auto request_lookup = flash_request_status_store->get(ticket, &request_status);
                            if (permanent_error && request_lookup.ok() &&
                                request_status.state == live::business::flash_sale::FlashSaleRequestState::kWaiting) {
                                const auto mark_status = flash_request_status_store->put({ticket, message.user_id, "",
                                    live::business::flash_sale::FlashSaleRequestState::kRejected,
                                    submit_status.message(), message.promotion_id});
                                if (mark_status.ok()) {
                                    bool completed = false;
                                    if (room.complete(ticket, lease_owner, &completed).ok() && completed) made_progress = true;
                                } else {
                                    bool released = false;
                                    (void)room.release(ticket, lease_owner, &released);
                                }
                            } else if (request_lookup.ok() &&
                                       request_status.state != live::business::flash_sale::FlashSaleRequestState::kWaiting) {
                                bool completed = false;
                                if (room.complete(ticket, lease_owner, &completed).ok() && completed) made_progress = true;
                            } else {
                                bool released = false;
                                (void)room.release(ticket, lease_owner, &released);
                            }
                            continue;
                        }
                        bool wait = result.code == live::business::flash_sale::SubmitCode::kNotStarted;
                        if (result.code == live::business::flash_sale::SubmitCode::kRejected) {
                            live::business::promotion::Promotion current_campaign;
                            if (promotions.get(message.promotion_id, &current_campaign).ok() &&
                                (current_campaign.state == live::business::promotion::PromotionState::kCreated ||
                                 current_campaign.state == live::business::promotion::PromotionState::kPreheating ||
                                 current_campaign.state == live::business::promotion::PromotionState::kPaused)) wait = true;
                        }
                        if (wait) {
                            bool released = false;
                            (void)room.release(ticket, lease_owner, &released);
                            continue;
                        }
                        if (result.code == live::business::flash_sale::SubmitCode::kDuplicate) {
                            live::business::flash_sale::FlashSaleRequestStatus current_status;
                            const auto status_lookup = flash_request_status_store->get(ticket, &current_status);
                            if (status_lookup.ok() && current_status.state == live::business::flash_sale::FlashSaleRequestState::kWaiting) {
                                const auto mark_status = flash_request_status_store->put({ticket, message.user_id, result.order_id,
                                    live::business::flash_sale::FlashSaleRequestState::kRejected,
                                    result.message.empty() ? "user was already admitted to this campaign" : result.message,
                                    message.promotion_id});
                                if (!mark_status.ok()) {
                                    bool released = false;
                                    (void)room.release(ticket, lease_owner, &released);
                                    continue;
                                }
                            }
                        } else if (result.code != live::business::flash_sale::SubmitCode::kQueued) {
                            const auto state = result.code == live::business::flash_sale::SubmitCode::kExhausted
                                ? live::business::flash_sale::FlashSaleRequestState::kSoldOut
                                : live::business::flash_sale::FlashSaleRequestState::kRejected;
                            const auto mark_status = flash_request_status_store->put({ticket, message.user_id, result.order_id, state,
                                result.message, message.promotion_id});
                            if (!mark_status.ok()) {
                                bool released = false;
                                (void)room.release(ticket, lease_owner, &released);
                                continue;
                            }
                        }
                        bool completed = false;
                        const auto complete_status = room.complete(ticket, lease_owner, &completed);
                        if (!complete_status.ok()) std::clog << "waiting-room completion failed: " << complete_status.message() << '\n';
                        if (complete_status.ok() && completed) made_progress = true;
                    }
                    if (!made_progress) std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            });
        }
    }
#endif

    std::atomic<bool> expiry_worker_stop{false};
    std::thread expiry_worker;
#ifdef ECOMMERCE_HAS_MYSQL
    if (run_background_workers && mysql_store != nullptr) {
        const auto expiry_worker_id = "expiry-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        expiry_worker = std::thread([&, expiry_worker_id] {
            while (!expiry_worker_stop.load(std::memory_order_relaxed)) {
                std::vector<live::business::order::Order> expired;
                const auto scan = mysql_store->claimExpiredOrders(expiry_worker_id, 100, &expired);
                if (!scan.ok()) {
                    std::clog << "expired order scan failed: " << scan.message() << '\n';
                } else {
                    for (const auto& candidate : expired) {
                        live::business::order::Order closed;
                        const auto transition = orders.transitionOrder(candidate.id,
                            live::business::order::OrderState::kPendingPayment,
                            live::business::order::OrderState::kExpired, &closed);
                        if (!transition.ok()) continue;  // Payment/cancel won the state race.
                        (void)flash_request_status_store->put({candidate.idempotency_key, candidate.user_id,
                            candidate.id, live::business::flash_sale::FlashSaleRequestState::kExpired,
                            "payment window expired; inventory released"});
                        live::business::inventory::InventoryReservationResult released;
                        const auto release_status = flash_inventory_gateway->release(candidate.sku_id, candidate.id, &released);
                        if (!release_status.ok()) {
                            std::clog << "expired order inventory release pending for " << candidate.id << ": "
                                      << release_status.message() << '\n';
                        }
                    }
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        });
    }
#endif

    std::unique_ptr<live::business::inventory::InventoryReconciler> inventory_reconciler;
#ifdef ECOMMERCE_HAS_MYSQL
    if (mysql_store != nullptr) {
        inventory_reconciler = std::make_unique<live::business::inventory::InventoryReconciler>(
            mysql_store.get(), flash_inventory_gateway.get());
    }
#endif

    auto publishEvent = [&](const live::messaging::Event& event) {
#ifdef ECOMMERCE_HAS_ROCKETMQ
        if (rocketmq_publisher != nullptr) return rocketmq_publisher->publish(event);
#endif
#ifdef ECOMMERCE_HAS_KAFKA
        if (kafka != nullptr) return kafka->publish(event);
#endif
        return broker.publish(event);
    };
#ifdef ECOMMERCE_HAS_MYSQL
    std::atomic<bool> mysql_outbox_stop{false};
    std::thread mysql_outbox_thread;
    if (run_background_workers && mysql_store != nullptr) {
        mysql_outbox_thread = std::thread([&] {
            while (!mysql_outbox_stop.load(std::memory_order_relaxed)) {
                (void)mysql_store->drainOutbox(publishEvent);
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
        });
    }
#endif
    // Replay records left by a previous process before serving new traffic.
    if (run_background_workers) {
        if (const auto replay_status = outbox.drain(publishEvent); !replay_status.ok()) {
            std::cerr << "outbox replay is pending: " << replay_status.message() << '\n';
        }
    }
    live::messaging::AsyncOutboxDispatcher outbox_dispatcher(&outbox, publishEvent);
    if (run_background_workers && !outbox_dispatcher.start().ok()) {
        std::cerr << "failed to start outbox dispatcher\n";
        return 1;
    }
#ifdef ECOMMERCE_HAS_KAFKA
    std::unique_ptr<live::messaging::KafkaConsumer> notification_consumer;
    std::unique_ptr<live::messaging::KafkaProducer> notification_publisher;
    std::atomic<bool> notification_worker_stop{false};
    std::thread notification_worker;
    if (run_background_workers && kafka_brokers != nullptr && kafka != nullptr) {
        notification_consumer = std::make_unique<live::messaging::KafkaConsumer>();
        notification_publisher = std::make_unique<live::messaging::KafkaProducer>();
        const auto topic = std::getenv("ECOMMERCE_NOTIFICATION_TOPIC") == nullptr
            ? std::string("notification.events") : std::string(std::getenv("ECOMMERCE_NOTIFICATION_TOPIC"));
        const char* group = std::getenv("ECOMMERCE_NOTIFICATION_GROUP");
        const auto consumer_status = notification_consumer->start(kafka_brokers,
            group == nullptr ? "ecommerce-notification-fanout" : group, {"order.events"});
        const auto publisher_status = notification_publisher->start(kafka_brokers, "ecommerce-notification-publisher");
        if (!consumer_status.ok() || !publisher_status.ok()) {
            std::clog << "notification fanout is disabled: " <<
                (!consumer_status.ok() ? consumer_status.message() : publisher_status.message()) << '\n';
            notification_consumer.reset();
            notification_publisher.reset();
        } else {
            notification_worker = std::thread([&] {
                while (!notification_worker_stop.load(std::memory_order_relaxed)) {
                    const auto poll_status = notification_consumer->poll(250, [&](const live::messaging::Event& event) {
                        const std::string prefix = "OrderCreated|";
                        if (event.payload.rfind(prefix, 0) != 0) return live::common::Status::Ok();
                        const auto order_end = event.payload.find('|', prefix.size());
                        if (order_end == std::string::npos) return live::common::Status::Ok();
                        const auto user_end = event.payload.find('|', order_end + 1);
                        if (user_end == std::string::npos) return live::common::Status::Ok();
                        const auto order_id = event.payload.substr(prefix.size(), order_end - prefix.size());
                        const auto user_id = event.payload.substr(order_end + 1, user_end - order_end - 1);
                        live::messaging::Event notification;
                        notification.event_id = "order-confirmation-" + order_id;
                        notification.topic = topic;
                        notification.key = order_id;
                        notification.payload = "OrderConfirmation|" + order_id + "|" + user_id;
                        return notification_publisher->publish(notification);
                    });
                    if (!poll_status.ok()) {
                        std::clog << "notification fanout poll failed: " << poll_status.message() << '\n';
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                }
            });
        }
    }
#endif

    const std::size_t http_max_connections = envSize("ECOMMERCE_HTTP_MAX_CONNECTIONS", 10000, 100000);
    const std::size_t http_worker_count = envSize("ECOMMERCE_HTTP_WORKERS", 8, 256);
    const std::size_t http_queue_capacity = envSize("ECOMMERCE_HTTP_QUEUE_CAPACITY", 4096, 100000);
    live::gateway::http::HttpServer server(&loop, "0.0.0.0", port, http_max_connections,
        [http_requests_metric, http_response_2xx_metric, http_response_3xx_metric,
         http_response_4xx_metric, http_response_5xx_metric, http_duration_metric,
         &request_logger](const live::gateway::http::HttpRequest& request,
                   const live::gateway::http::HttpResponse& response,
                   std::uint64_t elapsed_us) {
            http_requests_metric.add();
            switch (response.status_code / 100) {
                case 2: http_response_2xx_metric.add(); break;
                case 3: http_response_3xx_metric.add(); break;
                case 4: http_response_4xx_metric.add(); break;
                case 5: http_response_5xx_metric.add(); break;
                default: break;
            }
            http_duration_metric.observe(static_cast<double>(elapsed_us) / 1000.0);
            if (response.status_code >= 400) request_logger.logHttpError(request, response, elapsed_us);
        }, http_worker_count, http_queue_capacity);
    server.router().addRoute("POST", "/users/register", [&](const auto& request) {
        live::business::user::User user;
        const auto status = users.registerUser(formValue(request.body, "username"), formValue(request.body, "password"),
                                               live::business::user::UserRole::kCustomer, &user);
        if (!status.ok()) return jsonResponse(409, "Conflict", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(201, "Created", "{\"user_id\":\"" + jsonEscape(user.id) + "\"}");
    });
    server.router().addRoute("POST", "/users/login", [&](const auto& request) {
        live::business::user::Session session;
        const auto status = users.login(formValue(request.body, "username"), formValue(request.body, "password"), &session);
        if (!status.ok()) return jsonResponse(401, "Unauthorized", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(200, "OK", "{\"token\":\"" + jsonEscape(session.token) + "\",\"user_id\":\"" + jsonEscape(session.user.id) +
            "\",\"expires_at\":" + std::to_string(session.expires_at_unix_seconds) + "}");
    });
    server.router().addRoute("POST", "/users/logout", [&](const auto& request) {
        const auto authorization = request.headers.find("authorization");
        if (authorization == request.headers.end() || authorization->second.size() <= 7) {
            return jsonResponse(401, "Unauthorized", "{\"error\":\"Bearer token is required\"}");
        }
        const auto status = users.logout(authorization->second.substr(7));
        if (!status.ok() && status.code() != live::common::ErrorCode::kNotFound) {
            return jsonResponse(401, "Unauthorized", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        }
        return jsonResponse(204, "No Content", "");
    });
    server.router().addRoute("GET", "/health", [&](const auto&) {
        health_requests_metric.add();
        return jsonResponse(200, "OK", "{\"status\":\"ok\"}");
    });
    server.router().addRoute("GET", "/ready", [&](const auto&) {
        health_requests_metric.add();
        std::vector<std::string> unavailable;
#ifdef ECOMMERCE_HAS_REDIS
        if (redis != nullptr && !redis->ping().ok()) unavailable.emplace_back("redis");
#endif
#ifdef ECOMMERCE_HAS_MYSQL
        if (mysql_store != nullptr && !mysql_store->ping().ok()) unavailable.emplace_back("mysql");
#endif
        if (!unavailable.empty()) {
            std::ostringstream body;
            body << "{\"status\":\"not_ready\",\"unavailable\":[";
            for (std::size_t i = 0; i < unavailable.size(); ++i) {
                if (i != 0) body << ',';
                body << '"' << unavailable[i] << '"';
            }
            body << "]}";
            return jsonResponse(503, "Service Unavailable", body.str());
        }
        return jsonResponse(200, "OK", "{\"status\":\"ready\"}");
    });
    server.router().addRoute("GET", "/products/sku-1", [&](const auto&) {
        live::business::product::Product product;
        if (!products.getProduct("sku-1", &product).ok()) return jsonResponse(404, "Not Found", "{\"error\":\"product not found\"}");
        std::ostringstream body;
        body << "{\"id\":\"" << jsonEscape(product.id) << "\",\"name\":\"" << jsonEscape(product.name)
             << "\",\"price_cents\":" << product.price_cents << ",\"on_sale\":" << (product.on_sale ? "true" : "false") << "}";
        product_queries_metric.add();
        return jsonResponse(200, "OK", body.str());
    });
    server.router().addRoute("GET", "/stock/sku-1", [&](const auto&) {
        live::business::inventory::StockSnapshot stock;
        const auto stock_status =
#ifdef ECOMMERCE_HAS_MYSQL
            mysql_store != nullptr ? mysql_store->queryStock("sku-1", &stock) : flash_inventory_gateway->queryStock("sku-1", &stock);
#else
            flash_inventory_gateway->queryStock("sku-1", &stock);
#endif
        if (!stock_status.ok()) {
            if (stock_status.code() == live::common::ErrorCode::kNotFound) {
                return jsonResponse(404, "Not Found", "{\"error\":\"sku not found\"}");
            }
            return jsonResponse(503, "Service Unavailable", "{\"error\":\"inventory dependency unavailable\"}");
        }
        std::ostringstream body;
        body << "{\"available\":" << stock.available << ",\"reserved\":" << stock.reserved << ",\"sold\":" << stock.sold << "}";
        return jsonResponse(200, "OK", body.str());
    });
    server.router().addRoute("POST", "/flash-sale/orders", [&](const auto& request) {
        live::business::user::User current;
        if (const auto auth = authenticateRequest(request, &users, &current); !auth.ok()) return unauthorizedResponse(auth);
        const auto requested_campaign = formValue(request.body, "promotion_id");
        const std::string campaign_id = requested_campaign.empty() ? "promo-1" : requested_campaign;
        live::business::flash_sale::FlashSaleSubmitRequest submit;
        submit.promotion_id = campaign_id;
        submit.sku_id = formValue(request.body, "sku_id");
        submit.idempotency_key = formValue(request.body, "idempotency_key");
        if (submit.sku_id.empty()) submit.sku_id = "sku-1";
        submit.user_id = current.id;
        const auto quantity_text = formValue(request.body, "quantity");
        if (!quantity_text.empty() && !parseInt64(quantity_text, &submit.quantity)) {
            return jsonResponse(400, "Bad Request", "{\"error\":\"invalid quantity\"}");
        }
        if (submit.idempotency_key.empty()) return jsonResponse(400, "Bad Request", "{\"error\":\"idempotency_key is required\"}");
        if (submit.quantity != 1 || submit.promotion_id.size() > 64 || submit.sku_id.size() > 64 ||
            submit.idempotency_key.size() > 128 || !validIdentifier(submit.promotion_id) || !validIdentifier(submit.sku_id))
            return jsonResponse(400, "Bad Request", "{\"error\":\"invalid flash-sale request fields\"}");
        const auto client_ip = resolveClientAddress(request, trusted_proxy_ips);
#ifdef ECOMMERCE_HAS_REDIS
        if (shared_flash_sale_limiter != nullptr) {
            if (client_ip.empty()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"client address is unavailable\"}");
            live::traffic::RedisTokenBucket ip_limiter(redis.get(), "flashsale:ip-rate:" + client_ip,
                                                        flash_sale_ip_rate, flash_sale_ip_burst);
            bool ip_allowed = false;
            const auto ip_limit_status = ip_limiter.tryAcquire(&ip_allowed);
            if (!ip_limit_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"distributed IP limiter unavailable\"}");
            if (!ip_allowed) return jsonResponse(429, "Too Many Requests", "{\"error\":\"client IP rate limited\"}");
            live::traffic::RedisTokenBucket user_limiter(redis.get(), "flashsale:user-rate:" + current.id,
                                                         flash_sale_user_rate, flash_sale_user_burst);
            bool user_allowed = false;
            const auto user_limit_status = user_limiter.tryAcquire(&user_allowed);
            if (!user_limit_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"distributed user limiter unavailable\"}");
            if (!user_allowed) return jsonResponse(429, "Too Many Requests", "{\"error\":\"user rate limited\"}");
        }
#endif
        live::business::flash_sale::FlashSaleRequestStatus previous_request;
        const auto previous_status = flash_ingress_status_store->get(submit.idempotency_key, &previous_request);
        if (previous_status.ok()) {
            if (previous_request.user_id != current.id) return forbiddenResponse("idempotency key belongs to another user");
            if (previous_request.state == live::business::flash_sale::FlashSaleRequestState::kWaiting) {
                if (previous_request.promotion_id != campaign_id) return jsonResponse(409, "Conflict", "{\"error\":\"waiting-room ticket belongs to another campaign\"}");
#ifdef ECOMMERCE_HAS_REDIS
                std::size_t queue_position = 0;
                live::kv::RedisFlashSaleWaitingRoom room(redis.get(), campaign_id, flash_sale_waiting_capacity);
                const auto position_status = room.position(submit.idempotency_key, &queue_position);
                if (!position_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"waiting-room status unavailable\"}");
                return jsonResponse(202, "Accepted", "{\"request_id\":\"" + jsonEscape(submit.idempotency_key) +
                    "\",\"state\":\"WAITING\",\"queue_position\":" + std::to_string(queue_position) + "}");
#else
                return jsonResponse(503, "Service Unavailable", "{\"error\":\"waiting room requires Redis\"}");
#endif
            }
            if (previous_request.state != live::business::flash_sale::FlashSaleRequestState::kSubmitting) {
                return jsonResponse(200, "OK", "{\"request_id\":\"" + jsonEscape(previous_request.request_id) +
                    "\",\"order_id\":\"" + jsonEscape(previous_request.order_id) + "\",\"state\":\"" +
                    live::business::flash_sale::flashSaleRequestStateName(previous_request.state) + "\",\"message\":\"" +
                    jsonEscape(previous_request.message) + "\"}");
            }
        }
        if (previous_status.code() != live::common::ErrorCode::kNotFound) {
            return jsonResponse(503, "Service Unavailable", "{\"error\":\"request status unavailable\"}");
        }
        bool waiting_needed = false;
#ifdef ECOMMERCE_HAS_REDIS
        if (shared_flash_sale_limiter != nullptr) {
            bool allowed = false;
            const auto limit_status = shared_flash_sale_limiter->tryAcquire(&allowed);
            if (!limit_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"distributed admission limiter unavailable\"}");
            if (!allowed) waiting_needed = true;
            if (allowed) {
                live::traffic::RedisTokenBucket campaign_limiter(redis.get(),
                    "flashsale:{" + campaign_id + "}:entry_rate", flash_sale_campaign_rate, flash_sale_campaign_burst);
                const auto campaign_limit_status = campaign_limiter.tryAcquire(&allowed);
                if (!campaign_limit_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"distributed campaign limiter unavailable\"}");
                if (!allowed) waiting_needed = true;
            }
        } else
#endif
        if (!flash_sale_limiter.tryAcquire()) return jsonResponse(429, "Too Many Requests", "{\"error\":\"rate limited\"}");
        if (waiting_needed) {
#ifdef ECOMMERCE_HAS_REDIS
            if (redis != nullptr && waiting_room_enabled && flash_queue_mode != "local") {
                bool inserted = false;
                const live::business::flash_sale::FlashSaleRequestStatus waiting_status{
                    submit.idempotency_key, submit.user_id, "", live::business::flash_sale::FlashSaleRequestState::kWaiting,
                    "request is waiting for admission capacity", campaign_id};
                const auto claim_status = flash_ingress_status_store->putIfAbsent(waiting_status, &inserted);
                if (!claim_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"waiting-room status could not be stored\"}");
                if (!inserted) {
                    live::business::flash_sale::FlashSaleRequestStatus raced;
                    if (const auto lookup = flash_ingress_status_store->get(submit.idempotency_key, &raced); !lookup.ok())
                        return jsonResponse(503, "Service Unavailable", "{\"error\":\"request status unavailable\"}");
                    if (raced.user_id != current.id) return forbiddenResponse("idempotency key belongs to another user");
                    if (raced.state == live::business::flash_sale::FlashSaleRequestState::kSubmitting) {
                        if (raced.promotion_id != campaign_id) return jsonResponse(409, "Conflict", "{\"error\":\"idempotency key belongs to another campaign\"}");
                        bool transitioned = false;
                        const auto transition_status = flash_ingress_status_store->transition(submit.idempotency_key,
                            live::business::flash_sale::FlashSaleRequestState::kSubmitting,
                            live::business::flash_sale::FlashSaleRequestState::kWaiting, &transitioned);
                        if (!transition_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"waiting-room recovery failed\"}");
                        if (transitioned) {
                            raced.state = live::business::flash_sale::FlashSaleRequestState::kWaiting;
                        } else if (const auto lookup = flash_ingress_status_store->get(submit.idempotency_key, &raced); !lookup.ok()) {
                            return jsonResponse(503, "Service Unavailable", "{\"error\":\"request status unavailable\"}");
                        }
                    }
                    if (raced.state != live::business::flash_sale::FlashSaleRequestState::kWaiting)
                        return jsonResponse(200, "OK", "{\"request_id\":\"" + jsonEscape(raced.request_id) + "\",\"order_id\":\"" +
                            jsonEscape(raced.order_id) + "\",\"state\":\"" + live::business::flash_sale::flashSaleRequestStateName(raced.state) + "\"}");
                    if (raced.promotion_id != campaign_id) return jsonResponse(409, "Conflict", "{\"error\":\"idempotency key belongs to another campaign\"}");
                }
                live::messaging::FlashSaleOrderMessage waiting_message;
                waiting_message.promotion_id = campaign_id;
                waiting_message.sku_id = submit.sku_id;
                waiting_message.idempotency_key = submit.idempotency_key;
                waiting_message.user_id = submit.user_id;
                waiting_message.quantity = submit.quantity;
                live::kv::RedisFlashSaleWaitingRoom room(redis.get(), campaign_id, flash_sale_waiting_capacity);
                std::size_t queue_position = 0;
                const auto enqueue_status = room.enqueue(waiting_message, &queue_position);
                if (!enqueue_status.ok()) {
                    (void)flash_ingress_status_store->put({submit.idempotency_key, submit.user_id, "",
                        live::business::flash_sale::FlashSaleRequestState::kRejected, enqueue_status.message(), campaign_id});
                    return jsonResponse(enqueue_status.code() == live::common::ErrorCode::kResourceExhausted ? 429 : 503,
                        enqueue_status.code() == live::common::ErrorCode::kResourceExhausted ? "Too Many Requests" : "Service Unavailable",
                        "{\"error\":\"" + jsonEscape(enqueue_status.message()) + "\"}");
                }
                return jsonResponse(202, "Accepted", "{\"request_id\":\"" + jsonEscape(submit.idempotency_key) +
                    "\",\"state\":\"WAITING\",\"queue_position\":" + std::to_string(queue_position) + "}");
            }
#endif
            return jsonResponse(429, "Too Many Requests", "{\"error\":\"admission capacity exceeded\"}");
        }
        live::business::flash_sale::FlashSaleSubmitResult result;
        const auto status = flash_order_service->submit(submit, &result);
        if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        const char* state = result.code == live::business::flash_sale::SubmitCode::kQueued ? "QUEUED" :
            (result.code == live::business::flash_sale::SubmitCode::kDuplicate ? "DUPLICATE" : "REJECTED");
        const int http_status = result.code == live::business::flash_sale::SubmitCode::kQueued ? 202 :
            (result.code == live::business::flash_sale::SubmitCode::kDuplicate ? 200 : 409);
        return jsonResponse(http_status, http_status == 202 ? "Accepted" : (http_status == 200 ? "OK" : "Conflict"),
            "{\"request_id\":\"" + jsonEscape(result.request_id) + "\",\"order_id\":\"" + jsonEscape(result.order_id) + "\",\"state\":\"" + state + "\",\"message\":\"" + jsonEscape(result.message) + "\"}");
    });
    server.router().addRoute("GET", "/flash-sale/requests", [&](const auto& request) {
        live::business::user::User current;
        if (const auto auth = authenticateRequest(request, &users, &current); !auth.ok()) return unauthorizedResponse(auth);
        const auto request_id = queryValue(request.target, "request_id");
        live::business::flash_sale::FlashSaleRequestStatus result;
        const auto status = flash_request_status_store->get(request_id, &result);
        if (!status.ok()) return jsonResponse(status.code() == live::common::ErrorCode::kNotFound ? 404 : 503,
            status.code() == live::common::ErrorCode::kNotFound ? "Not Found" : "Service Unavailable",
            "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        if (result.user_id != current.id) return forbiddenResponse("request belongs to another user");
        std::string queue_position_field;
#ifdef ECOMMERCE_HAS_REDIS
        if (result.state == live::business::flash_sale::FlashSaleRequestState::kWaiting) {
            live::kv::RedisFlashSaleWaitingRoom room(redis.get(), result.promotion_id, flash_sale_waiting_capacity);
            std::size_t queue_position = 0;
            const auto position_status = room.position(result.request_id, &queue_position);
            if (!position_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"waiting-room status unavailable\"}");
            queue_position_field = ",\"queue_position\":" + std::to_string(queue_position);
        }
#endif
        return jsonResponse(200, "OK", "{\"request_id\":\"" + jsonEscape(result.request_id) + "\",\"order_id\":\"" +
            jsonEscape(result.order_id) + "\",\"state\":\"" + live::business::flash_sale::flashSaleRequestStateName(result.state) +
            "\",\"message\":\"" + jsonEscape(result.message) + "\"" + queue_position_field + "}");
    });
    server.router().addRoute("GET", "/metrics", [&](const auto&) {
        const bool queue_depth_known = flash_queue_mode != "rocketmq";
        active_connections_metric.set(static_cast<double>(server.activeConnections()));
        http_worker_queue_metric.set(static_cast<double>(server.pendingWorkerRequests()));
        flash_queue_depth_metric.set(queue_depth_known ? static_cast<double>(flash_order_queue->pending()) : -1.0);
        flash_queue_depth_known_metric.set(queue_depth_known ? 1.0 : 0.0);
        flash_processed_metric.set(static_cast<double>(flash_consumer->processed()));
        flash_rejected_metric.set(static_cast<double>(flash_consumer->rejected()));
        flash_dead_lettered_metric.set(static_cast<double>(flash_consumer->deadLettered()));
        return live::gateway::http::HttpResponse{200, "OK", {{"Content-Type", "text/plain; version=0.0.4"}}, metrics.renderPrometheus()};
    });
    server.router().addRoute("GET", "/debug/stats", [&](const auto&) {
        const bool queue_depth_known = flash_queue_mode != "rocketmq";
        return jsonResponse(200, "OK", "{\"http_requests\":" + std::to_string(http_requests_metric.value()) +
            ",\"http_active_connections\":" + std::to_string(server.activeConnections()) +
            ",\"http_worker_queue_depth\":" + std::to_string(server.pendingWorkerRequests()) +
            ",\"flash_sale_queue_depth\":" + (queue_depth_known ? std::to_string(flash_order_queue->pending()) : "null") +
            ",\"flash_sale_consumer_processed\":" + std::to_string(flash_consumer->processed()) +
            ",\"flash_sale_consumer_rejected\":" + std::to_string(flash_consumer->rejected()) +
            ",\"flash_sale_consumer_dead_lettered\":" + std::to_string(flash_consumer->deadLettered()) + "}");
    });
    const auto reconcileHandler = [&](const auto& request) {
        std::string actor;
        const int admin_auth = authorizeAdmin(request, admin_credentials, "admin", &actor);
        if (admin_auth != 0) return adminAuthResponse(admin_auth);
        if (inventory_reconciler != nullptr) {
            const std::string configured_skus = queryValue(request.target, "skus");
            const auto skus = splitCsv(configured_skus.empty() ? "sku-1" : configured_skus.c_str());
            live::business::inventory::ReconcileReport report;
            const auto status = inventory_reconciler->reconcile(skus, &report);
            metrics.increment(status.ok() ? "reconcile_success" : "reconcile_failure");
            if (!status.ok()) {
#ifdef ECOMMERCE_HAS_MYSQL
                if (mysql_store != nullptr) (void)mysql_store->recordAudit(actor, "INVENTORY_RECONCILIATION_FAILED", "inventory",
                    configured_skus.empty() ? "sku-1" : configured_skus, "", status.message());
#endif
                return jsonResponse(409, "Conflict", "{\"status\":\"failed\",\"error\":\"" + jsonEscape(status.message()) + "\"}");
            }
            metrics.increment("reconcile_mismatch_total", report.mismatches);
            metrics.increment("reconcile_repaired_total", report.repaired);
            metrics.increment("reconcile_blocked_total", report.blocked);
#ifdef ECOMMERCE_HAS_MYSQL
            if (mysql_store != nullptr) {
                const auto audit = mysql_store->recordAudit(actor, "INVENTORY_RECONCILIATION", "inventory",
                    configured_skus.empty() ? "sku-1" : configured_skus, "",
                    "checked=" + std::to_string(report.checked) + ",mismatches=" + std::to_string(report.mismatches) +
                    ",repaired=" + std::to_string(report.repaired) + ",blocked=" + std::to_string(report.blocked));
                if (!audit.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"reconciliation completed but audit persistence failed\"}");
            }
#endif
            return jsonResponse(200, "OK", "{\"status\":\"ok\",\"checked\":" + std::to_string(report.checked) +
                ",\"mismatches\":" + std::to_string(report.mismatches) + ",\"repaired\":" + std::to_string(report.repaired) +
                ",\"blocked\":" + std::to_string(report.blocked) + "}");
        }
        const auto status = inventory.validateInvariants();
        metrics.increment(status.ok() ? "reconcile_success" : "reconcile_failure");
#ifdef ECOMMERCE_HAS_MYSQL
        if (mysql_store != nullptr) {
            const auto audit = mysql_store->recordAudit(actor, status.ok() ? "INVENTORY_VALIDATION" : "INVENTORY_VALIDATION_FAILED",
                "inventory", "all", "", status.ok() ? "invariants=valid" : status.message());
            if (!audit.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"inventory validation completed but audit persistence failed\"}");
        }
#endif
        if (!status.ok()) return jsonResponse(500, "Internal Server Error", "{\"status\":\"failed\",\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(200, "OK", "{\"status\":\"ok\"}");
    };
    server.router().addRoute("POST", "/admin/reconciliation/run", reconcileHandler);
    server.router().addRoute("GET", "/admin/reconcile", reconcileHandler);  // Backward-compatible alias.
    server.router().addRoute("GET", "/admin/campaigns", [&](const auto& request) {
        const int admin_auth = authorizeAdmin(request, admin_credentials, "viewer", nullptr);
        if (admin_auth != 0) return adminAuthResponse(admin_auth);
        std::vector<live::business::promotion::Promotion> campaigns;
        if (const auto status = promotions.list(&campaigns); !status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"campaign query failed\"}");
        std::string body = "{\"campaigns\":[";
        for (std::size_t i = 0; i < campaigns.size(); ++i) {
            const auto& campaign = campaigns[i];
            if (i != 0) body += ',';
            body += "{\"campaign_id\":\"" + jsonEscape(campaign.id) + "\",\"sku_id\":\"" + jsonEscape(campaign.sku_id) +
                "\",\"state\":\"" + live::business::promotion::promotionStateName(campaign.state) + "\",\"participant_limit\":" +
                std::to_string(campaign.participant_limit) + ",\"starts_at\":" + std::to_string(campaign.starts_at) +
                ",\"ends_at\":" + std::to_string(campaign.ends_at) + "}";
        }
        return jsonResponse(200, "OK", body + "]}");
    });
    server.router().addRoute("GET", "/admin/audit", [&](const auto& request) {
        const int admin_auth = authorizeAdmin(request, admin_credentials, "admin", nullptr);
        if (admin_auth != 0) return adminAuthResponse(admin_auth);
        std::int64_t limit = 100;
        const auto limit_text = queryValue(request.target, "limit");
        if (!limit_text.empty() && !parseInt64(limit_text, &limit)) return jsonResponse(400, "Bad Request", "{\"error\":\"invalid limit\"}");
        if (limit <= 0) return jsonResponse(400, "Bad Request", "{\"error\":\"limit must be positive\"}");
        std::vector<live::business::promotion::AdminAuditRecord> records;
        const auto status = promotions.listAudit(static_cast<std::size_t>(std::min<std::int64_t>(limit, 1000)), &records);
        if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        std::string body = "{\"records\":[";
        for (std::size_t i = 0; i < records.size(); ++i) {
            const auto& record = records[i];
            if (i != 0) body += ',';
            body += "{\"id\":" + std::to_string(record.id) + ",\"actor\":\"" + jsonEscape(record.actor) +
                "\",\"action\":\"" + jsonEscape(record.action) + "\",\"resource_type\":\"" + jsonEscape(record.resource_type) +
                "\",\"resource_id\":\"" + jsonEscape(record.resource_id) + "\",\"previous_value\":\"" + jsonEscape(record.previous_value) +
                "\",\"new_value\":\"" + jsonEscape(record.new_value) + "\",\"created_at\":\"" + jsonEscape(record.created_at) + "\"}";
        }
        return jsonResponse(200, "OK", body + "]}");
    });
    server.router().addRoute("POST", "/admin/campaigns", [&](const auto& request) {
        std::lock_guard<std::mutex> allocation_lock(campaign_allocation_mutex);
        std::string actor;
        const int admin_auth = authorizeAdmin(request, admin_credentials, "operator", &actor);
        if (admin_auth != 0) return adminAuthResponse(admin_auth);
        const auto campaign_id = formValue(request.body, "campaign_id");
        const auto sku_id = formValue(request.body, "sku_id");
        std::int64_t participant_limit = 0;
        std::int64_t starts_at = 0;
        std::int64_t ends_at = 0;
        const auto limit_text = formValue(request.body, "participant_limit");
        const auto start_text = formValue(request.body, "starts_at");
        const auto end_text = formValue(request.body, "ends_at");
        if (campaign_id.empty() || sku_id.empty() || !parseInt64(limit_text, &participant_limit) || participant_limit <= 0 ||
            !validIdentifier(campaign_id) || !validIdentifier(sku_id) || start_text.empty() != end_text.empty() ||
            (!start_text.empty() && (!parseInt64(start_text, &starts_at) || !parseInt64(end_text, &ends_at) ||
                                      starts_at <= 0 || ends_at <= starts_at)))
            return jsonResponse(400, "Bad Request", "{\"error\":\"invalid campaign fields or scheduled window; starts_at and ends_at must be Unix seconds\"}");
        live::business::product::Product product;
        if (!products.getProduct(sku_id, &product).ok() || !product.on_sale) return jsonResponse(404, "Not Found", "{\"error\":\"active SKU not found\"}");
        live::business::inventory::StockSnapshot stock;
        if (!inventory.queryStock(sku_id, &stock).ok() || stock.available < participant_limit) return jsonResponse(409, "Conflict", "{\"error\":\"SKU stock is insufficient for campaign quota\"}");
#ifdef ECOMMERCE_HAS_MYSQL
        if (mysql_store == nullptr) {
#endif
            std::vector<live::business::promotion::Promotion> configured_campaigns;
            if (const auto list_status = promotions.list(&configured_campaigns); !list_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"campaign allocation query failed\"}");
            std::uint64_t allocated = 0;
            const auto allocation_now = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            for (const auto& campaign : configured_campaigns) {
                if (campaign.sku_id == sku_id && campaign.state != live::business::promotion::PromotionState::kFinished &&
                    (campaign.ends_at == 0 || campaign.ends_at > allocation_now))
                    allocated += campaign.participant_limit;
            }
            if (static_cast<std::uint64_t>(stock.available) < allocated + static_cast<std::uint64_t>(participant_limit))
                return jsonResponse(409, "Conflict", "{\"error\":\"campaign quotas exceed remaining SKU stock\"}");
#ifdef ECOMMERCE_HAS_MYSQL
        }
#endif
        const auto status = promotions.create({campaign_id, sku_id, static_cast<std::size_t>(participant_limit),
            live::business::promotion::PromotionState::kCreated, starts_at, ends_at}, actor);
        const bool campaign_conflict = status.code() == live::common::ErrorCode::kAlreadyExists ||
            status.code() == live::common::ErrorCode::kResourceExhausted;
        if (!status.ok()) return jsonResponse(campaign_conflict ? 409 : 503,
            campaign_conflict ? "Conflict" : "Service Unavailable",
            "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(201, "Created", "{\"campaign_id\":\"" + jsonEscape(campaign_id) + "\",\"state\":\"CREATED\"}");
    });
    server.router().addRoute("PATCH", "/admin/campaigns", [&](const auto& request) {
        std::lock_guard<std::mutex> allocation_lock(campaign_allocation_mutex);
        std::string actor;
        const int admin_auth = authorizeAdmin(request, admin_credentials, "operator", &actor);
        if (admin_auth != 0) return adminAuthResponse(admin_auth);
        const auto campaign_id = formValue(request.body, "campaign_id");
        const auto sku_id = formValue(request.body, "sku_id");
        std::int64_t participant_limit = 0;
        std::int64_t starts_at = 0;
        std::int64_t ends_at = 0;
        const auto start_text = formValue(request.body, "starts_at");
        const auto end_text = formValue(request.body, "ends_at");
        if (campaign_id.empty() || sku_id.empty() || !parseInt64(formValue(request.body, "participant_limit"), &participant_limit) ||
            participant_limit <= 0 || !validIdentifier(campaign_id) || !validIdentifier(sku_id) ||
            start_text.empty() != end_text.empty() || (!start_text.empty() &&
                (!parseInt64(start_text, &starts_at) || !parseInt64(end_text, &ends_at) || starts_at <= 0 || ends_at <= starts_at)))
            return jsonResponse(400, "Bad Request", "{\"error\":\"invalid campaign fields or scheduled window; starts_at and ends_at must be Unix seconds\"}");
        live::business::product::Product product;
        if (!products.getProduct(sku_id, &product).ok() || !product.on_sale) return jsonResponse(404, "Not Found", "{\"error\":\"active SKU not found\"}");
        live::business::inventory::StockSnapshot stock;
        if (!inventory.queryStock(sku_id, &stock).ok() || stock.available < participant_limit) return jsonResponse(409, "Conflict", "{\"error\":\"SKU stock is insufficient for campaign quota\"}");
#ifdef ECOMMERCE_HAS_MYSQL
        if (mysql_store == nullptr) {
#endif
            std::vector<live::business::promotion::Promotion> configured_campaigns;
            if (const auto list_status = promotions.list(&configured_campaigns); !list_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"campaign allocation query failed\"}");
            std::uint64_t allocated = 0;
            const auto allocation_now = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            for (const auto& campaign : configured_campaigns) {
                if (campaign.id != campaign_id && campaign.sku_id == sku_id &&
                    campaign.state != live::business::promotion::PromotionState::kFinished &&
                    (campaign.ends_at == 0 || campaign.ends_at > allocation_now))
                    allocated += campaign.participant_limit;
            }
            if (static_cast<std::uint64_t>(stock.available) < allocated + static_cast<std::uint64_t>(participant_limit))
                return jsonResponse(409, "Conflict", "{\"error\":\"campaign quotas exceed remaining SKU stock\"}");
#ifdef ECOMMERCE_HAS_MYSQL
        }
#endif
        live::business::promotion::Promotion update{campaign_id, sku_id, static_cast<std::size_t>(participant_limit),
            live::business::promotion::PromotionState::kCreated, starts_at, ends_at};
        const auto status = promotions.update(std::move(update), actor);
        if (!status.ok()) return jsonResponse(status.code() == live::common::ErrorCode::kNotFound ? 404 : 409,
            status.code() == live::common::ErrorCode::kNotFound ? "Not Found" : "Conflict",
            "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(200, "OK", "{\"campaign_id\":\"" + jsonEscape(campaign_id) + "\",\"state\":\"CREATED\"}");
    });
    const auto adminCampaignAction = [&](const auto& request, const char* action) {
        std::string actor;
        const int admin_auth = authorizeAdmin(request, admin_credentials, "operator", &actor);
        if (admin_auth != 0) return adminAuthResponse(admin_auth);
        const auto promotion_id = formValue(request.body, "campaign_id").empty()
            ? formValue(request.body, "promotion_id") : formValue(request.body, "campaign_id");
        if (promotion_id.empty()) return jsonResponse(400, "Bad Request", "{\"error\":\"promotion_id is required\"}");
        const std::string operation(action);
        const auto status = operation == "preheat" ? promotions.preheat(promotion_id, actor) :
            (operation == "start" ? promotions.start(promotion_id, actor) :
             (operation == "pause" ? promotions.pause(promotion_id, actor) :
              (operation == "resume" ? promotions.resume(promotion_id, actor) : promotions.finish(promotion_id, actor))));
        if (!status.ok()) {
            const bool missing = status.code() == live::common::ErrorCode::kNotFound;
            return jsonResponse(missing ? 404 : 409, missing ? "Not Found" : "Conflict",
                "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        }
        live::business::promotion::Promotion promotion;
        (void)promotions.get(promotion_id, &promotion);
        const char* state = promotion.state == live::business::promotion::PromotionState::kRunning ? "RUNNING" :
            (promotion.state == live::business::promotion::PromotionState::kPaused ? "PAUSED" : "FINISHED");
        return jsonResponse(200, "OK", "{\"promotion_id\":\"" + jsonEscape(promotion_id) + "\",\"state\":\"" + state + "\"}");
    };
    server.router().addRoute("POST", "/admin/campaigns/preheat", [&](const auto& request) { return adminCampaignAction(request, "preheat"); });
    server.router().addRoute("POST", "/admin/campaigns/start", [&](const auto& request) { return adminCampaignAction(request, "start"); });
    server.router().addRoute("POST", "/admin/campaigns/pause", [&](const auto& request) { return adminCampaignAction(request, "pause"); });
    server.router().addRoute("POST", "/admin/campaigns/resume", [&](const auto& request) { return adminCampaignAction(request, "resume"); });
    server.router().addRoute("POST", "/admin/campaigns/finish", [&](const auto& request) { return adminCampaignAction(request, "finish"); });
    server.router().addRoute("GET", "/admin/dlq", [&](const auto& request) {
        const int admin_auth = authorizeAdmin(request, admin_credentials, "admin", nullptr);
        if (admin_auth != 0) return adminAuthResponse(admin_auth);
#ifdef ECOMMERCE_HAS_REDIS
        if (flash_queue_mode == "redis") {
            auto* dead_letters = dynamic_cast<live::kv::RedisOrderQueue*>(flash_dead_letter_queue.get());
            if (dead_letters == nullptr) return jsonResponse(503, "Service Unavailable", "{\"error\":\"Redis DLQ is not configured\"}");
            std::int64_t limit = 100;
            const auto limit_text = queryValue(request.target, "limit");
            if (!limit_text.empty() && !parseInt64(limit_text, &limit)) return jsonResponse(400, "Bad Request", "{\"error\":\"invalid limit\"}");
            if (limit <= 0) return jsonResponse(400, "Bad Request", "{\"error\":\"limit must be positive\"}");
            std::vector<std::string> payloads;
            const auto status = dead_letters->peekRaw(static_cast<std::size_t>(std::min<std::int64_t>(limit, 1000)), &payloads);
            if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
            std::string body = "{\"queue\":\"redis\",\"messages\":[";
            for (std::size_t i = 0; i < payloads.size(); ++i) {
                if (i != 0) body += ',';
                live::messaging::FlashSaleOrderMessage message;
                if (live::messaging::FlashSaleOrderMessage::deserialize(payloads[i], &message).ok()) {
                    body += "{\"valid\":true,\"order_id\":\"" + jsonEscape(message.order_id) + "\",\"request_id\":\"" + jsonEscape(message.idempotency_key) +
                        "\",\"user_id\":\"" + jsonEscape(message.user_id) + "\",\"sku_id\":\"" + jsonEscape(message.sku_id) +
                        "\",\"quantity\":" + std::to_string(message.quantity) + "}";
                } else {
                    body += "{\"valid\":false,\"payload\":\"" + jsonEscape(payloads[i]) + "\"}";
                }
            }
            return jsonResponse(200, "OK", body + "]}");
        }
#endif
#ifdef ECOMMERCE_HAS_KAFKA
        if (flash_queue_mode != "rocketmq" && kafka_brokers != nullptr && kafka != nullptr) {
            const std::string topic = std::getenv("ECOMMERCE_FLASH_SALE_TOPIC") == nullptr
                ? "flash-sale-orders.DLQ" : std::string(std::getenv("ECOMMERCE_FLASH_SALE_TOPIC")) + ".DLQ";
            std::int64_t limit = 100;
            const auto limit_text = queryValue(request.target, "limit");
            if (!limit_text.empty() && !parseInt64(limit_text, &limit)) return jsonResponse(400, "Bad Request", "{\"error\":\"invalid limit\"}");
            if (limit <= 0) return jsonResponse(400, "Bad Request", "{\"error\":\"limit must be positive\"}");
            std::vector<live::messaging::Event> events;
            const auto status = live::messaging::KafkaDlqOperator::peek(kafka_brokers, topic,
                static_cast<std::size_t>(std::min<std::int64_t>(limit, 1000)), &events);
            if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
            std::string body = "{\"queue\":\"kafka\",\"topic\":\"" + jsonEscape(topic) + "\",\"messages\":[";
            for (std::size_t i = 0; i < events.size(); ++i) {
                if (i != 0) body += ',';
                live::messaging::FlashSaleOrderMessage message;
                if (live::messaging::FlashSaleOrderMessage::deserialize(events[i].payload, &message).ok()) {
                    body += "{\"valid\":true,\"event_id\":\"" + jsonEscape(events[i].event_id) + "\",\"order_id\":\"" +
                        jsonEscape(message.order_id) + "\",\"request_id\":\"" + jsonEscape(message.idempotency_key) +
                        "\",\"user_id\":\"" + jsonEscape(message.user_id) + "\",\"sku_id\":\"" +
                        jsonEscape(message.sku_id) + "\",\"quantity\":" + std::to_string(message.quantity) + "}";
                } else {
                    body += "{\"valid\":false,\"event_id\":\"" + jsonEscape(events[i].event_id) + "\",\"payload\":\"" +
                        jsonEscape(events[i].payload) + "\"}";
                }
            }
            return jsonResponse(200, "OK", body + "]}");
        }
#endif
#ifdef ECOMMERCE_HAS_ROCKETMQ
        if (flash_queue_mode == "rocketmq") {
            auto* rocketmq = dynamic_cast<live::messaging::RocketMqOrderQueue*>(flash_order_queue.get());
            if (rocketmq == nullptr) return jsonResponse(503, "Service Unavailable", "{\"error\":\"RocketMQ DLQ operations are not configured\"}");
            std::int64_t limit = 100;
            const auto limit_text = queryValue(request.target, "limit");
            if (!limit_text.empty() && !parseInt64(limit_text, &limit)) return jsonResponse(400, "Bad Request", "{\"error\":\"invalid limit\"}");
            if (limit <= 0) return jsonResponse(400, "Bad Request", "{\"error\":\"limit must be positive\"}");
            std::vector<live::messaging::RocketMqDlqMessage> messages;
            const auto status = rocketmq->peekDlq(static_cast<std::size_t>(std::min<std::int64_t>(limit, 1000)), &messages);
            if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
            std::string body = "{\"queue\":\"rocketmq\",\"messages\":[";
            for (std::size_t i = 0; i < messages.size(); ++i) {
                if (i != 0) body += ',';
                live::messaging::FlashSaleOrderMessage command;
                if (live::messaging::FlashSaleOrderMessage::deserialize(messages[i].payload, &command).ok()) {
                    body += "{\"valid\":true,\"message_id\":\"" + jsonEscape(messages[i].message_id) +
                        "\",\"order_id\":\"" + jsonEscape(command.order_id) + "\",\"request_id\":\"" +
                        jsonEscape(command.idempotency_key) + "\",\"user_id\":\"" + jsonEscape(command.user_id) +
                        "\",\"sku_id\":\"" + jsonEscape(command.sku_id) + "\",\"quantity\":" +
                        std::to_string(command.quantity) + "}";
                } else {
                    body += "{\"valid\":false,\"message_id\":\"" + jsonEscape(messages[i].message_id) +
                        "\",\"payload_size\":" + std::to_string(messages[i].payload.size()) + "}";
                }
            }
            return jsonResponse(200, "OK", body + "]}");
        }
#endif
        return jsonResponse(503, "Service Unavailable", "{\"error\":\"DLQ inspection is not available for the configured transport\"}");
    });
    server.router().addRoute("POST", "/admin/dlq/replay", [&](const auto& request) {
        const int admin_auth = authorizeAdmin(request, admin_credentials, "admin", nullptr);
        if (admin_auth != 0) return adminAuthResponse(admin_auth);
#ifdef ECOMMERCE_HAS_REDIS
        if (flash_queue_mode == "redis") {
            auto* dead_letters = dynamic_cast<live::kv::RedisOrderQueue*>(flash_dead_letter_queue.get());
            auto* target_queue = dynamic_cast<live::kv::RedisOrderQueue*>(flash_order_queue.get());
            if (dead_letters == nullptr || target_queue == nullptr) return jsonResponse(503, "Service Unavailable", "{\"error\":\"Redis DLQ replay is not configured\"}");
            std::int64_t max_messages = 10;
            const auto max_text = formValue(request.body, "max_messages");
            if (!max_text.empty() && !parseInt64(max_text, &max_messages)) return jsonResponse(400, "Bad Request", "{\"error\":\"invalid max_messages\"}");
            std::size_t replayed = 0;
            const auto status = dead_letters->replayTo(target_queue,
                static_cast<std::size_t>(std::max<std::int64_t>(1, std::min<std::int64_t>(max_messages, 1000))), &replayed);
            metrics.increment("redis_dlq_replayed_total", replayed);
            if (!status.ok()) return jsonResponse(409, "Conflict", "{\"replayed\":" + std::to_string(replayed) + ",\"error\":\"" + jsonEscape(status.message()) + "\"}");
            return jsonResponse(200, "OK", "{\"replayed\":" + std::to_string(replayed) + "}");
        }
#endif
#ifdef ECOMMERCE_HAS_ROCKETMQ
        if (flash_queue_mode == "rocketmq") {
            auto* rocketmq = dynamic_cast<live::messaging::RocketMqOrderQueue*>(flash_order_queue.get());
            if (rocketmq == nullptr) return jsonResponse(503, "Service Unavailable", "{\"error\":\"RocketMQ DLQ operations are not configured\"}");
            std::int64_t max_messages = 10;
            const auto max_text = formValue(request.body, "max_messages");
            if (!max_text.empty() && !parseInt64(max_text, &max_messages)) return jsonResponse(400, "Bad Request", "{\"error\":\"invalid max_messages\"}");
            std::size_t replayed = 0;
            const auto status = rocketmq->replayDlq(static_cast<std::size_t>(std::max<std::int64_t>(1, std::min<std::int64_t>(max_messages, 1000))), &replayed);
            metrics.increment("rocketmq_dlq_replayed_total", replayed);
            if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"replayed\":" + std::to_string(replayed) +
                ",\"error\":\"" + jsonEscape(status.message()) + "\"}");
            return jsonResponse(200, "OK", "{\"replayed\":" + std::to_string(replayed) + "}");
        }
#endif
#ifdef ECOMMERCE_HAS_KAFKA
        if (kafka_brokers == nullptr || kafka == nullptr) return jsonResponse(503, "Service Unavailable", "{\"error\":\"Kafka is not configured\"}");
        const std::string dlq_topic = formValue(request.body, "topic").empty()
            ? (std::getenv("ECOMMERCE_FLASH_SALE_TOPIC") == nullptr ? "flash-sale-orders.DLQ" : std::string(std::getenv("ECOMMERCE_FLASH_SALE_TOPIC")) + ".DLQ")
            : formValue(request.body, "topic");
        const std::string target_topic = formValue(request.body, "target_topic").empty()
            ? (std::getenv("ECOMMERCE_FLASH_SALE_TOPIC") == nullptr ? "flash-sale-orders" : std::getenv("ECOMMERCE_FLASH_SALE_TOPIC"))
            : formValue(request.body, "target_topic");
        std::int64_t max_messages = 10;
        const auto max_text = formValue(request.body, "max_messages");
        if (!max_text.empty() && !parseInt64(max_text, &max_messages)) return jsonResponse(400, "Bad Request", "{\"error\":\"invalid max_messages\"}");
        std::size_t replayed = 0;
        const auto status = live::messaging::KafkaDlqOperator::replay(kafka_brokers, dlq_topic, target_topic,
                                                                        static_cast<std::size_t>(std::max<std::int64_t>(1, std::min<std::int64_t>(max_messages, 1000))), &replayed);
        if (!status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        metrics.increment("kafka_dlq_replayed_total", replayed);
        return jsonResponse(200, "OK", "{\"replayed\":" + std::to_string(replayed) + "}");
#else
        return jsonResponse(503, "Service Unavailable", "{\"error\":\"Kafka support is not compiled\"}");
#endif
    });
    server.router().addRoute("GET", "/orders", [&](const auto& request) {
        live::business::user::User current;
        if (const auto auth = authenticateRequest(request, &users, &current); !auth.ok()) return unauthorizedResponse(auth);
        live::business::order::Order order;
        if (!orders.getOrder(queryValue(request.target, "order_id"), &order).ok()) return jsonResponse(404, "Not Found", "{\"error\":\"order not found\"}");
        if (order.user_id != current.id) return forbiddenResponse("order belongs to another user");
        const char* state = order.state == live::business::order::OrderState::kPaid ? "PAID" :
            (order.state == live::business::order::OrderState::kExpired ? "EXPIRED" :
             (order.state == live::business::order::OrderState::kCancelled ? "CANCELLED" : "PENDING_PAYMENT"));
        return jsonResponse(200, "OK", "{\"order_id\":\"" + jsonEscape(order.id) + "\",\"state\":\"" + state + "\"}");
    });
    server.router().addRoute("POST", "/orders/pay", [&](const auto& request) {
        live::business::user::User current;
        if (const auto auth = authenticateRequest(request, &users, &current); !auth.ok()) return unauthorizedResponse(auth);
        const auto order_id = formValue(request.body, "order_id");
        live::business::order::Order order;
        if (!orders.getOrder(order_id, &order).ok()) return jsonResponse(404, "Not Found", "{\"error\":\"order not found\"}");
        if (order.user_id != current.id) return forbiddenResponse("order belongs to another user");
        live::business::order::Order transitioned;
        const auto transition = orders.transitionOrder(order_id, live::business::order::OrderState::kPendingPayment,
            live::business::order::OrderState::kPaid, &transitioned);
        if (!transition.ok()) return jsonResponse(409, "Conflict", "{\"error\":\"" + jsonEscape(transition.message()) + "\"}");
        const auto request_status_update = flash_request_status_store->put({order.idempotency_key, order.user_id, order.id,
            live::business::flash_sale::FlashSaleRequestState::kPaid, "mock payment succeeded"});
        if (!request_status_update.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"payment committed; request status cache update pending\"}");
        live::business::inventory::InventoryReservationResult result;
        const auto stock_status = flash_inventory_gateway->confirm(order.sku_id, order.id, &result);
        if (!stock_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"payment recorded; inventory confirmation pending\"}");
        return jsonResponse(200, "OK", "{\"order_id\":\"" + jsonEscape(order.id) + "\",\"state\":\"PAID\",\"payment\":\"MOCK_SUCCEEDED\"}");
    });
    server.router().addRoute("POST", "/payments/callback/mock", [&](const auto& request) {
#ifndef ECOMMERCE_HAS_MYSQL
        return jsonResponse(503, "Service Unavailable", "{\"error\":\"durable payment callbacks require MySQL\"}");
#else
        if (mysql_store == nullptr) return jsonResponse(503, "Service Unavailable", "{\"error\":\"durable payment callbacks require MySQL\"}");
        const char* secret_value = std::getenv("ECOMMERCE_PAYMENT_CALLBACK_SECRET");
        if (secret_value == nullptr || *secret_value == '\0') return jsonResponse(503, "Service Unavailable", "{\"error\":\"payment callback secret is not configured\"}");
        const auto timestamp_header = request.headers.find("x-payment-timestamp");
        const auto signature_header = request.headers.find("x-payment-signature");
        std::int64_t timestamp = 0;
        if (timestamp_header == request.headers.end() || signature_header == request.headers.end() ||
            !parseInt64(timestamp_header->second, &timestamp)) return jsonResponse(401, "Unauthorized", "{\"error\":\"invalid payment signature headers\"}");
        const auto now = static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        if (timestamp < now - 300 || timestamp > now + 300 ||
            !live::common::security::verifyTimestampedHmacSha256(secret_value, timestamp_header->second,
                                                                  request.body, signature_header->second))
            return jsonResponse(401, "Unauthorized", "{\"error\":\"invalid or expired payment callback signature\"}");
        const auto order_id = formValue(request.body, "order_id");
        const auto transaction_id = formValue(request.body, "transaction_id");
        std::int64_t amount_cents = -1;
        if (order_id.empty() || transaction_id.empty() || transaction_id.size() > 128 ||
            !parseInt64(formValue(request.body, "amount_cents"), &amount_cents) || amount_cents < 0)
            return jsonResponse(400, "Bad Request", "{\"error\":\"invalid callback payload\"}");
        live::business::order::Order paid;
        const auto status = orders.confirmPayment(order_id, "mock", transaction_id, amount_cents, &paid);
        if (!status.ok()) {
            const auto code = status.code() == live::common::ErrorCode::kNotFound ? 404 : 409;
            return jsonResponse(code, code == 404 ? "Not Found" : "Conflict", "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        }
        const auto request_status_update = flash_request_status_store->put({paid.idempotency_key, paid.user_id, paid.id,
            live::business::flash_sale::FlashSaleRequestState::kPaid, "payment callback confirmed"});
        if (!request_status_update.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"payment committed; request status cache update pending\"}");
        live::business::inventory::InventoryReservationResult result;
        const auto stock_status = flash_inventory_gateway->confirm(paid.sku_id, paid.id, &result);
        if (!stock_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"payment committed; inventory projection will reconcile\"}");
        return jsonResponse(200, "OK", "{\"order_id\":\"" + jsonEscape(paid.id) + "\",\"state\":\"PAID\"}");
#endif
    });
    server.router().addRoute("POST", "/orders/cancel", [&](const auto& request) {
        live::business::user::User current;
        if (const auto auth = authenticateRequest(request, &users, &current); !auth.ok()) return unauthorizedResponse(auth);
        const auto order_id = formValue(request.body, "order_id");
        live::business::order::Order order;
        if (!orders.getOrder(order_id, &order).ok()) return jsonResponse(404, "Not Found", "{\"error\":\"order not found\"}");
        if (order.user_id != current.id) return forbiddenResponse("order belongs to another user");
        live::business::order::Order transitioned;
        const auto transition = orders.transitionOrder(order_id, live::business::order::OrderState::kPendingPayment,
            live::business::order::OrderState::kCancelled, &transitioned);
        if (!transition.ok()) return jsonResponse(409, "Conflict", "{\"error\":\"" + jsonEscape(transition.message()) + "\"}");
        const auto request_status_update = flash_request_status_store->put({order.idempotency_key, order.user_id, order.id,
            live::business::flash_sale::FlashSaleRequestState::kCancelled, "order cancelled"});
        if (!request_status_update.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"order cancelled; request status cache update pending\"}");
        live::business::inventory::InventoryReservationResult result;
        const auto stock_status = flash_inventory_gateway->release(order.sku_id, order.id, &result);
        if (!stock_status.ok()) return jsonResponse(503, "Service Unavailable", "{\"error\":\"order cancelled; inventory release pending\"}");
        return jsonResponse(200, "OK", "{\"order_id\":\"" + jsonEscape(order.id) + "\",\"state\":\"CANCELLED\"}");
    });
    server.router().addRoute("GET", "/promotions/promo-1", [&](const auto&) {
        live::business::promotion::Promotion promotion;
        if (!promotions.get("promo-1", &promotion).ok()) return jsonResponse(404, "Not Found", "{\"error\":\"promotion not found\"}");
        const char* state = promotion.state == live::business::promotion::PromotionState::kRunning ? "RUNNING" :
            (promotion.state == live::business::promotion::PromotionState::kPaused ? "PAUSED" :
             (promotion.state == live::business::promotion::PromotionState::kFinished ? "FINISHED" : "PREHEATING"));
        return jsonResponse(200, "OK", "{\"promotion_id\":\"" + promotion.id + "\",\"state\":\"" + state + "\",\"limit\":" + std::to_string(promotion.participant_limit) + "}");
    });
    server.router().addRoute("GET", "/campaigns", [&](const auto& request) {
        const auto campaign_id = queryValue(request.target, "campaign_id");
        if (campaign_id.empty()) return jsonResponse(400, "Bad Request", "{\"error\":\"campaign_id is required\"}");
        live::business::promotion::Promotion promotion;
        const auto status = promotions.get(campaign_id, &promotion);
        if (!status.ok()) return jsonResponse(status.code() == live::common::ErrorCode::kNotFound ? 404 : 503,
            status.code() == live::common::ErrorCode::kNotFound ? "Not Found" : "Service Unavailable",
            "{\"error\":\"" + jsonEscape(status.message()) + "\"}");
        return jsonResponse(200, "OK", "{\"campaign_id\":\"" + jsonEscape(promotion.id) + "\",\"sku_id\":\"" +
            jsonEscape(promotion.sku_id) + "\",\"state\":\"" + live::business::promotion::promotionStateName(promotion.state) +
            "\",\"participant_limit\":" + std::to_string(promotion.participant_limit) + ",\"starts_at\":" +
            std::to_string(promotion.starts_at) + ",\"ends_at\":" + std::to_string(promotion.ends_at) + "}");
    });
    const auto stopBackgroundWorkers = [&] {
        waiting_room_stop.store(true, std::memory_order_relaxed);
        for (auto& worker : waiting_room_workers) if (worker.joinable()) worker.join();
        campaign_refresh_stop.store(true, std::memory_order_relaxed);
        if (campaign_refresh_thread.joinable()) campaign_refresh_thread.join();
#ifdef ECOMMERCE_HAS_KAFKA
        notification_worker_stop.store(true, std::memory_order_relaxed);
        if (notification_worker.joinable()) notification_worker.join();
        if (notification_consumer != nullptr) notification_consumer->stop();
        if (notification_publisher != nullptr) notification_publisher->stop();
#endif
        if (flash_consumer != nullptr) flash_consumer->stop();
        expiry_worker_stop.store(true, std::memory_order_relaxed);
        if (expiry_worker.joinable()) expiry_worker.join();
        outbox_dispatcher.stop();
#ifdef ECOMMERCE_HAS_MYSQL
        mysql_outbox_stop.store(true, std::memory_order_relaxed);
        if (mysql_outbox_thread.joinable()) mysql_outbox_thread.join();
#endif
    };
#ifdef ECOMMERCE_HAS_MYSQL
    if (run_background_workers && mysql_store != nullptr) {
        campaign_refresh_thread = std::thread([&] {
            while (!campaign_refresh_stop.load(std::memory_order_relaxed)) {
                const auto refresh_status = promotions.restore();
                if (!refresh_status.ok()) {
                    std::clog << "campaign refresh failed: " << refresh_status.message() << '\n';
                }
                for (int i = 0; i < 20 && !campaign_refresh_stop.load(std::memory_order_relaxed); ++i) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        });
    }
#endif
    if (!run_gateway) {
        std::cout << "ecommerce background worker started\n";
        while (shutdown_requested == 0) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        stopBackgroundWorkers();
        snapshot_worker.stop();
        return 0;
    }

    const auto listen_status = server.start();
    if (!listen_status.ok()) {
        std::cerr << "failed to start gateway: " << listen_status.message() << '\n';
        stopBackgroundWorkers();
        snapshot_worker.stop();
        return 1;
    }
    std::cout << "ecommerce API listening on 0.0.0.0:" << server.port() << '\n';
    std::thread loop_thread([&loop] { loop.loop(); });
    while (shutdown_requested == 0) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // Stop accepting on the event-loop thread, then drain HTTP workers while
    // the loop is still alive so their response callbacks are not lost.
    std::promise<void> accepting_stopped;
    auto accepting_stopped_future = accepting_stopped.get_future();
    loop.runInLoop([&server, &accepting_stopped] {
        server.stopAcceptingInLoop();
        accepting_stopped.set_value();
    });
    accepting_stopped_future.wait();
    server.stopWorkers();
    stopBackgroundWorkers();
    loop.runInLoop([&loop, &server] {
        server.stopInLoop();
        loop.quit();
    });
    loop_thread.join();
    snapshot_worker.stop();
    return 0;
}
