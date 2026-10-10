#pragma once

#include "business/order/order_transaction_store.h"
#include "business/flash_sale/flash_sale_order_repository.h"
#include "business/flash_sale/flash_sale_request_status_store.h"
#include "business/promotion/promotion_service.h"
#include "storage/mysql/mysql_connection_pool.h"
#include "observability/metrics/metrics_registry.h"

#include <mysql/mysql.h>

#include <cstdint>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace live::storage::mysql {

class MySqlTransactionStore final : public ::live::business::order::IOrderTransactionStore,
                                    public ::live::business::flash_sale::IFlashSaleOrderRepository,
                                    public ::live::business::flash_sale::IFlashSaleRequestStatusStore,
                                    public ::live::business::promotion::IPromotionRepository {
public:
    MySqlTransactionStore();
    ~MySqlTransactionStore() override;

    common::Status connect(const std::string& host, std::uint16_t port,
                           const std::string& user, const std::string& password,
                           const std::string& database);
    common::Status connect(const std::vector<std::string>& hosts, std::uint16_t port,
                           const std::string& user, const std::string& password,
                           const std::string& database, std::size_t pool_size);
    void close();
    bool connected() const;
    common::Status ping() const;
    void setMetrics(::live::observability::MetricsRegistry* metrics) { metrics_ = metrics; }

    common::Status getOrder(const std::string& order_id, ::live::business::order::Order* order) const override;
    common::Status transitionOrder(const std::string& order_id,
                                   ::live::business::order::OrderState expected,
                                   ::live::business::order::OrderState next,
                                   ::live::business::order::Order* order) override;
    common::Status confirmPayment(const std::string& order_id, const std::string& provider,
                                  const std::string& provider_transaction_id,
                                  std::int64_t amount_cents,
                                  ::live::business::order::Order* order) override;
    common::Status put(const ::live::business::flash_sale::FlashSaleRequestStatus& status) override;
    common::Status putIfAbsent(const ::live::business::flash_sale::FlashSaleRequestStatus& status,
                               bool* inserted) override;
    common::Status get(const std::string& request_id,
                       ::live::business::flash_sale::FlashSaleRequestStatus* status) const override;
    common::Status transition(const std::string& request_id,
                              ::live::business::flash_sale::FlashSaleRequestState expected,
                              ::live::business::flash_sale::FlashSaleRequestState next,
                              bool* updated) override;
    common::Status initializeSku(const std::string& sku_id, std::int64_t quantity) override;
    common::Status initializeProduct(const std::string& sku_id, const std::string& name,
                                    std::int64_t price_cents);
    common::Status queryStock(const std::string& sku_id, ::live::business::inventory::StockSnapshot* snapshot) const override;
    common::Status listExpiredOrders(std::size_t limit,
                                     std::vector<::live::business::order::Order>* orders) const override;
    common::Status claimExpiredOrders(const std::string& owner, std::size_t limit,
                                      std::vector<::live::business::order::Order>* orders) override;
    common::Status drainOutbox(const std::function<common::Status(const ::live::messaging::Event&)>& publish,
                               std::size_t limit = 100) override;
    common::Status persistQueuedOrder(const ::live::messaging::FlashSaleOrderMessage& message,
                                      ::live::business::order::Order* order) override;
    common::Status create(const ::live::business::promotion::Promotion& promotion, const std::string& actor) override;
    common::Status update(const ::live::business::promotion::Promotion& previous,
                          const ::live::business::promotion::Promotion& next,
                          const std::string& actor) override;
    common::Status list(std::vector<::live::business::promotion::Promotion>* promotions) const override;
    common::Status transition(const std::string& id,
                              ::live::business::promotion::PromotionState expected,
                              ::live::business::promotion::PromotionState next,
                              const std::string& actor) override;
    common::Status listAudit(std::size_t limit,
                             std::vector<::live::business::promotion::AdminAuditRecord>* records) const override;
    common::Status recordAudit(const std::string& actor, const std::string& action,
                               const std::string& resource_type, const std::string& resource_id,
                               const std::string& previous_value, const std::string& new_value);

private:
    common::Status execute(MYSQL* connection, const std::string& sql) const;
    common::Status begin(MYSQL* connection) const;
    common::Status rollback(MYSQL* connection) const;
    common::Status commit(MYSQL* connection) const;
    std::string quote(MYSQL* connection, const std::string& value) const;
    common::Status loadOrder(MYSQL* connection, const std::string& order_id, ::live::business::order::Order* order) const;
    common::Status appendOutbox(MYSQL* connection, const ::live::messaging::Event& event) const;
    void recordSql(const char* operation, const common::Status& status,
                   std::chrono::steady_clock::time_point started) const;
    std::unique_ptr<MySqlConnectionPool> pool_;
    ::live::observability::MetricsRegistry* metrics_{nullptr};
};

}  // namespace live::storage::mysql
