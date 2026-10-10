#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"
#include "messaging/order_message.h"
#include "storage/mysql/mysql_transaction_store.h"

#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <stdexcept>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

const char* envOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value == nullptr || *value == '\0' ? fallback : value;
}

}  // namespace

int main() {
    const char* host = std::getenv("ECOMMERCE_MYSQL_TEST_HOST");
    if (host == nullptr || *host == '\0') {
        std::puts("mysql_transaction_test skipped: ECOMMERCE_MYSQL_TEST_HOST is not configured");
        return 0;
    }
    try {
        const auto suffix = std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
        const std::string sku_id = "integration-sku-" + suffix;
        live::storage::mysql::MySqlTransactionStore store;
        check(store.connect(host, static_cast<std::uint16_t>(std::strtoul(envOr("ECOMMERCE_MYSQL_TEST_PORT", "3306"), nullptr, 10)),
                            envOr("ECOMMERCE_MYSQL_TEST_USER", "ecommerce"),
                            envOr("ECOMMERCE_MYSQL_TEST_PASSWORD", "ecommerce"),
                            envOr("ECOMMERCE_MYSQL_TEST_DATABASE", "ecommerce")).ok(), "mysql connect");
        check(store.initializeSku(sku_id, 2).ok(), "mysql inventory setup");
        check(store.initializeProduct(sku_id, "integration product", 19900).ok(), "mysql product setup");

        live::messaging::FlashSaleOrderMessage rejected_message;
        rejected_message.promotion_id = "mysql-rejected-promo";
        rejected_message.order_id = "mysql-rejected-order-" + suffix;
        rejected_message.idempotency_key = "mysql-rejected-idem-" + suffix;
        rejected_message.user_id = "mysql-rejected-user";
        rejected_message.sku_id = sku_id;
        rejected_message.quantity = 1;
        check(store.put({rejected_message.idempotency_key, rejected_message.user_id, rejected_message.order_id,
                         live::business::flash_sale::FlashSaleRequestState::kRejected, "rejected before enqueue"}).ok(),
              "mysql rejected request setup");
        live::business::order::Order rejected_order;
        check(!store.persistQueuedOrder(rejected_message, &rejected_order).ok(),
              "late command for a rejected request must not create an order");
        live::business::inventory::StockSnapshot rejected_stock;
        check(store.queryStock(sku_id, &rejected_stock).ok() && rejected_stock.available == 2 &&
              rejected_stock.reserved == 0 && rejected_stock.sold == 0,
              "rejected request must not change durable inventory");

        live::messaging::FlashSaleOrderMessage message;
        message.promotion_id = "mysql-promo";
        message.order_id = "mysql-flash-order-" + suffix;
        message.idempotency_key = "mysql-flash-idem-" + suffix;
        message.user_id = "mysql-user";
        message.sku_id = sku_id;
        message.quantity = 1;
        check(store.put({message.idempotency_key, message.user_id, message.order_id,
                         live::business::flash_sale::FlashSaleRequestState::kQueued, "queued"}).ok(),
              "mysql request status setup");
        bool request_transitioned = false;
        check(store.transition(message.idempotency_key, live::business::flash_sale::FlashSaleRequestState::kQueued,
                              live::business::flash_sale::FlashSaleRequestState::kSubmitting, &request_transitioned).ok() &&
              request_transitioned, "mysql compare-and-set request submission claim");
        check(store.transition(message.idempotency_key, live::business::flash_sale::FlashSaleRequestState::kSubmitting,
                              live::business::flash_sale::FlashSaleRequestState::kQueued, &request_transitioned).ok() &&
              request_transitioned, "mysql compare-and-set request queue acknowledgement");
        check(store.transition(message.idempotency_key, live::business::flash_sale::FlashSaleRequestState::kSubmitting,
                              live::business::flash_sale::FlashSaleRequestState::kRejected, &request_transitioned).ok() &&
              !request_transitioned, "mysql request status compare-and-set rejects stale transition");

        live::business::order::Order first;
        check(store.persistQueuedOrder(message, &first).ok(), "mysql flash-sale persistence");
        live::business::flash_sale::FlashSaleRequestStatus pending_status;
        check(store.get(message.idempotency_key, &pending_status).ok() &&
              pending_status.state == live::business::flash_sale::FlashSaleRequestState::kPendingPayment,
              "order and durable request state created together");
        live::business::order::Order duplicate;
        check(store.persistQueuedOrder(message, &duplicate).ok() && duplicate.id == first.id,
              "mysql flash-sale idempotency");
        live::business::order::Order loaded;
        check(store.getOrder(first.id, &loaded).ok() && loaded.sku_id == sku_id && loaded.quantity == 1 && loaded.unit_price_cents == 19900,
              "mysql order query");
        live::business::inventory::StockSnapshot stock;
        check(store.queryStock(sku_id, &stock).ok() && stock.available == 1 && stock.reserved == 1 && stock.sold == 0,
              "mysql inventory reservation mirror");
        live::business::order::Order paid;
        check(store.transitionOrder(first.id, live::business::order::OrderState::kPendingPayment,
            live::business::order::OrderState::kPaid, &paid).ok() && paid.state == live::business::order::OrderState::kPaid,
            "mysql payment transition");
        check(store.transitionOrder(first.id, live::business::order::OrderState::kPendingPayment,
            live::business::order::OrderState::kPaid, &paid).ok(), "mysql payment transition idempotency");
        live::business::flash_sale::FlashSaleRequestStatus request_status;
        check(store.get(message.idempotency_key, &request_status).ok() &&
              request_status.state == live::business::flash_sale::FlashSaleRequestState::kPaid,
              "mysql request state transaction");
        check(store.queryStock(sku_id, &stock).ok() && stock.available == 1 && stock.reserved == 0 && stock.sold == 1,
              "mysql paid inventory ledger");
        std::vector<live::business::order::Order> claimed_expired;
        check(store.claimExpiredOrders("integration-expiry-worker", 10, &claimed_expired).ok() && claimed_expired.empty(),
              "mysql distributed expiry claim query");
        int published = 0;
        check(store.drainOutbox([&published](const live::messaging::Event&) {
            ++published;
            return live::common::Status::Ok();
        }).ok() && published == 2, "mysql outbox drain");
        message.order_id = "mysql-callback-order-" + suffix;
        message.idempotency_key = "mysql-callback-idem-" + suffix;
        check(store.put({message.idempotency_key, message.user_id, message.order_id,
                         live::business::flash_sale::FlashSaleRequestState::kQueued, "queued"}).ok(),
              "mysql callback request setup");
        live::business::order::Order callback_order;
        check(store.persistQueuedOrder(message, &callback_order).ok(), "mysql callback order persistence");
        live::business::order::Order callback_paid;
        check(store.confirmPayment(callback_order.id, "mock", "provider-tx-" + suffix, 19900, &callback_paid).ok() &&
              callback_paid.state == live::business::order::OrderState::kPaid, "mysql signed payment callback transition");
        check(store.confirmPayment(callback_order.id, "mock", "provider-tx-" + suffix, 19900, &callback_paid).ok(),
              "mysql payment callback idempotency");
        check(!store.confirmPayment(callback_order.id, "mock", "different-provider-tx", 19900, &callback_paid).ok(),
              "different callback transaction must not replace an existing payment");
        int callback_events = 0;
        check(store.drainOutbox([&callback_events](const live::messaging::Event&) {
            ++callback_events;
            return live::common::Status::Ok();
        }).ok() && callback_events == 2, "mysql callback outbox events");
        const std::string campaign_id = "integration-campaign-" + suffix;
        const std::string campaign_sku = "integration-campaign-sku-" + suffix;
        check(store.initializeSku(campaign_sku, 10).ok(), "mysql campaign allocation stock setup");
        const std::int64_t campaign_start = 1900000000;
        const std::int64_t campaign_end = 1900003600;
        check(store.create({campaign_id, campaign_sku, 6, live::business::promotion::PromotionState::kCreated,
                            campaign_start, campaign_end}, "integration-operator").ok(),
              "mysql campaign create and audit");
        check(!store.create({campaign_id + "-overflow", campaign_sku, 5,
                             live::business::promotion::PromotionState::kCreated, campaign_start, campaign_end},
                            "integration-operator").ok(), "mysql prevents aggregate campaign quota over-allocation");
        live::messaging::FlashSaleOrderMessage campaign_order_message;
        campaign_order_message.promotion_id = campaign_id;
        campaign_order_message.order_id = "mysql-campaign-order-" + suffix;
        campaign_order_message.idempotency_key = "mysql-campaign-idem-" + suffix;
        campaign_order_message.user_id = "mysql-campaign-user";
        campaign_order_message.sku_id = campaign_sku;
        campaign_order_message.quantity = 1;
        check(store.put({campaign_order_message.idempotency_key, campaign_order_message.user_id,
                         campaign_order_message.order_id,
                         live::business::flash_sale::FlashSaleRequestState::kQueued, "queued"}).ok(),
              "mysql campaign request setup");
        live::business::order::Order campaign_order;
        check(store.persistQueuedOrder(campaign_order_message, &campaign_order).ok(),
              "mysql campaign order reservation");
        live::business::order::Order expired_campaign_order;
        check(store.transitionOrder(campaign_order.id, live::business::order::OrderState::kPendingPayment,
                                    live::business::order::OrderState::kExpired, &expired_campaign_order).ok(),
              "mysql campaign order releases reservation");

        // Once the original order expires, its released quota can be resized
        // before allocating the remainder to another campaign.
        check(store.update({campaign_id, campaign_sku, 6, live::business::promotion::PromotionState::kCreated, campaign_start, campaign_end},
                           {campaign_id, campaign_sku, 7, live::business::promotion::PromotionState::kCreated, campaign_start, campaign_end},
                           "integration-operator").ok(), "mysql campaign update and audit");
        check(store.create({campaign_id + "-after-release", campaign_sku, 3,
                            live::business::promotion::PromotionState::kCreated, campaign_start, campaign_end},
                           "integration-operator").ok(), "released campaign quota may be allocated again");
        check(!store.create({campaign_id + "-no-free-quota", campaign_sku, 1,
                             live::business::promotion::PromotionState::kCreated, campaign_start, campaign_end},
                            "integration-operator").ok(),
              "expired order must not permanently free campaign quota beyond actual inventory");
        std::vector<live::business::promotion::Promotion> campaigns;
        check(store.list(&campaigns).ok(), "mysql campaign list");
        bool found_campaign = false;
        for (const auto& campaign : campaigns) found_campaign = found_campaign ||
            (campaign.id == campaign_id && campaign.starts_at == campaign_start && campaign.ends_at == campaign_end);
        check(found_campaign, "mysql campaign restore snapshot");
        check(store.transition(campaign_id, live::business::promotion::PromotionState::kCreated,
                               live::business::promotion::PromotionState::kPreheating, "integration-operator").ok(),
              "mysql campaign transition and audit");
        check(store.list(&campaigns).ok(), "mysql campaign re-list");
        bool state_updated = false;
        for (const auto& campaign : campaigns) if (campaign.id == campaign_id)
            state_updated = campaign.state == live::business::promotion::PromotionState::kPreheating;
        check(state_updated, "mysql campaign state persistence");
        std::vector<live::business::promotion::AdminAuditRecord> audit_records;
        check(store.listAudit(100, &audit_records).ok() && !audit_records.empty(), "mysql management audit query");
        check(store.recordAudit("integration-admin", "INVENTORY_RECONCILIATION", "inventory", campaign_sku,
                                "", "checked=1,repaired=0").ok(), "mysql reconciliation audit write");
        check(store.listAudit(100, &audit_records).ok(), "mysql audit query after reconciliation");
        bool found_reconciliation_audit = false;
        for (const auto& record : audit_records) found_reconciliation_audit = found_reconciliation_audit ||
            (record.actor == "integration-admin" && record.action == "INVENTORY_RECONCILIATION" && record.resource_id == campaign_sku);
        check(found_reconciliation_audit, "mysql reconciliation audit is queryable");
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "mysql_transaction_test failed: %s\n", error.what());
        return 1;
    }
}
