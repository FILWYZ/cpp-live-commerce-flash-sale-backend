#include "business/flash_sale/flash_sale_order_consumer.h"
#include "business/flash_sale/flash_sale_order_service.h"
#include "business/inventory/inventory_gateway.h"
#include "business/inventory/inventory_service.h"
#include "business/order/order_service.h"
#include "business/promotion/promotion_service.h"
#include "messaging/order_queue.h"
#include "storage/local_engine/local_kv_engine.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

class FakeRepository final : public live::business::flash_sale::IFlashSaleOrderRepository {
public:
    live::common::Status persistQueuedOrder(const live::messaging::FlashSaleOrderMessage& message,
                                            live::business::order::Order* order) override {
        if (order == nullptr) return live::common::Status::InvalidArgument("order output missing");
        const auto found = orders.find(message.idempotency_key);
        if (found != orders.end()) {
            *order = found->second;
            return live::common::Status::Ok();
        }
        *order = {message.order_id, message.idempotency_key, message.user_id, message.sku_id,
                  message.quantity, live::business::order::OrderState::kPendingPayment};
        orders.emplace(message.idempotency_key, *order);
        ++persisted;
        return live::common::Status::Ok();
    }
    live::common::Status getOrder(const std::string& order_id,
                                  live::business::order::Order* order) const override {
        for (const auto& [key, value] : orders) {
            (void)key;
            if (value.id == order_id) { *order = value; return live::common::Status::Ok(); }
        }
        return live::common::Status::NotFound("order not found");
    }
    std::atomic<int> persisted{0};
    std::unordered_map<std::string, live::business::order::Order> orders;
};

class FailingQueue final : public live::messaging::IOrderQueue {
public:
    live::common::Status push(const live::messaging::FlashSaleOrderMessage& message) override {
        return fail ? live::common::Status::Internal("injected queue failure") : accepted.push(message);
    }
    live::common::Status pop(live::messaging::FlashSaleOrderMessage* message, std::chrono::milliseconds timeout) override {
        return accepted.pop(message, timeout);
    }
    std::size_t pending() const override { return accepted.pending(); }
    bool fail{true};
private:
    live::messaging::InMemoryOrderQueue accepted;
};

class AckFailureQueue final : public live::messaging::IOrderQueue {
public:
    live::common::Status push(const live::messaging::FlashSaleOrderMessage& message) override {
        return accepted.push(message);
    }
    live::common::Status pop(live::messaging::FlashSaleOrderMessage* message,
                             std::chrono::milliseconds timeout) override {
        return accepted.pop(message, timeout);
    }
    live::common::Status acknowledge(const live::messaging::FlashSaleOrderMessage& message,
                                     bool processed) override {
        if (processed && fail_next_ack.exchange(false)) {
            (void)accepted.push(message);  // Models RabbitMQ redelivery after commit but before ACK.
            return live::common::Status::Internal("injected lost ACK");
        }
        return live::common::Status::Ok();
    }
    std::size_t pending() const override { return accepted.pending(); }
    std::atomic<bool> fail_next_ack{true};
private:
    live::messaging::InMemoryOrderQueue accepted;
};

class FakePromotionRepository final : public live::business::promotion::IPromotionRepository {
public:
    live::common::Status create(const live::business::promotion::Promotion& promotion, const std::string&) override {
        rows.push_back(promotion);
        return live::common::Status::Ok();
    }
    live::common::Status update(const live::business::promotion::Promotion& previous,
                                const live::business::promotion::Promotion& next, const std::string& actor) override {
        for (auto& row : rows) if (row.id == previous.id && row.state == previous.state) {
            row = next;
            last_actor = actor;
            return live::common::Status::Ok();
        }
        return live::common::Status::Aborted("campaign update conflict");
    }
    live::common::Status list(std::vector<live::business::promotion::Promotion>* promotions) const override {
        *promotions = rows;
        return live::common::Status::Ok();
    }
    live::common::Status transition(const std::string& id, live::business::promotion::PromotionState expected,
                                    live::business::promotion::PromotionState next, const std::string& actor) override {
        for (auto& row : rows) if (row.id == id && row.state == expected) {
            row.state = next;
            last_actor = actor;
            ++transitions;
            return live::common::Status::Ok();
        }
        return live::common::Status::Aborted("state conflict");
    }
    live::common::Status listAudit(std::size_t, std::vector<live::business::promotion::AdminAuditRecord>* records) const override {
        records->clear();
        return live::common::Status::Ok();
    }
    std::vector<live::business::promotion::Promotion> rows;
    mutable std::string last_actor;
    int transitions{0};
};

}  // namespace

int main() {
    live::business::flash_sale::InMemoryFlashSaleRequestStatusStore fast_status;
    live::business::flash_sale::InMemoryFlashSaleRequestStatusStore durable_status;
    check(durable_status.put({"durable-key", "owner", "order", live::business::flash_sale::FlashSaleRequestState::kPendingPayment, "durable"}).ok(),
          "durable request status seed");
    live::business::flash_sale::ReadThroughFlashSaleRequestStatusStore read_through(&fast_status, &durable_status);
    live::business::flash_sale::FlashSaleRequestStatus read_status;
    check(read_through.get("durable-key", &read_status).ok() &&
          read_status.state == live::business::flash_sale::FlashSaleRequestState::kPendingPayment,
          "gateway status lookup falls back to durable store");
    check(read_through.put({"fast-key", "owner", "order-2", live::business::flash_sale::FlashSaleRequestState::kQueued, "queued"}).ok() &&
          fast_status.get("fast-key", &read_status).ok() && !durable_status.get("fast-key", &read_status).ok(),
          "gateway status write only touches the fast store");
    check(fast_status.put({"durable-key", "owner", "order", live::business::flash_sale::FlashSaleRequestState::kPaid, "stale cache"}).ok() &&
          read_through.get("durable-key", &read_status).ok() &&
          read_status.state == live::business::flash_sale::FlashSaleRequestState::kPendingPayment,
          "durable terminal state reconciliation overrides stale terminal cache");
    std::atomic<int> status_claim_winners{0};
    std::vector<std::thread> status_claimers;
    for (int i = 0; i < 32; ++i) {
        status_claimers.emplace_back([&, i] {
            bool inserted = false;
            check(fast_status.putIfAbsent({"atomic-claim", "owner-" + std::to_string(i), "order",
                live::business::flash_sale::FlashSaleRequestState::kQueued, "claim"}, &inserted).ok(),
                "atomic request status claim");
            if (inserted) ++status_claim_winners;
        });
    }
    for (auto& claimer : status_claimers) claimer.join();
    check(status_claim_winners.load() == 1, "request idempotency claim must have exactly one winner");

    FakePromotionRepository promotion_repository;
    promotion_repository.rows.push_back({"restored", "restored-sku", 3, live::business::promotion::PromotionState::kPaused});
    live::business::promotion::PromotionService restored_promotions(std::make_unique<live::business::promotion::FlashSaleAdmissionGate>(), &promotion_repository);
    check(restored_promotions.restore().ok(), "promotion state restore");
    live::business::promotion::AdmissionDecision restored_decision;
    check(restored_promotions.admissionGate()->tryAcquire("restored", "user", &restored_decision).ok() &&
          restored_decision.code == live::business::promotion::AdmissionCode::kPaused,
          "restored paused campaign must reject admission");
    check(restored_promotions.resume("restored", "operator-1").ok() && promotion_repository.last_actor == "operator-1",
          "campaign lifecycle transition should persist the actor");
    live::business::promotion::Promotion editable{"draft", "sku-old", 2};
    check(restored_promotions.create(editable, "operator-1").ok(), "promotion create persistence");
    editable.sku_id = "sku-new";
    editable.participant_limit = 4;
    check(restored_promotions.update(editable, "operator-2").ok() && promotion_repository.last_actor == "operator-2",
          "created promotion can be updated with audit actor");
    check(restored_promotions.preheat("draft").ok() &&
          !restored_promotions.update(editable, "operator-2").ok(),
          "started promotions must not be editable");

    const auto root = std::filesystem::temp_directory_path() / "flash_sale_pipeline_test";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    live::storage::LocalKVEngine storage;
    check(storage.open((root / "snapshot").string(), (root / "wal").string()).ok(), "storage open");
    live::business::inventory::InventoryService inventory(&storage);
    check(inventory.addSku("flash-sku", 100).ok(), "inventory setup");
    live::business::inventory::LocalInventoryGateway inventory_gateway(&inventory);

    live::business::promotion::PromotionService promotion;
    check(promotion.create({"flash-1", "flash-sku", 100}).ok(), "promotion setup");
    check(promotion.preheat("flash-1").ok(), "promotion preheat");
    check(promotion.start("flash-1").ok(), "promotion start");

    const auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    check(promotion.create({"scheduled", "flash-sku", 10, live::business::promotion::PromotionState::kCreated,
                            now_seconds + 3600, now_seconds + 7200}).ok(), "scheduled campaign setup");
    check(promotion.preheat("scheduled").ok() && promotion.start("scheduled").ok(), "scheduled campaign activation");
    check(promotion.create({"expired-window", "flash-sku", 10, live::business::promotion::PromotionState::kCreated,
                            now_seconds - 7200, now_seconds - 3600}).ok(), "expired campaign setup");
    check(promotion.preheat("expired-window").ok() && promotion.start("expired-window").ok(), "expired campaign activation");

    live::messaging::InMemoryOrderQueue queue;
    live::business::flash_sale::InMemoryFlashSaleRequestStatusStore request_status;
    live::business::flash_sale::FlashSaleOrderService service(promotion.admissionGate(), &inventory_gateway, &queue, &request_status, &promotion);
    live::business::flash_sale::FlashSaleOrderService scheduled_service(
        promotion.admissionGate(), &inventory_gateway, &queue, &request_status, &promotion);
    live::business::flash_sale::FlashSaleSubmitResult scheduled_result;
    check(scheduled_service.submit({"scheduled", "flash-sku", "early-user", "early-key", 1}, &scheduled_result).ok() &&
          scheduled_result.code == live::business::flash_sale::SubmitCode::kNotStarted,
          "future campaign must reject requests before its start time");
    check(scheduled_service.submit({"expired-window", "flash-sku", "late-user", "late-key", 1}, &scheduled_result).ok() &&
          scheduled_result.code == live::business::flash_sale::SubmitCode::kFinished,
          "expired campaign must reject requests after its end time");
    live::business::flash_sale::FlashSaleSubmitResult invalid_campaign_order;
    check(!service.submit({"flash-1", "different-sku", "wrong-sku-user", "wrong-sku-key", 1}, &invalid_campaign_order).ok(),
          "campaign must reject a SKU that is not configured for it");
    check(!service.submit({"flash-1", "flash-sku", "multi-user", "multi-key", 2}, &invalid_campaign_order).ok(),
          "campaign must enforce the one-unit per-user limit");
    std::atomic<int> queued{0};
    std::vector<std::thread> workers;
    for (int i = 0; i < 1000; ++i) {
        workers.emplace_back([&, i] {
            live::business::flash_sale::FlashSaleSubmitResult result;
            const auto status = service.submit({"flash-1", "flash-sku", "user-" + std::to_string(i),
                                                "idem-" + std::to_string(i), 1}, &result);
            check(status.ok(), "flash-sale submit");
            if (result.code == live::business::flash_sale::SubmitCode::kQueued) ++queued;
        });
    }
    for (auto& worker : workers) worker.join();
    check(queued.load() == 100, "flash-sale quota");
    check(queue.pending() == 100, "flash-sale queue size");
    live::business::flash_sale::FlashSaleRequestStatus queued_status;
    std::string queued_id;
    std::string queued_user;
    std::string queued_order_id;
    std::string cancelled_id;
    for (int i = 0; i < 1000 && queued_id.empty(); ++i) {
        const auto candidate = "idem-" + std::to_string(i);
        if (request_status.get(candidate, &queued_status).ok() &&
            queued_status.state == live::business::flash_sale::FlashSaleRequestState::kQueued) {
            queued_id = candidate;
            queued_user = "user-" + std::to_string(i);
            queued_order_id = queued_status.order_id;
        }
    }
    for (int i = 0; i < 1000 && cancelled_id.empty(); ++i) {
        const auto candidate = "idem-" + std::to_string(i);
        if (candidate != queued_id && request_status.get(candidate, &queued_status).ok() &&
            queued_status.state == live::business::flash_sale::FlashSaleRequestState::kQueued) cancelled_id = candidate;
    }
    check(!queued_id.empty() && !cancelled_id.empty(),
          "request should be queryable after queue acceptance");
    live::business::flash_sale::FlashSaleSubmitResult duplicate_request;
    check(service.submit({"flash-1", "flash-sku", queued_user, queued_id, 1}, &duplicate_request).ok() &&
          duplicate_request.code == live::business::flash_sale::SubmitCode::kDuplicate &&
          duplicate_request.order_id == queued_order_id,
          "replayed idempotency key should return the original request");

    FakeRepository repository;
    live::business::flash_sale::FlashSaleOrderConsumer consumer(
        &queue, nullptr, &repository, &inventory_gateway, promotion.admissionGate(), 5, 1, &request_status);
    while (queue.pending() != 0) check(consumer.processOnce(std::chrono::milliseconds(10)).ok(), "consumer process");
    live::business::inventory::StockSnapshot stock;
    check(inventory.queryStock("flash-sku", &stock).ok(), "stock query");
    check(stock.available == 0 && stock.reserved == 100 && stock.sold == 0, "pending-payment inventory reservation");
    check(repository.persisted.load() == 100, "consumer persistence");

    check(inventory.addSku("ack-window-sku", 1).ok(), "ack-window inventory setup");
    live::business::promotion::PromotionService ack_window_promotion;
    check(ack_window_promotion.create({"ack-window-campaign", "ack-window-sku", 1}).ok() &&
          ack_window_promotion.preheat("ack-window-campaign").ok() &&
          ack_window_promotion.start("ack-window-campaign").ok(), "ack-window campaign setup");
    AckFailureQueue ack_failure_queue;
    live::messaging::FlashSaleOrderMessage ack_window_message;
    ack_window_message.promotion_id = "ack-window-campaign";
    ack_window_message.order_id = "ack-window-order";
    ack_window_message.idempotency_key = "ack-window-request";
    ack_window_message.user_id = "ack-window-user";
    ack_window_message.sku_id = "ack-window-sku";
    ack_window_message.quantity = 1;
    check(ack_failure_queue.push(ack_window_message).ok(), "ack-window message enqueue");
    const int persisted_before_ack_retry = repository.persisted.load();
    live::business::flash_sale::FlashSaleOrderConsumer ack_window_consumer(
        &ack_failure_queue, nullptr, &repository, &inventory_gateway, ack_window_promotion.admissionGate(),
        5, 1, &request_status);
    check(!ack_window_consumer.processOnce(std::chrono::milliseconds(10)).ok() &&
          repository.persisted.load() == persisted_before_ack_retry + 1 && ack_failure_queue.pending() == 1,
          "order commit survives a lost broker ACK and command is redelivered");
    check(ack_window_consumer.processOnce(std::chrono::milliseconds(10)).ok() &&
          repository.persisted.load() == persisted_before_ack_retry + 1 && ack_failure_queue.pending() == 0,
          "redelivery after commit is idempotent and acknowledged");
    live::business::inventory::StockSnapshot ack_window_stock;
    check(inventory.queryStock("ack-window-sku", &ack_window_stock).ok() &&
          ack_window_stock.available == 0 && ack_window_stock.reserved == 1 && ack_window_stock.sold == 0,
          "lost ACK redelivery preserves the inventory invariant without a duplicate reservation");

    live::messaging::InMemoryOrderQueue rejected_queue;
    live::business::flash_sale::InMemoryFlashSaleRequestStatusStore rejected_status;
    live::messaging::FlashSaleOrderMessage late_rejected_message;
    late_rejected_message.promotion_id = "flash-1";
    late_rejected_message.order_id = "late-rejected-order";
    late_rejected_message.idempotency_key = "late-rejected-request";
    late_rejected_message.user_id = "late-rejected-user";
    late_rejected_message.sku_id = "flash-sku";
    late_rejected_message.quantity = 1;
    check(rejected_status.put({late_rejected_message.idempotency_key, late_rejected_message.user_id,
          late_rejected_message.order_id, live::business::flash_sale::FlashSaleRequestState::kRejected,
          "request was rejected before a delayed command arrived"}).ok(), "terminal rejection status setup");
    check(rejected_queue.push(late_rejected_message).ok(), "late rejected command enqueue");
    live::business::flash_sale::FlashSaleOrderConsumer rejected_consumer(
        &rejected_queue, nullptr, &repository, &inventory_gateway, promotion.admissionGate(), 5, 1, &rejected_status);
    check(rejected_consumer.processOnce(std::chrono::milliseconds(10)).ok() && rejected_queue.pending() == 0 &&
          repository.persisted.load() == persisted_before_ack_retry + 1,
          "consumer acknowledges a late command without processing a terminal rejected request");
    check(request_status.get(queued_id, &queued_status).ok() &&
          queued_status.state == live::business::flash_sale::FlashSaleRequestState::kPendingPayment,
          "request should advance to pending-payment state");
    live::business::inventory::InventoryReservationResult inventory_result;
    check(inventory_gateway.confirm("flash-sku", queued_status.order_id, &inventory_result).ok() &&
          inventory_result.decision == live::business::inventory::InventoryDecision::kSuccess,
          "payment should confirm the held reservation");
    live::business::flash_sale::FlashSaleRequestStatus cancelled_request;
    check(request_status.get(cancelled_id, &cancelled_request).ok(), "second pending request lookup");
    check(inventory_gateway.release("flash-sku", cancelled_request.order_id, &inventory_result).ok() &&
          inventory_result.decision == live::business::inventory::InventoryDecision::kSuccess,
          "cancel should release the held reservation");
    check(inventory.queryStock("flash-sku", &stock).ok() &&
          stock.available == 1 && stock.reserved == 98 && stock.sold == 1,
          "payment and cancellation inventory totals");

    live::business::promotion::PromotionService compensation_promotion;
    check(compensation_promotion.create({"flash-2", "flash-sku", 1}).ok(), "compensation promotion setup");
    check(compensation_promotion.preheat("flash-2").ok(), "compensation promotion preheat");
    check(compensation_promotion.start("flash-2").ok(), "compensation promotion start");
    check(compensation_promotion.pause("flash-2").ok(), "promotion pause");
    live::business::promotion::AdmissionDecision paused_admission;
    check(compensation_promotion.admissionGate()->tryAcquire("flash-2", "paused-user", &paused_admission).ok() &&
          paused_admission.code == live::business::promotion::AdmissionCode::kPaused,
          "paused promotion should reject admission");
    check(compensation_promotion.resume("flash-2").ok(), "promotion resume");
    FailingQueue failing_queue;
    live::business::flash_sale::InMemoryFlashSaleRequestStatusStore recovery_status;
    live::business::flash_sale::FlashSaleOrderService failing_service(
        compensation_promotion.admissionGate(), &inventory_gateway, &failing_queue, &recovery_status, &compensation_promotion);
    live::business::flash_sale::FlashSaleSubmitResult failed;
    const auto status = failing_service.submit({"flash-2", "flash-sku", "rollback-user", "rollback-idem", 1}, &failed);
    check(!status.ok(), "queue failure should be visible");
    live::business::flash_sale::FlashSaleRequestStatus recovery_request;
    check(recovery_status.get("rollback-idem", &recovery_request).ok() &&
          recovery_request.state == live::business::flash_sale::FlashSaleRequestState::kSubmitting,
          "failed queue send leaves a recoverable idempotency claim");
    failing_queue.fail = false;
    live::business::flash_sale::FlashSaleSubmitResult recovered;
    check(failing_service.submit({"flash-2", "flash-sku", "rollback-user", "rollback-idem", 1}, &recovered).ok() &&
          recovered.code == live::business::flash_sale::SubmitCode::kQueued && failing_queue.pending() == 1,
          "same idempotency key retries a previously failed queue send");
    check(recovery_status.get("rollback-idem", &recovery_request).ok() &&
          recovery_request.state == live::business::flash_sale::FlashSaleRequestState::kQueued,
          "successful retry advances the submission state atomically");
    check(inventory.queryStock("flash-sku", &stock).ok(), "compensated stock query");
    check(stock.available == 1 && stock.reserved == 98 && stock.sold == 1, "compensated stock invariant");
    check(inventory.validateInvariants().ok(), "compensated inventory invariant");

    std::filesystem::remove_all(root);
    std::cout << "flash_sale_pipeline_test passed\n";
    return 0;
}
