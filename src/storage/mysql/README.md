# MySQL 持久化契约

本目录只定义秒杀消费后台需要落库的交易账本和 Outbox 边界。LocalKV/WAL 用于零依赖单机运行，MySQL 适配用于多实例和真实依赖联调。

必须落库的关键约束：

- `orders.idempotency_key` 唯一，防止重复消费创建订单。
- 订单状态从 `PENDING_PAYMENT` 通过条件更新迁移到 `PAID`、`EXPIRED` 或 `CANCELLED`；支付和关单并发时只有一个迁移能成功。
- `inventory(sku_id)` 在落单事务中将可用库存转为预留；支付/关单事务同步更新库存镜像和 Outbox，数据库路径不能绕过库存账本。
- `order_items.unit_price_cents` 保存下单价格快照；`payments.provider_transaction_id` 唯一，模拟回调可安全重放。
- `flash_sale_requests.request_id` 持久化 QUEUED/PROCESSING/SOLD_OUT/PENDING_PAYMENT 等受理状态，供用户按幂等键查询。
- `campaigns` 持久化活动 SKU、参与配额、Unix 秒时间窗和生命周期状态；活动状态变更与 `admin_audit_logs` 操作记录在同一 MySQL 事务中。服务启动恢复活动时同步 Redis Admission Gate 的状态。
- `orders.campaign_id` 和配额剩余量查询与活动 SKU 配额行锁共同防止活动间重复分配；`PENDING_PAYMENT` 和 `PAID` 订单占用活动配额，`EXPIRED`/`CANCELLED` 的库存释放后配额可再次分配。
- 管理审计记录 actor、操作、资源、变更前后值；管理凭证可配置 viewer/operator/admin 角色，活动写操作使用凭证 actor。
- `outbox_events.event_id` 唯一，消费者按事件 ID 幂等。
- Outbox Dispatcher 用行锁、`SKIP LOCKED` 和带过期时间的领取租约协调多实例；Broker ACK 后才标记 published，失败记录带重试次数与错误。
- 超时关单任务按 `expiry_claimed_by`/`expiry_lease_until` 领取订单；MySQL 事务用 `FOR UPDATE SKIP LOCKED` 分配任务，崩溃后租约过期可重新领取，订单终态迁移时清除租约。

`schema.sql` 由 Compose 在 MySQL 数据卷首次初始化时执行。升级已有开发数据卷时，需要备份后应用对应 DDL；修改初始化脚本不会自动迁移已有表。

`campaigns.starts_at/ends_at` 与 `orders.campaign_id` 是本轮新增字段；新数据库由 `schema.sql` 建立。旧开发数据库必须停写后按一次性脚本 [`migrations/001_campaign_windows_and_allocations.sql`](migrations/001_campaign_windows_and_allocations.sql) 升级。迁移前创建的订单没有 campaign_id，脚本不会推断历史订单归属；如需延续旧活动，应先结束旧活动或核对剩余配额后再启用新的活动配额分配。
