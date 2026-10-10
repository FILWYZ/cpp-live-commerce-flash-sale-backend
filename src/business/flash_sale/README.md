# flash_sale

秒杀入口和消费编排：入口完成活动准入、请求幂等登记并发布抢购命令；状态按 `SUBMITTING -> QUEUED -> PROCESSING` 前进，队列写失败时同一 request_id 可重试，队列 ACK 后的状态 CAS 不会覆盖已被消费者推进的状态。消费者幂等预留库存、创建待支付订单并处理失败重试和 DLQ。订单保持库存预留直到模拟支付确认、取消或超时关单；状态迁移与持久化库存镜像在 MySQL 事务内完成，再对 Redis 执行幂等确认/释放。

Redis 加共享命令队列模式下，全局或活动速率超限的合格请求可进入 Redis FIFO 等待室，状态为 `WAITING`；后台 worker 按共享令牌预算放行。用户和 IP 限额直接拒绝，等待室满返回 429。

请求状态接口支持内存、Redis KV 和 MySQL 实现。MySQL 可用时状态与订单均持久化，用户可用幂等键查询处理进度。

本地与 Redis List 队列仍用于开发联调。可选 RocketMQ 5.x C++ SDK adapter 使用同步 Broker 确认和 SimpleConsumer ACK/重投；Redis List 的 `BRPOP` 会在业务提交前删除消息，不能作为生产级可靠队列。RocketMQ adapter 仅在安装 SDK 并重新配置构建后可用。

当前活动规则固定为每位用户每个活动限购 1 件；下单服务会核对请求 SKU 与活动配置，拒绝跨活动 SKU 注入和 quantity 大于 1 的请求。要支持可配置的多件限购，需把活动限购计数加入 Redis 原子准入脚本，并与取消/过期释放规则一起设计，不能只放宽 HTTP 参数校验。
