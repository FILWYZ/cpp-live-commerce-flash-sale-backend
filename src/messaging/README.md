# messaging

负责秒杀命令异步化和业务事件可靠投递。模块包含内存、Redis List、Kafka/Redpanda 适配器，以及可选的 RocketMQ 5.x C++ SDK 适配器。RocketMQ adapter 通过同步发送等待 Broker 返回结果，SimpleConsumer 在业务处理成功后显式 ACK；必须安装 Apache `rocketmq` CMake package 才会编译该适配器。内存 Broker 只供测试，Redis List 适合本地/开发联调，不具备消费者确认和崩溃恢复语义。

## 事件与可靠性

- 事件：`OrderCreated`、`StockReserved`、`OrderPaid`、`OrderCancelled`、`PromotionFinished`。
- 订单写库与事件发送之间采用 Transactional Outbox 思路。
- `AsyncOutboxDispatcher` 在后台线程 drain `FileOutbox`，避免 Kafka/下游发布延迟阻塞 HTTP 请求线程；失败记录保留并在重试或重启后继续处理。

Redis List 队列的运营 DLQ 接口可查看消息解析结果或原始损坏 payload，并通过 Redis Lua 在同一 hash slot 内有界地从 DLQ 原子移动到命令队列；回放前校验最旧消息格式，目标队列满时不丢消息。Kafka DLQ 查看使用临时独立 consumer group，不推进业务 consumer group offset；Kafka 重放按数量限制消费并在目标 Topic ACK 后提交临时组 offset。RocketMQ 5.x 使用普通应用 DLQ Topic（默认 `flashsale_dlq`），因为 RocketMQ 5.3.2 Proxy 对 C++ SDK 读取 Broker 系统 `%DLQ%<consumer-group>` 返回 `Action is not supported`。应用重试耗尽后，消费者先发布至应用 DLQ Topic，再 ACK 原命令；管理接口保留读取的投递句柄，有界重放只重发格式有效的秒杀命令，目标命令 Topic Broker ACK 后才 ACK 应用 DLQ 消息。应用 DLQ 故障注入、查询和重放已由独立脚本实测；Broker 原生 DLQ 仍是明确边界。
- 投递语义按至少一次设计，消费者必须以 `event_id`/业务幂等键去重。
- RocketMQ 命令消息使用 `request_id` 作为消息 Key；消费成功后 ACK，失败时先执行 5 次本地重试，耗尽后投递应用 DLQ Topic 并 ACK 原消息。应用 DLQ 重放按至少一次处理设计，消费者通过 request_id 幂等吸收重复命令。Broker 系统 DLQ 的查询/重放不由当前 Proxy SDK 路径提供。
- MySQL Outbox Dispatcher 用 `FOR UPDATE SKIP LOCKED` 领取记录并设置 30 秒租约，Broker 投递成功后才标记 published；Dispatcher 崩溃可能造成重复发布，下游必须按事件 ID 幂等。
- `InMemoryBroker` 支持按订阅配置最大投递次数；超过次数的事件转入订阅级死信主题（默认 `<topic>.DLQ`），并保留原始事件标识与失败上下文，便于测试故障恢复和后续人工补偿。
- Kafka/Redpanda Consumer 保留已有 Consumer Group、失败重试与 DLQ 工具。默认 Docker Compose 尚未构建 RocketMQ C++ SDK，因此仍使用 Redis List 队列进行开发联调；生产/故障演练需使用已安装 SDK 的构建并配置 RocketMQ 5.x gRPC Proxy。整个系统按至少一次处理设计，不宣称恰好一次。
