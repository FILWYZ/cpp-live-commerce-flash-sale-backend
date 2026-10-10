# 消息传输模块

项目主线只使用 RabbitMQ 传递秒杀命令和 Outbox 事件。`RabbitMqOrderQueue` 声明持久化 direct exchange/queue，发送持久化消息并等待 Publisher Confirm；消费者使用 manual ACK。生产路径按至少一次语义设计，依靠 `request_id`/事件 ID 幂等吸收重复投递。

## 命令路径

- API 只在 Broker Confirm 成功后返回 `202`；连接或确认失败时 fail-closed。
- Consumer 成功处理并提交 MySQL 事务后 ACK；临时错误有限重试，失败消息进入应用 DLQ。
- DLQ 查询/重放受数量上限约束。消息回放仍可能重复，业务幂等是正确性保障。
- `InMemoryBroker` 只用于本地测试，不代表持久化消息语义。

## Outbox 事件

订单状态、库存账本、request status 和 Outbox 事件在同一 MySQL 事务中提交。Dispatcher 将未发布事件投递到 RabbitMQ 并确认后更新状态。若发布成功、标记前崩溃，事件可能再次投递，下游必须按稳定事件 ID 幂等处理。

## 支持范围

RabbitMQ 是唯一外部 MQ。旧 RocketMQ/Kafka/Redpanda 与 Redis List adapter 不属于当前主线；项目没有宣称恰好一次投递或 Broker 集群 HA。使用 `deploy/scripts/benchmark_rabbitmq.sh` 执行真实服务链路压测，使用 `rabbitmq_smoke_test` 验证确认、持久消息投递、manual ACK 和事件发布。
