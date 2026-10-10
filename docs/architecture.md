# 系统架构

## 进程划分

| 进程 | 职责 |
|---|---|
| `api_server` | HTTP/Reactor、身份认证、请求校验、令牌桶准入、幂等处理、RabbitMQ 发布和请求状态查询 |
| `order_consumer` | 消费命令、通过 Redis Lua 预留库存、执行 MySQL 订单事务、有界重试和命令死信队列处理 |
| `reconciler` | 处理订单超时状态迁移、库存对账，以及 MySQL Outbox 事件发布和重试 |

三个入口共享应用装配逻辑，并通过 `ECOMMERCE_PROCESS_ROLE` 固定各自职责。`ecommerce_api_demo` 作为单进程本地开发程序保留。

## 消息拓扑

- 直连交换机：`flashsale.direct`
- 命令队列：`flashsale.command.queue`，路由键为 `flashsale.command`
- 死信队列：`flashsale.command.dlq`，路由键为 `flashsale.command.dlq`
- Outbox 事件使用事件主题作为路由键。

命令队列和死信队列均为持久化队列，发布的消息设置为持久化。命令发布会等待 Publisher Confirm。订单处理完成并提交事务，或已持久化记录最终业务结果后，消费者才会发送 ACK。

## 数据归属

- MySQL 保存持久化订单、库存账本、请求状态和 Outbox 状态。
- Redis 保存快速准入限流、原子库存预留和短期请求状态缓存。
- RabbitMQ 保存处理中命令，并传递下游事件。

Redis 与 MySQL 不共享分布式事务。故障恢复依靠幂等操作和 Reconciler 对账。
