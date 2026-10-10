# 一致性与故障恢复

## 库存不变量

对每个活动 SKU，必须始终满足：

```text
available >= 0
reserved >= 0
sold >= 0
available + reserved + sold = total
```

Redis Lua 使用操作 ID 原子预留库存。MySQL 在同一事务中保存订单和持久化库存账本。Redis 是快速预留路径的状态投影，不是持久化订单账本。

## 命令处理

```text
RabbitMQ 投递
  -> 按 request_id 检查幂等性
  -> Redis Lua 预留库存
  -> MySQL 事务（订单 + 库存账本 + 请求状态 + Outbox）
  -> COMMIT
  -> 手动 ACK
```

如果进程在事务提交前退出，RabbitMQ 会重新投递消息。如果进程在事务提交后、ACK 前退出，相同的 `request_id` 和 MySQL 唯一约束会使重复事务返回已有订单，不会创建第二笔有效订单。

临时错误会在有限次数内重试。重试耗尽后，命令会先写入持久化应用死信队列，再对原消息发送 ACK。释放 Redis 预留前会先检查持久化订单状态；数据库结果不确定时，预留会保留到后续对账处理。

## Outbox 事件发布

订单事务会同时写入 Outbox 行。Reconciler 使用 Publisher Confirm 发布消息，再将记录标记为已发布。如果在确认成功和更新 MySQL 标记之间发生崩溃，事件可能重复发布；下游消费者应根据 `event_id` 去重。

## 支付状态

目前只提供模拟的 `/orders/pay` 和 `/orders/cancel` 接口。支付和超时状态迁移使用条件更新；支付会确认预留库存，取消或超时会释放库存。重复库存状态变更通过订单/预留 ID 保证幂等。
