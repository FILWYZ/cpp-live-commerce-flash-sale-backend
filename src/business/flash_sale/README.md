# 秒杀订单处理模块（`flash_sale`）

秒杀入口与消费编排。API 完成活动准入、请求幂等登记和 RabbitMQ command 发布；只有 Publisher Confirm 成功后才返回 `202`。消费者处理命令、通过 Redis Lua 原子预留库存，并在 MySQL 事务中写入订单、持久化库存镜像、请求状态和 Outbox 事件。数据库事务提交后才 ACK RabbitMQ。

失败路径遵循至少一次语义：重试可能导致重复消费，因此以 `request_id`、数据库唯一约束及条件状态迁移实现幂等；超过重试上限的消息进入应用 DLQ，可有界查询和重放。模拟支付、取消和超时会在事务中推进订单状态，之后幂等确认已售或释放预留库存。

Redis 负责共享 Token Bucket 与热库存操作，不实现消息队列或 Waiting Room。MySQL 是订单最终事实来源，Redis/MySQL 差异由补偿和对账收敛。当前活动规则为每位用户每活动限购一件。
