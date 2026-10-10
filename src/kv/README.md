# 键值存储模块

统一 KV 访问抽象，隔离业务与具体 KV 后端。当前生产业务路径使用 Redis 保存短期状态、共享 Token Bucket 限流和秒杀库存预留；MySQL 保留订单、库存账本与请求终态等持久化事实。

Redis Lua 在单次脚本内原子执行限流和库存预留/释放。Redis 与 MySQL 不构成分布式事务：提交失败必须通过幂等重试、补偿和 reconciler 对账收敛。Redis 不承载消息队列或 Waiting Room。

`IKVStore`、`KVClient` 和本地 Memory KV/WAL/Snapshot 代码仍用于抽象与恢复练习；本地存储不是交易订单的持久化主库。
