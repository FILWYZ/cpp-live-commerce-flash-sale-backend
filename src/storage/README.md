# 存储模块

负责自研 KV 的本地持久化、恢复和元数据管理。

## 分阶段实现

1. Memory KV：先验证并发读写和 API 正确性。
2. WAL：写入路径为 `Append WAL -> fsync -> Apply -> ACK`。
3. Snapshot：启动时 `Load Snapshot -> Replay WAL`，服务运行期间由 `SnapshotWorker` 周期性压缩 WAL，优雅退出时再执行一次最终快照。
4. MySQL Repository：为订单、库存和 Outbox 提供事务、索引和连接池适配；它是外部持久化能力，不与 LocalKV 混称。

业务订单、库存和主要交易对象使用业务前缀隔离；库存多键状态更新使用单条批量 WAL 记录。LocalKV 是零依赖开发与恢复路径，MySQL 是可选的关系型持久化路径。
