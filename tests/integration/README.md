# 集成测试

验证真实依赖参与的跨模块链路：Redis Lua 并发预留与限流、MySQL 事务/唯一约束，以及 RabbitMQ 传输冒烟测试。通过 `deploy/scripts/external_validation.sh` 启动隔离的 Redis、MySQL、RabbitMQ 服务，构建外部测试目标并运行 CTest。

故障注入结果单独记录在 `docs/failure_matrix.md`，只有实际执行并检查业务状态、队列与库存不变量的用例才标记为通过。
