# 测试策略

测试围绕受支持的交易链路展开：HTTP/Reactor -> 准入 -> 命令队列 -> Redis 库存预留 -> MySQL 订单事务 -> Outbox 与恢复。

## 本地 CTest 测试集

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

- `first_stage_test`：HTTP 边界、EventLoop 唤醒、缓存、会话、Outbox 和限流。
- `storage_recovery_test`：本地引擎 WAL/Snapshot 工具及恢复行为。
- `commerce_concurrency_test`：并发库存预留和防止超卖。
- `security_outbox_test`：会话行为及 Outbox 重试/重启。
- `flash_sale_pipeline_test`：准入、异步处理、幂等和补偿。
- `mysql_transaction_test`：MySQL 订单、库存账本、幂等和 Outbox 事务。

## 真实依赖测试

运行 [`../deploy/scripts/external_validation.sh`](../deploy/scripts/external_validation.sh)，在隔离的依赖服务上验证 Redis Lua 库存与令牌桶行为、MySQL 事务行为，以及 RabbitMQ 发布确认/持久化投递/手动 ACK 行为。最近一次结果为 CTest 7/7 通过，RabbitMQ 传输冒烟测试通过。

运行 `RUN_TSAN=1 ./deploy/scripts/test.sh`，可依次执行常规测试集、ASAN+UBSAN 测试集和 TSAN 测试集。最近一次运行中三个测试集均为 6/6 通过。

## 故障场景

已执行的 Redis、RabbitMQ、MySQL 和 Outbox 恢复场景记录在 [`../docs/failure_matrix.md`](../docs/failure_matrix.md)。隔离故障脚本通过可选的一次性进程崩溃钩子，验证事务提交前、事务提交后但 ACK 前，以及 Outbox Confirm 后但更新发布标记前的消息重新投递。集成测试还单独模拟 ACK 失败并验证幂等性。每次恢复测试都检查：

```text
有效订单数 <= 初始库存
available + reserved + sold = total
```
