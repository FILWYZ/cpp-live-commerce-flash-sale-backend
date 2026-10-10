# LiveCommerce 主线收敛执行路线

## 项目定位

**LiveCommerce — 基于 C++17 Reactor 的高并发秒杀与可靠异步订单系统**

主线只覆盖：HTTP 接入、异步削峰、库存一致性、可靠落库与恢复。网络层保留 POSIX Socket/epoll/Reactor；基础设施固定 Redis、RabbitMQ、MySQL；不继续扩展第二套 MQ、RPC、服务注册或支付渠道集成。

## 目标架构

```text
Client -> api_server -> RabbitMQ command queue -> order_consumer
                                                  | Redis Lua reserve
                                                  | MySQL transaction
                                                  | Order + inventory + request + outbox
                                                  v
                                           RabbitMQ event exchange

reconciler：超时关闭/取消、库存对账、Outbox 发布与恢复
```

系统按至少一次消息投递设计。HTTP 返回 `202` 只表示 RabbitMQ Publisher Confirm 已确认命令，不表示下单成功。消费者必须在 MySQL 事务提交后 ACK；重复投递通过 request ID、唯一约束和条件状态迁移吸收。库存不变量为 `available + reserved + sold = total`。

## 三个核心进程

- `api_server`：HTTP、认证、校验、Redis Token Bucket、幂等准入、RabbitMQ Confirm 发布和状态查询。
- `order_consumer`：命令校验、Redis Lua 预留、MySQL 事务落单、Outbox 写入、手动 ACK/重试/DLQ。
- `reconciler`：超时订单关闭、库存释放/对账、Outbox 重试及恢复任务。

`ecommerce_api_demo` 保留为本地组合开发入口；部署和演示 Compose 使用三个独立角色。

## 收敛范围

- 保留：Reactor/epoll、Redis Lua Token Bucket 与库存预留、RabbitMQ、MySQL、Transactional Outbox、幂等、重试/DLQ、模拟支付/取消、观测指标、Compose。
- 删除主线：RocketMQ、Kafka/Redpanda、Redis List 队列、Waiting Room、支付 HMAC Webhook、通知扇出、RPC/服务发现/Kubernetes 扩展。
- 本地 WAL/Snapshot 只保留为存储恢复练习，不属于交易订单事实来源。

## 执行顺序与验收

1. **主线收敛与入口拆分**：固定三个角色入口，剥离历史 MQ/等待室/支付回调主线，精简 Compose 和文档。
2. **消息可靠性**：RabbitMQ durable topology、persistent message、Publisher Confirm、manual ACK、有限重试与 DLQ；验证 broker 不可用时入口失败关闭。
3. **业务一致性**：检查 request/order 唯一性、事务边界、Redis reservation 补偿、Outbox 重发、超时释放；库存守恒且有效订单不超过活动库存。
4. **自动化测试**：常规 CTest、真实 Redis/MySQL/RabbitMQ 集成 smoke、ASAN/UBSAN；在可用环境运行 TSAN。
5. **故障演练**：覆盖 DB commit 前崩溃、commit 后 ACK 前崩溃、Redis/MySQL/RabbitMQ 不可用、Outbox 发布中断与重启恢复；只把实际执行并核对结果的场景记为通过。
6. **性能闭环**：固定容器资源、配置和 workload，记录请求数、并发、RPS、p50/p95/p99、错误数、最终订单数及库存状态。先得到基线，再根据 profile 结果优化并对同一 workload 复测。
7. **交付整理**：README 首屏说明定位、架构、保证、基准和快速启动；保留可复现脚本、报告与明确的测试边界。

## 当前验收记录

- 本地 RelWithDebInfo 构建成功，常规 CTest 6/6、ASAN+UBSAN 6/6、TSAN 6/6 通过。
- 10 秒本地秒杀流水线 soak：1,000,000 条命令排空，库存不变量通过；该结果是内存管线样本，不代表外部依赖吞吐。
- 真实 Redis、MySQL、RabbitMQ 外部依赖 CTest 7/7 通过；RabbitMQ durable queue、Publisher Confirm、手动 ACK、事件发布 smoke 通过。
- 六类真实故障演练全部通过：消费者在 Redis 预留后/事务提交前崩溃、事务提交后/ACK 前崩溃、Redis 不可用、MySQL 不可用恢复、RabbitMQ 不可用恢复、Outbox Confirm 后/已发布标记前崩溃。RabbitMQ/MySQL 恢复通过同幂等键重试；Outbox 事件过租约后第二次发布成功并标记。另验证消费者 transport 断开后的未 ACK redelivery。最终落库 5 单，库存 `available=95,reserved=5,sold=0`。
- 当前版本 RabbitMQ 单节点全链路压测：1,000/1,000 返回 202，并全部到达 `PENDING_PAYMENT`；并发 300，吞吐 245.29 RPS，p50/p95/p99 为 1050.245/1109.768/1127.305 ms，队列清空且库存 `0/1000/0` 守恒。详见基准 JSON；这是单次本机样本，不构成容量承诺。

## 能力边界

单机 Compose 与单次压测仅证明该环境下的可复现行为，不构成生产级 HA 或容量承诺。支付仅为模拟状态机；没有真实支付回调、短信/邮件通知、多节点故障转移或生产监控告警。性能报告必须保留环境和 workload 参数，不能把本地模拟结果与 RabbitMQ 全链路结果混为一谈。
