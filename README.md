# C++ 直播电商秒杀消费后台

LiveCommerce 是一个基于 C++17 Reactor 的高并发秒杀与异步订单系统，核心链路只有四步：**高并发接入、异步削峰、库存一致性、可靠落库与恢复**。

```text
客户端 --HTTP/1.1--> API 服务 --Publisher Confirm--> RabbitMQ 命令队列
                                                        |
                                                        v 事务提交后手动 ACK
                                                  订单消费者
                                           Redis Lua 预留 + MySQL 事务
                                           订单 + 账本 + 状态 + Outbox
                                                        |
                                                        v
                                                   对账进程
                                      超时处理 + Outbox 重试 + 库存修复
```

## 一致性与可靠性保证

- API 仅在 RabbitMQ Publisher Confirm 后返回 `202 QUEUED`。
- Consumer 仅在 MySQL 事务提交后 ACK；重复投递由 `request_id`、唯一约束和条件状态迁移处理。
- Redis Lua 原子预留库存；MySQL 保存订单和库存账本。
- 订单、请求状态、库存账本和 Outbox 事件在同一 MySQL 事务提交。
- Reconciler 处理超时订单、库存对账和 Outbox 重试；RabbitMQ DLQ 支持查询和有界重放。
- 核心库存不变量：`available + reserved + sold = total`。

Redis、MySQL 与 RabbitMQ 分别承担准入/短期状态、持久交易账本和可靠消息队列职责。Redis 与 MySQL 不使用分布式事务，故障后通过幂等重试、Outbox 和对账收敛。

## 技术栈

`C++17` · POSIX Socket · epoll/Reactor · HTTP/1.1 · Redis/Lua · RabbitMQ · MySQL · Transactional Outbox · CMake · Docker Compose · ASAN/UBSAN/TSAN

## 启动三进程服务

Docker Compose 会启动三个独立应用进程（`api_server`、`order_consumer`、`reconciler`），以及 Redis、MySQL 和 RabbitMQ：

```bash
./deploy/scripts/demo_stack.sh demo
```

演示脚本会注册用户、提交订单、等待最终状态并检查库存快照。Compose 使用独立项目和数据卷；如需保留演示数据，请勿删除对应数据卷。

也可以在本机构建服务程序。RabbitMQ 支持需要安装 `rabbitmq-c`；安装 Redis 和 MySQL 开发库后会启用对应适配器。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

`ecommerce_api_demo` 是单进程本地开发程序。默认使用内存队列，也可通过 `ECOMMERCE_FLASH_SALE_QUEUE` 选择已配置的传输方式。独立服务程序为 `build/api_server`、`build/order_consumer` 和 `build/reconciler`；拆分部署时需要共享 Redis、MySQL 和 RabbitMQ。

## 测试与故障验证

```bash
./deploy/scripts/test.sh
RUN_TSAN=1 ./deploy/scripts/test.sh
./deploy/scripts/demo_stack.sh demo
```

测试覆盖 Reactor/HTTP 行为、并发库存预留、请求幂等、订单处理链路、MySQL 事务和 Outbox 恢复。构建 RabbitMQ 适配器后，可通过冒烟测试检查持久化命令投递、Publisher Confirm、手动 ACK 和事件确认发布：

```bash
ECOMMERCE_RABBITMQ_HOST=127.0.0.1 ./build/rabbitmq_smoke_test
```

隔离运行的 [`deploy/scripts/fault_validation.py`](deploy/scripts/fault_validation.py) 会验证 Redis、RabbitMQ 和 MySQL 故障，以及 Redis 预留后/事务提交前、事务提交后/ACK 前、Outbox Confirm 后/发布标记前的进程崩溃，并检查依赖恢复后的重试。最近一次运行产生 5 笔持久订单，最终库存为 `available=95,reserved=5,sold=0`；重新投递的 Outbox 事件在第 2 次尝试后发布，库存等不变量均成立。详情见 [`docs/failure_matrix.md`](docs/failure_matrix.md)。

## 性能测试

运行隔离的端到端准入与订单结算压测：

```bash
BENCH_REQUESTS=1000 BENCH_CONCURRENCY=300 ECOMMERCE_DEMO_STOCK=1000 \
  ./deploy/scripts/benchmark_rabbitmq.sh
```

最近一次记录的测试在并发数 300 时接收了 1,000/1,000 个请求，准入吞吐为 245.29 RPS，p50/p95/p99 延迟为 1050.245/1109.768/1127.305 毫秒；所有订单均完成结算，库存守恒检查通过。报告包含 HTTP 结果、订单结算、队列深度和库存状态，并记录队列及库存模式；本地内存测试结果不会冒充 RabbitMQ 全链路容量。每次压测都应记录主机和容器版本、工作线程数、连接池大小及负载参数。调优前先采集性能分析数据，并使用相同负载对比。

## 项目范围

项目聚焦交易链路。支付仅实现模拟支付、取消和超时状态机，不包含真实支付回调、排队室、Redis List 队列、Kafka/Redpanda、RocketMQ、RPC 框架、服务注册中心或 Kubernetes 部署。本地 WAL/Snapshot 工具仍可用于存储恢复练习，但不作为交易事实来源。

## 目录结构

```text
apps/        应用装配逻辑和三个进程入口
src/         Reactor、HTTP、秒杀业务域、RabbitMQ、Redis、MySQL 和可观测性
tests/       单元、并发、持久化和恢复测试
benchmarks/  负载工具和实测报告
deploy/      Docker 镜像、Compose 服务栈和演示/压测脚本
docs/        架构、一致性、故障演练和性能测试方法
```
