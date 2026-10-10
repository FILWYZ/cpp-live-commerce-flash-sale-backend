# 性能与负载测试

## RabbitMQ 全链路测试

```bash
BENCH_REQUESTS=1000 BENCH_CONCURRENCY=300 ECOMMERCE_DEMO_STOCK=1000 \
  ./deploy/scripts/benchmark_rabbitmq.sh
```

脚本会启动隔离的三进程 Compose 服务，并测量经过身份验证的 HTTP 准入过程。之后等待已接收请求完成结算，记录最终状态、检查队列深度，并验证 `available + reserved + sold = initial_stock`。会话准备和结算轮询单独报告，不计入准入延迟窗口。

建议测试并发数：`100`、`300`、`600`、`1000`。可增加 `BENCH_RATE=20` 这类限速测试，观察稳定负载下的表现。比较代码改动前后的结果时，应固定软件版本、硬件、队列模式、工作线程数、连接池大小、库存及编译器/构建类型。

## 本地测试工具

- `http_load_generator`：HTTP 请求吞吐及 P50/P95/P99 延迟。
- `http_business_load_generator`：经过身份验证的业务只读接口混合负载。
- `flash_sale_soak_test`：进程内有界队列和消费者稳定性测试。
- `inventory_benchmark`：进程内并发库存预留测试。
- `mysql_capacity_test`：真实 MySQL 查询样本。

本地内存测试和只读接口的测试数据不能代表 RabbitMQ 交易系统的性能。当前仓库只保留与现行 RabbitMQ 主线对应的测试报告。

## 当前测试结果

成功运行 `benchmark_rabbitmq.sh` 后，会在此处更新 RabbitMQ 全链路测试结果。不能根据单节点样本推断集群容量。测试约定见 [`../docs/performance.md`](../docs/performance.md)。

### RabbitMQ 单节点样本（2026-10-10）

`results/rabbitmq-single-node.json` 记录了最近一次 1,000 个请求的测试：客户端并发数为 300，库存使用 Redis，RabbitMQ 3.13、Redis 7、MySQL 8.4。全部请求均返回 202；准入吞吐为 245.29 RPS，p50/p95/p99 延迟为 1050.245/1109.768/1127.305 毫秒。1,000 个已接收请求均进入 `PENDING_PAYMENT`，队列积压为零，库存 `available=0, reserved=1000, sold=0` 与初始库存 1,000 相符。会话准备耗时 252.397 秒，且结算轮询均未计入准入延迟。这只是本地单节点样本，不代表容量保证。`/debug/stats` 中的消费者计数器仅在单个进程内统计，并非全局汇总；报告明确了这一点，也没有声称测得 DLQ 数量。
