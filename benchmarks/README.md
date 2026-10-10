# benchmarks

当前基准围绕后端主链路组织：

- `flash_sale/inventory_benchmark`：并发预占和库存不变量。
- `tools/http_load_generator`：单接口吞吐与尾延迟。
- `tools/http_business_load_generator`：带鉴权的混合业务读写。
- `tools/mysql_capacity_test`：索引查询和数据规模样本。

真实 Redis → RocketMQ → MySQL 秒杀入口压测：

```sh
BENCH_REQUESTS=1000 BENCH_CONCURRENCY=1000 ECOMMERCE_DEMO_STOCK=1000 \
  ./deploy/scripts/benchmark_rocketmq.sh
```

`BENCH_RATE` 大于 0 时按给定请求速率匀速发送，可用于稳定性观察；准备会话和订单最终状态核验均不计入入口吞吐。

2026-10-10 单机 macOS 26.6.2 arm64（10 个逻辑 CPU）、RocketMQ 5.3.2 单 Broker、8 个 HTTP worker、4 个消费 worker、MySQL pool 16 实测：

| 并发请求 | 请求数 | 受理吞吐 | p50 | p95 | p99 | 最终状态 / 错误 |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 300 | 600 | 322.16 QPS | 788.846 ms | 1073.027 ms | 1079.644 ms | 600/600 `PENDING_PAYMENT`，无 5xx/传输错误 |
| 600 | 600 | 504.50 QPS | 1080.593 ms | 1128.916 ms | 1146.631 ms | 600/600 `PENDING_PAYMENT`，无 5xx/传输错误 |
| 1000 | 1000 | 740.42 QPS | 1178.078 ms | 1273.085 ms | 1294.088 ms | 1000/1000 `PENDING_PAYMENT`，无 5xx/传输错误 |
| 20（限速提交） | 600 | 2.00 QPS，持续 300.515 秒 | 501.174 ms | 510.637 ms | 511.943 ms | 600/600 `PENDING_PAYMENT`，无 5xx/传输错误 |

所有轮次库存守恒均通过。1000 并发是当前最高已验证点，不代表容量上限；容量点测试为单机短突发，不代表生产或集群 QPS。稳态轮次为 5 分钟、20 worker 上限、目标 2 HTTP QPS。对应 JSON 报告在 `results/rocketmq-single-node-{300c,600c,1000c}-20261010.json` 和 `results/rocketmq-single-node-soak-5m-2rps-20261010.json`。

`deploy/scripts/rocketmq_dlq_validation.sh` 使用隔离环境模拟 MySQL 中断，验证 5 次应用重试耗尽、消息进入应用 DLQ Topic、管理端查询、MySQL 恢复后重放并最终落单；它不验证 RocketMQ Broker 系统 `%DLQ%<group>` 操作。每次正式压测应记录硬件、编译参数、数据规模、并发数、持续时间、吞吐、p99/p999 和失败数；运行逻辑和结果解释统一见 [`PROJECT_TECHNICAL_GUIDE.md`](../PROJECT_TECHNICAL_GUIDE.md)。
