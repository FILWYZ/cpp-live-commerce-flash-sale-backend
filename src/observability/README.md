# observability

负责指标、结构化日志、链路追踪、健康检查和运行状态输出。

## 第一批指标

`qps`、`p50/p99/p999 latency`、`active_connections`、`event_loop_lag`、`queue_depth`、`cache_hit_ratio`、`singleflight_coalesced`、`rate_limited_total`、`stock_reserve_success/fail`、`order_idempotent_hit`、`consumer_lag`、`wal_fsync_latency`。

提供 `/metrics`、`/health` 和 `/debug/stats`。运行指标包括 HTTP 请求/状态/耗时、活跃连接数、有界 HTTP worker 队列深度、可查询的命令队列深度和消费者处理/拒绝/DLQ 计数。RocketMQ Broker lag 不由当前 C++ adapter 读取：RocketMQ 模式下 `/debug/stats` 返回 `flash_sale_queue_depth: null`，Prometheus 指标 `flash_sale_queue_depth_known` 为 `0`，深度 gauge 为 `-1`；需从 Broker/运维平台补充该项。HTTP 网关会生成或透传 `X-Request-Id`，错误请求输出结构化 JSON 日志，包含 `request_id`、方法、目标、状态码和耗时字段。
