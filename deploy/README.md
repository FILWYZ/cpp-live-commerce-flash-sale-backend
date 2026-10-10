# deploy

- `docker`：构建秒杀消费后台镜像。
- `compose`：启动本地 API、Redis、MySQL 和 Redpanda；API 只暴露 HTTP 端口。该 Compose 用于开发联调，秒杀命令仍走 Redis List。
- `scripts`：构建、测试和本地联调入口。

`docker-compose.multi.yml` 展示两个无状态 Gateway 和一个独立 Worker：Gateway 只接收 HTTP 请求，Worker 负责消费秒杀命令、过期订单扫描和 Outbox 投递。两个角色共享 Redis 与 MySQL；设置 `ECOMMERCE_PROCESS_ROLE=all`（默认）仍可在单进程中运行两类职责。拆分模式启动时会校验 Redis、MySQL 和共享命令队列配置。部署探针 `/health` 只表示进程存活，`/ready` 会检查已配置的 Redis 和 MySQL 连接，负载均衡器应使用 `/ready`。

Compose 会等待 Redis、MySQL 和 Redpanda healthcheck 成功后启动 Gateway，避免容器仅按创建顺序启动时，API 抢先连接尚未就绪的依赖。healthcheck 用于本地开发启动排序，不覆盖依赖运行期间的断连恢复。

`./deploy/scripts/demo_stack.sh demo` 以独立 Compose project `live-commerce-internship-demo` 启动并验证注册、登录、抢购准入、Redis 消费、MySQL 下单和状态查询。不同 project name 创建独立 volume，因此不会打开同目录早期 `compose` 项目留下的旧 MySQL 数据。若要重置演示数据，只删除 `live-commerce-internship-demo` 项目卷；不要删除或复用旧卷，除非已经备份并迁移其中的业务数据。当前仓库提供新数据库的完整 `schema.sql`，旧版订单/支付模型并没有自动升级脚本。

已有默认 `compose` 项目若因 `campaigns` 表缺失或旧 `orders`/`payments` 字段不匹配而无法启动，先保留旧卷并运行上述隔离演示。当前的 `001_campaign_windows_and_allocations.sql` 只适用于已有当前版基础表的数据库，不会把早期 `orders(total_cents, address_snapshot)` 和旧支付模型转换成现在的结构。需要保留旧数据时，应先备份，再编写基于业务映射的显式迁移并校验订单、支付和库存账本；确认仅是可丢弃开发数据时，才对旧 Compose project 执行 `docker compose -p compose -f deploy/compose/docker-compose.yml down -v`，然后使用 `./deploy/scripts/demo_stack.sh demo` 创建新卷。不要将新 `schema.sql` 直接导入含有旧表的库。

Compose 启动后，可通过 `ECOMMERCE_REDIS_HOST=redis ECOMMERCE_KAFKA_BROKERS=redpanda:9092` 让 API 使用外部依赖。

本地 Compose 为签名回调配置 `ECOMMERCE_PAYMENT_CALLBACK_SECRET`（默认值仅用于开发）。`POST /payments/callback/mock` 校验 `x-payment-timestamp` 和 `x-payment-signature`；签名为 `HMAC-SHA256(secret, timestamp + "." + raw_body)` 的小写十六进制值，时间戳允许与服务时间相差最多 5 分钟。回调事务会校验订单金额、唯一支付交易号，并在 MySQL 同一事务中写支付、订单、库存账本和 Outbox；真实支付渠道仍需独立适配和验签密钥管理。

配置 Kafka/Redpanda 时会启动通知 fan-out consumer group：消费 `order.events` 并投递稳定 `event_id` 的 `OrderConfirmation` 到 `ECOMMERCE_NOTIFICATION_TOPIC`（默认 `notification.events`）。消费者投递失败时不提交输入 offset；输入重放可能再次发布相同通知事件，下游仍需按 `event_id` 去重。实际短信/邮件渠道连接器尚未实现。

RocketMQ C++ adapter 使用 Apache RocketMQ 5.1.1 SDK（CMake package 名为 `rocketmq`；支持官方导出的 `rocketmq::rocketmq_shared` target）。推荐使用下面的端到端脚本，自动构建 SDK/Gateway 并启动依赖；首次构建会下载 SDK 源码并编译，需网络连接：

```bash
./deploy/scripts/benchmark_rocketmq.sh
```

脚本每次使用带时间戳的独立 Compose project 和全新 MySQL/Redis 卷，通过真实 Gateway 请求验证 Redis 库存、RocketMQ 命令和 MySQL 订单事务。默认 200 请求、100 并发、库存 100，压测报告保存到 `benchmarks/results/rocketmq-single-node.json`。参数可通过 `BENCH_REQUESTS`、`BENCH_CONCURRENCY`、`ECOMMERCE_DEMO_STOCK`、`ECOMMERCE_HTTP_WORKERS`、`ECOMMERCE_FLASH_CONSUMER_WORKERS` 和 `ECOMMERCE_MYSQL_POOL_SIZE` 配置。脚本成功后停止容器但保留项目卷；失败时保留现场，并在终端报告本次 project 名称以便查看日志。

需要手动将本机 API 连接 RocketMQ 时，可安装 SDK 并按以下方式构建和运行：

```bash
cmake -S . -B build -DENABLE_ROCKETMQ=ON
ECOMMERCE_FLASH_SALE_QUEUE=rocketmq \
ECOMMERCE_ROCKETMQ_ENDPOINT=rocketmq-proxy:8081 \
ECOMMERCE_ROCKETMQ_COMMAND_TOPIC=flashsale.command \
ECOMMERCE_ROCKETMQ_CONSUMER_GROUP=flashsale-command-consumers \
./build/ecommerce_api_demo 8080
```

RocketMQ 5.x C++ SDK 使用 gRPC，需要连接启用了 gRPC Proxy 的 RocketMQ 5.0+ 服务。下面的 Compose profile 可单独启动 Broker/Proxy，适合 SDK 联调。

Compose 提供可选单节点 RocketMQ 5.3.2 NameServer、Broker 和 gRPC Proxy profile，镜像版本与 [Apache RocketMQ 官方 Docker Compose 快速开始](https://rocketmq.apache.org/docs/quickStart/03quickstartWithDockercompose/)一致。Proxy 将 Broker 路由公布在 gRPC 端口 8081，因此 Compose 同时发布该端口；启动服务后，宿主机运行的 API 连接 `localhost:8081`：

```bash
docker compose -f deploy/compose/docker-compose.yml up -d redis mysql redpanda
docker compose -f deploy/compose/docker-compose.yml --profile rocketmq up -d rocketmq-namesrv rocketmq-broker rocketmq-proxy
cmake -S . -B build -DENABLE_ROCKETMQ=ON
cmake --build build -j4
ECOMMERCE_REDIS_HOST=localhost \
ECOMMERCE_INVENTORY_MODE=redis \
ECOMMERCE_FLASH_SALE_QUEUE=rocketmq \
ECOMMERCE_ROCKETMQ_ENDPOINT=localhost:8081 \
ECOMMERCE_ROCKETMQ_COMMAND_TOPIC=flashsale.command \
ECOMMERCE_ROCKETMQ_CONSUMER_GROUP=flashsale-command-consumers \
ECOMMERCE_MYSQL_HOST=localhost \
ECOMMERCE_MYSQL_PORT=13306 \
ECOMMERCE_MYSQL_USER=ecommerce \
ECOMMERCE_MYSQL_PASSWORD=ecommerce \
ECOMMERCE_MYSQL_DATABASE=ecommerce \
./build/ecommerce_api_demo 18082
```

该 profile 是持久化卷支持的单 Broker 开发节点，不提供 Broker 副本、自动故障切换或生产容量保证。默认 Compose `gateway` 仍运行 Redis List 队列；RocketMQ override 才会将 Gateway 切换到 RocketMQ。RocketMQ 使用独立应用 DLQ 普通 Topic（默认逻辑名 `flashsale.dlq`，物理名 `flashsale_dlq`）：消费者本地重试耗尽后先写入 DLQ，再 ACK 原命令；`GET /admin/dlq` 查询、`POST /admin/dlq/replay` 有界重放回命令 Topic。当前 RocketMQ 5.3.2 Proxy 对 C++ SDK 的 Broker 系统 `%DLQ%<group>` 消费返回 `Action is not supported`，所以项目不声称支持 Broker 原生 DLQ 的读取/重放。`flashsale.command` 等逻辑 Topic 会映射为 RocketMQ 允许的物理 Topic；Compose 会在 Broker 健康后创建命令、应用 DLQ 和事件 Topic。

`./deploy/scripts/benchmark_rocketmq.sh` 使用 `docker-compose.rocketmq.yml` override 构建启用 RocketMQ SDK 的 Gateway 镜像，运行 Redis 准入/库存、RocketMQ 命令与 Outbox 事件、MySQL 订单事务的真实请求链路。SDK 源码固定为 `cpp-5.1.1`，RocketMQ API proto 固定到明确的 Git revision；首次构建需要网络和较多时间。压测报告包含环境、请求并发、吞吐、延迟分位数、HTTP 拒绝/错误统计、消费者落库收敛和库存守恒；会话准备阶段单独计时，不混入 HTTP 提交吞吐。默认是单机单 Broker 演示，不可把测得数据表述成生产容量。

`BENCH_RATE=2 BENCH_REQUESTS=600 BENCH_CONCURRENCY=20 ./deploy/scripts/benchmark_rocketmq.sh` 可按目标速率匀速提交，用于真实 Redis/RocketMQ/MySQL 稳定性观察。`deploy/scripts/rocketmq_dlq_validation.sh` 在隔离 Compose project 内模拟 MySQL 暂时不可用，验证应用重试耗尽、应用 DLQ 查询、恢复 MySQL、重放和订单处理；测试沿用 120 秒消息可见性周期，避免数据库故障时处理时间超过 Broker lease 并触发并发重复投递。脚本也检查 MySQL 进程重启后的连接池自动恢复。验收报告分别写入 `benchmarks/results/`。脚本只执行带唯一 project 名称的 `docker compose down`，保留数据卷。
