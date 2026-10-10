# C++ 直播电商秒杀消费后台

一个面向大厂后端实习面试的 C++17 工程实践项目。项目只围绕“秒杀入口 + 异步消费 + 订单落库”展开，用于展示高并发接入、库存一致性、消息可靠性、MySQL 事务、故障恢复和测试工程能力。

## 项目目标

在突发秒杀流量下：

1. 入口快速完成鉴权、限流、活动准入和抢购命令入队；
2. 由消费者异步完成库存预留和订单持久化，把突发流量与 MySQL 写入解耦；
3. 通过幂等、事务、Outbox、重试、DLQ 和补偿保证订单最终可恢复；
4. 通过单元测试、集成测试、真实 Redis/MySQL/Kafka 测试和压测验证设计。

## 核心链路

```text
HTTP/Reactor
  -> Session 鉴权与全局/IP/用户/活动级 Token Bucket 限流（Redis 模式下多实例共享；仅可信代理地址可启用 X-Forwarded-For）
  -> 活动状态、用户去重和 quota
  -> RocketMQ 5.x Broker ACK（可选 SDK adapter；开发回退队列见下文）
  -> 秒杀订单消费者
  -> Redis Lua 原子库存预留
  -> MySQL 事务：库存账本 + PENDING_PAYMENT 订单 + 请求状态 + Outbox
  -> 模拟支付/取消/超时关单
  -> MySQL 条件状态迁移 + 库存确认/释放 + Outbox 事件
  -> 下游事件投递、重试、DLQ、补偿和对账
```

入口返回 `202 QUEUED` 只代表命令已进入配置的队列，不代表库存预留或订单创建成功。配置 Redis 时，Gateway 把请求状态写入 24 小时 Redis 快速状态，不在请求线程同步写 MySQL；命令入队使用 `SUBMITTING -> QUEUED` 条件状态迁移，发送失败后可用原幂等键安全重试，Broker 已接收但状态回写失败时重复消息由消费者幂等处理。消费者落单事务同时建立 MySQL 持久状态，后续消费者状态镜像到 Redis/MySQL。`GET /flash-sale/requests?request_id=...` 先查 Redis，终态会核对 MySQL，缓存未命中时回源 MySQL。无 Redis 的开发配置会退回同步状态仓储。下单后库存保持预留，模拟支付成功后转为已售，取消和超时关单则释放库存。

Redis 加共享命令队列模式还提供有界 FIFO 等待室：IP 与用户预算通过但全局/活动预算暂时耗尽时，入口返回 `202 WAITING` 和 `queue_position`；用户继续查询 `/flash-sale/requests?request_id=...` 可获取新排位。后台多个 worker 通过 Redis 租约放行请求，服务重启后未完成票据可在租约过期后恢复。使用 `ECOMMERCE_FLASH_SALE_WAITING_ROOM=false` 可关闭等待室，等待队列容量/worker 数分别由 `ECOMMERCE_FLASH_SALE_WAITING_CAPACITY` 和 `ECOMMERCE_FLASH_SALE_WAITING_WORKERS` 设置。Redis 不可用或命令队列使用进程内 local 模式时不会静默退回进程内等待室。

运营端通过 `x-admin-token` 调用管理 API。默认 `ECOMMERCE_ADMIN_TOKEN` 作为 `admin` 角色兼容入口；生产可用 `ECOMMERCE_ADMIN_CREDENTIALS` 配置 `actor=role=token` 记录，多个记录用分号分隔，角色为 `viewer`、`operator` 或 `admin`。viewer 可读活动，operator 可创建/编辑/控制活动，admin 可查审计、DLQ、执行重放和库存对账；活动写操作将凭证 actor 写入审计记录。活动可配置成对的 `starts_at`/`ends_at` Unix 秒时间窗；MySQL 模式持久化窗口并在启动时恢复。MySQL 会锁定 SKU 库存行，核验同 SKU 未结束活动的总剩余配额，并在订单上保存 campaign_id；无 MySQL 时使用保守的进程内配额校验。Redis List 模式提供 `GET /admin/dlq?limit=100` 查询 DLQ、`POST /admin/dlq/replay` 有界重放；Kafka 模式支持查看和重放 Kafka DLQ。RocketMQ 使用独立的应用 DLQ 普通 Topic（默认 `flashsale.dlq`）：消费本地重试耗尽后先发布 DLQ 并确认原消息；管理 API 查询该 Topic 并将记录有界重放回命令 Topic。RocketMQ 5.3.2 Proxy 不支持当前 C++ SDK 对 Broker 系统 `%DLQ%<group>` Topic 的消费操作，因此项目不宣称 Broker 原生 DLQ 管理能力。SDK 5.1.1 的正常消息发布、消费和 ACK 已实测，应用 DLQ 的重试/查询/重放验收脚本见部署指南。

支付演示除用户主动模拟支付接口外，还提供 MySQL 持久化的签名回调 `POST /payments/callback/mock`。需配置 `ECOMMERCE_PAYMENT_CALLBACK_SECRET`，按 `HMAC-SHA256(secret, timestamp + "." + raw_body)` 生成 `x-payment-signature`，并提供 `x-payment-timestamp`；回调检查 5 分钟时钟偏差、订单金额与支付交易幂等。

Kafka/Redpanda 配置时，通知 fan-out worker 消费 `order.events` 的 `OrderCreated`，向 `ECOMMERCE_NOTIFICATION_TOPIC`（默认 `notification.events`）发布稳定 `event_id` 的 `OrderConfirmation`。这是下游通知适配边界；短信、邮件或推送发送器和其交付回执目前不在项目范围内。

RocketMQ 5.x C++ SDK adapter 通过 Apache SDK 5.1.1 构建；`deploy/scripts/benchmark_rocketmq.sh` 会自动构建启用 SDK 的 Gateway 镜像。Compose 的 RocketMQ profile 提供单节点开发 NameServer/Broker/Proxy。默认 Compose 适合快速开发演示，使用 Redis List 命令队列；面试展示完整消息链路和压测时应使用下面的 RocketMQ 脚本。`local` 和 Redis List 不提供 RocketMQ Broker 的消费确认及崩溃恢复语义。Kafka/Redpanda 用于 Outbox 事件和通知 fan-out 演示。

## 技术栈

```text
C++17 · POSIX Socket · Reactor/EventLoop · HTTP/1.1
Redis · Lua · MySQL · RocketMQ 5.x (optional) · Kafka/Redpanda (legacy event adapter)
WAL/Snapshot · Transactional Outbox · CMake · Docker Compose
ASAN/UBSAN/TSAN · CTest · P50/P95/P99 压测
```

## 目录

```text
apps/        服务启动、依赖组装和路由注册
src/         network/gateway/business/cache/kv/storage/messaging 等后端实现
tests/       单元、集成、恢复和外部依赖测试
tools/       HTTP、Redis、MySQL、Kafka 和长稳测试工具
benchmarks/  并发库存基准
configs/     开发和压测配置
deploy/      Docker、Compose、构建测试和故障切换脚本
```

## 快速运行

### 零依赖本地模式

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
ctest --test-dir build --output-on-failure
ECOMMERCE_INVENTORY_MODE=local ./build/ecommerce_api_demo 8080
```

### Redis/MySQL/Redpanda 开发联调模式

```bash
docker compose -f deploy/compose/docker-compose.yml up --build
```

### 干净环境演示

推荐用项目自带脚本启动一套独立开发环境。它使用 `live-commerce-internship-demo` Compose 项目和全新命名卷，不会读写旧的默认 `compose` MySQL 卷；服务端口映射为 Gateway `18080`、Redis `16379`、MySQL `13316`、Redpanda `19093`。首次运行会构建镜像、等待依赖和 `/ready` 健康检查，然后注册用户、登录、提交一次秒杀请求并轮询到订单状态：

```bash
./deploy/scripts/demo_stack.sh demo
```

后续可用 `./deploy/scripts/demo_stack.sh stop` 停服务，`./deploy/scripts/demo_stack.sh start` 保留数据并再次启动，`./deploy/scripts/demo_stack.sh down` 移除容器和网络但保留卷。仅对这套可丢弃的演示数据重置时，执行 `docker compose -p live-commerce-internship-demo -f deploy/compose/docker-compose.yml down -v`；旧 `compose` 卷不会被这条命令删除。

### Redis、MySQL、RocketMQ 端到端压力测试

仓库提供启用 Apache RocketMQ C++ SDK 的独立镜像。构建阶段按固定 SDK tag 和 proto revision 获取源码，随后构建 Gateway；第一次运行会下载和编译 SDK，耗时明显高于默认开发镜像。脚本启动隔离 Compose 项目、创建干净数据库和单节点 RocketMQ 5.3.2，再对真实 HTTP 抢购入口施压：

```bash
./deploy/scripts/benchmark_rocketmq.sh
```

默认 200 个唯一用户并发提交（100 个请求并发度），默认库存/活动额度为 100；客户端和会话准备不计入测量，报告单列准备时间。测量统计 HTTP 提交吞吐、p50/p95/p99、状态码、消费者落库收敛和 `available + reserved + sold` 库存守恒。该 HTTP 压测不注入消费者故障或验证 Broker 系统 DLQ；应用 DLQ 故障闭环由 `deploy/scripts/rocketmq_dlq_validation.sh` 单独验收。可用 `BENCH_REQUESTS`、`BENCH_CONCURRENCY`、`BENCH_RATE`、`ECOMMERCE_DEMO_STOCK`、`ECOMMERCE_HTTP_WORKERS`、`ECOMMERCE_FLASH_CONSUMER_WORKERS` 和 `ECOMMERCE_MYSQL_POOL_SIZE` 调整负载。报告写到 `benchmarks/results/rocketmq-single-node.json`；脚本通过后停止容器并保留卷，失败时保留现场供排查。

## 文档入口

部署、演示和故障恢复说明见 [deploy/README.md](deploy/README.md)；真实依赖压测口径和报告见 [benchmarks/README.md](benchmarks/README.md)；模块实现说明见各源码目录中的 README。

## 项目边界

本项目只覆盖秒杀消费后台；支付仅提供模拟成功/取消与超时关单，不接真实支付渠道；通知只发布稳定事件，不接真实短信/邮件服务；不包含购物车、优惠券、物流等业务域。RocketMQ 的应用 DLQ 查询/重放已由故障注入脚本实测；Broker 原生系统 DLQ 管理仍不在当前 Proxy SDK 能力范围内。单节点压测用于解释当前实现路径和瓶颈，不代表集群容量或生产高可用结论；当前没有 Redis Cluster、MySQL 主从、Broker 副本、跨机房容灾或生产级故障恢复保证。
