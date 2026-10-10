# 部署与本地验证

`docker-compose.yml` 定义了受支持的本地服务栈：`api_server`、`order_consumer`、`reconciler`、Redis、MySQL 和 RabbitMQ。RabbitMQ 命令队列和死信队列均为持久化队列。Redis、MySQL 和 RabbitMQ 的健康检查通过后，应用服务才会启动。

## 端到端演示

```bash
./deploy/scripts/demo_stack.sh demo
```

演示脚本使用独立的 Compose 项目和非默认映射端口，发送一笔经过身份验证的秒杀请求，等待持久化状态并打印库存及消费者结果。`start`、`stop` 和 `down` 操作的都是该隔离项目。执行 `down` 会删除该项目的容器和网络，数据卷仍会保留，可供之后重新 `start` 使用。

## 外部依赖集成测试

```bash
./deploy/scripts/external_validation.sh
```

脚本会创建带时间戳的项目，只启动所需依赖，将当前数据库结构加载到隔离的验证数据库，运行真实 Redis/MySQL 测试和 RabbitMQ 传输冒烟测试，最后只清理本次项目创建的容器和数据卷。可通过 `ECOMMERCE_EXTERNAL_PROJECT` 指定固定的测试项目名。

## RabbitMQ 性能测试

```bash
BENCH_REQUESTS=1000 BENCH_CONCURRENCY=300 ECOMMERCE_DEMO_STOCK=1000 \
  ./deploy/scripts/benchmark_rabbitmq.sh
```

每次测试都会创建唯一的 Compose 项目，并使用独立的数据库、Redis 和 RabbitMQ 数据卷。成功后会删除容器和网络，JSON 报告保存在 `benchmarks/results/`；失败时会保留项目以便检查。可使用 `ECOMMERCE_GATEWAY_PUBLISHED_PORT`、`ECOMMERCE_REDIS_PUBLISHED_PORT`、`ECOMMERCE_MYSQL_PUBLISHED_PORT` 和 `ECOMMERCE_RABBITMQ_PUBLISHED_PORT` 修改映射端口。

设置 `BENCH_RATE` 可按固定速率提交请求，以采集稳定负载样本。报告会将会话准备、最终订单结算与准入延迟分别统计。单机测试样本不构成生产容量承诺。

## 配置

主要消息传输配置项包括 `ECOMMERCE_RABBITMQ_HOST`、`ECOMMERCE_RABBITMQ_PORT`、`ECOMMERCE_RABBITMQ_USER`、`ECOMMERCE_RABBITMQ_PASSWORD`、`ECOMMERCE_RABBITMQ_VHOST`、`ECOMMERCE_RABBITMQ_EXCHANGE`、`ECOMMERCE_RABBITMQ_COMMAND_QUEUE` 和 `ECOMMERCE_RABBITMQ_DLQ_QUEUE`。拆分进程部署时，各角色必须连接同一组 Redis、MySQL 和 RabbitMQ 服务。

HTTP 服务通过 `/health` 提供存活检查，通过 `/ready` 提供依赖就绪检查。排队准入依赖 RabbitMQ；进程开始接收流量前会检查 RabbitMQ 连接。
