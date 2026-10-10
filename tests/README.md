# 秒杀后台测试

测试围绕一条主链路展开：HTTP 接入、活动准入、库存原子预留、异步队列、消费者、MySQL 事务、Outbox 和恢复补偿。

关键不变量：库存 `available >= 0`；库存为 100、并发预留 10000 时成功数为 100；同一幂等键最多落一笔订单；重复消息不会重复扣减 MySQL 库存。

## 测试目标

- `first_stage_test`：HTTP 边界、线程唤醒、缓存 Singleflight、用户恢复、Outbox 重试和限流。
- `storage_recovery_test`：WAL/Snapshot CRC、批量恢复、单写者锁和孤儿库存预占释放。
- `commerce_concurrency_test`：热点 SKU 并发预占不超卖。
- `security_outbox_test`：Session Token、注销失效、Outbox 重启恢复和校验。
- `flash_sale_pipeline_test`：活动闸门、库存预留、异步消费者、入队失败补偿。
- `mysql_transaction_test`：真实 MySQL 的秒杀订单事务、幂等、库存镜像和 Outbox。
- Redis/Kafka 外部测试：真实 Redis Lua/List、Kafka 消费者组、重试和 DLQ。

## 执行

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_MYSQL=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

外部依赖测试需要先启动 Compose，并配置 `-DENABLE_EXTERNAL_TESTS=ON`。MySQL 测试使用：

```bash
ECOMMERCE_MYSQL_TEST_HOST=127.0.0.1 \
ECOMMERCE_MYSQL_TEST_PORT=13306 \
ECOMMERCE_MYSQL_TEST_DATABASE=ecommerce \
./build/mysql_transaction_test
```

可用 Docker daemon、Redis/MySQL/Kafka 客户端开发库时，也可直接运行统一外部验收脚本。脚本会等待 Redis、MySQL 和 Redpanda 健康后，构建外部测试并执行 CTest；依赖容器会保持运行，数据保留在 Compose 持久化卷中：

```bash
./deploy/scripts/external_validation.sh
```

RocketMQ SDK/Proxy 的编译与运行验收仍需单独按 `deploy/README.md` 配置；该脚本不宣称覆盖 RocketMQ。

长稳样本：

```bash
FLASH_SOAK_SECONDS=30 ./build/flash_sale_soak_test
```

HTTP 工具分别用于单接口和只读业务路径基准，不等价于生产容量结论：

```bash
./build/http_load_generator 127.0.0.1 8080 /health 10000 64
./build/http_business_load_generator 127.0.0.1 8080 10000 64
```
