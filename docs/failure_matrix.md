# 故障演练记录

## 当前工作树已验证的场景

测试于 2026-10-10 在全新隔离的 Docker Compose 项目中执行。脚本检查 HTTP 结果、持久化订单数量和最终库存不变量，结束后清理本次创建的容器和数据卷。

| 场景 | 结果 | 证据与范围 |
|---|---|---|
| Redis 不可用 | 通过 | 提交返回 503，库存保持 `100/0/0`。Redis 重启并依次重启 API、消费者和对账进程后，活动状态投影及认证请求链路恢复。 |
| RabbitMQ 不可用后恢复 | 通过 | 故障期间发布请求返回 503，不会返回 202；使用相同幂等键重试后返回 202，订单进入 `PENDING_PAYMENT`。 |
| MySQL 不可用后恢复 | 通过 | 故障期间提交返回 503，不会产生虚假的待处理订单；MySQL 恢复后以同一请求重试，最终完成下单。由于入队前会检查活动状态，此场景需要客户端重试。 |
| 停止并重启 Reconciler | 通过 | Reconciler 停止时有一条 Outbox 记录未发布；进程重启后积压清零。 |
| 消费者在 Redis 预留后、MySQL COMMIT 前被杀死 | 通过，进程级 | 一次性测试钩子在指定位置终止订单消费者容器。Docker 随后重启容器，RabbitMQ 重新投递未确认命令，最终产生一笔持久订单且没有遗留库存预留。 |
| 消费者在 COMMIT 后、RabbitMQ ACK 前被杀死 | 通过，进程级 | 一次性测试钩子在业务处理返回、发送 ACK 前终止容器。消息重新投递后仅保留一笔订单，没有重复预留。 |
| Reconciler 在 Outbox Publisher Confirm 后、已发布标记前被杀死 | 通过，进程级 | 一次性测试钩子在确认发布后终止进程。领取租约过期后事件再次发布（`attempts=2`）并标记为已发布，待处理记录归零。 |
| RabbitMQ 消费连接断开且消息未 ACK | 通过，传输层 | `rabbitmq_smoke_test` 保留一条未确认的持久命令，关闭消费者连接，并验证替代消费者收到重新投递的消息后完成 ACK。 |
| 持久化订单写入后 ACK 丢失 | 通过，确定性单元模拟 | `flash_sale_pipeline_test` 单独注入 ACK 失败，并验证重试后数据库仍只有一笔订单。 |

完整故障脚本最终提交了 5 笔恢复订单。初始库存为 100，最终库存为 `available=95,reserved=5,sold=0`。库存不变量成立，已接收请求均未丢失。三个进程崩溃钩子只有在设置 `ECOMMERCE_ENABLE_TEST_FAULTS=1`、命名 `ECOMMERCE_TEST_CRASH_POINT` 并指定标记文件路径后才会启用；默认关闭。

这些测试验证了本地单节点 Compose 环境中的重试语义，不代表已验证 Broker 仲裁队列、多节点故障转移或生产环境自动化恢复。

## 复现方法

```bash
ECOMMERCE_FAULT_PROJECT=live-commerce-fault-run \
ECOMMERCE_GATEWAY_PUBLISHED_PORT=18084 \
ECOMMERCE_REDIS_PUBLISHED_PORT=16384 \
ECOMMERCE_MYSQL_PUBLISHED_PORT=13321 \
ECOMMERCE_RABBITMQ_PUBLISHED_PORT=5675 \
ECOMMERCE_RABBITMQ_MANAGEMENT_PUBLISHED_PORT=15675 \
python3 deploy/scripts/fault_validation.py
```

每次恢复测试都应核验：

```text
有效订单数 <= 初始库存
available + reserved + sold = total
```
