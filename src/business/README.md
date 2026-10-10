# 秒杀业务域

项目只保留一条交易主线，跨模块调用通过 Service/Repository 接口完成：

- `user`：注册、登录、Session 与请求鉴权。
- `product`：秒杀 SKU 元数据和商品查询。
- `promotion`：活动生命周期、资格判断和用户去重。
- `inventory`：可用、预留、已售库存，以及 Redis/MySQL 对账。
- `flash_sale`：准入、RabbitMQ 命令消费、重试、补偿与 DLQ。
- `order`：订单状态机、事务落单、幂等和查询。

业务层通过 `kv`、`messaging` 和 `storage` 访问 Redis、RabbitMQ 与 MySQL。核心链路是“Redis 快速准入与预留 + RabbitMQ 异步削峰 + MySQL 事务落单与 Outbox”。
