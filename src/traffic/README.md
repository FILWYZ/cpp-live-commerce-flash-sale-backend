# traffic

活动入口在 Redis 配置模式下使用 `RedisTokenBucket`，通过 Redis Lua 和 Redis 服务端时间原子扣减共享令牌，默认值为全体 Gateway 实例共享 10,000 次/秒、2,000 次突发容量；客户端 IP 限制为 200 次/秒、500 次突发；认证用户限制为 2 次/秒、5 次突发；每个活动单独共享 10,000 次/秒、2,000 次突发容量。可通过 `ECOMMERCE_FLASH_SALE_RATE_PER_SECOND`、`ECOMMERCE_FLASH_SALE_BURST`、`ECOMMERCE_FLASH_SALE_IP_RATE_PER_SECOND`、`ECOMMERCE_FLASH_SALE_IP_BURST`、`ECOMMERCE_FLASH_SALE_USER_RATE_PER_SECOND`、`ECOMMERCE_FLASH_SALE_USER_BURST`、`ECOMMERCE_FLASH_SALE_CAMPAIGN_RATE_PER_SECOND` 和 `ECOMMERCE_FLASH_SALE_CAMPAIGN_BURST` 调整预算。服务按 TCP 对端地址识别 IP；仅当对端地址列在 `ECOMMERCE_TRUSTED_PROXY_IPS` 精确地址列表中才读取 `X-Forwarded-For` 首项。受信代理必须清理并重写该头。Redis 限流依赖不可用时入口 fail-closed 返回 503，防止各实例静默退化为各自一份限额。

未配置 Redis 的本地开发模式使用进程内 `TokenBucket`，同一组 10,000 次/秒、2,000 次突发参数只对单进程生效。`tools/redis_concurrency_test` 在外部测试环境验证并发请求不会突破共享全局桶或用户桶容量。

当 IP 和用户限额通过、全局或活动预算耗尽时，Redis 加共享命令队列模式可将请求放入有界 FIFO 等待室，并以 `202 WAITING` 返回排位。等待室命令与请求幂等键一一对应；后台多 worker 使用 Redis 租约领取票据，取得共享全局/活动令牌后调用同一 `FlashSaleOrderService` 入命令队列。worker 崩溃后票据仍在有序集合中，租约到期后可由其他 worker 重领。队列容量和 worker 数量由 `ECOMMERCE_FLASH_SALE_WAITING_CAPACITY`、`ECOMMERCE_FLASH_SALE_WAITING_WORKERS` 控制；`ECOMMERCE_FLASH_SALE_WAITING_ROOM=false` 可关闭等待室，超额流量改为 429。用户和 IP 限流仍直接拒绝，以免滥用等待室。进程内 local 命令队列不会启用共享等待室。

队列背压由 HTTP Server 内部的有界业务线程池负责；重试由具体的 MySQL、Outbox 和消息消费者根据业务语义实现，不再额外维护一套未接入主链路的通用治理框架。
