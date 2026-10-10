# 流量控制模块

Redis 配置模式使用 `RedisTokenBucket` 通过 Lua 与 Redis 服务端时间原子扣减共享令牌。默认限制包括全局、活动、IP 和认证用户维度，可由环境变量配置。IP 使用 TCP 对端地址；仅当对端地址在 `ECOMMERCE_TRUSTED_PROXY_IPS` 精确列表中时才读取 `X-Forwarded-For` 首项。

Redis 限流不可用时入口 fail-closed 返回 503。未配置 Redis 的本地开发模式使用进程内 `TokenBucket`，预算只对单进程生效。超过令牌预算的请求返回 429；项目不提供 Waiting Room。
