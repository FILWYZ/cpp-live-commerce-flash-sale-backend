# 缓存模块

负责业务主缓存和热点保护，不直接承担持久化 KV 的职责。

## 主实现方案

`Sharded LRU + TTL + Singleflight`

- 分片降低全局锁竞争。
- TTL 控制商品、活动等数据的新鲜度。
- Singleflight 合并同一热点 Key 的并发回源请求，防止缓存击穿。
- 回源顺序：本地缓存 -> `KVClient` -> 业务存储。

LRU、LFU、LRU-K、ARC 只在 `benchmarks/cache` 中做算法对比，不让业务主路径同时维护多套淘汰策略。
