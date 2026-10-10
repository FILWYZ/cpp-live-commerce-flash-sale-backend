# inventory

维护 `available/reserved/sold` 三类库存，提供 `ReserveStock`、`ConfirmStock`、`ReleaseStock` 和 `QueryStock`。每次操作必须携带 `business_operation_id`，保证重试幂等并维持 `available >= 0`。

当前默认路径使用互斥保护库存与预留记录，并通过 LocalKV/WAL 恢复；可选的 `RedisInventoryFastPath` 使用按 SKU 同槽的 Lua 脚本，将检查、预留、确认、释放和补偿作为原子操作。`POST /admin/reconciliation/run` 会校验 Redis/MySQL 镜像并在 MySQL 审计表记录修复结果；旧的 `GET /admin/reconcile` 作为兼容入口保留。无 MySQL 的本地模式只校验进程内库存不变量，不能提供跨实例修复。
