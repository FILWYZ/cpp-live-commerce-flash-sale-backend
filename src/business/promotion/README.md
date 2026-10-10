# 活动模块

统一秒杀活动域，支持创建、预热、开始、暂停、恢复、结束、资格判断和查询。活动状态为 `CREATED -> PREHEATING -> RUNNING <-> PAUSED -> FINISHED`，库存操作统一交给 `inventory`。

`FlashSaleAdmissionGate` 将活动状态、配额和用户幂等从业务流程中独立出来。默认构建使用进程内实现；配置 Redis 时，Gateway 使用 `RedisFlashSaleGate` 进行跨进程 admission。闸门只是流量过滤层，不等同于最终库存扣减。

管理接口当前提供 `GET/POST/PATCH /admin/campaigns`、`POST /admin/campaigns/preheat|start|pause|resume|finish` 和 `GET /admin/audit?limit=100`。创建和编辑字段包括 `campaign_id`、`sku_id`、`participant_limit`；可选 `starts_at`、`ends_at` 使用 Unix 秒且必须成对配置，活动窗口外的请求会被拒绝；PATCH 仅允许编辑 CREATED 活动。入口当前固定每用户每活动限购 1 件。配置 MySQL 时，活动状态、时间窗和管理审计写入 `campaigns` 与 `admin_audit_logs`，启动会恢复活动并同步 Redis 门禁；MySQL 事务会对 SKU 余量行加锁并核验所有未结束活动剩余配额之和，防止多个活动重复分配；无 MySQL 时进程内校验提供保守保护。
