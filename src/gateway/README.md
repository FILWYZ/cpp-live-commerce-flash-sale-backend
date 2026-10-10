# 接入网关

负责客户端接入层：HTTP API、Session、请求边界、路由、鉴权入口和连接管理。

## 主要功能

- HTTP：用户认证、商品查询、活动详情、库存查询、秒杀下单、订单查询和运维接口。
- 连接生命周期：HTTP Keep-Alive、请求体限制、断线清理和慢客户端隔离。
- HTTP/1.1 请求边界校验：Host、严格 Content-Length、重复 Header 和不支持的 Transfer-Encoding 会被拒绝。
- 需要用户身份的写接口使用 `Authorization: Bearer <token>`；业务层以认证用户为准，不信任请求体中的 `user_id`。
- 只做协议适配与请求编排，不在 Gateway 内直接实现库存扣减或订单状态迁移。
