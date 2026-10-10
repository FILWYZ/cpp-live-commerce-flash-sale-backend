# 配置文件

存放本地开发和压测配置，不把环境参数硬编码进 C++。

- `dev.yaml`：单实例开发配置。
- `benchmark.yaml`：本地接口和业务压测参数。
- 多角色及真实依赖演练使用 `deploy/compose/docker-compose.yml`，由环境变量设置端口、连接池、工作线程数和活动库存。
