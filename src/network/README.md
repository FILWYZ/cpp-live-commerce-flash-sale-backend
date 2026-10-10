# 网络模块

从现有 Mymoduo Reactor 网络库演进，负责 Linux 高性能网络基础设施：`EventLoop`、`Channel`、`epoll`、`Buffer`、`eventfd`、`timerfd`、线程模型和对象生命周期。

## 实现边界

- 第一阶段使用非阻塞 socket + epoll LT，先保证连接、读写和错误处理正确；ET 作为后续对比实验。
- Buffer 支持动态扩容、读写指针、输出缓冲和 `readv` 聚合读取。
- 使用 RAII、`shared_ptr/weak_ptr` 和连接状态管理，避免异步回调访问已析构连接。
- 内置写队列高水位、慢客户端隔离、连接数上限和优雅停机。

`network` 不承载业务协议和订单逻辑；HTTP 解析和路由放在 `gateway`。
