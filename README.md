# RStone

RStone 是一个计划实现的基于 Raft 的分布式 KV 存储项目。当前仓库从空白项目开始，推荐先按实现文档逐层搭建：

- [分层实现文档](docs/IMPLEMENTATION_PLAN.md)
- [RStone 架构设计](docs/ARCHITECTURE.md)
- [RStone 项目学习文档](docs/STUDY_GUIDE.md)
- [RStone 代码阅读教学文档](docs/CODE_READING_GUIDE.md)
- [Web Dashboard 实现文档](docs/WEB_DASHBOARD_IMPLEMENTATION.md)
- [后续优化与扩展路线](docs/FUTURE_OPTIMIZATION_ROADMAP.md)
- [简历项目描述](docs/RESUME_DESCRIPTION.md)

核心目标：

- 分层架构：Gateway 无状态接入，PD 管理元数据和调度，Store 承载数据副本。
- Multi-Raft 存储：key space 按 Region 切分，每个 Region 多副本组成独立 Raft Group。
- RPC 框架：基于 ZeroMQ 的轻量级 TCP RPC。
- 存储引擎：集成 LevelDB，持久化 Raft 日志、Region 元数据和业务 KV。
- 读写一致性：Leader 提供强一致读写，Follower 支持可选最终一致读。
- 负载均衡：PD 进行 Region/Leader 调度，Nginx 负载均衡多个无状态 Gateway。

## 当前开发入口

当前环境下 `cmake` 进程会被系统直接杀掉，因此仓库同时提供手动构建脚本：

```bash
scripts/test_manual.sh
```

运行 Gateway 端到端 demo：

```bash
scripts/build_manual.sh
./build/rstone-server --role gateway --config config/gateway.yaml --demo
```

启动本地三进程 TCP 集群：

```bash
scripts/run_local_cluster.sh
./build/rstone-cli --endpoint 127.0.0.1:8081 put user:1 alice
./build/rstone-cli --endpoint 127.0.0.1:8081 get user:1
./build/rstone-cli --endpoint 127.0.0.1:8081 transfer-leader 1 2
./build/rstone-cli --endpoint 127.0.0.1:8081 remove-peer 1 3
./build/rstone-cli --endpoint 127.0.0.1:8081 split 1 m
./build/rstone-cli --endpoint 127.0.0.1:8081 status
scripts/stop_local_cluster.sh
```

运行本地端到端验证：

```bash
scripts/e2e_local_cluster.sh
```

运行本地故障恢复验证：

```bash
scripts/e2e_recovery_cluster.sh
```

运行单 key 强一致读写历史检查：

```bash
scripts/consistency_check.sh
```

运行三 Store 分布式 Raft 复制验证：

```bash
scripts/e2e_distributed_cluster.sh
```

运行三 Store 分布式恢复验证：

```bash
scripts/e2e_distributed_recovery_cluster.sh
```

运行 Dashboard 端到端验证：

```bash
scripts/e2e_dashboard.sh
```

运行本地 benchmark：

```bash
scripts/benchmark.sh 100
```

Nginx TCP stream 入口配置示例：

```bash
nginx -c /absolute/path/to/RStone/config/nginx.conf -p /absolute/path/to/RStone
./build/rstone-cli --endpoint 127.0.0.1:18081 put user:1 alice
```

已实现的代码层：

- 基础 `Status/ErrorCode/Config`。
- 文件型 KV 引擎和 `WriteBatch`。
- RPC 抽象、消息编解码、进程内 RPC 和 TCP RPC transport。
- PD 元数据、Store 注册、Region 路由、Split、Peer 变更。
- PD RPC service 和 Store RPC service。
- RaftNode 选举、投票、AppendEntries、日志复制基础逻辑。
- RaftStorage hard state 和 log 持久化，以及 Store 重启恢复。
- Snapshot 导出和恢复。
- Single-Region 三副本复制。
- Multi-Region / Multi-Raft 原型。
- Region Split 数据迁移：右半区 key range 迁入新 Region，原 Region 删除迁移数据。
- Region catalog 持久化：split 后重启可恢复多个 Region 和对应数据。
- PD catalog 持久化：Store/Region/Peer 元数据和 next id 可在 PD 重启后恢复。
- 故障恢复 e2e：覆盖 Gateway 重启、PD 重启、Store 重启、split 后 Store 恢复。
- 一致性检查脚本：覆盖 put/get/delete、leader transfer、Gateway 重启、Region split 后的强一致读取。
- TransferLeader 调度闭环：PD 更新 leader hint，Store 切换 Region leader，Gateway 清理路由缓存。
- AddPeer/RemovePeer 成员变更闭环：新增 peer 同步 leader 数据和 Raft log，移除 peer 后继续写入。
- Gateway 路由缓存、stale route 自动刷新、RPC Gateway client 和客户端 SDK。
- `cluster.Status` 可观测接口：汇总 Gateway route cache、PD Store/Region、Store Region runtime 状态。
- `rstone-server --serve` 支持 PD、Store、Gateway 三种 TCP 服务进程。
- `rstone-cli` 支持通过 Gateway TCP RPC 执行 `put/get/delete/split/transfer-leader/add-peer/remove-peer/status`。
- 本地集群脚本会等待 PD/Store/Gateway 监听就绪后再返回，避免启动竞态。
- 分布式单 Region Raft 路径：每个 Store 进程承载一个本地 Peer，Leader 通过 TCP RPC 向其他 Store 发送 RequestVote/AppendEntries，多数派提交后应用状态机。
- 分布式恢复验证：覆盖 follower 离线后补日志、leader 下线后手动迁移到其他 peer、旧 leader 重启后追平日志。
- Gateway 动态 Store 路由：服务模式可按 PD 返回的 Region leader endpoint 访问目标 Store。
- Web Dashboard：Gateway 可启动 `9090` HTTP 页面，实时展示 PD、Store、Region 和 Raft runtime 状态。
- Nginx stream 配置示例：`config/nginx.conf`。
- Benchmark 脚本：`scripts/benchmark.sh` 输出 put/get 耗时与吞吐。
