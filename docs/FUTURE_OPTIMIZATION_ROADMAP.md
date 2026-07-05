# RStone 后续优化与扩展路线文档

本文档用于记录 RStone 当前距离“完整分布式数据库/分布式 KV 存储系统”还缺哪些能力，以及后续应该如何分阶段优化扩展。

当前 RStone 已经具备可运行、可测试的分布式 KV 原型能力：

- Gateway / PD / Store 三层架构。
- 基础 KV 读写。
- PD 元数据管理。
- 单进程 Multi-Region 原型。
- 三 Store 跨进程单 Region Raft 复制。
- Leader transfer。
- Follower 离线后追日志。
- Dashboard 状态展示。

但它还不是完整或生产级分布式数据库。后续优化需要围绕一致性、分片、调度、存储、运维和可靠性继续推进。

## 1. 当前主要不足

### 1.1 分布式 Multi-Region 还未完全实现

当前有两条 Store 路径：

- 旧路径：`MultiRegionCluster`，在一个进程里模拟多个 Region 和多个 Peer。
- 新路径：`DistributedRegionNode`，真实三 Store 跨进程单 Region Raft。

问题：

- Region Split 主要还在旧的单进程模拟路径。
- 真实跨 Store 路径目前重点是单 Region。
- 多 Region 的路由、复制、迁移、恢复还没有完全分布式化。

后续目标：

- 每个 Store 进程可以承载多个 Region Peer。
- 每个 Region 独立运行一个 Raft Group。
- Gateway 根据 key 路由到对应 Region Leader。
- Region Split 后左右 Region 都能在多个 Store 上运行。

### 1.2 Raft 还不是完整生产级实现

当前 Raft 已有：

- RequestVote。
- AppendEntries。
- 日志复制。
- 多数派提交。
- Leader transfer 原型。
- HardState / Log 持久化。
- 落后 follower 追日志。

仍缺：

- 周期性 heartbeat。
- 自动 election timeout。
- 自动 Leader 选举。
- PreVote。
- CheckQuorum。
- ReadIndex。
- Lease Read。
- InstallSnapshot。
- 日志压缩。
- 完整 membership change。
- Joint Consensus。

后续目标：

- Store 进程启动后自动参与 Raft，而不是依赖手动 leader hint。
- Leader 宕机后集群能自动选出新 Leader。
- 日志过长后自动 snapshot + compact。
- 新增副本时可以通过 snapshot 快速追平。

### 1.3 分布式 AddPeer / RemovePeer 不完整

当前旧路径支持 AddPeer / RemovePeer 原型，新分布式路径还不完整。

问题：

- 分布式路径不能完整地把新 Peer 加入 Raft Group。
- 新 Peer 追数据缺少 snapshot install。
- RemovePeer 没有完整的 Raft 配置变更安全保障。

后续目标：

- PD 生成 add/remove peer operator。
- Store 执行 conf change log。
- 新 Peer 先作为 Learner 追数据。
- 数据追平后提升为 Voter。
- RemovePeer 通过 Raft 日志提交后再生效。

### 1.4 存储引擎仍是文件型 KV

当前实现：

- `FileKvEngine`
- 用文件持久化 key/value。
- 适合教学和调试。

问题：

- 性能低。
- 不支持高效范围扫描。
- 没有 compaction。
- 没有 Write-Ahead Log 级别的存储优化。
- 与真实 LSM 存储引擎差距较大。

后续目标：

- 接入 LevelDB。
- 保留 `KvEngine` 抽象。
- 新增 `LevelDbEngine`。
- Raft log、Region meta、业务 KV 分前缀存储。
- 支持 batch write、range scan、snapshot read。

### 1.5 RPC 还不是 ZeroMQ

当前实现：

- 自研 TCP RPC。
- 支持请求/响应。
- 能满足本地多进程验证。

问题：

- 没有连接池。
- 没有超时控制。
- 没有重试策略。
- 没有异步 pipeline。
- 没有 ZeroMQ。
- 没有 protobuf。

后续目标：

- 保留 `RpcClient` / `RpcServer` 抽象。
- 新增 `ZmqRpcClient` / `ZmqRpcServer`。
- RPC 消息切换到 protobuf。
- 增加 request timeout。
- 增加连接复用。
- 增加错误码标准化。

### 1.6 读一致性模型还比较简化

当前：

- 强一致读主要依赖读 Leader。
- Follower 读作为最终一致读。

问题：

- Leader 读没有实现 ReadIndex。
- 没有 Lease Read。
- 网络分区时旧 Leader 读可能存在风险。

后续目标：

- 实现 ReadIndex。
- Gateway 强一致读默认走 ReadIndex。
- 可选 Lease Read 优化低延迟读。
- Follower read 必须显式声明 eventual consistency。

### 1.7 缺少事务和 MVCC

当前是简单 KV：

- `Put`
- `Get`
- `Delete`
- `Batch`

问题：

- 没有事务。
- 没有 MVCC。
- 没有时间戳服务。
- 没有隔离级别。

后续可选目标：

- 增加 MVCC key 编码。
- PD 提供 timestamp oracle。
- 支持 snapshot read。
- 支持乐观事务。
- 支持简单两阶段提交。

如果项目目标仍然是分布式 KV，可以不做完整 SQL，但 MVCC 是很有价值的扩展点。

### 1.8 缺少完整调度系统

当前 PD 有基础元数据和调度雏形。

问题：

- 没有 Store heartbeat 周期上报。
- 没有 Region heartbeat。
- 没有容量统计。
- 没有热点统计。
- 没有自动 balance leader。
- 没有自动 balance region。
- 没有 operator 执行状态机。

后续目标：

- Store 周期上报磁盘、Region 数量、Leader 数量。
- Region Leader 周期上报 Region 状态。
- PD 根据负载生成调度 operator。
- 支持 Leader balance。
- 支持 Region replica balance。
- 支持 Down Store 检测和副本补齐。

### 1.9 故障测试还不够

已有：

- 本地 e2e。
- 恢复 e2e。
- 分布式恢复 e2e。
- 一致性检查脚本。

仍缺：

- 网络分区测试。
- 随机杀进程测试。
- 磁盘损坏模拟。
- 慢节点模拟。
- 高并发读写测试。
- 长时间稳定性测试。
- 通用线性一致性检查。

后续目标：

- 新增 `tests/chaos/`。
- 新增随机故障注入脚本。
- 记录操作历史。
- 引入更严格的 linearizability checker。
- 跑长时间 stress。

### 1.10 Dashboard 还只是只读基础版

当前 Dashboard 已有：

- 集群摘要。
- Store 列表。
- Region 表。
- Raft runtime。
- warning。
- raw fields。

仍缺：

- 多 Store runtime 汇总。
- Region 拓扑图。
- 日志复制进度可视化。
- Store 离线高亮。
- Leader transfer 操作按钮。
- Split Region 操作按钮。
- 历史指标曲线。
- Prometheus 风格 metrics。

后续目标：

- 增加 `/api/metrics`。
- 增加 Store/Region 拓扑视图。
- 增加操作审计日志。
- 增加控制类 API，但需要安全确认。

## 2. 推荐分阶段路线

## 阶段 1：补齐分布式 Multi-Region

目标：

- 把旧 `MultiRegionCluster` 的能力迁移到真实跨 Store 路径。
- 每个 Store 支持多个 `DistributedRegionNode`。
- Store 内部维护 `region_id -> DistributedRegionNode`。
- Gateway 根据 PD 路由到目标 Region Leader。

任务：

1. 新增 `DistributedStore` 或 `StoreNode`。
2. 支持多个 Region Peer。
3. `StoreService` 根据 `region_id` 分发到对应 Peer。
4. Split Region 在所有 Store 上创建新 Peer。
5. Split 后迁移右半区数据。
6. 新增分布式 Split e2e。

验收：

```bash
scripts/e2e_distributed_split.sh
```

应覆盖：

- 写入左右两边 key。
- Split。
- 左右 Region 都能读写。
- Dashboard 显示两个 Region。

## 阶段 2：自动 Raft 心跳和选举

目标：

- 不依赖手动 leader hint。
- Leader 宕机后自动选主。

任务：

1. 每个 Peer 增加 tick loop。
2. Leader 周期发送 heartbeat。
3. Follower 维护 election timeout。
4. timeout 后成为 Candidate。
5. 发送 RequestVote。
6. 多数派同意后成为 Leader。
7. PD 接收 Region heartbeat 更新 leader。

验收：

- 杀掉 Leader。
- 不手动 transfer。
- 新 Leader 自动产生。
- Gateway 后续写入成功。

## 阶段 3：Snapshot 和日志压缩

目标：

- Raft log 不无限增长。
- 新副本可以快速追数据。

任务：

1. 设置 snapshot threshold。
2. 状态机生成 snapshot。
3. 删除旧日志。
4. 实现 InstallSnapshot RPC。
5. follower 落后太多时安装 snapshot。
6. 新 Peer 加入时先安装 snapshot。

验收：

- 连续写入大量 key。
- 日志被 compact。
- 删除 follower 数据后能通过 snapshot 恢复。

## 阶段 4：完整成员变更

目标：

- 分布式 AddPeer / RemovePeer 安全可用。

任务：

1. AddPeer 先创建 Learner。
2. Learner 追日志或安装 snapshot。
3. 追平后 promote learner。
4. RemovePeer 写入 conf change log。
5. 支持 Joint Consensus 或简化安全变更流程。

验收：

- 三副本增加到四副本。
- 四副本删除到三副本。
- 变更过程中持续读写。
- Leader 变更时不丢数据。

## 阶段 5：LevelDB 存储引擎

目标：

- 替换文件型 KV，提高写入和扫描性能。

任务：

1. 新增 `LevelDbEngine`。
2. 编译配置可选择 `FileKvEngine` 或 `LevelDbEngine`。
3. 迁移 RaftStorage。
4. 迁移业务 KV。
5. benchmark 对比。

验收：

- 所有测试在 LevelDB 后端通过。
- benchmark 输出 LevelDB 写入吞吐。

## 阶段 6：ZeroMQ + Protobuf RPC

目标：

- 更接近项目原始规划。
- RPC 协议更清晰、跨语言更方便。

任务：

1. 定义 protobuf schema。
2. 新增 ZeroMQ transport。
3. 保留 TCP RPC 作为 fallback。
4. CLI/Gateway/Store/PD 切换到新 RPC。
5. 增加协议兼容测试。

验收：

- 本地集群使用 ZeroMQ 通信。
- 所有 e2e 通过。

## 阶段 7：调度系统

目标：

- PD 从“元数据中心”升级为“元数据 + 调度中心”。

任务：

1. Store heartbeat。
2. Region heartbeat。
3. Down Store 检测。
4. Leader balance。
5. Region replica balance。
6. Operator 状态机。

验收：

- 人为制造 Leader 倾斜。
- PD 自动迁移部分 Leader。
- Dashboard 显示调度变化。

## 阶段 8：Dashboard 运维控制台

目标：

- 从只读观察升级为基础运维工具。

任务：

1. 增加 Store/Region 拓扑图。
2. 增加历史指标。
3. 增加 Transfer Leader 按钮。
4. 增加 Split Region 表单。
5. 增加操作确认。
6. 增加操作日志。

验收：

- 页面上能看到 Region 分布。
- 页面上能执行 transfer leader。
- 操作后状态实时刷新。

## 3. 优先级建议

建议按下面顺序推进：

| 优先级 | 模块 | 原因 |
| --- | --- | --- |
| P0 | 分布式 Multi-Region | 决定项目是否真正从单 Region 原型进入分布式存储系统 |
| P0 | 自动心跳和选举 | 决定 Raft 是否像真实系统一样运行 |
| P1 | Snapshot / Log Compaction | 决定系统能否长期运行 |
| P1 | 分布式 AddPeer / RemovePeer | 决定副本管理是否完整 |
| P1 | Dashboard 拓扑和告警 | 提升可观察性 |
| P2 | LevelDB | 提升存储真实性和性能 |
| P2 | ZeroMQ / Protobuf | 对齐技术栈规划 |
| P2 | 调度系统 | 提升集群自管理能力 |
| P3 | MVCC / Transaction | 扩展为更完整数据库 |

## 4. 简历表达建议

当前阶段可以写：

> 实现了一个基于 Raft 的分布式 KV 存储原型，包含 Gateway / PD / Store 三层架构、Region 元数据管理、Raft 日志复制、多数派提交、持久化恢复、跨进程三 Store 复制、Leader Transfer、落后副本追日志和 Web Dashboard 状态展示。

不建议写：

> 实现了生产级分布式数据库。

等完成 Multi-Region 分布式化、自动选举、Snapshot、成员变更、LevelDB 后，可以升级描述为：

> 实现了一个具备 Multi-Raft、多 Region 分片、动态副本管理、快照恢复和可视化运维能力的分布式 KV 存储系统。

## 5. 下一步推荐任务

最推荐下一步做：

**把 `DistributedRegionNode` 升级为多 Region 的 `DistributedStoreNode`。**

原因：

- 这是当前项目最大的结构性缺口。
- 完成后，Region Split、Dashboard、PD 路由、Raft 复制会真正串起来。
- 它是后续 AddPeer、Snapshot、调度系统的基础。

建议拆分：

1. 新增 `DistributedStoreNode`。
2. 内部维护 `std::map<RegionId, DistributedRegionNode>`。
3. `StoreService` 根据请求中的 `region_id` 找对应 Region。
4. 启动时从 PD 拉取所有 Region。
5. Split 时在所有 Store 创建新 Region Peer。
6. Dashboard 展示多个 runtime region。

完成这个阶段后，RStone 的整体完成度会从“单 Region 分布式原型”明显提升到“Multi-Raft 分布式存储系统原型”。
