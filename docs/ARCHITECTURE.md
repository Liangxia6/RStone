# RStone 架构设计

本文档描述 RStone 的分布式 KV 存储架构：接入层无状态，元数据和调度独立，存储层采用 Region + Multi-Raft。

RStone 的目标是实现一个结构清晰、可分片、可调度、可横向扩展的分布式 KV 存储系统。

## 1. 架构总览

```text
Client / CLI / SDK
        |
        v
+-----------------------+
| RStone Gateway        |
| Stateless API Layer   |
+-----------------------+
        |
        | metadata lookup / route cache
        v
+-----------------------+
| RStone PD             |
| Metadata & Scheduler  |
+-----------------------+
        |
        | region route / scheduling command
        v
+------------------------------------------------+
| RStone Store Cluster                           |
|                                                |
| Store-1        Store-2        Store-3          |
| Region-1 Peer  Region-1 Peer  Region-1 Peer   |
| Region-2 Peer  Region-2 Peer  Region-2 Peer   |
| Multi-Raft     Multi-Raft     Multi-Raft       |
| LevelDB        LevelDB        LevelDB          |
+------------------------------------------------+
```

RStone 拆成三类核心组件：

- **RStone Gateway**：无状态接入层，负责客户端协议、请求解析、路由缓存、Leader redirect 和错误转换。
- **RStone PD**：元数据和调度中心，负责 Store 注册、Region 元数据、Leader 分布、Region split/merge、迁移调度。
- **RStone Store**：真正保存数据的存储节点，内部运行多个 Raft Group，每个 Region 对应一个 Raft Group。

## 2. 组件职责

### 2.1 RStone Gateway

Gateway 是无状态接入层，第一版只提供 KV API，不实现 SQL 引擎。

职责：

- 对外提供 HTTP 或 ZeroMQ Client API。
- 接收 `Get/Put/Delete/Batch/Scan` 请求。
- 从 PD 获取 Region 路由表。
- 根据 key 定位 Region。
- 根据 Region 找到当前 Leader Store。
- 将写请求转发到 Region Leader。
- 对 `NOT_LEADER`、`REGION_NOT_FOUND`、`STALE_EPOCH` 等错误执行重试或刷新路由。

特性：

- 无状态，可水平扩展。
- 不直接存储数据。
- 可以被 Nginx 负载均衡。

第一版可把 Gateway 和 Store 放在同一个进程里，后续再拆分为独立进程。

### 2.2 RStone PD

PD 负责全局元数据和调度。

职责：

- 维护 Store 列表和健康状态。
- 维护 Region 元数据：key range、epoch、peer 列表、leader。
- 接收 Store heartbeat 和 Region heartbeat。
- 分配全局唯一 ID：store_id、region_id、peer_id。
- 判断 Region 是否需要 split。
- 判断副本是否缺失并下发补副本任务。
- 做 Leader balance 和 Region balance。
- 管理 placement rule，例如副本数、机架隔离、节点标签。

第一版实现范围：

- Store 注册。
- Region 路由查询。
- Region heartbeat。
- 静态三副本分布。
- 简单 leader cache。

第二版再做：

- 自动 split。
- 副本迁移。
- learner。
- joint consensus 成员变更。
- PD 自身高可用。

注意：PD 是元数据中心，不应该参与每次数据读写的主路径。Gateway 应缓存路由，只有缓存失效或 Region 变更时再访问 PD。

### 2.3 RStone Store

Store 是数据节点。

职责：

- 持有多个 Region Peer。
- 每个 Region Peer 参与一个 Raft Group。
- Region Leader 处理写请求。
- Follower 复制日志并可提供最终一致读。
- 将提交后的 Raft Log apply 到 LevelDB。
- 定期向 PD 汇报 Store 和 Region 状态。
- 支持 snapshot、日志压缩、Region split 和副本迁移。

Store 内部结构：

```text
Store
  +-- RpcServer
  +-- RpcClient
  +-- StoreMeta
  +-- RaftRouter
  +-- MultiRaftManager
  |     +-- Region 1 RaftGroup
  |     +-- Region 2 RaftGroup
  |     +-- Region N RaftGroup
  +-- ApplyWorker
  +-- LevelDBEngine
```

## 3. Region 模型

Region 是 RStone 的基本数据分片和调度单位。

```text
Region {
  region_id: 101,
  start_key: "user:",
  end_key: "user;",
  epoch: {
    conf_ver: 3,
    version: 8
  },
  peers: [
    { peer_id: 1, store_id: 1, role: Voter },
    { peer_id: 2, store_id: 2, role: Voter },
    { peer_id: 3, store_id: 3, role: Voter }
  ],
  leader_peer_id: 1
}
```

关键点：

- 一个 Region 管理一个连续 key range：`[start_key, end_key)`。
- 一个 Region 有多个 Peer，分布在不同 Store 上。
- 一个 Region 的所有 Peer 组成一个 Raft Group。
- 一个 Raft Group 同一时刻只有一个 Leader。
- 写请求必须发给该 Region 的 Leader。
- Region epoch 用来识别路由是否过期。

## 4. Multi-Raft 设计

单 Raft Group 只能支撑一个分片，扩展性有限。RStone 采用 Multi-Raft 让不同 Region 独立复制和调度：

```text
Region 1 -> Raft Group 1
Region 2 -> Raft Group 2
Region 3 -> Raft Group 3
...
```

每个 Store 上会同时运行多个 Region Peer：

```text
Store-1:
  Region-1 Peer-A
  Region-2 Peer-B
  Region-3 Peer-C
```

好处：

- 数据可以按 key range 分散到多个 Region。
- 不同 Region 可以有不同 Leader，提高并发写入能力。
- Region 可以迁移，支持负载均衡。
- 热点 Region 可以 split。

第一版不必实现复杂调度，可以先固定 16 个 Region，每个 Region 三副本。

## 5. 数据读写路径

### 5.1 写入路径

```text
Client
  -> Gateway
  -> route key to Region
  -> find Region Leader Store
  -> Store Leader proposes Raft log
  -> replicate to followers
  -> majority committed
  -> apply to LevelDB
  -> return success
```

写入成功条件：

- 请求到达正确 Region Leader。
- Raft 日志复制到多数 Peer。
- Leader apply 成功。

如果请求打到 Follower：

- Store 返回 `NOT_LEADER`，携带最新 Leader hint。
- Gateway 刷新缓存并重试。

### 5.2 强一致读路径

```text
Client
  -> Gateway
  -> Region Leader
  -> ReadIndex / Leader lease validation
  -> wait applied_index >= read_index
  -> read LevelDB
  -> return value
```

第一版建议使用 ReadIndex，避免直接读 Leader 本地状态导致旧 Leader 读问题。

### 5.3 最终一致读路径

```text
Client
  -> Gateway
  -> any Region Peer
  -> local read
  -> return value + applied_index
```

Follower read 必须明确标记为 eventual consistency，不能和强一致读混用。

## 6. 存储层设计

RStone 使用 LevelDB 作为本地 LSM 存储引擎。

建议 key prefix：

```text
raft/log/{region_id}/{index}
raft/meta/{region_id}/term
raft/meta/{region_id}/vote
raft/meta/{region_id}/commit_index
region/meta/{region_id}
kv/{region_id}/{user_key}
```

重要原则：

- 客户端写入先进入 Raft Log。
- 日志提交后再 apply 到 `kv/` 前缀。
- LevelDB `WriteBatch` 用于一次 apply 中的原子写入。
- 不要声称 LevelDB 原生支持完整事务；RStone 可以实现命令级原子写入和批量写入。

## 7. 调度与负载均衡

RStone 的负载均衡拆成两层：

### 7.1 内部调度

由 RStone PD 完成：

- Region 数量均衡。
- Leader 数量均衡。
- 副本分布均衡。
- 热点 Region split。
- Store 下线后的副本补齐。

更准确的内部表述是：

> 使用 Region 路由和 PD 调度实现内部数据分布与负载均衡。

一致性哈希可以保留为早期简单路由实验，但正式架构以 Region range + PD 调度为核心。

### 7.2 外部负载均衡

由 Nginx 或 LB 负责：

- 对多个 Gateway 做负载均衡。
- 健康检查异常 Gateway。
- 统一暴露外部访问入口。

推荐：

```text
Client -> Nginx -> Gateway Cluster -> Store Cluster
```

如果客户端协议是 ZeroMQ，Nginx 只能做 TCP stream 层代理；如果想要 HTTP 语义、健康检查和调试便利，建议 Gateway 暴露 HTTP API。

## 8. 与原始方案的变化

原始方案：

```text
Client -> Node -> Single Raft Group -> LevelDB
```

新的分层方案：

```text
Client -> Stateless Gateway -> PD Metadata -> Region Leader -> Multi-Raft -> LevelDB
```

主要变化：

- 从单 Group 变为 Multi-Raft。
- 从 hash node 变为 Region range 路由。
- 从节点自管理变为 PD 统一管理元数据和调度。
- 从 Master/Slave 表述变为 Leader/Follower。
- 从“内部负载均衡”升级为“Region 调度、Leader balance、数据迁移”。

## 9. 分阶段实现建议

### 阶段 1：单 Region Raft KV

- 一个 Region。
- 三个 Store。
- 一个 Raft Group。
- LevelDB 持久化。
- Put/Get/Delete。

### 阶段 2：Gateway + 路由缓存

- Gateway 接收请求。
- Gateway 查询 PD 获取 Region。
- 请求转发到 Region Leader。
- 支持 leader redirect。

### 阶段 3：最小 PD

- Store 注册。
- Store heartbeat。
- Region heartbeat。
- Region route 查询。
- Leader cache。

### 阶段 4：多 Region 静态分片

- 预创建多个 Region。
- 每个 Region 一个 Raft Group。
- key 按 range 路由。
- Store 管理多个 Raft Group。

### 阶段 5：Region Split

- 根据 Region size 或 key count 触发 split。
- 更新 Region epoch。
- Gateway 处理 stale route。

### 阶段 6：调度与成员变更

- Add peer。
- Remove peer。
- Transfer leader。
- Region balance。
- Leader balance。

### 阶段 7：可观测性和压测

- 每个 Region 的 leader、term、commit index、applied index。
- Gateway 路由缓存命中率。
- PD 调度日志。
- Store 写入延迟、Raft 复制延迟、apply 延迟。

## 10. 简历表述重点

简历重点应突出 RStone 自身的系统设计：

- Gateway、PD、Store 三层架构。
- 使用 Region 作为数据分片和调度单位。
- 每个 Region 多副本组成独立 Raft Group。
- Gateway 无状态，支持路由缓存和 leader redirect。
- PD 管理元数据、心跳和调度。
- Store 集成 LevelDB，负责 Raft 日志持久化和 KV 状态机 apply。

不要过度声称：

- 不要说实现了 SQL 引擎，除非真的实现。
- 不要说完整分布式事务，除非实现了 MVCC + 2PC。
- 不要说 LevelDB 原生支持事务。
