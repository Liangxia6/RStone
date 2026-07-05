# RStone 分层实现文档

本文档面向从空白工程开始实现 RStone。最终目标是构建一个基于 Raft 的分布式 KV 存储系统，采用 Gateway、PD、Store 三层架构，以 Region 作为数据分片单位，以 Multi-Raft 支撑多分片复制和调度。

实现原则：

- 先做可运行闭环，再逐步扩展。
- 先实现单 Region，再扩展 Multi-Raft。
- 先实现静态配置，再实现动态调度。
- 先保证一致性正确，再优化性能。
- LevelDB 只作为本地存储引擎，分布式一致性由 Raft 保证。

## 1. 总体目标

RStone 的核心能力：

- **接入层**：Gateway 无状态接入，负责客户端协议、请求路由、路由缓存和错误重试。
- **元数据层**：PD 负责 Store 注册、Region 元数据、Leader 信息、心跳和基础调度。
- **存储层**：Store 保存实际数据，内部运行多个 Region Peer 和 Raft Group。
- **共识层**：实现 Raft 选举、心跳、日志复制、多数派提交、快照和成员变更。
- **存储引擎**：集成 LevelDB，持久化 Raft 日志、Region 元数据、状态机 KV 数据。
- **一致性语义**：Leader 提供强一致读写，Follower 支持显式最终一致读。
- **负载均衡**：内部通过 PD 做 Region/Leader 调度，外部通过 Nginx 负载均衡多个 Gateway。

第一版最小可行目标：

- 一个 PD。
- 一个 Gateway。
- 三个 Store。
- 一个 Region。
- 一个 Raft Group。
- 支持 `Put/Get/Delete`。
- Leader 写入多数派提交。
- Leader 强一致读。
- Follower 最终一致读。

第二版再扩展：

- 多 Region。
- Multi-Raft。
- Region route cache。
- snapshot 和日志压缩。
- Region split。
- AddPeer/RemovePeer/TransferLeader。
- Gateway 集群和 Nginx 入口。

## 2. 技术栈

推荐使用 C++17 或 C++20。

基础依赖：

- 构建系统：CMake。
- RPC 通信：ZeroMQ + cppzmq。
- 序列化：Protocol Buffers，或原型阶段使用 JSON。
- 存储引擎：LevelDB。
- 测试框架：GoogleTest。
- 日志：spdlog。
- 配置：YAML-CPP。
- 命令行：CLI11 或 cxxopts。
- HTTP Gateway：第一版可用简单 HTTP 库，或先只暴露 ZeroMQ 客户端协议。

建议第一阶段使用 JSON 序列化降低调试成本，等 RPC 和 Raft 流程稳定后再切到 Protobuf。

## 3. 逻辑架构

```text
Client / SDK / CLI
        |
        v
+----------------------+
| Nginx / LoadBalancer |
+----------------------+
        |
        v
+----------------------+
| RStone Gateway       |
| Stateless API Layer  |
+----------------------+
        |
        | route lookup / route cache
        v
+----------------------+
| RStone PD            |
| Metadata & Scheduler |
+----------------------+
        |
        | region leader route
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

三类进程：

- `rstone-gateway`：客户端入口。
- `rstone-pd`：元数据和调度中心。
- `rstone-store`：数据节点。

为了降低早期开发成本，第一版也可以做成一个 `rstone-server` 可执行文件，通过配置选择运行模式：

```bash
rstone-server --role pd --config config/pd.yaml
rstone-server --role gateway --config config/gateway.yaml
rstone-server --role store --config config/store1.yaml
```

## 4. 推荐目录结构

```text
RStone/
  CMakeLists.txt
  README.md
  docs/
    ARCHITECTURE.md
    IMPLEMENTATION_PLAN.md
    RESUME_DESCRIPTION.md
    RPC_PROTOCOL.md
    RAFT_DESIGN.md
    STORAGE_DESIGN.md
    DEPLOYMENT.md
  config/
    pd.yaml
    gateway.yaml
    store1.yaml
    store2.yaml
    store3.yaml
    nginx.conf
  include/rstone/
    common/
      status.h
      error_code.h
      types.h
      config.h
      clock.h
      logging.h
    rpc/
      rpc_server.h
      rpc_client.h
      rpc_message.h
      rpc_registry.h
    pd/
      pd_server.h
      metadata_store.h
      scheduler.h
      heartbeat.h
      id_allocator.h
    gateway/
      gateway_server.h
      route_cache.h
      kv_handler.h
    store/
      store_server.h
      store_meta.h
      raft_router.h
      apply_worker.h
    raft/
      raft_node.h
      raft_log.h
      raft_storage.h
      raft_message.h
      replicator.h
      snapshot.h
    region/
      region.h
      region_epoch.h
      peer.h
      region_router.h
    storage/
      leveldb_engine.h
      write_batch.h
      kv_codec.h
    client/
      rstone_client.h
  src/
    common/
    rpc/
    pd/
    gateway/
    store/
    raft/
    region/
    storage/
    client/
    main.cpp
  tests/
    unit/
    integration/
    chaos/
  scripts/
    run_local_cluster.sh
    stop_local_cluster.sh
    clean_data.sh
    benchmark.sh
  tools/
    rstone_cli.cpp
```

## 5. 核心数据模型

### 5.1 Store

Store 是物理数据节点。

```cpp
struct StoreInfo {
  uint64_t store_id;
  std::string host;
  int raft_port;
  int client_port;
  std::string state;  // Up, Down, Tombstone
  std::map<std::string, std::string> labels;
  int64_t last_heartbeat_ms;
};
```

### 5.2 Region

Region 是数据分片和调度单位，管理一个连续 key range。

```cpp
struct RegionEpoch {
  uint64_t conf_ver;
  uint64_t version;
};

struct Peer {
  uint64_t peer_id;
  uint64_t store_id;
  std::string role;  // Voter, Learner
};

struct RegionInfo {
  uint64_t region_id;
  std::string start_key;
  std::string end_key;
  RegionEpoch epoch;
  std::vector<Peer> peers;
  uint64_t leader_peer_id;
};
```

规则：

- key 属于 Region，当且仅当 `start_key <= key < end_key`。
- Region split 会增加 `version`。
- Peer 变更会增加 `conf_ver`。
- Gateway 和 Store 都必须校验 Region epoch，避免使用过期路由。

### 5.3 Raft Log Entry

```cpp
struct LogEntry {
  uint64_t region_id;
  uint64_t index;
  uint64_t term;
  EntryType type;  // Normal, ConfigChange, Split, Noop
  std::string command;
};
```

### 5.4 KV Command

```cpp
enum class KvOpType {
  Put,
  Delete,
  Batch,
  Cas
};

struct KvCommand {
  KvOpType type;
  std::string key;
  std::string value;
  uint64_t expected_version;
  std::vector<KvOperation> operations;
};
```

写命令必须进入对应 Region 的 Raft 日志。读命令根据一致性级别选择 Leader Read 或 Follower Read。

## 6. RPC 设计

### 6.1 ZeroMQ 模式

第一版建议：

- 内部 RPC：ZeroMQ `REQ/REP`，优先简单可调试。
- 后续升级：`DEALER/ROUTER`，支持异步并发、连接复用和批量复制。
- Gateway 对外：可以先用 HTTP，也可以直接使用 ZeroMQ Client API。

如果引入 Nginx，推荐：

```text
Client -> Nginx HTTP -> Gateway -> ZeroMQ internal RPC
```

Nginx 不负责理解 Region、Leader 或 Raft，只负责外部流量分发和健康检查。

### 6.2 RPC Envelope

统一请求格式：

```json
{
  "request_id": "uuid",
  "method": "store.KvPut",
  "source": "gateway-1",
  "target": "store-1",
  "deadline_ms": 3000,
  "payload": {}
}
```

统一响应格式：

```json
{
  "request_id": "uuid",
  "ok": true,
  "error_code": "",
  "error_message": "",
  "payload": {}
}
```

### 6.3 Gateway RPC

客户端 API：

- `kv.Get`
- `kv.Put`
- `kv.Delete`
- `kv.Batch`
- `kv.Scan`，后续实现
- `cluster.Status`

### 6.4 PD RPC

Gateway 调用：

- `pd.GetRegionByKey`
- `pd.GetRegion`
- `pd.GetStore`
- `pd.GetClusterStatus`

Store 调用：

- `pd.RegisterStore`
- `pd.StoreHeartbeat`
- `pd.RegionHeartbeat`
- `pd.AllocId`
- `pd.AskSplit`
- `pd.ReportSplit`

调度相关：

- `pd.GetOperator`
- `pd.ReportOperatorResult`

第一版可以只实现注册、心跳和路由查询。

### 6.5 Store RPC

Gateway 到 Store：

- `store.KvGet`
- `store.KvPut`
- `store.KvDelete`
- `store.KvBatch`

Store 到 Store：

- `raft.RequestVote`
- `raft.AppendEntries`
- `raft.InstallSnapshot`
- `raft.TimeoutNow`

错误响应必须携带足够的重试信息：

```json
{
  "error_code": "NOT_LEADER",
  "leader": {
    "store_id": 2,
    "peer_id": 8,
    "host": "127.0.0.1",
    "client_port": 8102
  },
  "region": {
    "region_id": 101,
    "epoch": { "conf_ver": 1, "version": 1 }
  }
}
```

## 7. PD 实现

### 7.1 PD 职责

PD 是集群元数据中心和调度入口。

第一版职责：

- 分配 ID。
- 注册 Store。
- 保存 Store 心跳。
- 保存 Region 元数据。
- 保存 Region Leader 信息。
- 支持按 key 查询 Region。
- 支持查询 Store 地址。

第二版职责：

- 检测 Store 下线。
- 补齐副本。
- Transfer Leader。
- Add Peer / Remove Peer。
- Region split。
- Leader balance。
- Region balance。

### 7.2 PD 元数据存储

第一版可以用内存 + 本地文件快照；更稳妥的是直接使用 LevelDB。

建议 key prefix：

```text
pd/id/next
pd/store/{store_id}
pd/region/{region_id}
pd/region_index/{start_key}
pd/operator/{region_id}
```

Region 查询需要按 key 找到所属 range。可以先用内存有序 map：

```cpp
std::map<std::string, RegionInfo> regions_by_start_key;
```

查询逻辑：

1. 找到 `start_key <= key` 的最后一个 Region。
2. 判断 `key < end_key`。
3. 返回 Region 和 Leader。

### 7.3 心跳

Store heartbeat：

```text
store_id
capacity
available
region_count
leader_count
start_time
```

Region heartbeat：

```text
region_id
epoch
peers
leader
approximate_size
approximate_keys
written_bytes
read_bytes
```

PD 通过心跳更新：

- Store 是否存活。
- Region 当前 Leader。
- Region 大小。
- 调度候选任务。

### 7.4 调度 Operator

Operator 是 PD 下发给 Store 的调度动作。

第一版可定义但不实现复杂调度：

```text
TransferLeader(region_id, target_peer)
AddPeer(region_id, target_store)
RemovePeer(region_id, peer_id)
SplitRegion(region_id, split_key)
```

Store 在 heartbeat 时拉取 operator，执行后上报结果。

## 8. Store 实现

### 8.1 Store 内部模块

```text
StoreServer
  +-- RpcServer
  +-- RpcClient
  +-- StoreMeta
  +-- RaftRouter
  +-- MultiRaftManager
  +-- ApplyWorker
  +-- LevelDBEngine
  +-- HeartbeatWorker
```

模块说明：

- `StoreMeta`：保存本 Store 上有哪些 Region Peer。
- `RaftRouter`：根据 region_id 把 Raft 消息投递到对应 RaftNode。
- `MultiRaftManager`：管理多个 RaftNode 的生命周期。
- `ApplyWorker`：按 Region 顺序 apply 已提交日志。
- `HeartbeatWorker`：向 PD 上报 Store 和 Region 状态。

### 8.2 Store 启动流程

1. 加载配置。
2. 打开 LevelDB。
3. 从本地存储加载 Store ID。
4. 如果没有 Store ID，向 PD 注册并获取。
5. 加载本地 Region 元数据。
6. 为每个 Region Peer 创建 RaftNode。
7. 启动 RPC server。
8. 启动 Raft timer。
9. 启动 apply worker。
10. 启动 heartbeat worker。

### 8.3 Region Peer 生命周期

Region Peer 可能处于：

- `Initializing`
- `Normal`
- `ApplyingSnapshot`
- `Splitting`
- `Tombstone`

第一版只需要 `Normal` 和 `Tombstone`。

## 9. Raft 实现

### 9.1 节点角色

每个 Region Peer 内部运行一个 RaftNode，角色包括：

- Follower
- Candidate
- Leader

状态转换：

```text
Follower -- election timeout --> Candidate
Candidate -- majority votes --> Leader
Candidate -- higher term discovered --> Follower
Leader -- higher term discovered --> Follower
```

### 9.2 持久化状态

每个 Region 独立持久化：

- `current_term`
- `voted_for`
- `log_entries`
- `commit_index`
- `last_applied`
- `snapshot_meta`
- `region_meta`

建议 key prefix：

```text
raft/meta/{region_id}/current_term
raft/meta/{region_id}/voted_for
raft/meta/{region_id}/commit_index
raft/meta/{region_id}/last_applied
raft/log/{region_id}/{index}
raft/snapshot/{region_id}/meta
region/meta/{region_id}
```

### 9.3 选举

流程：

1. Follower 启动随机 election timeout。
2. 超时未收到合法 Leader 心跳，切换 Candidate。
3. `current_term += 1`。
4. 投票给自己并持久化 `current_term/voted_for`。
5. 并发发送 `RequestVote`。
6. 获得多数派投票后成为 Leader。
7. Leader 立即写入 noop 日志并广播心跳。

建议参数：

```text
heartbeat_interval_ms = 50
election_timeout_min_ms = 150
election_timeout_max_ms = 300
```

投票条件：

- 一个 term 内最多投一票。
- 候选人 term 不能小于本地 term。
- 候选人日志至少和本地一样新。

日志新旧比较：

```text
candidate.last_log_term > local.last_log_term
or
candidate.last_log_term == local.last_log_term
and candidate.last_log_index >= local.last_log_index
```

### 9.4 日志复制

Leader 收到写请求后：

1. 根据 key 找到 Region。
2. 确认本 Peer 是 Region Leader。
3. 构造 LogEntry。
4. 追加本地日志并持久化。
5. 并发向 Follower 发送 `AppendEntries`。
6. 当前 term 的日志被多数派复制后推进 `commit_index`。
7. ApplyWorker 顺序 apply 到 LevelDB。
8. apply 完成后返回客户端成功。

Follower 处理 `AppendEntries`：

1. 如果 leader term 小于本地 term，拒绝。
2. 如果本地不存在 `prev_log_index/prev_log_term`，拒绝。
3. 如果存在冲突日志，删除冲突 index 之后的日志。
4. 追加新日志。
5. 根据 `leader_commit` 推进本地 `commit_index`。

### 9.5 Apply

ApplyWorker 对每个 Region 按 index 顺序 apply：

```text
while last_applied < commit_index:
  entry = log[last_applied + 1]
  result = state_machine.apply(entry)
  last_applied += 1
  persist last_applied
```

注意：

- 未提交日志不能写入业务 KV。
- apply 必须幂等，重启恢复时可能重复执行边界日志。
- Batch 命令使用 LevelDB WriteBatch 原子写入。

### 9.6 ReadIndex

强一致读走 Leader：

1. Gateway 将读请求路由到 Region Leader。
2. Leader 发起 ReadIndex。
3. Leader 确认自己仍被多数派认可。
4. 记录当前 commit index 作为 read index。
5. 等待 `last_applied >= read_index`。
6. 从 LevelDB 读取并返回。

Follower read 只能作为最终一致读，API 必须显式声明。

### 9.7 Snapshot

触发条件：

- 单 Region 日志数量超过阈值。
- Follower 落后太多。
- 新 Peer 加入需要快速同步。

流程：

1. 暂停对应 Region 的 apply。
2. 导出 Region key range 下的 KV 数据。
3. 写入 snapshot 文件和元数据。
4. 记录 `last_included_index/term`。
5. 裁剪旧日志。
6. Follower 通过 `InstallSnapshot` 恢复。

第一版可以先不做在线一致快照，等日志复制正确后再实现。

## 10. LevelDB 存储设计

### 10.1 Key Prefix

建议使用一个 LevelDB 实例，用 prefix 隔离不同逻辑数据：

```text
local/store_id
pd/...
region/meta/{region_id}
raft/meta/{region_id}/...
raft/log/{region_id}/{index}
kv/{region_id}/{encoded_user_key}
```

如果 PD 和 Store 是独立进程，PD 和 Store 分别使用自己的 LevelDB。

### 10.2 WriteBatch

LevelDB 的 `WriteBatch` 用于本地原子批量写入：

- 持久化 Raft 日志。
- apply 一个 Batch 命令。
- 更新 `last_applied`。

不要把它描述为完整事务。RStone 的一致性来自：

- Raft 日志顺序。
- 多数派提交。
- 状态机按 index 顺序 apply。

### 10.3 Value 编码

建议业务 value 带版本号：

```json
{
  "value": "bytes",
  "version": 42,
  "expire_at_ms": 0
}
```

这样后续可以支持：

- CAS。
- TTL。
- 调试读到的数据版本。

## 11. Gateway 实现

### 11.1 请求处理流程

写请求：

```text
Client -> Gateway -> route key -> Region Leader Store -> Raft propose -> apply -> response
```

强一致读：

```text
Client -> Gateway -> route key -> Region Leader Store -> ReadIndex -> read -> response
```

最终一致读：

```text
Client -> Gateway -> route key -> any Region Peer -> local read -> response
```

### 11.2 Route Cache

Gateway 维护本地路由缓存：

```cpp
struct RouteEntry {
  RegionInfo region;
  StoreInfo leader_store;
  int64_t last_update_ms;
};
```

缓存失效条件：

- Store 返回 `NOT_LEADER`。
- Store 返回 `REGION_NOT_FOUND`。
- Store 返回 `STALE_EPOCH`。
- 缓存 TTL 到期。

失效后 Gateway 向 PD 重新查询 Region。

### 11.3 重试策略

可重试错误：

- `NOT_LEADER`
- `REGION_NOT_FOUND`
- `STALE_EPOCH`
- `RPC_TIMEOUT`

不可重试错误：

- `INVALID_ARGUMENT`
- `CAS_FAILED`
- `KEY_NOT_FOUND`
- `WRITE_CONFLICT`，后续事务化后再定义

重试必须带 deadline，避免无限重试。

## 12. 一致性语义

### 12.1 写一致性

写入成功条件：

- 写入被对应 Region Leader 接收。
- Raft 日志持久化。
- 日志复制到多数派。
- 日志被提交。
- Leader 状态机 apply 成功。

### 12.2 强一致读

强一致读只由 Region Leader 提供，并使用 ReadIndex。

默认读一致性应为：

```text
Consistency::LINEARIZABLE
```

### 12.3 最终一致读

Follower 可以提供最终一致读，但必须显式指定：

```text
Consistency::EVENTUAL
```

响应建议携带：

- `served_by`
- `region_id`
- `peer_id`
- `applied_index`
- `leader_commit_index`，如果已知

## 13. 配置设计

### 13.1 PD 配置

```yaml
role: pd
pd:
  id: pd1
  host: 127.0.0.1
  client_port: 7000
  data_dir: ./data/pd1
```

### 13.2 Store 配置

```yaml
role: store
store:
  host: 127.0.0.1
  raft_port: 7101
  client_port: 8101
  data_dir: ./data/store1

pd:
  endpoints:
    - 127.0.0.1:7000

raft:
  heartbeat_interval_ms: 50
  election_timeout_min_ms: 150
  election_timeout_max_ms: 300
  snapshot_threshold: 100000
```

### 13.3 Gateway 配置

```yaml
role: gateway
gateway:
  id: gateway1
  host: 127.0.0.1
  http_port: 18080
  route_cache_ttl_ms: 30000

pd:
  endpoints:
    - 127.0.0.1:7000
```

## 14. 分阶段实现路线

### 阶段 0：工程骨架

目标：项目可构建、可测试、可运行空服务。

任务：

1. 创建 CMake 工程。
2. 建立目录结构。
3. 接入 GoogleTest。
4. 接入 spdlog。
5. 定义 `Status/ErrorCode`。
6. 实现 YAML 配置加载。
7. 实现 `rstone-server --role ... --config ...`。

验收：

- `cmake --build` 成功。
- `ctest` 成功。
- 三种 role 都能启动并打印配置。

### 阶段 1：LevelDB 存储封装

目标：实现可靠的本地 KV 和元数据读写。

任务：

1. 实现 `LevelDBEngine`。
2. 支持 `Get/Put/Delete/WriteBatch`。
3. 实现 key prefix 编码。
4. 实现 value version 编码。
5. 编写重启恢复测试。

验收：

- 写入后重启数据不丢。
- WriteBatch 原子写入。
- prefix scan 可用于 Region snapshot。

### 阶段 2：RPC 框架

目标：完成服务间基础通信。

任务：

1. 定义 RPC envelope。
2. 实现 RpcServer handler 注册。
3. 实现 RpcClient 超时、错误码、重试上限。
4. 实现 `cluster.Ping`。
5. 实现 JSON 序列化。

验收：

- Gateway 能 ping PD。
- Store 能 ping PD。
- Store 之间能互相 ping。
- 超时返回明确错误。

### 阶段 3：最小 PD

目标：PD 能管理 Store 和 Region 路由。

任务：

1. 实现 `AllocId`。
2. 实现 `RegisterStore`。
3. 实现 Store heartbeat。
4. 实现 Region 元数据持久化。
5. 实现 `GetRegionByKey`。
6. 初始化一个默认 Region。

验收：

- 三个 Store 能注册到 PD。
- PD 能显示 Store 列表。
- PD 能返回 key 所属 Region。
- PD 能返回 Region Leader hint。

### 阶段 4：单 Region Raft

目标：三 Store 上一个 Region 能选主和复制日志。

任务：

1. 实现 RaftNode 基础状态。
2. 实现 election timer。
3. 实现 `RequestVote`。
4. 实现心跳 `AppendEntries`。
5. 实现 term 持久化。
6. 实现单 Region peer 初始化。

验收：

- 三个 Store 启动后能选出一个 Leader。
- 停止 Leader 后能重新选主。
- 恢复旧 Leader 后不会出现双 Leader。

### 阶段 5：单 Region KV 写入

目标：Put/Delete 经 Raft 多数派提交后 apply。

任务：

1. 实现 Raft log 持久化。
2. 实现 AppendEntries 日志复制。
3. 实现 `next_index/match_index`。
4. 实现 commit index 推进。
5. 实现 ApplyWorker。
6. 将 `Put/Delete` 编码为 KvCommand。

验收：

- 写入 Leader 成功后，三个 Store 最终都有数据。
- 写到 Follower 返回 `NOT_LEADER`。
- Leader 崩溃后，新 Leader 能继续写入。
- 重启 Store 后能从日志恢复。

### 阶段 6：Gateway 接入

目标：客户端只访问 Gateway，不直接感知 Store。

任务：

1. 实现 Gateway `Put/Get/Delete`。
2. 实现 RouteCache。
3. Gateway 向 PD 查询 Region。
4. Gateway 将请求转发到 Region Leader。
5. 处理 `NOT_LEADER` 并刷新路由。

验收：

- CLI 访问 Gateway 可完成读写。
- Leader 切换后 Gateway 能自动刷新路由。
- PD 宕机短时间内，Gateway 可使用缓存路由继续访问已有 Region。

### 阶段 7：读一致性

目标：区分强一致读和最终一致读。

任务：

1. 实现 ReadIndex。
2. Gateway API 增加 consistency 参数。
3. Leader 支持 linearizable read。
4. Follower 支持 eventual read。
5. 响应带 applied index。

验收：

- 默认读为强一致。
- Follower 强一致读返回 redirect。
- Follower eventual read 可返回本地已应用数据。

### 阶段 8：Multi-Raft 和静态多 Region

目标：Store 能同时管理多个 Region。

任务：

1. 定义多个初始 Region range。
2. PD 持久化多个 Region。
3. Store 创建多个 Region Peer。
4. RaftRouter 按 region_id 分发消息。
5. Gateway 按 key 路由到不同 Region。

验收：

- 不同 key 落到不同 Region。
- 每个 Region 独立选 Leader。
- 一个 Region Leader 切换不影响其他 Region 读写。

### 阶段 9：Snapshot 和日志压缩

目标：长期运行不会无限增长日志。

任务：

1. 实现 snapshot meta。
2. 实现 Region snapshot 导出。
3. 实现 Region snapshot 恢复。
4. 实现 `InstallSnapshot`。
5. 实现日志裁剪。

验收：

- 日志超过阈值后可压缩。
- 落后节点可通过 snapshot 追上。
- snapshot 恢复后 KV 数据正确。

### 阶段 10：Region Split

目标：热点或过大的 Region 可拆分。

任务：

1. Store 统计 approximate size 和 keys。
2. Store 向 PD 发起 `AskSplit`。
3. PD 分配新 region_id 和 peer_id。
4. Store 写入 Split 日志。
5. apply 后更新本地 Region 元数据。
6. 上报 `ReportSplit`。
7. Gateway 处理 `STALE_EPOCH`。

验收：

- Region 达到阈值后能拆分成两个 Region。
- 新旧 key range 路由正确。
- 旧 route cache 会被刷新。

### 阶段 11：成员变更和调度

目标：支持副本迁移和基础负载均衡。

任务：

1. 实现 learner。
2. 实现 AddPeer。
3. 实现 RemovePeer。
4. 实现 TransferLeader。
5. PD 根据 Store 负载生成 operator。
6. Store 执行 operator 并上报结果。

验收：

- 可以给 Region 增加副本。
- 可以移除副本。
- 可以迁移 Leader。
- Store 下线后 PD 能补副本。

### 阶段 12：Gateway 集群和 Nginx

目标：提供统一外部入口。

任务：

1. Gateway 无状态化。
2. 实现 `/health`。
3. 实现 `/metrics`。
4. 编写 Nginx 配置。
5. 多 Gateway 压测。

验收：

- Nginx 后挂多个 Gateway。
- 停止一个 Gateway 后请求仍可成功。
- Gateway 扩容不影响 Store 和 PD。

### 阶段 13：可观测性和压测

目标：能定位问题并评估性能。

任务：

1. 结构化日志。
2. 指标输出。
3. 编写 benchmark。
4. 故障注入脚本。
5. 一致性检查工具。

核心指标：

- QPS。
- P50/P95/P99 延迟。
- Region leader 分布。
- Raft commit index。
- Raft applied index。
- Raft replication lag。
- Gateway route cache hit rate。
- PD heartbeat 延迟。

验收：

- 能观察每个 Region 的 leader、term、commit index。
- 能压测读写吞吐。
- 能复现 Leader 崩溃、Follower 落后、Gateway 重启等故障。

## 15. 测试策略

### 15.1 单元测试

覆盖：

- key range 命中。
- Region epoch 比较。
- Raft 投票规则。
- Raft log matching。
- commit index 推进。
- LevelDB WriteBatch。
- RouteCache 失效。
- RPC 序列化。

### 15.2 集成测试

覆盖：

- Store 注册 PD。
- Gateway 查询 PD。
- 三 Store 单 Region 选主。
- Put/Delete 日志复制。
- Leader 崩溃重新选举。
- Gateway 路由刷新。
- 多 Region 独立读写。

### 15.3 故障测试

模拟：

- Leader 进程崩溃。
- Follower 落后。
- Store 重启。
- Gateway 重启。
- PD 短暂不可用。
- RPC 超时。
- 网络分区。

### 15.4 一致性测试

建议实现简单历史检查器：

- 记录每个请求开始时间、结束时间、操作和返回值。
- 对强一致读写检查是否可线性化。
- 第一版先做单 key 检查。
- 后续扩展到多 key 和 Batch。

## 16. 错误码

统一错误码：

```text
OK
INVALID_ARGUMENT
KEY_NOT_FOUND
CAS_FAILED
NOT_LEADER
REGION_NOT_FOUND
STALE_EPOCH
EPOCH_NOT_MATCH
STORE_NOT_FOUND
PD_UNAVAILABLE
RAFT_NOT_READY
TERM_OUTDATED
LOG_CONFLICT
SNAPSHOT_REQUIRED
RPC_TIMEOUT
RPC_ERROR
STORAGE_ERROR
CONFIG_CHANGE_IN_PROGRESS
```

重要错误处理：

- `NOT_LEADER`：携带 leader hint。
- `STALE_EPOCH`：携带最新 Region 信息。
- `REGION_NOT_FOUND`：Gateway 重新查 PD。
- `PD_UNAVAILABLE`：Gateway 可短时间使用缓存路由。

## 17. 并发模型

建议线程模型：

- Gateway worker pool：处理客户端请求。
- PD RPC worker：处理注册、心跳、路由查询。
- Store RPC worker：处理 KV 和 Raft RPC。
- Raft timer：处理 election timeout 和 heartbeat。
- Replicator worker：Leader 向 Follower 复制日志。
- ApplyWorker：按 Region apply committed log。
- HeartbeatWorker：Store 向 PD 汇报状态。

关键约束：

- 单个 RaftNode 的状态变更必须串行化。
- RPC handler 不应长时间持有 Raft mutex。
- apply 必须按 Region 内日志 index 顺序执行。
- 持久化 term/vote/log 后才能返回相关成功响应。
- Gateway 重试必须受 deadline 限制。

## 18. 本地部署拓扑

第一版本地集群：

```text
pd1       127.0.0.1:7000
gateway1 127.0.0.1:18080
store1   raft 7101, client 8101
store2   raft 7102, client 8102
store3   raft 7103, client 8103
```

启动：

```bash
./rstone-server --role pd --config config/pd.yaml
./rstone-server --role store --config config/store1.yaml
./rstone-server --role store --config config/store2.yaml
./rstone-server --role store --config config/store3.yaml
./rstone-server --role gateway --config config/gateway.yaml
```

CLI：

```bash
./rstone-cli --endpoint 127.0.0.1:18080 put user:1 alice
./rstone-cli --endpoint 127.0.0.1:18080 get user:1 --consistency linearizable
./rstone-cli --endpoint 127.0.0.1:18080 get user:1 --consistency eventual
```

## 19. 里程碑验收表

| 里程碑 | 能力 | 验收 |
| --- | --- | --- |
| M0 | 工程骨架 | CMake + ctest 成功 |
| M1 | 本地存储 | LevelDB 重启不丢数据 |
| M2 | RPC | Gateway/PD/Store 可通信 |
| M3 | 最小 PD | Store 注册、Region 查询 |
| M4 | 单 Region 选主 | 三 Store 稳定一个 Leader |
| M5 | 单 Region 写入 | Put/Delete 多数派提交 |
| M6 | Gateway 接入 | 客户端只访问 Gateway |
| M7 | 读一致性 | Leader 强读、Follower 弱读 |
| M8 | Multi-Raft | 多 Region 独立读写 |
| M9 | Snapshot | 日志压缩和恢复 |
| M10 | Region Split | key range 动态拆分 |
| M11 | 调度 | AddPeer/RemovePeer/TransferLeader |
| M12 | 外部入口 | Nginx + 多 Gateway |
| M13 | 可观测性 | metrics、benchmark、故障测试 |

## 20. 实现顺序建议

推荐顺序：

1. 工程骨架。
2. LevelDB 封装。
3. RPC 框架。
4. 最小 PD。
5. 单 Region Raft 选主。
6. 单 Region 日志复制。
7. Gateway 接入。
8. ReadIndex 和读一致性。
9. Multi-Raft 和静态多 Region。
10. Snapshot。
11. Region Split。
12. 成员变更和调度。
13. Nginx、多 Gateway、压测。

不要一开始同时做 Multi-Raft、Region Split、成员变更和 snapshot。Raft 的基础安全性必须先被单 Region 测试固定下来。

## 21. 简历对应实现边界

如果只完成阶段 0 到阶段 7，可以写：

- 实现 Gateway、PD、Store 三层原型。
- 实现单 Region 三副本 Raft KV。
- 实现 Leader 强一致读写和 Follower 最终一致读。
- 实现 ZeroMQ RPC 和 LevelDB 持久化。

如果完成阶段 8，可以写：

- 实现 Region + Multi-Raft。
- 支持多 Region 独立选主和复制。
- Gateway 根据 Region 路由请求。

如果完成阶段 10 到阶段 11，可以写：

- 支持 Region split。
- 支持副本变更和基础调度。
- 支持 Leader balance 和 Region balance。

不要在未完成前写：

- 完整分布式事务。
- 完整 SQL 引擎。
- 完整自动弹性伸缩。
- LevelDB 原生事务。
