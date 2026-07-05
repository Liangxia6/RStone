# RStone 项目学习文档

这份文档面向第一次阅读 RStone 的同学。目标不是让你记住每一个函数，而是先建立一张清楚的地图：这个项目为什么这样分层、每层解决什么问题、一次读写请求如何穿过系统、Raft 在哪里发挥作用、当前代码已经做到什么、还缺什么。

## 1. 项目定位

RStone 是一个基于 Raft 的分布式 KV 存储项目。它当前更适合被理解为“可运行、可测试的分布式数据库内核原型”，而不是生产级数据库。

它已经具备这些核心能力：

- Gateway / PD / Store 三层架构。
- PD 管理 Store、Region、Leader 路由和基础调度。
- Store 保存数据，并运行 Raft Peer。
- Gateway 接收客户端请求，查询 PD 路由，再把请求发给 Region Leader。
- Raft 负责日志复制、多数派提交和状态机应用。
- 本地多进程脚本可以启动 PD、Gateway、多个 Store。
- 支持基础 KV：`put/get/delete/batch`。
- 支持旧路径的 Multi-Region、Region Split、Peer 变更原型。
- 支持新路径的三 Store 跨进程单 Region Raft 复制和恢复验证。

当前仍然不是生产级系统，主要原因是：

- 存储引擎还是文件型 KV，不是 LevelDB。
- RPC 是自研 TCP RPC，不是 ZeroMQ。
- 分布式 Multi-Region/Split 还没有完全迁移到真实跨 Store Raft 路径。
- Raft 还缺自动心跳、自动超时选举、完整 snapshot install、生产级 membership change。
- 还没有完整事务、MVCC、权限、监控、限流和复杂故障注入。

## 2. 推荐学习顺序

不要一上来读所有源码。建议按下面顺序：

1. 先跑起来。
2. 再理解目录结构。
3. 读基础类型和序列化。
4. 读 RPC。
5. 读 PD 元数据。
6. 读 Gateway 请求转发。
7. 读 Store 服务层。
8. 读 RaftNode。
9. 读分布式 Store 间复制。
10. 最后读测试和脚本。

推荐命令：

```bash
scripts/test_manual.sh
scripts/e2e_local_cluster.sh
scripts/e2e_distributed_cluster.sh
scripts/e2e_distributed_recovery_cluster.sh
```

如果你只想先看系统是否能跑：

```bash
scripts/run_local_cluster.sh
./build/rstone-cli --endpoint 127.0.0.1:18080 put user:1 alice
./build/rstone-cli --endpoint 127.0.0.1:18080 get user:1
./build/rstone-cli --endpoint 127.0.0.1:18080 status
scripts/stop_local_cluster.sh
```

## 3. 总体架构

RStone 有三类进程：

```text
Client / CLI
    |
    v
Gateway
    |
    | 查询路由 / 缓存路由
    v
PD
    |
    | 返回 Region Leader 所在 Store
    v
Store Leader
    |
    | Raft 日志复制
    v
Store Follower
```

三层职责：

- Gateway：客户端入口，无状态，负责把 KV 请求路由到正确 Store。
- PD：元数据中心，记录 Store、Region、Peer、Leader。
- Store：数据节点，承载 Region Peer，执行 Raft 和 KV 状态机。

## 4. 核心概念

### 4.1 Store

Store 是物理数据节点。代码中对应 `StoreInfo`：

```cpp
struct StoreInfo {
  StoreId store_id;
  Endpoint raft_endpoint;
  Endpoint client_endpoint;
  std::string state;
};
```

学习重点：

- `store_id` 是 PD 分配或配置指定的节点 ID。
- `client_endpoint` 是 Gateway/CLI/Store RPC 调用的 TCP 入口。
- `raft_endpoint` 当前主要作为元数据保留，真实 RPC 仍走 TCP service。

### 4.2 Region

Region 是数据分片。每个 Region 管理一个连续 key range：

```text
start_key <= key < end_key
```

代码中对应 `RegionInfo`：

```cpp
struct RegionInfo {
  RegionId region_id;
  std::string start_key;
  std::string end_key;
  RegionEpoch epoch;
  std::vector<Peer> peers;
  PeerId leader_peer_id;
};
```

学习重点：

- Region 是路由单位。
- Region 也是 Raft Group 的边界。
- Region Split 会改变 key range，并提升版本。
- Peer 变更会提升配置版本。

### 4.3 Peer

Peer 是某个 Store 上的 Region 副本：

```cpp
struct Peer {
  PeerId peer_id;
  StoreId store_id;
  PeerRole role;
};
```

一个 Region 有多个 Peer，其中一个是 Leader。

### 4.4 Raft Log

Raft 日志条目对应 `LogEntry`：

```cpp
struct LogEntry {
  RegionId region_id;
  LogIndex index;
  Term term;
  EntryType type;
  std::string command;
};
```

KV 写入不会直接修改 KV Engine，而是先编码成 command，写入 Raft Log。日志被多数派复制并提交后，才应用到 KV Engine。

## 5. 一次写请求的完整链路

以命令为例：

```bash
./build/rstone-cli --endpoint 127.0.0.1:18080 put user:1 alice
```

完整流程：

1. CLI 构造 `kv.Put` RPC 请求，发给 Gateway。
2. Gateway 调用 `RpcGatewayClient::Put`。
3. Gateway 先查本地 route cache。
4. 缓存没有命中时，Gateway 调用 PD 的 `pd.GetRegionByKey`。
5. PD 根据 key 找到 Region，并返回 leader Store。
6. Gateway 调用 Store 的 `store.KvPut`。
7. Store Leader 把 put 操作编码成 KV command。
8. Raft Leader 调用 `Propose` 生成日志。
9. Leader 通过 `store.RaftAppendEntries` 把日志复制到 follower Store。
10. 多数派复制成功后，Leader 提交日志。
11. 已提交日志通过 `ApplyKvCommand` 写入 KV Engine。
12. Leader 广播 commit index，Follower 也应用日志。
13. Gateway 收到成功响应，CLI 输出 `OK`。

## 6. 一次读请求的完整链路

```bash
./build/rstone-cli --endpoint 127.0.0.1:18080 get user:1
```

流程：

1. CLI 发送 `kv.Get` 给 Gateway。
2. Gateway 查 PD 或 route cache，找到 Region Leader。
3. Gateway 调用 Store 的 `store.KvGet`。
4. Store 从本地 KV Engine 读取 value。
5. Gateway 返回给 CLI。

当前代码中，强一致读主要通过“读 Leader”实现。更严格的 Raft ReadIndex/LeaseRead 机制还没有实现。

## 7. 两条 Store 路径

项目目前存在两条 Store 实现路径，这是阅读代码时最容易混淆的地方。

### 7.1 旧路径：单进程 MultiRegionCluster

相关文件：

- `include/rstone/store/single_region_cluster.h`
- `src/store/single_region_cluster.cpp`
- `include/rstone/store/multi_region_cluster.h`
- `src/store/multi_region_cluster.cpp`

特点：

- 一个进程里模拟多个 Store。
- 适合验证 Region Split、Peer 变更、恢复等上层逻辑。
- 不是严格的跨进程 Raft。

### 7.2 新路径：DistributedRegionNode

相关文件：

- `include/rstone/store/distributed_region_node.h`
- `src/store/distributed_region_node.cpp`

特点：

- 每个 Store 进程只承载自己的本地 Peer。
- Store 间通过 TCP RPC 发送 `RequestVote` 和 `AppendEntries`。
- 支持三 Store 单 Region Raft 复制。
- 支持 follower 离线后追日志。
- 支持手动 transfer leader。

阅读时建议先看旧路径理解“Raft + KV 状态机”的简单闭环，再看新路径理解“跨进程复制”。

## 8. 重要源码地图

基础层：

- `include/rstone/common/types.h`：系统核心数据结构。
- `include/rstone/common/status.h`：错误处理。
- `src/common/config.cpp`：轻量配置解析。
- `src/common/serialization.cpp`：Store/Region 字段序列化。

RPC：

- `include/rstone/rpc/rpc_message.h`：请求/响应结构。
- `src/rpc/rpc_codec.cpp`：RPC 消息编码。
- `src/rpc/tcp_rpc.cpp`：TCP RPC server/client。

PD：

- `src/pd/metadata_store.cpp`：内存元数据表。
- `src/pd/pd_server.cpp`：PD 核心逻辑。
- `src/pd/pd_service.cpp`：PD RPC handler。

Gateway：

- `src/gateway/rpc_gateway_client.cpp`：真实服务模式下的 Gateway 客户端逻辑。
- `src/gateway/gateway_service.cpp`：Gateway RPC handler。
- `src/gateway/route_cache.cpp`：路由缓存。

Raft：

- `src/raft/raft_node.cpp`：Raft 状态机核心。
- `src/raft/raft_storage.cpp`：Raft hard state 和 log 持久化。
- `src/raft/snapshot.cpp`：快照导出/恢复。

Store：

- `src/store/store_service.cpp`：Store RPC handler。
- `src/store/single_region_cluster.cpp`：单进程单 Region Raft 模拟。
- `src/store/multi_region_cluster.cpp`：单进程 Multi-Region 原型。
- `src/store/distributed_region_node.cpp`：跨进程单 Region Raft。

入口：

- `src/main.cpp`：PD / Store / Gateway 三类服务启动逻辑。
- `tools/rstone_cli.cpp`：命令行客户端。

## 9. 推荐调试实验

### 实验 1：观察基础写入

```bash
scripts/run_local_cluster.sh
./build/rstone-cli --endpoint 127.0.0.1:18080 put a 1
./build/rstone-cli --endpoint 127.0.0.1:18080 get a
./build/rstone-cli --endpoint 127.0.0.1:18080 status
scripts/stop_local_cluster.sh
```

重点观察：

- `pd.store_count`
- `pd.region_count`
- `store.region_count`
- `runtime_leader_peer_id`

### 实验 2：观察分布式复制

```bash
scripts/e2e_distributed_cluster.sh
```

这个脚本会：

- 启动一个 PD。
- 启动三个 Store。
- 启动一个 Gateway。
- 写入 key。
- 直连 follower Store 验证数据已经应用。
- transfer leader 到 peer2。
- 再写入并验证。

### 实验 3：观察恢复和追日志

```bash
scripts/e2e_distributed_recovery_cluster.sh
```

这个脚本会：

- 停掉 store3。
- 在 store3 离线期间继续写。
- 重启 store3。
- 通过后续写入触发 store3 追日志。
- 停掉当前 leader store1。
- transfer leader 到 store2。
- 重启 store1，并验证旧 leader 能追平日志。

## 10. 学习时要特别注意的边界

### 10.1 强一致读还比较简化

当前强一致读依赖 Gateway 把请求发给 Leader。严格生产系统通常还需要 ReadIndex 或 lease read。

### 10.2 Split 有旧路径和新路径差异

旧路径支持 Multi-Region Split。新分布式路径当前重点是单 Region 跨进程 Raft，Multi-Region 分布式化仍需继续迁移。

### 10.3 Snapshot 还没有进入完整 Raft 安装流程

代码里有 snapshot 导出和恢复，但还不是完整的 Raft InstallSnapshot 协议。

### 10.4 LevelDB 和 ZeroMQ 还没有真正接入

当前代码通过抽象层保留替换空间，但实现仍是文件型 KV 和 TCP RPC。

## 11. 如何判断自己读懂了

如果你能回答下面问题，就说明已经掌握主线：

1. Gateway 为什么需要 route cache？
2. PD 如何根据 key 找到 Region？
3. Region 和 Peer 的区别是什么？
4. 为什么写入不能直接写 KV Engine？
5. Raft Log 什么时候应用到状态机？
6. Store 离线期间漏掉日志，重启后如何追上？
7. 旧的 MultiRegionCluster 和新的 DistributedRegionNode 有什么区别？
8. `TransferLeader` 为什么需要同时更新 PD 和 Store 运行时状态？
9. 当前项目为什么还不是生产级数据库？

## 12. 下一步学习方向

建议按这个路线继续深入：

1. 先把 `DistributedRegionNode` 彻底读懂。
2. 再尝试把 Region Split 迁移到分布式路径。
3. 然后实现自动心跳和选举超时。
4. 再补完整 snapshot install。
5. 最后替换 LevelDB / ZeroMQ，完善 benchmark 和故障注入。
