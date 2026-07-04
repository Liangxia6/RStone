# RStone 代码阅读教学文档

这份文档是“按代码走一遍系统”的教学材料。你可以一边打开源码，一边按这里的顺序读。

## 1. 第一站：程序入口

先看：

- `src/main.cpp`

核心函数：

- `main`
- `RunPdService`
- `RunStoreService`
- `RunDistributedStoreService`
- `RunGatewayService`

阅读目标：

- 明白同一个 `rstone-server` 如何根据 `--role` 启动成 PD、Store 或 Gateway。
- 明白普通 Store 模式和分布式 Store 模式的区别。
- 明白服务启动时如何注册 RPC handler。

关键问题：

```text
为什么 Store 启动时要先向 PD 注册？
为什么分布式 Store 要等所有 Store 到齐后再启动本地 Raft peer？
为什么 Gateway 需要同时持有 PD client 和 Store client？
```

## 2. 第二站：核心数据结构

看：

- `include/rstone/common/types.h`

重点结构：

- `Endpoint`
- `StoreInfo`
- `RegionInfo`
- `Peer`
- `LogEntry`

阅读方式：

先不要管函数实现，只看字段。你要把这些结构和真实分布式系统概念对应起来：

```text
StoreInfo  -> 物理节点
RegionInfo -> 数据分片
Peer       -> 某个 Store 上的某个 Region 副本
LogEntry   -> Raft 日志
```

## 3. 第三站：RPC 是怎么工作的

看：

- `include/rstone/rpc/rpc_message.h`
- `src/rpc/rpc_codec.cpp`
- `src/rpc/tcp_rpc.cpp`

阅读目标：

- `RpcRequest` 里有什么。
- `RpcResponse` 里有什么。
- RPC payload 为什么是字符串。
- `EncodeFields/DecodeFields` 如何把字段塞进 payload。
- TCP Server 如何根据 `method` 找 handler。

建议从这个调用链读：

```text
TcpRpcClient::Call
  -> EncodeRpcRequest
  -> socket write
  -> TcpRpcServer accept
  -> DecodeRpcRequest
  -> RpcServer::Handle
  -> handler
  -> EncodeRpcResponse
```

## 4. 第四站：PD 如何管理元数据

看：

- `src/pd/metadata_store.cpp`
- `src/pd/pd_server.cpp`
- `src/pd/pd_service.cpp`

推荐顺序：

1. `PdMetadataStore::PutStore`
2. `PdMetadataStore::PutRegion`
3. `PdMetadataStore::GetRegionByKey`
4. `PdServer::RegisterStore`
5. `PdServer::BootstrapDefaultRegion`
6. `PdServer::TransferLeader`
7. `PdService::HandleGetRegionByKey`

理解重点：

- PD 的核心是元数据，不存业务数据。
- `GetRegionByKey` 是路由的源头。
- `leader_peer_id` 决定 Gateway 应该把请求发给哪个 Store。
- PD catalog 持久化后，PD 重启可以恢复 Store/Region 元数据。

## 5. 第五站：Gateway 如何路由

看：

- `src/gateway/rpc_gateway_client.cpp`
- `src/gateway/route_cache.cpp`
- `src/gateway/gateway_service.cpp`

写请求主线：

```text
GatewayService::HandlePut
  -> RpcGatewayClient::Put
  -> ResolveRoute
  -> pd.GetRegionByKey
  -> store.KvPut
```

重点函数：

- `RpcGatewayClient::ResolveRoute`
- `RpcGatewayClient::CallStoreWithRouteRetry`
- `RpcGatewayClient::IsRouteStaleError`

理解重点：

- Gateway 本身不保存数据。
- Gateway 缓存路由是为了减少 PD 查询。
- Region split 或 leader 变化后，旧缓存可能失效。
- 失效后需要清 cache 并重新问 PD。

## 6. 第六站：StoreService 是分发层

看：

- `src/store/store_service.cpp`

它的作用是把 RPC 方法分发到不同 Store 内核：

```text
store.KvPut          -> Put
store.KvGet          -> Get
store.TransferLeader -> TransferLeader
store.RaftRequestVote
store.RaftAppendEntries
```

这里要注意三种后端：

- `single_cluster_`
- `multi_cluster_`
- `distributed_node_`

阅读技巧：

看到 `if (distributed_node_ != nullptr)`，说明这是新分布式路径。

看到 `multi_cluster_ != nullptr`，说明这是旧 Multi-Region 原型路径。

## 7. 第七站：RaftNode 核心逻辑

看：

- `include/rstone/raft/raft_node.h`
- `src/raft/raft_node.cpp`

推荐顺序：

1. `BecomeFollower`
2. `BecomeCandidate`
3. `BecomeLeader`
4. `HandleRequestVote`
5. `HandleAppendEntries`
6. `Propose`
7. `SetCommitIndex`
8. `TakeCommittedEntries`
9. `Restore`

理解重点：

- Candidate 发起投票。
- Leader 接收写请求，生成日志。
- Follower 通过 AppendEntries 接收日志。
- 只有 commit index 之前的日志才可以应用。
- `last_applied` 表示状态机已经应用到哪里。

一个写入在 Raft 内的状态变化：

```text
Propose -> log append -> replicate -> commit_index 前进 -> TakeCommittedEntries -> Apply
```

## 8. 第八站：旧路径 SingleRegionCluster

看：

- `src/store/single_region_cluster.cpp`

这个文件适合理解最小闭环：

```text
Put
  -> ProposeAndReplicate
  -> leader Propose
  -> follower HandleAppendEntries
  -> majority
  -> leader ApplyCommitted
  -> BroadcastCommit
  -> follower ApplyCommitted
```

它在一个进程中模拟多个 Store，所以读起来比真实分布式路径简单。

建议你先读它，再读 `DistributedRegionNode`。

## 9. 第九站：新路径 DistributedRegionNode

看：

- `src/store/distributed_region_node.cpp`

这是当前最重要的文件之一。

推荐顺序：

1. `Bootstrap`
2. `EnsureLeader`
3. `ProposeAndReplicate`
4. `ReplicateToStore`
5. `BroadcastCommit`
6. `HandleRequestVote`
7. `HandleAppendEntries`
8. `TransferLeader`

核心流程：

```text
Put
  -> ProposeAndReplicate
  -> EnsureLeader
  -> RaftNode::Propose
  -> ReplicateToStore
  -> store.RaftAppendEntries
  -> majority success
  -> ApplyCommitted
  -> BroadcastCommit
```

重点理解 `ReplicateToStore`：

- 首先尝试发送从 `first_index` 开始的日志。
- 如果 follower 缺日志或日志冲突，follower 返回自己的 `match_index`。
- Leader 回退 next index，并重新发送后缀。
- 这就是 follower 重启后追日志的基础。

## 10. 第十站：KV 状态机

看：

- `src/storage/kv_command.cpp`
- `src/storage/file_kv_engine.cpp`

理解重点：

- `EncodePutCommand` 把 KV 操作变成字符串。
- `ApplyKvCommand` 把字符串命令应用到 KV Engine。
- Raft 只复制日志命令，不理解业务语义。
- KV Engine 只负责本地持久化，不负责分布式一致性。

## 11. 第十一站：测试怎么读

推荐先读这些测试：

- `tests/unit/raft_node_test.cpp`
- `tests/unit/single_region_cluster_test.cpp`
- `tests/unit/multi_region_cluster_test.cpp`
- `tests/unit/store_service_test.cpp`
- `tests/unit/tcp_gateway_e2e_test.cpp`

脚本测试：

- `scripts/e2e_local_cluster.sh`
- `scripts/e2e_recovery_cluster.sh`
- `scripts/e2e_distributed_cluster.sh`
- `scripts/e2e_distributed_recovery_cluster.sh`
- `scripts/consistency_check.sh`

测试阅读方法：

1. 先看脚本启动了哪些进程。
2. 再看它执行了哪些 CLI 命令。
3. 再看它验证了哪些结果。
4. 最后回源码找对应 handler。

## 12. 重点调用链速查

### Gateway Put

```text
tools/rstone_cli.cpp
  -> kv.Put
src/gateway/gateway_service.cpp
  -> GatewayService::HandlePut
src/gateway/rpc_gateway_client.cpp
  -> RpcGatewayClient::Put
  -> ResolveRoute
  -> CallStoreWithRouteRetry
src/store/store_service.cpp
  -> StoreService::HandlePut
src/store/distributed_region_node.cpp
  -> DistributedRegionNode::Put
```

### Raft 写复制

```text
DistributedRegionNode::Put
  -> ProposeAndReplicate
  -> EnsureLeader
  -> RaftNode::Propose
  -> ReplicateToStore
  -> StoreService::HandleRaftAppendEntries
  -> DistributedRegionNode::HandleAppendEntries
  -> RaftNode::HandleAppendEntries
  -> ApplyCommitted
```

### Transfer Leader

```text
CLI transfer-leader
  -> GatewayService::HandleTransferLeader
  -> RpcGatewayClient::TransferLeader
  -> pd.TransferLeader
  -> store.TransferLeader
  -> DistributedRegionNode::TransferLeader
  -> EnsureLeader
  -> RequestVote
```

### Follower 追日志

```text
Follower restart
  -> Restore hard state/log
Leader next write
  -> ReplicateToStore
  -> AppendEntries fail
  -> follower returns match_index
  -> leader retries with older suffix
  -> follower appends missing logs
  -> BroadcastCommit
  -> follower applies logs
```

## 13. 建议做的阅读练习

练习 1：给 `put user:1 alice` 画一张自己的调用链图。

练习 2：在 `DistributedRegionNode::ReplicateToStore` 加日志，观察 follower 离线恢复时的 retry。

练习 3：手动修改 `distributed_store1.yaml` 的端口，观察 Gateway 是否仍按 PD 路由。

练习 4：在 `RaftNode::HandleAppendEntries` 里断点，观察日志冲突时如何截断。

练习 5：尝试解释为什么当前强一致读还不是完整 ReadIndex。

## 14. 读完后的下一步编码任务

如果你想继续开发，推荐顺序：

1. 给分布式路径补 Region Split。
2. 给分布式路径补 AddPeer/RemovePeer。
3. 增加周期性 heartbeat。
4. 增加自动 election timeout。
5. 实现 InstallSnapshot。
6. 接入 LevelDB。
7. 接入 ZeroMQ。
8. 增加 chaos 测试。
