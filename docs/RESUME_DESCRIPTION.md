# RStone 简历项目描述

下面给出两版简历描述：第一版适合已经实现核心功能；第二版适合项目还在开发中、但架构设计已经完成。实际投递时请按真实完成度选择，避免被追问时过度承诺。

## 版本 A：核心功能已实现

**RStone：基于 Raft 的分布式 KV 存储系统**  
**2025.01 - 2025.06**

- **整体架构**：实现 Gateway、PD 和 Store 三层架构；Gateway 作为无状态接入层负责请求转发和路由缓存，PD 负责集群元数据、Region 路由和节点心跳管理，Store 负责数据持久化和 Raft 副本复制。
- **Multi-Raft 存储**：将 key space 按 Region 切分，每个 Region 的多个副本组成独立 Raft Group；实现 Region Leader 选举、心跳检测、日志复制和多数派提交，提高系统横向扩展能力。
- **RPC 通信框架**：基于 ZeroMQ 封装轻量级 TCP RPC，用于 Gateway、PD 和 Store 之间的跨节点通信，支持请求超时、错误码、Leader redirect 和路由刷新。
- **持久化存储**：集成 LevelDB 作为本地 LSM 存储引擎，分别持久化 Raft 日志、Region 元数据和业务 KV 数据；基于 WriteBatch 实现日志 apply 阶段的原子批量写入。
- **一致性读写**：写请求统一路由到 Region Leader，经 Raft 多数派复制并 apply 后返回；强一致读通过 Leader ReadIndex 保证线性一致性，Follower 支持可选最终一致读。
- **调度与负载均衡**：通过 PD 维护 Store/Region 心跳信息，支持 Region 路由、Leader 分布统计和基础调度；外部接入层结合 Nginx 对多个 Gateway 进行负载均衡。

## 版本 B：架构设计完成，核心模块开发中

**RStone：基于 Raft 架构设计的分布式 KV 存储系统**  
**2025.01 - 2025.06**

- **架构设计**：设计 Gateway、PD、Store 三层架构，包含无状态接入层、元数据调度层和 Multi-Raft 存储层，降低请求路由、集群调度和数据复制之间的耦合。
- **Region 分片模型**：设计基于 key range 的 Region 数据分片模型，每个 Region 多副本组成独立 Raft Group，由 Region Leader 处理写请求并通过多数派复制保证数据安全。
- **Raft 共识模块**：实现/设计 Leader 选举、心跳检测、日志复制、commit index 推进和状态机 apply 流程，为多副本 KV 数据提供强一致写入能力。
- **通信与存储**：基于 ZeroMQ 设计轻量级 TCP RPC，用于节点间 Raft 消息和客户端请求转发；集成 LevelDB 作为本地持久化引擎，存储 Raft 日志、元数据和 KV 数据。
- **一致性策略**：设计 Leader 强一致读写与 Follower 最终一致读的读写路径，支持 Leader redirect、路由缓存刷新和 Region epoch 校验。
- **扩展能力**：预留 Region split、Leader balance、Replica balance、成员变更和 Gateway + Nginx 外部负载均衡能力，支撑后续横向扩展。

## 推荐精简版

如果简历空间有限，可以用这一版：

**RStone：基于 Raft 的分布式 KV 存储**  
**2025.01 - 2025.06**

- 设计 Gateway、PD、Store 三层架构，Gateway 负责无状态接入和路由缓存，PD 管理 Store/Region 元数据和心跳，Store 负责数据持久化与 Raft 复制。
- 基于 Region 对 key space 进行 range 分片，每个 Region 多副本组成独立 Raft Group，实现 Leader 选举、心跳检测、日志复制和多数派提交。
- 基于 ZeroMQ 封装轻量级 TCP RPC，支持 Gateway、PD、Store 间通信，以及 Leader redirect、请求超时和路由刷新。
- 集成 LevelDB 作为本地 LSM 存储引擎，持久化 Raft 日志、Region 元数据和业务 KV，使用 WriteBatch 保证 apply 阶段原子批量写入。
- 实现 Leader 强一致读写和 Follower 最终一致读，结合 Nginx + Gateway 提供统一外部访问入口，并预留 Region split 与调度能力。

## 面试解释口径

如果被问“为什么要拆 Gateway、PD、Store”，可以这样回答：

> Gateway 做无状态请求接入和路由缓存，PD 维护集群元数据、Region 路由和调度信息，Store 专注于数据持久化和 Raft 复制。这样可以把请求入口、元数据管理和数据复制解耦，后续扩容 Gateway、迁移 Region 或调整副本分布时不会互相影响。

如果被问“为什么不用 Master/Slave”，可以这样回答：

> Raft 语义里更准确的是 Leader/Follower。每个 Region 的 Raft Group 都会独立选出 Leader，Leader 负责该 Region 的写入和强一致读；Follower 复制日志，可以提供最终一致读。所以我在项目里使用 Leader/Follower，而不是 Master/Slave。

如果被问“LevelDB 支持事务吗”，可以这样回答：

> LevelDB 本身不提供完整分布式事务。我这里使用 LevelDB 的 WriteBatch 保证单次 apply 的本地原子批量写入，分布式一致性由 Raft 日志顺序和多数派提交保证。如果要做完整分布式事务，需要进一步实现 MVCC 和 2PC。

## 不建议写的表述

下面这些表述容易被追问或显得过度包装：

- “LevelDB 支持分布式事务”
- “Slave 强一致读”
- “一致性哈希实现完整调度”
- “Nginx 直接代理 ZeroMQ RPC 并感知 Leader”

更推荐的替代表述：

- “Gateway、PD、Store 三层架构和 Region + Multi-Raft 模型”
- “基于 LevelDB WriteBatch 实现 apply 阶段原子写入”
- “Leader 提供强一致读写，Follower 支持最终一致读”
- “PD 维护 Region 元数据并进行基础调度”
- “Nginx 负载均衡多个无状态 Gateway”
