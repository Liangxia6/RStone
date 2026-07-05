# RStone 分布式状态展示网页实现文档

本文档用于指导实现一个 RStone Web Dashboard，用来实时展示分布式 KV 数据库的运行状态。目标是先做一个可在本地集群上使用的状态观察页面，后续再扩展为完整运维控制台。

## 1. 实现目标

Dashboard 需要实时展示：

- PD 状态：Store 数量、Region 数量、Region 元数据、Leader 信息。
- Store 状态：每个 Store 的地址、状态、承载 Peer、Raft 角色、日志进度。
- Region 状态：Region key range、epoch、peers、leader peer。
- Raft 状态：Leader/Follower/Candidate、commit index、last applied、last log index。
- Gateway 状态：路由缓存大小、当前连接的 PD/Store 状态。
- 集群健康状态：节点是否在线、Leader 是否存在、副本是否齐全、日志是否落后。
- 操作结果：最近一次刷新时间、错误信息、请求耗时。

第一阶段只做只读展示，不做控制类操作。后续再考虑加入：

- Transfer Leader。
- Split Region。
- AddPeer / RemovePeer。
- 手动刷新。
- 简单压测入口。

## 2. 页面形态

建议做成一个轻量级单页 Web 控制台：

```text
RStone Dashboard
├── 顶部状态栏
│   ├── 集群健康状态
│   ├── Store 数量
│   ├── Region 数量
│   ├── Leader 数量
│   └── 最近刷新时间
│
├── Cluster Overview
│   ├── PD 状态
│   ├── Gateway 状态
│   └── 全局错误提示
│
├── Store Nodes
│   ├── Store-1
│   ├── Store-2
│   └── Store-3
│
├── Region Table
│   ├── Region ID
│   ├── Key Range
│   ├── Epoch
│   ├── Leader Peer
│   └── Peer 列表
│
├── Raft Runtime
│   ├── Peer ID
│   ├── Role
│   ├── Commit Index
│   ├── Last Applied
│   └── Last Log Index
│
└── Event / Error Panel
    ├── 最近刷新错误
    ├── RPC 错误
    └── 状态异常提示
```

页面设计应偏工程运维风格：信息密度高、层级清晰、便于扫描，不做营销式首页。

## 3. 技术方案

### 3.1 推荐方案

为了保持项目简单，推荐第一阶段使用：

- 后端：在 `rstone-server --role gateway --serve` 中新增 HTTP Dashboard 服务，或新增单独 `rstone-dashboard` 服务。
- 前端：原生 HTML/CSS/JavaScript。
- 数据刷新：浏览器定时轮询 HTTP JSON API。
- 默认端口：`9090`。
- 状态来源：复用现有 `cluster.Status` / `pd.Status` / `store.Status` RPC。

推荐优先采用“Gateway 内置 Dashboard HTTP 服务”：

```text
Browser
  |
  | HTTP GET /api/status
  v
Gateway Dashboard HTTP Server
  |
  | internal call
  v
RpcGatewayClient::GetStatus
  |
  | pd.Status / store.Status
  v
PD + Store
```

原因：

- Gateway 本来就是客户端入口。
- Gateway 已经知道 PD 和 Store。
- 复用现有状态查询逻辑，改动小。
- 用户只需要启动现有 Gateway 进程即可打开网页。

### 3.2 可选方案

后续可以拆成独立进程：

```bash
rstone-dashboard --gateway 127.0.0.1:18080 --listen 127.0.0.1:9090
```

优点：

- Dashboard 和 Gateway 解耦。
- Dashboard 崩溃不影响 Gateway。
- 更接近真实运维组件。

缺点：

- 多一个二进制和配置文件。
- 初期开发成本更高。

第一阶段建议先内置在 Gateway。

## 4. 后端接口设计

### 4.1 API 列表

第一阶段提供三个 HTTP 接口：

```text
GET /              返回 Dashboard HTML
GET /app.js        返回前端 JS
GET /style.css     返回前端 CSS
GET /api/status    返回集群状态 JSON
GET /api/health    返回简单健康状态
```

### 4.2 `/api/health`

响应示例：

```json
{
  "ok": true,
  "service": "rstone-dashboard",
  "timestamp_ms": 1730000000000
}
```

### 4.3 `/api/status`

响应示例：

```json
{
  "ok": true,
  "timestamp_ms": 1730000000000,
  "refresh_cost_ms": 3,
  "summary": {
    "healthy": true,
    "store_count": 3,
    "region_count": 1,
    "route_cache_size": 1,
    "warnings": []
  },
  "gateway": {
    "route_cache_size": 1
  },
  "pd": {
    "store_count": 3,
    "region_count": 1,
    "stores": [
      {
        "store_id": 1,
        "client_endpoint": "127.0.0.1:8101",
        "raft_endpoint": "127.0.0.1:7101",
        "state": "Up",
        "last_heartbeat_ms": 0
      }
    ],
    "regions": [
      {
        "region_id": 1,
        "start_key": "",
        "end_key": "",
        "conf_ver": 1,
        "version": 1,
        "leader_peer_id": 1,
        "peers": [
          {
            "peer_id": 1,
            "store_id": 1,
            "role": "Voter"
          }
        ]
      }
    ]
  },
  "store": {
    "region_count": 1,
    "regions": [
      {
        "region_id": 1,
        "runtime_role": "Leader",
        "runtime_commit_index": 10,
        "runtime_last_applied": 10,
        "runtime_last_log_index": 10
      }
    ]
  },
  "raw": {
    "gateway.route_cache_size": "1",
    "pd.store_count": "3"
  }
}
```

第一阶段后端可以先把现有 `FieldMap` 转成 JSON。为了降低实现复杂度，JSON 可以手写生成，但建议集中封装到一个工具函数中，避免到处拼接字符串。

## 5. 状态数据来源

当前已有状态入口：

- `RpcGatewayClient::GetStatus`
- `GatewayService::HandleStatus`
- `PdService::HandleStatus`
- `StoreService::HandleStatus`
- CLI 的 `status` 命令

现有 CLI 状态输出类似：

```text
gateway.route_cache_size=1
pd.store_count=3
pd.region_count=1
store.region_count=1
store.region0.runtime_role=Leader
```

Dashboard 第一阶段可以直接复用这些字段，再做结构化整理。

需要重点解析的字段前缀：

```text
gateway.*
pd.store_count
pd.store0.*
pd.region_count
pd.region0.*
store.region_count
store.region0.*
```

## 6. 后端模块设计

建议新增模块：

```text
include/rstone/dashboard/
  dashboard_server.h
  status_json.h

src/dashboard/
  dashboard_server.cpp
  status_json.cpp

web/
  index.html
  app.js
  style.css
```

### 6.1 `DashboardServer`

职责：

- 监听 HTTP 端口。
- 返回静态页面资源。
- 处理 `/api/status`。
- 调用 `RpcGatewayClient::GetStatus` 获取状态。

建议接口：

```cpp
class DashboardServer {
 public:
  explicit DashboardServer(RpcGatewayClient* gateway);

  Status Start(const std::string& host, int port);
  void Stop();

 private:
  RpcGatewayClient* gateway_ = nullptr;
};
```

### 6.2 `StatusJson`

职责：

- 把 `FieldMap` 转为 JSON 字符串。
- 计算 summary。
- 生成 warnings。

建议接口：

```cpp
std::string BuildDashboardStatusJson(const FieldMap& fields,
                                     bool ok,
                                     const std::string& error_message,
                                     int64_t refresh_cost_ms);
```

### 6.3 HTTP 实现选择

为了避免引入复杂依赖，第一阶段可以实现一个最小 HTTP Server：

- 只支持 `GET`。
- 只处理固定路径。
- 每个连接读一段请求文本。
- 返回 `HTTP/1.1 200 OK`。
- 不支持 keep-alive。

这和当前 `TcpRpcServer` 的风格一致。

后续如果要升级，可以换成：

- cpp-httplib。
- Drogon。
- Boost.Beast。

## 7. 前端页面设计

### 7.1 页面布局

建议页面结构：

```html
<header>
  <h1>RStone Dashboard</h1>
  <div id="cluster-health"></div>
</header>

<main>
  <section id="summary"></section>
  <section id="stores"></section>
  <section id="regions"></section>
  <section id="raft"></section>
  <section id="events"></section>
</main>
```

### 7.2 刷新策略

前端每 1000ms 轮询一次：

```js
setInterval(fetchStatus, 1000);
```

页面上需要显示：

- 最近刷新时间。
- 刷新耗时。
- 如果连续失败，显示红色错误状态。
- 如果恢复成功，清除错误状态。

### 7.3 可视化规则

Store 状态：

- Up：绿色。
- Down / Tombstone / Unknown：红色或灰色。

Raft 角色：

- Leader：蓝色或绿色强调。
- Follower：普通样式。
- Candidate：黄色。

健康判断：

- 没有 PD 数据：不健康。
- `store_count < 3`：warning。
- Region 没有 leader：error。
- commit index 和 last applied 不一致：warning。
- follower last log index 明显落后 leader：warning。

## 8. 配置设计

在 Gateway 配置中新增：

```yaml
dashboard:
  enabled: true
  host: 127.0.0.1
  port: 9090
  refresh_interval_ms: 1000
```

示例：

```yaml
role: gateway
gateway:
  id: gateway1
  host: 127.0.0.1
  http_port: 18080
  store_routing: dynamic
dashboard:
  enabled: true
  host: 127.0.0.1
  port: 9090
pd:
  endpoints: 127.0.0.1:7000
store:
  endpoint: 127.0.0.1:8101
```

## 9. 分阶段实现计划

### 阶段 1：只读状态 API

目标：

- 新增 `/api/health`。
- 新增 `/api/status`。
- 将 `RpcGatewayClient::GetStatus` 的 FieldMap 转成 JSON。

完成标准：

```bash
curl http://127.0.0.1:9090/api/health
curl http://127.0.0.1:9090/api/status
```

能够返回 JSON。

### 阶段 2：静态 Dashboard 页面

目标：

- 返回 `index.html`、`app.js`、`style.css`。
- 页面能显示 summary、store、region、raft 表格。
- 页面能每秒刷新。

完成标准：

打开：

```text
http://127.0.0.1:9090/
```

可以看到实时状态。

### 阶段 3：健康检查与告警

目标：

- 计算集群健康状态。
- 显示 warning/error。
- 显示刷新失败次数。
- 显示请求耗时。

完成标准：

- 停掉一个 Store，页面出现 warning/error。
- Store 恢复后页面恢复正常。

### 阶段 4：分布式恢复可视化

目标：

- 配合 `scripts/e2e_distributed_recovery_cluster.sh`。
- 展示 follower 离线、恢复、追日志。
- 展示 leader transfer 后 leader_peer_id 变化。

完成标准：

- 执行恢复脚本时，页面能看出 Store/Peer 状态变化。

### 阶段 5：控制操作

后续可加入：

- `POST /api/transfer-leader`
- `POST /api/split-region`
- `POST /api/put`
- `POST /api/delete`

第一阶段不建议实现写操作，避免 Dashboard 影响数据库行为。

## 10. 测试计划

### 10.1 单元测试

新增测试：

```text
tests/unit/status_json_test.cpp
tests/unit/dashboard_server_test.cpp
```

测试内容：

- FieldMap 到 JSON 的转换。
- 缺字段时的容错。
- 健康状态判断。
- HTTP 路径匹配。

### 10.2 端到端测试

新增脚本：

```text
scripts/e2e_dashboard.sh
```

测试流程：

1. 构建项目。
2. 启动分布式集群。
3. 等待 Dashboard 监听。
4. curl `/api/health`。
5. curl `/api/status`。
6. 检查 JSON 包含 `store_count`、`region_count`、`Leader`。
7. 停止集群。

### 10.3 人工验证

```bash
scripts/e2e_distributed_cluster.sh
scripts/run_local_cluster.sh
open http://127.0.0.1:9090/
```

## 11. 目录变更清单

预计新增：

```text
include/rstone/dashboard/dashboard_server.h
include/rstone/dashboard/status_json.h
src/dashboard/dashboard_server.cpp
src/dashboard/status_json.cpp
web/index.html
web/app.js
web/style.css
tests/unit/status_json_test.cpp
tests/unit/dashboard_server_test.cpp
scripts/e2e_dashboard.sh
```

预计修改：

```text
CMakeLists.txt
scripts/build_manual.sh
src/main.cpp
config/gateway.yaml
config/distributed_gateway.yaml
README.md
```

## 12. 风险与注意事项

### 12.1 不要阻塞 Gateway 主服务

Dashboard HTTP 服务需要独立线程，不能影响 Gateway TCP RPC 服务。

### 12.2 状态接口失败要降级展示

如果 PD 或 Store 暂时不可用，Dashboard 不能崩溃，应该返回：

```json
{
  "ok": false,
  "error": "..."
}
```

前端显示错误即可。

### 12.3 不要过度刷新

默认 1 秒刷新一次即可。后续可以提供暂停刷新和手动刷新。

### 12.4 第一阶段不要做控制操作

控制类接口会改变集群状态，需要权限和安全校验。先做只读观察更稳。

## 13. 最终验收标准

实现完成后，应满足：

- 启动 Gateway 后可以打开 Dashboard。
- Dashboard 每秒刷新状态。
- 能看到 PD、Store、Region、Raft 信息。
- 能展示 Leader、Follower、commit index、last applied、last log index。
- 分布式 e2e 运行时页面状态会变化。
- Store 离线或状态接口失败时页面有错误提示。
- 所有原有测试继续通过。
- 新增 dashboard e2e 通过。

推荐最终验证命令：

```bash
scripts/test_manual.sh
scripts/e2e_local_cluster.sh
scripts/e2e_distributed_cluster.sh
scripts/e2e_distributed_recovery_cluster.sh
scripts/e2e_dashboard.sh
```
