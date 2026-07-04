#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace rstone {

using StoreId = std::uint64_t;
using RegionId = std::uint64_t;
using PeerId = std::uint64_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

enum class Role {
  kPd,
  kGateway,
  kStore,
  kUnknown,
};

enum class PeerRole {
  kVoter,
  kLearner,
};

enum class Consistency {
  kLinearizable,
  kEventual,
};

struct Endpoint {
  // 网络地址。当前 RPC 服务主要使用 client_endpoint，raft_endpoint 作为元数据保留。
  std::string host = "127.0.0.1";
  int port = 0;

  std::string ToString() const { return host + ":" + std::to_string(port); }
};

struct StoreInfo {
  // 物理 Store 节点信息，由 PD 持久化并返回给 Gateway 做路由。
  StoreId store_id = 0;
  Endpoint raft_endpoint;
  Endpoint client_endpoint;
  std::string state = "Up";
  std::map<std::string, std::string> labels;
  std::int64_t last_heartbeat_ms = 0;
};

struct RegionEpoch {
  // conf_ver 表示副本配置版本，version 表示 key range 版本。
  std::uint64_t conf_ver = 1;
  std::uint64_t version = 1;
};

struct Peer {
  // Peer 是某个 Store 上的 Region 副本；多个 Peer 组成一个 Raft Group。
  PeerId peer_id = 0;
  StoreId store_id = 0;
  PeerRole role = PeerRole::kVoter;
};

struct RegionInfo {
  // Region 是数据分片和 Raft Group 的基本单位，负责一个连续 key range。
  RegionId region_id = 0;
  std::string start_key;
  std::string end_key;
  RegionEpoch epoch;
  std::vector<Peer> peers;
  PeerId leader_peer_id = 0;
};

}  // namespace rstone
