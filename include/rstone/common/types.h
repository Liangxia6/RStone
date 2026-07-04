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
  std::string host = "127.0.0.1";
  int port = 0;

  std::string ToString() const { return host + ":" + std::to_string(port); }
};

struct StoreInfo {
  StoreId store_id = 0;
  Endpoint raft_endpoint;
  Endpoint client_endpoint;
  std::string state = "Up";
  std::map<std::string, std::string> labels;
  std::int64_t last_heartbeat_ms = 0;
};

struct RegionEpoch {
  std::uint64_t conf_ver = 1;
  std::uint64_t version = 1;
};

struct Peer {
  PeerId peer_id = 0;
  StoreId store_id = 0;
  PeerRole role = PeerRole::kVoter;
};

struct RegionInfo {
  RegionId region_id = 0;
  std::string start_key;
  std::string end_key;
  RegionEpoch epoch;
  std::vector<Peer> peers;
  PeerId leader_peer_id = 0;
};

}  // namespace rstone
