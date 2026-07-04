#include "rstone/common/serialization.h"

#include <sstream>

namespace rstone {
namespace {

std::string Key(const std::string& prefix, const std::string& name) {
  return prefix.empty() ? name : prefix + "." + name;
}

std::string GetOr(const FieldMap& fields, const std::string& key, const std::string& fallback) {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return fallback;
  }
  return it->second;
}

std::uint64_t GetUint64Or(const FieldMap& fields, const std::string& key,
                          std::uint64_t fallback) {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return fallback;
  }
  try {
    return static_cast<std::uint64_t>(std::stoull(it->second));
  } catch (...) {
    return fallback;
  }
}

int GetIntOr(const FieldMap& fields, const std::string& key, int fallback) {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return fallback;
  }
  try {
    return std::stoi(it->second);
  } catch (...) {
    return fallback;
  }
}

}  // namespace

void PutStoreFields(FieldMap* fields, const StoreInfo& store, const std::string& prefix) {
  (*fields)[Key(prefix, "store_id")] = std::to_string(store.store_id);
  (*fields)[Key(prefix, "raft_host")] = store.raft_endpoint.host;
  (*fields)[Key(prefix, "raft_port")] = std::to_string(store.raft_endpoint.port);
  (*fields)[Key(prefix, "client_host")] = store.client_endpoint.host;
  (*fields)[Key(prefix, "client_port")] = std::to_string(store.client_endpoint.port);
  (*fields)[Key(prefix, "state")] = store.state;
  (*fields)[Key(prefix, "last_heartbeat_ms")] = std::to_string(store.last_heartbeat_ms);
}

Status GetStoreFields(const FieldMap& fields, StoreInfo* store, const std::string& prefix) {
  if (store == nullptr) {
    return Status::InvalidArgument("store must not be null");
  }
  store->store_id = GetUint64Or(fields, Key(prefix, "store_id"), 0);
  store->raft_endpoint.host = GetOr(fields, Key(prefix, "raft_host"), "127.0.0.1");
  store->raft_endpoint.port = GetIntOr(fields, Key(prefix, "raft_port"), 0);
  store->client_endpoint.host = GetOr(fields, Key(prefix, "client_host"), "127.0.0.1");
  store->client_endpoint.port = GetIntOr(fields, Key(prefix, "client_port"), 0);
  store->state = GetOr(fields, Key(prefix, "state"), "Up");
  store->last_heartbeat_ms =
      static_cast<std::int64_t>(GetUint64Or(fields, Key(prefix, "last_heartbeat_ms"), 0));
  return Status::Ok();
}

void PutRegionFields(FieldMap* fields, const RegionInfo& region, const std::string& prefix) {
  (*fields)[Key(prefix, "region_id")] = std::to_string(region.region_id);
  (*fields)[Key(prefix, "start_key")] = region.start_key;
  (*fields)[Key(prefix, "end_key")] = region.end_key;
  (*fields)[Key(prefix, "conf_ver")] = std::to_string(region.epoch.conf_ver);
  (*fields)[Key(prefix, "version")] = std::to_string(region.epoch.version);
  (*fields)[Key(prefix, "leader_peer_id")] = std::to_string(region.leader_peer_id);
  (*fields)[Key(prefix, "peer_count")] = std::to_string(region.peers.size());
  for (std::size_t i = 0; i < region.peers.size(); ++i) {
    const auto peer_prefix = Key(prefix, "peer" + std::to_string(i));
    (*fields)[Key(peer_prefix, "peer_id")] = std::to_string(region.peers[i].peer_id);
    (*fields)[Key(peer_prefix, "store_id")] = std::to_string(region.peers[i].store_id);
    (*fields)[Key(peer_prefix, "role")] = PeerRoleName(region.peers[i].role);
  }
}

Status GetRegionFields(const FieldMap& fields, RegionInfo* region, const std::string& prefix) {
  if (region == nullptr) {
    return Status::InvalidArgument("region must not be null");
  }
  region->region_id = GetUint64Or(fields, Key(prefix, "region_id"), 0);
  region->start_key = GetOr(fields, Key(prefix, "start_key"), "");
  region->end_key = GetOr(fields, Key(prefix, "end_key"), "");
  region->epoch.conf_ver = GetUint64Or(fields, Key(prefix, "conf_ver"), 1);
  region->epoch.version = GetUint64Or(fields, Key(prefix, "version"), 1);
  region->leader_peer_id = GetUint64Or(fields, Key(prefix, "leader_peer_id"), 0);
  const auto peer_count = GetUint64Or(fields, Key(prefix, "peer_count"), 0);
  region->peers.clear();
  for (std::uint64_t i = 0; i < peer_count; ++i) {
    const auto peer_prefix = Key(prefix, "peer" + std::to_string(i));
    Peer peer;
    peer.peer_id = GetUint64Or(fields, Key(peer_prefix, "peer_id"), 0);
    peer.store_id = GetUint64Or(fields, Key(peer_prefix, "store_id"), 0);
    peer.role = ParsePeerRole(GetOr(fields, Key(peer_prefix, "role"), "Voter"));
    region->peers.push_back(peer);
  }
  return Status::Ok();
}

std::string PeerRoleName(PeerRole role) {
  switch (role) {
    case PeerRole::kVoter:
      return "Voter";
    case PeerRole::kLearner:
      return "Learner";
  }
  return "Voter";
}

PeerRole ParsePeerRole(const std::string& value) {
  if (value == "Learner") {
    return PeerRole::kLearner;
  }
  return PeerRole::kVoter;
}

}  // namespace rstone
