#include "rstone/region/region.h"

namespace rstone {

bool ContainsKey(const RegionInfo& region, const std::string& key) {
  const bool after_start = region.start_key.empty() || key >= region.start_key;
  const bool before_end = region.end_key.empty() || key < region.end_key;
  return after_start && before_end;
}

bool EpochEqual(const RegionEpoch& lhs, const RegionEpoch& rhs) {
  return lhs.conf_ver == rhs.conf_ver && lhs.version == rhs.version;
}

bool EpochStale(const RegionEpoch& request, const RegionEpoch& current) {
  return request.conf_ver < current.conf_ver || request.version < current.version;
}

const Peer* FindPeer(const RegionInfo& region, PeerId peer_id) {
  for (const auto& peer : region.peers) {
    if (peer.peer_id == peer_id) {
      return &peer;
    }
  }
  return nullptr;
}

const Peer* FindPeerByStore(const RegionInfo& region, StoreId store_id) {
  for (const auto& peer : region.peers) {
    if (peer.store_id == store_id) {
      return &peer;
    }
  }
  return nullptr;
}

}  // namespace rstone
