#pragma once

#include <string>

#include "rstone/common/types.h"

namespace rstone {

bool ContainsKey(const RegionInfo& region, const std::string& key);
bool EpochEqual(const RegionEpoch& lhs, const RegionEpoch& rhs);
bool EpochStale(const RegionEpoch& request, const RegionEpoch& current);
const Peer* FindPeer(const RegionInfo& region, PeerId peer_id);
const Peer* FindPeerByStore(const RegionInfo& region, StoreId store_id);

}  // namespace rstone
