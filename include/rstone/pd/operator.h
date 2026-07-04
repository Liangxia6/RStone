#pragma once

#include <string>

#include "rstone/common/types.h"

namespace rstone {

enum class OperatorType {
  kTransferLeader,
  kAddPeer,
  kRemovePeer,
  kSplitRegion,
};

struct Operator {
  OperatorType type = OperatorType::kTransferLeader;
  RegionId region_id = 0;
  StoreId target_store_id = 0;
  PeerId target_peer_id = 0;
  PeerId remove_peer_id = 0;
  std::string split_key;
};

}  // namespace rstone
