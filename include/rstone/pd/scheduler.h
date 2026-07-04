#pragma once

#include <optional>

#include "rstone/pd/metadata_store.h"
#include "rstone/pd/operator.h"

namespace rstone {

class Scheduler {
 public:
  explicit Scheduler(const PdMetadataStore* metadata);

  std::optional<Operator> MakeTransferLeaderOperator(RegionId region_id,
                                                     StoreId target_store_id) const;
  std::optional<Operator> MakeAddPeerOperator(RegionId region_id,
                                             StoreId target_store_id) const;
  std::optional<Operator> MakeRemovePeerOperator(RegionId region_id,
                                                PeerId peer_id) const;

 private:
  const PdMetadataStore* metadata_ = nullptr;
};

}  // namespace rstone
