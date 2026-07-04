#pragma once

#include <map>
#include <optional>

#include "rstone/common/status.h"
#include "rstone/common/types.h"

namespace rstone {

class PdMetadataStore {
 public:
  StoreId AllocStoreId();
  RegionId AllocRegionId();
  PeerId AllocPeerId();
  void SetNextIds(StoreId next_store_id, RegionId next_region_id, PeerId next_peer_id);
  StoreId next_store_id() const { return next_store_id_; }
  RegionId next_region_id() const { return next_region_id_; }
  PeerId next_peer_id() const { return next_peer_id_; }

  Status PutStore(const StoreInfo& store);
  std::optional<StoreInfo> GetStore(StoreId store_id) const;
  std::vector<StoreInfo> ListStores() const;

  Status PutRegion(const RegionInfo& region);
  std::optional<RegionInfo> GetRegion(RegionId region_id) const;
  std::optional<RegionInfo> GetRegionByKey(const std::string& key) const;
  std::vector<RegionInfo> ListRegions() const;
  Status UpdateRegionLeader(RegionId region_id, PeerId leader_peer_id);

 private:
  StoreId next_store_id_ = 1;
  RegionId next_region_id_ = 1;
  PeerId next_peer_id_ = 1;
  std::map<StoreId, StoreInfo> stores_;
  std::map<RegionId, RegionInfo> regions_;
  std::map<std::string, RegionId> regions_by_start_key_;
};

}  // namespace rstone
