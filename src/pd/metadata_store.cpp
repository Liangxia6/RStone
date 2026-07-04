#include "rstone/pd/metadata_store.h"

#include "rstone/region/region.h"

namespace rstone {

StoreId PdMetadataStore::AllocStoreId() { return next_store_id_++; }

RegionId PdMetadataStore::AllocRegionId() { return next_region_id_++; }

PeerId PdMetadataStore::AllocPeerId() { return next_peer_id_++; }

void PdMetadataStore::SetNextIds(StoreId next_store_id, RegionId next_region_id,
                                 PeerId next_peer_id) {
  next_store_id_ = next_store_id;
  next_region_id_ = next_region_id;
  next_peer_id_ = next_peer_id;
}

Status PdMetadataStore::PutStore(const StoreInfo& store) {
  if (store.store_id == 0) {
    return Status::InvalidArgument("store_id must be non-zero");
  }
  stores_[store.store_id] = store;
  return Status::Ok();
}

std::optional<StoreInfo> PdMetadataStore::GetStore(StoreId store_id) const {
  const auto it = stores_.find(store_id);
  if (it == stores_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::vector<StoreInfo> PdMetadataStore::ListStores() const {
  std::vector<StoreInfo> result;
  for (const auto& [unused_id, store] : stores_) {
    (void)unused_id;
    result.push_back(store);
  }
  return result;
}

Status PdMetadataStore::PutRegion(const RegionInfo& region) {
  if (region.region_id == 0) {
    return Status::InvalidArgument("region_id must be non-zero");
  }
  regions_[region.region_id] = region;
  regions_by_start_key_[region.start_key] = region.region_id;
  return Status::Ok();
}

std::optional<RegionInfo> PdMetadataStore::GetRegion(RegionId region_id) const {
  const auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<RegionInfo> PdMetadataStore::GetRegionByKey(const std::string& key) const {
  if (regions_by_start_key_.empty()) {
    return std::nullopt;
  }

  auto it = regions_by_start_key_.upper_bound(key);
  if (it == regions_by_start_key_.begin()) {
    return std::nullopt;
  }
  --it;
  const auto region_it = regions_.find(it->second);
  if (region_it == regions_.end()) {
    return std::nullopt;
  }
  if (!ContainsKey(region_it->second, key)) {
    return std::nullopt;
  }
  return region_it->second;
}

std::vector<RegionInfo> PdMetadataStore::ListRegions() const {
  std::vector<RegionInfo> result;
  for (const auto& [unused_id, region] : regions_) {
    (void)unused_id;
    result.push_back(region);
  }
  return result;
}

Status PdMetadataStore::UpdateRegionLeader(RegionId region_id, PeerId leader_peer_id) {
  auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    return {ErrorCode::kRegionNotFound, "region not found"};
  }
  if (FindPeer(it->second, leader_peer_id) == nullptr) {
    return Status::InvalidArgument("leader peer is not in region");
  }
  it->second.leader_peer_id = leader_peer_id;
  return Status::Ok();
}

}  // namespace rstone
