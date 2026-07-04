#pragma once

#include <optional>
#include <string>
#include <vector>

#include "rstone/pd/metadata_store.h"

namespace rstone {

class PdServer {
 public:
  Status Open(const std::string& data_dir);

  PdMetadataStore& metadata() { return metadata_; }
  const PdMetadataStore& metadata() const { return metadata_; }

  Status RegisterStore(StoreInfo* store);
  Status StoreHeartbeat(StoreId store_id, std::int64_t now_ms);
  Status BootstrapDefaultRegion(const std::vector<StoreId>& store_ids);
  Status SplitRegion(RegionId region_id, const std::string& split_key,
                     RegionInfo* left, RegionInfo* right);
  Status AddPeer(RegionId region_id, StoreId store_id, Peer* added_peer);
  Status RemovePeer(RegionId region_id, PeerId peer_id);
  Status TransferLeader(RegionId region_id, PeerId target_peer_id);

  std::optional<RegionInfo> GetRegionByKey(const std::string& key) const;
  std::optional<StoreInfo> GetStore(StoreId store_id) const;
  std::optional<StoreInfo> GetRegionLeaderStore(const RegionInfo& region) const;

 private:
  Status Persist() const;
  Status Load();
  bool Persistent() const { return !data_dir_.empty(); }

  PdMetadataStore metadata_;
  std::string data_dir_;
};

}  // namespace rstone
