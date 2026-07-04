#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rstone/common/status.h"
#include "rstone/common/types.h"
#include "rstone/store/single_region_cluster.h"

namespace rstone {

class MultiRegionCluster {
 public:
  Status Bootstrap(const std::string& data_dir, std::size_t store_count,
                   const std::vector<RegionInfo>& regions, bool reset_data = true);

  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Batch(const std::vector<KvMutation>& mutations);
  Status Get(const std::string& key, Consistency consistency, std::string* value) const;
  Status SplitRegion(RegionId region_id, const std::string& split_key,
                     RegionInfo* left, RegionInfo* right);
  Status TransferLeader(RegionId region_id, PeerId target_peer_id);
  Status AddPeer(RegionId region_id, StoreId store_id, PeerId peer_id);
  Status RemovePeer(RegionId region_id, PeerId peer_id);

  SingleRegionCluster* FindRegionCluster(RegionId region_id);
  const SingleRegionCluster* FindRegionCluster(RegionId region_id) const;
  std::optional<RegionInfo> FindRegionByKey(const std::string& key) const;
  std::size_t RegionCount() const { return clusters_.size(); }
  std::vector<RegionInfo> ListRegions() const { return regions_; }

 private:
  Status PersistRegionCatalog() const;
  Status LoadRegionCatalog(std::vector<RegionInfo>* regions) const;
  bool RegionCatalogExists() const;

  std::vector<RegionInfo> regions_;
  std::map<RegionId, std::unique_ptr<SingleRegionCluster>> clusters_;
  std::string data_dir_;
  std::size_t store_count_ = 0;
};

}  // namespace rstone
