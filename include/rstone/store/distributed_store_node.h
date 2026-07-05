#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rstone/store/distributed_region_node.h"

namespace rstone {

class DistributedStoreNode {
 public:
  Status Bootstrap(const std::string& data_dir, const StoreInfo& local_store,
                   const std::vector<StoreInfo>& stores,
                   const std::vector<RegionInfo>& regions);

  Status Put(RegionId region_id, const std::string& key, const std::string& value);
  Status Delete(RegionId region_id, const std::string& key);
  Status Batch(RegionId region_id, const std::vector<KvMutation>& mutations);
  Status Get(RegionId region_id, const std::string& key, Consistency consistency,
             std::string* value) const;
  Status TransferLeader(RegionId region_id, PeerId target_peer_id);

  RequestVoteResponse HandleRequestVote(const RequestVoteRequest& request);
  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request);

  std::vector<RegionInfo> ListRegions() const;
  const DistributedRegionNode* FindRegion(RegionId region_id) const;
  DistributedRegionNode* FindRegion(RegionId region_id);

 private:
  Status CheckRegionKey(RegionId region_id, const std::string& key) const;

  StoreInfo local_store_;
  std::vector<StoreInfo> stores_;
  std::string data_dir_;
  std::map<RegionId, std::unique_ptr<DistributedRegionNode>> regions_;
};

}  // namespace rstone
