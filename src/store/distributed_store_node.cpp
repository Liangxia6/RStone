#include "rstone/store/distributed_store_node.h"

#include "rstone/region/region.h"

namespace rstone {

Status DistributedStoreNode::Bootstrap(const std::string& data_dir,
                                       const StoreInfo& local_store,
                                       const std::vector<StoreInfo>& stores,
                                       const std::vector<RegionInfo>& regions) {
  if (regions.empty()) {
    return Status::InvalidArgument("distributed store needs at least one region");
  }
  data_dir_ = data_dir;
  local_store_ = local_store;
  stores_ = stores;
  regions_.clear();

  for (const auto& region : regions) {
    bool has_local_peer = false;
    for (const auto& peer : region.peers) {
      if (peer.store_id == local_store_.store_id) {
        has_local_peer = true;
        break;
      }
    }
    if (!has_local_peer) {
      continue;
    }
    auto node = std::make_unique<DistributedRegionNode>();
    auto status = node->Bootstrap(data_dir_, local_store_, stores_, region);
    if (!status.ok()) {
      return status;
    }
    regions_[region.region_id] = std::move(node);
  }

  if (regions_.empty()) {
    return Status::InvalidArgument("local store has no peer in any region");
  }
  return Status::Ok();
}

Status DistributedStoreNode::Put(RegionId region_id, const std::string& key,
                                 const std::string& value) {
  auto status = CheckRegionKey(region_id, key);
  if (!status.ok()) {
    return status;
  }
  return regions_.at(region_id)->Put(key, value);
}

Status DistributedStoreNode::Delete(RegionId region_id, const std::string& key) {
  auto status = CheckRegionKey(region_id, key);
  if (!status.ok()) {
    return status;
  }
  return regions_.at(region_id)->Delete(key);
}

Status DistributedStoreNode::Batch(RegionId region_id,
                                   const std::vector<KvMutation>& mutations) {
  for (const auto& mutation : mutations) {
    auto status = CheckRegionKey(region_id, mutation.key);
    if (!status.ok()) {
      return status;
    }
  }
  auto* node = FindRegion(region_id);
  if (node == nullptr) {
    return {ErrorCode::kRegionNotFound, "region not found in local store"};
  }
  return node->Batch(mutations);
}

Status DistributedStoreNode::Get(RegionId region_id, const std::string& key,
                                 Consistency consistency, std::string* value) const {
  auto status = CheckRegionKey(region_id, key);
  if (!status.ok()) {
    return status;
  }
  return regions_.at(region_id)->Get(key, consistency, value);
}

Status DistributedStoreNode::TransferLeader(RegionId region_id, PeerId target_peer_id) {
  auto* node = FindRegion(region_id);
  if (node == nullptr) {
    return {ErrorCode::kRegionNotFound, "region not found in local store"};
  }
  return node->TransferLeader(target_peer_id);
}

RequestVoteResponse DistributedStoreNode::HandleRequestVote(const RequestVoteRequest& request) {
  auto* node = FindRegion(request.region_id);
  if (node == nullptr) {
    return {};
  }
  return node->HandleRequestVote(request);
}

AppendEntriesResponse DistributedStoreNode::HandleAppendEntries(
    const AppendEntriesRequest& request) {
  auto* node = FindRegion(request.region_id);
  if (node == nullptr) {
    return {};
  }
  return node->HandleAppendEntries(request);
}

std::vector<RegionInfo> DistributedStoreNode::ListRegions() const {
  std::vector<RegionInfo> result;
  for (const auto& [unused_id, node] : regions_) {
    (void)unused_id;
    result.push_back(node->region());
  }
  return result;
}

const DistributedRegionNode* DistributedStoreNode::FindRegion(RegionId region_id) const {
  const auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    return nullptr;
  }
  return it->second.get();
}

DistributedRegionNode* DistributedStoreNode::FindRegion(RegionId region_id) {
  const auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    return nullptr;
  }
  return it->second.get();
}

Status DistributedStoreNode::CheckRegionKey(RegionId region_id, const std::string& key) const {
  const auto* node = FindRegion(region_id);
  if (node == nullptr) {
    return {ErrorCode::kRegionNotFound, "region not found in local store"};
  }
  if (!ContainsKey(node->region(), key)) {
    return {ErrorCode::kStaleEpoch, "request key is outside target region"};
  }
  return Status::Ok();
}

}  // namespace rstone
