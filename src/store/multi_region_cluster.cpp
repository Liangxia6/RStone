#include "rstone/store/multi_region_cluster.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

#include "rstone/common/serialization.h"
#include "rstone/region/region.h"
#include "rstone/rpc/rpc_codec.h"

namespace rstone {

Status MultiRegionCluster::Bootstrap(const std::string& data_dir, std::size_t store_count,
                                     const std::vector<RegionInfo>& regions, bool reset_data) {
  if (regions.empty()) {
    return Status::InvalidArgument("regions must not be empty");
  }

  data_dir_ = data_dir;
  store_count_ = store_count;
  if (reset_data) {
    std::filesystem::remove_all(data_dir_);
  }
  std::filesystem::create_directories(data_dir_);

  if (!reset_data && RegionCatalogExists()) {
    auto status = LoadRegionCatalog(&regions_);
    if (!status.ok()) {
      return status;
    }
  } else {
    regions_ = regions;
  }

  clusters_.clear();
  for (const auto& region : regions_) {
    auto cluster = std::make_unique<SingleRegionCluster>();
    auto status = cluster->Bootstrap(data_dir + "/region-" + std::to_string(region.region_id),
                                     store_count, region.region_id, reset_data);
    if (!status.ok()) {
      return status;
    }
    clusters_[region.region_id] = std::move(cluster);
  }
  return PersistRegionCatalog();
}

Status MultiRegionCluster::Put(const std::string& key, const std::string& value) {
  auto region = FindRegionByKey(key);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found for key"};
  }
  return clusters_.at(region->region_id)->Put(key, value);
}

Status MultiRegionCluster::Delete(const std::string& key) {
  auto region = FindRegionByKey(key);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found for key"};
  }
  return clusters_.at(region->region_id)->Delete(key);
}

Status MultiRegionCluster::Batch(const std::vector<KvMutation>& mutations) {
  if (mutations.empty()) {
    return Status::Ok();
  }
  const auto region = FindRegionByKey(mutations.front().key);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found for key"};
  }
  for (const auto& mutation : mutations) {
    const auto mutation_region = FindRegionByKey(mutation.key);
    if (!mutation_region || mutation_region->region_id != region->region_id) {
      return Status::InvalidArgument("batch must target a single region");
    }
  }
  return clusters_.at(region->region_id)->Batch(mutations);
}

Status MultiRegionCluster::Get(const std::string& key, Consistency consistency,
                               std::string* value) const {
  auto region = FindRegionByKey(key);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found for key"};
  }
  const auto* cluster = FindRegionCluster(region->region_id);
  if (cluster == nullptr) {
    return {ErrorCode::kRegionNotFound, "region cluster not found"};
  }
  if (consistency == Consistency::kLinearizable) {
    const auto* leader = cluster->Leader();
    if (leader == nullptr) {
      return {ErrorCode::kNotLeader, "no leader"};
    }
    return leader->Get(key, value);
  }
  if (cluster->stores().empty()) {
    return Status::Internal("no stores");
  }
  return cluster->stores().front()->Get(key, value);
}

Status MultiRegionCluster::SplitRegion(RegionId region_id, const std::string& split_key,
                                       RegionInfo* left, RegionInfo* right) {
  auto region_it = std::find_if(regions_.begin(), regions_.end(),
                                [region_id](const RegionInfo& region) {
                                  return region.region_id == region_id;
                                });
  if (region_it == regions_.end()) {
    return {ErrorCode::kRegionNotFound, "region not found"};
  }
  if (!ContainsKey(*region_it, split_key) || split_key == region_it->start_key ||
      (!region_it->end_key.empty() && split_key == region_it->end_key)) {
    return Status::InvalidArgument("split key is outside region boundary");
  }
  auto* source_cluster = FindRegionCluster(region_id);
  if (source_cluster == nullptr) {
    return {ErrorCode::kRegionNotFound, "source region cluster not found"};
  }

  RegionInfo left_region = *region_it;
  RegionInfo right_region = *region_it;
  left_region.end_key = split_key;
  left_region.epoch.version += 1;

  RegionId next_region_id = 1;
  for (const auto& region : regions_) {
    next_region_id = std::max(next_region_id, region.region_id + 1);
  }
  right_region.region_id = next_region_id;
  right_region.start_key = split_key;
  right_region.epoch.version = left_region.epoch.version;
  for (auto& peer : right_region.peers) {
    peer.peer_id += next_region_id * 1000;
  }
  if (!right_region.peers.empty()) {
    right_region.leader_peer_id = right_region.peers.front().peer_id;
  }

  auto right_cluster = std::make_unique<SingleRegionCluster>();
  auto status = right_cluster->Bootstrap(data_dir_ + "/region-" +
                                             std::to_string(right_region.region_id),
                                         store_count_, right_region.region_id, true);
  if (!status.ok()) {
    return status;
  }

  const auto migrated = source_cluster->ExportRangeFromLeader(split_key, region_it->end_key);
  std::vector<KvMutation> puts;
  std::vector<KvMutation> deletes;
  for (const auto& [key, value] : migrated) {
    if (key.rfind("raft/", 0) == 0 || key.rfind("region/", 0) == 0 ||
        key.rfind("pd/", 0) == 0 || key.rfind("local/", 0) == 0) {
      continue;
    }
    puts.push_back({KvMutationType::kPut, key, value});
    deletes.push_back({KvMutationType::kDelete, key, ""});
  }
  status = right_cluster->Batch(puts);
  if (!status.ok()) {
    return status;
  }
  status = source_cluster->Batch(deletes);
  if (!status.ok()) {
    return status;
  }

  *region_it = left_region;
  regions_.push_back(right_region);
  clusters_[right_region.region_id] = std::move(right_cluster);
  status = PersistRegionCatalog();
  if (!status.ok()) {
    return status;
  }

  if (left != nullptr) {
    *left = left_region;
  }
  if (right != nullptr) {
    *right = right_region;
  }
  return Status::Ok();
}

Status MultiRegionCluster::TransferLeader(RegionId region_id, PeerId target_peer_id) {
  auto* cluster = FindRegionCluster(region_id);
  if (cluster == nullptr) {
    return {ErrorCode::kRegionNotFound, "region cluster not found"};
  }
  auto status = cluster->TransferLeader(target_peer_id);
  if (!status.ok()) {
    return status;
  }
  for (auto& region : regions_) {
    if (region.region_id == region_id) {
      region.leader_peer_id = target_peer_id;
      break;
    }
  }
  return PersistRegionCatalog();
}

Status MultiRegionCluster::AddPeer(RegionId region_id, StoreId store_id, PeerId peer_id) {
  auto* cluster = FindRegionCluster(region_id);
  if (cluster == nullptr) {
    return {ErrorCode::kRegionNotFound, "region cluster not found"};
  }
  auto status = cluster->AddPeer(store_id, peer_id);
  if (!status.ok()) {
    return status;
  }
  for (auto& region : regions_) {
    if (region.region_id == region_id) {
      region.peers.push_back(Peer{peer_id, store_id, PeerRole::kVoter});
      region.epoch.conf_ver += 1;
      break;
    }
  }
  return PersistRegionCatalog();
}

Status MultiRegionCluster::RemovePeer(RegionId region_id, PeerId peer_id) {
  auto* cluster = FindRegionCluster(region_id);
  if (cluster == nullptr) {
    return {ErrorCode::kRegionNotFound, "region cluster not found"};
  }
  auto status = cluster->RemovePeer(peer_id);
  if (!status.ok()) {
    return status;
  }
  for (auto& region : regions_) {
    if (region.region_id == region_id) {
      region.peers.erase(std::remove_if(region.peers.begin(), region.peers.end(),
                                        [peer_id](const Peer& peer) {
                                          return peer.peer_id == peer_id;
                                        }),
                         region.peers.end());
      if (region.leader_peer_id == peer_id && !region.peers.empty()) {
        region.leader_peer_id = region.peers.front().peer_id;
      }
      region.epoch.conf_ver += 1;
      break;
    }
  }
  return PersistRegionCatalog();
}

SingleRegionCluster* MultiRegionCluster::FindRegionCluster(RegionId region_id) {
  const auto it = clusters_.find(region_id);
  if (it == clusters_.end()) {
    return nullptr;
  }
  return it->second.get();
}

const SingleRegionCluster* MultiRegionCluster::FindRegionCluster(RegionId region_id) const {
  const auto it = clusters_.find(region_id);
  if (it == clusters_.end()) {
    return nullptr;
  }
  return it->second.get();
}

std::optional<RegionInfo> MultiRegionCluster::FindRegionByKey(const std::string& key) const {
  for (const auto& region : regions_) {
    if (ContainsKey(region, key)) {
      return region;
    }
  }
  return std::nullopt;
}

Status MultiRegionCluster::PersistRegionCatalog() const {
  FieldMap fields;
  fields["region_count"] = std::to_string(regions_.size());
  for (std::size_t i = 0; i < regions_.size(); ++i) {
    PutRegionFields(&fields, regions_[i], "region" + std::to_string(i));
  }

  std::filesystem::create_directories(data_dir_);
  const auto path = std::filesystem::path(data_dir_) / "regions.catalog";
  std::ofstream output(path);
  if (!output) {
    return Status::IoError("failed to write region catalog");
  }
  output << EncodeFields(fields);
  return Status::Ok();
}

Status MultiRegionCluster::LoadRegionCatalog(std::vector<RegionInfo>* regions) const {
  if (regions == nullptr) {
    return Status::InvalidArgument("regions must not be null");
  }
  const auto path = std::filesystem::path(data_dir_) / "regions.catalog";
  std::ifstream input(path);
  if (!input) {
    return Status::IoError("failed to read region catalog");
  }
  std::string encoded((std::istreambuf_iterator<char>(input)),
                      std::istreambuf_iterator<char>());
  FieldMap fields;
  auto status = DecodeFields(encoded, &fields);
  if (!status.ok()) {
    return status;
  }
  const auto count = static_cast<std::size_t>(std::stoull(fields["region_count"]));
  regions->clear();
  for (std::size_t i = 0; i < count; ++i) {
    RegionInfo region;
    status = GetRegionFields(fields, &region, "region" + std::to_string(i));
    if (!status.ok()) {
      return status;
    }
    regions->push_back(region);
  }
  return Status::Ok();
}

bool MultiRegionCluster::RegionCatalogExists() const {
  return std::filesystem::exists(std::filesystem::path(data_dir_) / "regions.catalog");
}

}  // namespace rstone
