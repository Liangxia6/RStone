#include "rstone/pd/pd_server.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

#include "rstone/common/serialization.h"
#include "rstone/region/region.h"
#include "rstone/rpc/rpc_codec.h"

namespace rstone {

Status PdServer::Open(const std::string& data_dir) {
  data_dir_ = data_dir;
  std::filesystem::create_directories(data_dir_);
  if (std::filesystem::exists(std::filesystem::path(data_dir_) / "pd.catalog")) {
    return Load();
  }
  return Persist();
}

Status PdServer::RegisterStore(StoreInfo* store) {
  if (store == nullptr) {
    return Status::InvalidArgument("store must not be null");
  }
  if (store->store_id == 0) {
    store->store_id = metadata_.AllocStoreId();
  } else if (store->store_id >= metadata_.next_store_id()) {
    // 配置指定固定 store_id 时，也要推进 next_store_id，避免后续自动分配冲突。
    metadata_.SetNextIds(store->store_id + 1, metadata_.next_region_id(),
                         metadata_.next_peer_id());
  }
  auto status = metadata_.PutStore(*store);
  if (!status.ok()) {
    return status;
  }
  return Persist();
}

Status PdServer::StoreHeartbeat(StoreId store_id, std::int64_t now_ms) {
  auto store = metadata_.GetStore(store_id);
  if (!store) {
    return {ErrorCode::kStoreNotFound, "store not found"};
  }
  store->last_heartbeat_ms = now_ms;
  store->state = "Up";
  auto status = metadata_.PutStore(*store);
  if (!status.ok()) {
    return status;
  }
  return Persist();
}

Status PdServer::BootstrapDefaultRegion(const std::vector<StoreId>& store_ids) {
  if (!metadata_.ListRegions().empty()) {
    return Status::Ok();
  }
  if (store_ids.empty()) {
    return Status::InvalidArgument("default region needs at least one store");
  }

  RegionInfo region;
  // 默认 Region 使用空 start/end 表示覆盖全部 key range。
  region.region_id = metadata_.AllocRegionId();
  region.start_key = "";
  region.end_key = "";
  for (const auto store_id : store_ids) {
    if (!metadata_.GetStore(store_id)) {
      return {ErrorCode::kStoreNotFound, "store not found for default region"};
    }
    Peer peer;
    // 每个 Store 获得一个 Peer，多个 Peer 组成该 Region 的 Raft Group。
    peer.peer_id = metadata_.AllocPeerId();
    peer.store_id = store_id;
    peer.role = PeerRole::kVoter;
    region.peers.push_back(peer);
  }
  region.leader_peer_id = region.peers.front().peer_id;
  auto status = metadata_.PutRegion(region);
  if (!status.ok()) {
    return status;
  }
  return Persist();
}

Status PdServer::SplitRegion(RegionId region_id, const std::string& split_key,
                             RegionInfo* left, RegionInfo* right) {
  auto region = metadata_.GetRegion(region_id);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found"};
  }
  if (!ContainsKey(*region, split_key) || split_key == region->start_key ||
      (!region->end_key.empty() && split_key == region->end_key)) {
    return Status::InvalidArgument("split key is outside region boundary");
  }

  RegionInfo left_region = *region;
  RegionInfo right_region = *region;

  // 左 Region 复用原 region_id，右 Region 分配新 region_id。
  left_region.end_key = split_key;
  left_region.epoch.version += 1;

  right_region.region_id = metadata_.AllocRegionId();
  right_region.start_key = split_key;
  right_region.epoch.version = left_region.epoch.version;
  right_region.peers.clear();
  for (const auto& peer : region->peers) {
    Peer new_peer;
    new_peer.peer_id = metadata_.AllocPeerId();
    new_peer.store_id = peer.store_id;
    new_peer.role = peer.role;
    right_region.peers.push_back(new_peer);
  }
  right_region.leader_peer_id = right_region.peers.empty() ? 0 : right_region.peers.front().peer_id;

  auto status = metadata_.PutRegion(left_region);
  if (!status.ok()) {
    return status;
  }
  status = metadata_.PutRegion(right_region);
  if (!status.ok()) {
    return status;
  }
  status = Persist();
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

Status PdServer::AddPeer(RegionId region_id, StoreId store_id, Peer* added_peer) {
  auto region = metadata_.GetRegion(region_id);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found"};
  }
  if (!metadata_.GetStore(store_id)) {
    return {ErrorCode::kStoreNotFound, "store not found"};
  }
  if (FindPeerByStore(*region, store_id) != nullptr) {
    return Status::InvalidArgument("store already has peer for region");
  }

  Peer peer;
  peer.peer_id = metadata_.AllocPeerId();
  peer.store_id = store_id;
  peer.role = PeerRole::kVoter;
  region->peers.push_back(peer);
  region->epoch.conf_ver += 1;
  auto status = metadata_.PutRegion(*region);
  if (!status.ok()) {
    return status;
  }
  status = Persist();
  if (!status.ok()) {
    return status;
  }
  if (added_peer != nullptr) {
    *added_peer = peer;
  }
  return Status::Ok();
}

Status PdServer::RemovePeer(RegionId region_id, PeerId peer_id) {
  auto region = metadata_.GetRegion(region_id);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found"};
  }
  const auto old_size = region->peers.size();
  region->peers.erase(std::remove_if(region->peers.begin(), region->peers.end(),
                                     [peer_id](const Peer& peer) {
                                       return peer.peer_id == peer_id;
                                     }),
                      region->peers.end());
  if (region->peers.size() == old_size) {
    return Status::InvalidArgument("peer not found");
  }
  if (region->peers.empty()) {
    return Status::InvalidArgument("cannot remove last peer");
  }
  if (region->leader_peer_id == peer_id) {
    region->leader_peer_id = region->peers.front().peer_id;
  }
  region->epoch.conf_ver += 1;
  auto status = metadata_.PutRegion(*region);
  if (!status.ok()) {
    return status;
  }
  return Persist();
}

Status PdServer::TransferLeader(RegionId region_id, PeerId target_peer_id) {
  auto region = metadata_.GetRegion(region_id);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found"};
  }
  if (FindPeer(*region, target_peer_id) == nullptr) {
    return Status::InvalidArgument("target leader peer not found");
  }
  region->leader_peer_id = target_peer_id;
  auto status = metadata_.PutRegion(*region);
  if (!status.ok()) {
    return status;
  }
  return Persist();
}

std::optional<RegionInfo> PdServer::GetRegionByKey(const std::string& key) const {
  return metadata_.GetRegionByKey(key);
}

std::optional<StoreInfo> PdServer::GetStore(StoreId store_id) const {
  return metadata_.GetStore(store_id);
}

std::optional<StoreInfo> PdServer::GetRegionLeaderStore(const RegionInfo& region) const {
  // Region 记录的是 leader_peer_id，需要先找到 Peer，再映射到 Store。
  const Peer* leader = FindPeer(region, region.leader_peer_id);
  if (leader == nullptr) {
    return std::nullopt;
  }
  return metadata_.GetStore(leader->store_id);
}

Status PdServer::Persist() const {
  if (!Persistent()) {
    return Status::Ok();
  }
  FieldMap fields;
  fields["next_store_id"] = std::to_string(metadata_.next_store_id());
  fields["next_region_id"] = std::to_string(metadata_.next_region_id());
  fields["next_peer_id"] = std::to_string(metadata_.next_peer_id());

  const auto stores = metadata_.ListStores();
  fields["store_count"] = std::to_string(stores.size());
  for (std::size_t i = 0; i < stores.size(); ++i) {
    PutStoreFields(&fields, stores[i], "store" + std::to_string(i));
  }

  const auto regions = metadata_.ListRegions();
  fields["region_count"] = std::to_string(regions.size());
  for (std::size_t i = 0; i < regions.size(); ++i) {
    PutRegionFields(&fields, regions[i], "region" + std::to_string(i));
  }

  std::filesystem::create_directories(data_dir_);
  std::ofstream output(std::filesystem::path(data_dir_) / "pd.catalog");
  if (!output) {
    return Status::IoError("failed to write pd catalog");
  }
  output << EncodeFields(fields);
  return Status::Ok();
}

Status PdServer::Load() {
  std::ifstream input(std::filesystem::path(data_dir_) / "pd.catalog");
  if (!input) {
    return Status::IoError("failed to read pd catalog");
  }
  std::string encoded((std::istreambuf_iterator<char>(input)),
                      std::istreambuf_iterator<char>());
  FieldMap fields;
  auto status = DecodeFields(encoded, &fields);
  if (!status.ok()) {
    return status;
  }

  metadata_ = PdMetadataStore();
  const auto store_count = static_cast<std::size_t>(std::stoull(fields["store_count"]));
  for (std::size_t i = 0; i < store_count; ++i) {
    StoreInfo store;
    status = GetStoreFields(fields, &store, "store" + std::to_string(i));
    if (!status.ok()) {
      return status;
    }
    status = metadata_.PutStore(store);
    if (!status.ok()) {
      return status;
    }
  }

  const auto region_count = static_cast<std::size_t>(std::stoull(fields["region_count"]));
  for (std::size_t i = 0; i < region_count; ++i) {
    RegionInfo region;
    status = GetRegionFields(fields, &region, "region" + std::to_string(i));
    if (!status.ok()) {
      return status;
    }
    status = metadata_.PutRegion(region);
    if (!status.ok()) {
      return status;
    }
  }

  metadata_.SetNextIds(static_cast<StoreId>(std::stoull(fields["next_store_id"])),
                       static_cast<RegionId>(std::stoull(fields["next_region_id"])),
                       static_cast<PeerId>(std::stoull(fields["next_peer_id"])));
  return Status::Ok();
}

}  // namespace rstone
