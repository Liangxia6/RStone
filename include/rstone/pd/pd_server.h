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

  // Store 启动后向 PD 注册自己的地址和状态；PD 负责分配或确认 store_id。
  Status RegisterStore(StoreInfo* store);
  Status StoreHeartbeat(StoreId store_id, std::int64_t now_ms);
  // 默认 Region 覆盖整个 key space，是本地集群启动后的第一个 Raft Group。
  Status BootstrapDefaultRegion(const std::vector<StoreId>& store_ids);
  // 元数据层的 split：只更新 Region 边界和 Peer 信息，数据迁移由 Store 完成。
  Status SplitRegion(RegionId region_id, const std::string& split_key,
                     RegionInfo* left, RegionInfo* right);
  Status AddPeer(RegionId region_id, StoreId store_id, Peer* added_peer);
  Status RemovePeer(RegionId region_id, PeerId peer_id);
  Status TransferLeader(RegionId region_id, PeerId target_peer_id);

  // Gateway 根据 key 查询 Region，再根据 leader_peer_id 找到 Leader Store。
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
