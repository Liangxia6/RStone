#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rstone/raft/raft_node.h"
#include "rstone/raft/raft_storage.h"
#include "rstone/storage/file_kv_engine.h"

namespace rstone {

class SingleRegionStore {
 public:
  SingleRegionStore(StoreId store_id, PeerId peer_id, RegionId region_id,
                    std::vector<PeerId> peers);

  Status Open(const std::string& data_dir);
  StoreId store_id() const { return store_id_; }
  PeerId peer_id() const { return peer_id_; }
  RaftNode& raft() { return raft_; }
  const RaftNode& raft() const { return raft_; }
  KvEngine& engine() { return engine_; }
  const KvEngine& engine() const { return engine_; }

  Status PersistHardState();
  Status PersistLogEntry(const LogEntry& entry);
  Status RestoreRaftState();
  Status ApplyCommitted();
  Status Get(const std::string& key, std::string* value) const;

 private:
  StoreId store_id_ = 0;
  PeerId peer_id_ = 0;
  RaftNode raft_;
  FileKvEngine engine_;
  std::unique_ptr<RaftStorage> raft_storage_;
};

class SingleRegionCluster {
 public:
  Status Bootstrap(const std::string& data_dir, std::size_t store_count,
                   RegionId region_id = 1, bool reset_data = true);
  Status ElectLeader(PeerId peer_id);
  Status TransferLeader(PeerId peer_id);
  Status AddPeer(StoreId store_id, PeerId peer_id);
  Status RemovePeer(PeerId peer_id);
  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Batch(const std::vector<KvMutation>& mutations);
  Status GetFromStore(StoreId store_id, const std::string& key, std::string* value) const;
  std::vector<std::pair<std::string, std::string>> ExportRangeFromLeader(
      const std::string& start_key, const std::string& end_key) const;

  SingleRegionStore* Leader();
  const SingleRegionStore* Leader() const;
  SingleRegionStore* FindByStoreId(StoreId store_id);
  const std::vector<std::unique_ptr<SingleRegionStore>>& stores() const { return stores_; }

 private:
  Status ProposeAndReplicate(const std::string& command);
  Status BroadcastCommit(LogIndex commit_index);

  RegionId region_id_ = 1;
  PeerId leader_peer_id_ = 0;
  std::vector<PeerId> peer_ids_;
  std::vector<std::unique_ptr<SingleRegionStore>> stores_;
  std::string data_dir_;
};

}  // namespace rstone
