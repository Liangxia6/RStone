#include "rstone/store/single_region_cluster.h"

#include <algorithm>
#include <filesystem>

#include "rstone/storage/kv_command.h"

namespace rstone {

SingleRegionStore::SingleRegionStore(StoreId store_id, PeerId peer_id, RegionId region_id,
                                     std::vector<PeerId> peers)
    : store_id_(store_id),
      peer_id_(peer_id),
      raft_(region_id, peer_id, std::move(peers)) {}

Status SingleRegionStore::Open(const std::string& data_dir) {
  auto status = engine_.Open(data_dir + "/store-" + std::to_string(store_id_));
  if (!status.ok()) {
    return status;
  }
  raft_storage_ = std::make_unique<RaftStorage>(&engine_);
  return RestoreRaftState();
}

Status SingleRegionStore::PersistHardState() {
  if (!raft_storage_) {
    return Status::InvalidArgument("raft storage is not initialized");
  }
  HardState state;
  state.current_term = raft_.current_term();
  state.voted_for = raft_.voted_for().value_or(0);
  state.commit_index = raft_.commit_index();
  state.last_applied = raft_.last_applied();
  return raft_storage_->SaveHardState(raft_.region_id(), state);
}

Status SingleRegionStore::PersistLogEntry(const LogEntry& entry) {
  if (!raft_storage_) {
    return Status::InvalidArgument("raft storage is not initialized");
  }
  return raft_storage_->AppendLog(entry);
}

Status SingleRegionStore::RestoreRaftState() {
  if (!raft_storage_) {
    return Status::InvalidArgument("raft storage is not initialized");
  }

  HardState hard_state;
  auto status = raft_storage_->LoadHardState(raft_.region_id(), &hard_state);
  if (!status.ok()) {
    if (status.code() == ErrorCode::kKeyNotFound) {
      return Status::Ok();
    }
    return status;
  }

  std::vector<LogEntry> entries;
  status = raft_storage_->LoadLog(raft_.region_id(), &entries);
  if (!status.ok()) {
    return status;
  }

  std::optional<PeerId> voted_for;
  if (hard_state.voted_for != 0) {
    voted_for = hard_state.voted_for;
  }
  return raft_.Restore(hard_state.current_term, voted_for, hard_state.commit_index,
                       hard_state.last_applied, std::move(entries));
}

Status SingleRegionStore::ApplyCommitted() {
  for (const auto& entry : raft_.TakeCommittedEntries()) {
    if (entry.type == EntryType::kNoop) {
      continue;
    }
    auto status = ApplyKvCommand(&engine_, entry.command);
    if (!status.ok()) {
      return status;
    }
  }
  return PersistHardState();
}

Status SingleRegionStore::Get(const std::string& key, std::string* value) const {
  return engine_.Get(key, value);
}

Status SingleRegionCluster::Bootstrap(const std::string& data_dir, std::size_t store_count,
                                      RegionId region_id, bool reset_data) {
  if (store_count == 0) {
    return Status::InvalidArgument("store_count must be non-zero");
  }
  region_id_ = region_id;
  data_dir_ = data_dir;
  if (reset_data) {
    std::filesystem::remove_all(data_dir);
  }
  std::filesystem::create_directories(data_dir);

  peer_ids_.clear();
  stores_.clear();
  for (std::size_t i = 0; i < store_count; ++i) {
    peer_ids_.push_back(static_cast<PeerId>(i + 1));
  }

  for (std::size_t i = 0; i < store_count; ++i) {
    auto store = std::make_unique<SingleRegionStore>(
        static_cast<StoreId>(i + 1), static_cast<PeerId>(i + 1), region_id_, peer_ids_);
    auto status = store->Open(data_dir);
    if (!status.ok()) {
      return status;
    }
    stores_.push_back(std::move(store));
  }
  for (const auto& store : stores_) {
    if (store->raft().role() == RaftRole::kLeader) {
      leader_peer_id_ = store->peer_id();
      return Status::Ok();
    }
  }
  return ElectLeader(peer_ids_.front());
}

Status SingleRegionCluster::ElectLeader(PeerId peer_id) {
  SingleRegionStore* elected = nullptr;
  for (auto& store : stores_) {
    if (store->peer_id() == peer_id) {
      elected = store.get();
      break;
    }
  }
  if (elected == nullptr) {
    return Status::InvalidArgument("leader peer not found");
  }

  elected->raft().BecomeCandidate();
  auto status = elected->PersistHardState();
  if (!status.ok()) {
    return status;
  }
  const Term term = elected->raft().current_term();
  int votes = 1;
  for (auto& store : stores_) {
    if (store->peer_id() == peer_id) {
      continue;
    }
    RequestVoteRequest request;
    request.region_id = region_id_;
    request.term = term;
    request.candidate_id = peer_id;
    request.last_log_index = elected->raft().last_log_index();
    request.last_log_term = elected->raft().last_log_term();
    const auto response = store->raft().HandleRequestVote(request);
    status = store->PersistHardState();
    if (!status.ok()) {
      return status;
    }
    if (response.vote_granted) {
      ++votes;
    }
  }
  if (votes <= static_cast<int>(stores_.size() / 2)) {
    return Status::Internal("failed to elect leader");
  }
  elected->raft().BecomeLeader();
  status = elected->PersistHardState();
  if (!status.ok()) {
    return status;
  }
  leader_peer_id_ = peer_id;
  return Status::Ok();
}

Status SingleRegionCluster::TransferLeader(PeerId peer_id) {
  if (leader_peer_id_ == peer_id) {
    return Status::Ok();
  }
  return ElectLeader(peer_id);
}

Status SingleRegionCluster::AddPeer(StoreId store_id, PeerId peer_id) {
  if (FindByStoreId(store_id) != nullptr) {
    return Status::InvalidArgument("store already exists in region cluster");
  }
  if (std::find(peer_ids_.begin(), peer_ids_.end(), peer_id) != peer_ids_.end()) {
    return Status::InvalidArgument("peer already exists in region cluster");
  }
  peer_ids_.push_back(peer_id);
  for (auto& store : stores_) {
    (void)store;
  }
  auto new_store = std::make_unique<SingleRegionStore>(store_id, peer_id, region_id_, peer_ids_);
  auto status = new_store->Open(data_dir_);
  if (!status.ok()) {
    return status;
  }

  std::vector<KvMutation> bootstrap;
  const auto* leader = Leader();
  if (leader != nullptr) {
    for (const auto& [key, value] : leader->engine().ScanRange("", "")) {
      if (key.rfind("raft/", 0) == 0 || key.rfind("region/", 0) == 0 ||
          key.rfind("pd/", 0) == 0 || key.rfind("local/", 0) == 0) {
        continue;
      }
      bootstrap.push_back({KvMutationType::kPut, key, value});
    }
  }
  if (leader != nullptr) {
    status = new_store->raft().Restore(leader->raft().current_term(),
                                       leader->raft().voted_for(),
                                       leader->raft().commit_index(),
                                       leader->raft().last_applied(),
                                       leader->raft().log());
    if (!status.ok()) {
      return status;
    }
    for (const auto& entry : leader->raft().log()) {
      status = new_store->PersistLogEntry(entry);
      if (!status.ok()) {
        return status;
      }
    }
    status = new_store->PersistHardState();
    if (!status.ok()) {
      return status;
    }
  }
  stores_.push_back(std::move(new_store));
  if (!bootstrap.empty()) {
    return stores_.back()->engine().WriteBatch(bootstrap);
  }
  return Status::Ok();
}

Status SingleRegionCluster::RemovePeer(PeerId peer_id) {
  if (stores_.size() <= 1) {
    return Status::InvalidArgument("cannot remove last peer");
  }
  const auto store_it =
      std::find_if(stores_.begin(), stores_.end(), [peer_id](const auto& store) {
        return store->peer_id() == peer_id;
      });
  if (store_it == stores_.end()) {
    return Status::InvalidArgument("peer not found");
  }
  stores_.erase(store_it);
  peer_ids_.erase(std::remove(peer_ids_.begin(), peer_ids_.end(), peer_id), peer_ids_.end());
  if (leader_peer_id_ == peer_id) {
    leader_peer_id_ = 0;
    return ElectLeader(stores_.front()->peer_id());
  }
  return Status::Ok();
}

Status SingleRegionCluster::Put(const std::string& key, const std::string& value) {
  return ProposeAndReplicate(EncodePutCommand(key, value));
}

Status SingleRegionCluster::Delete(const std::string& key) {
  return ProposeAndReplicate(EncodeDeleteCommand(key));
}

Status SingleRegionCluster::Batch(const std::vector<KvMutation>& mutations) {
  if (mutations.empty()) {
    return Status::Ok();
  }
  return ProposeAndReplicate(EncodeBatchCommand(mutations));
}

Status SingleRegionCluster::GetFromStore(StoreId store_id, const std::string& key,
                                         std::string* value) const {
  for (const auto& store : stores_) {
    if (store->store_id() == store_id) {
      return store->Get(key, value);
    }
  }
  return {ErrorCode::kStoreNotFound, "store not found"};
}

std::vector<std::pair<std::string, std::string>> SingleRegionCluster::ExportRangeFromLeader(
    const std::string& start_key, const std::string& end_key) const {
  const auto* leader = Leader();
  if (leader == nullptr) {
    return {};
  }
  return leader->engine().ScanRange(start_key, end_key);
}

SingleRegionStore* SingleRegionCluster::Leader() {
  for (auto& store : stores_) {
    if (store->peer_id() == leader_peer_id_) {
      return store.get();
    }
  }
  return nullptr;
}

const SingleRegionStore* SingleRegionCluster::Leader() const {
  for (const auto& store : stores_) {
    if (store->peer_id() == leader_peer_id_) {
      return store.get();
    }
  }
  return nullptr;
}

SingleRegionStore* SingleRegionCluster::FindByStoreId(StoreId store_id) {
  for (auto& store : stores_) {
    if (store->store_id() == store_id) {
      return store.get();
    }
  }
  return nullptr;
}

Status SingleRegionCluster::ProposeAndReplicate(const std::string& command) {
  SingleRegionStore* leader = Leader();
  if (leader == nullptr) {
    return {ErrorCode::kNotLeader, "no leader"};
  }

  LogEntry entry;
  auto status = leader->raft().Propose(EntryType::kNormal, command, &entry);
  if (!status.ok()) {
    return status;
  }
  status = leader->PersistLogEntry(entry);
  if (!status.ok()) {
    return status;
  }

  int replicated = 1;
  for (auto& store : stores_) {
    if (store->peer_id() == leader_peer_id_) {
      continue;
    }
    AppendEntriesRequest request;
    request.region_id = region_id_;
    request.term = leader->raft().current_term();
    request.leader_id = leader_peer_id_;
    request.prev_log_index = entry.index - 1;
    request.prev_log_term = entry.index == 1 ? 0 : leader->raft().log()[entry.index - 2].term;
    request.entries.push_back(entry);
    request.leader_commit = leader->raft().commit_index();
    const auto response = store->raft().HandleAppendEntries(request);
    if (response.success) {
      status = store->PersistLogEntry(entry);
      if (!status.ok()) {
        return status;
      }
      status = store->PersistHardState();
      if (!status.ok()) {
        return status;
      }
      ++replicated;
    }
  }

  if (replicated <= static_cast<int>(stores_.size() / 2)) {
    return Status::Internal("failed to replicate entry to majority");
  }

  leader->raft().SetCommitIndex(entry.index);
  status = leader->ApplyCommitted();
  if (!status.ok()) {
    return status;
  }
  return BroadcastCommit(entry.index);
}

Status SingleRegionCluster::BroadcastCommit(LogIndex commit_index) {
  const SingleRegionStore* leader = Leader();
  if (leader == nullptr) {
    return {ErrorCode::kNotLeader, "no leader"};
  }

  for (auto& store : stores_) {
    if (store->peer_id() == leader_peer_id_) {
      continue;
    }
    AppendEntriesRequest request;
    request.region_id = region_id_;
    request.term = leader->raft().current_term();
    request.leader_id = leader_peer_id_;
    request.prev_log_index = store->raft().last_log_index();
    request.prev_log_term = store->raft().last_log_term();
    request.leader_commit = commit_index;
    const auto response = store->raft().HandleAppendEntries(request);
    if (!response.success) {
      return Status::Internal("failed to broadcast commit");
    }
    auto status = store->ApplyCommitted();
    if (!status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

}  // namespace rstone
