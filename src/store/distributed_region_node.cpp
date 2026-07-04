#include "rstone/store/distributed_region_node.h"

#include <algorithm>
#include <filesystem>

#include "rstone/rpc/rpc_codec.h"
#include "rstone/rpc/tcp_rpc.h"
#include "rstone/storage/kv_command.h"

namespace rstone {
namespace {

int EntryTypeToInt(EntryType type) {
  switch (type) {
    case EntryType::kNormal:
      return 0;
    case EntryType::kConfigChange:
      return 1;
    case EntryType::kSplit:
      return 2;
    case EntryType::kNoop:
      return 3;
  }
  return 0;
}

EntryType EntryTypeFromInt(int value) {
  switch (value) {
    case 1:
      return EntryType::kConfigChange;
    case 2:
      return EntryType::kSplit;
    case 3:
      return EntryType::kNoop;
    case 0:
    default:
      return EntryType::kNormal;
  }
}

std::string GetOr(const FieldMap& fields, const std::string& key, const std::string& fallback) {
  const auto it = fields.find(key);
  return it == fields.end() ? fallback : it->second;
}

}  // namespace

Status DistributedRegionNode::Bootstrap(const std::string& data_dir,
                                        const StoreInfo& local_store,
                                        const std::vector<StoreInfo>& stores,
                                        const RegionInfo& region) {
  local_store_ = local_store;
  stores_ = stores;
  region_ = region;
  const Peer* local_peer = FindLocalPeer();
  if (local_peer == nullptr) {
    return Status::InvalidArgument("local store does not host a peer for region");
  }
  local_peer_ = *local_peer;

  std::vector<PeerId> peer_ids;
  for (const auto& peer : region_.peers) {
    peer_ids.push_back(peer.peer_id);
  }
  raft_ = std::make_unique<RaftNode>(region_.region_id, local_peer_.peer_id, peer_ids);

  const auto path = std::filesystem::path(data_dir) /
                    ("region-" + std::to_string(region_.region_id));
  auto status = engine_.Open(path.string());
  if (!status.ok()) {
    return status;
  }
  raft_storage_ = std::make_unique<RaftStorage>(&engine_);

  HardState hard_state;
  status = raft_storage_->LoadHardState(region_.region_id, &hard_state);
  if (status.ok()) {
    std::vector<LogEntry> entries;
    status = raft_storage_->LoadLog(region_.region_id, &entries);
    if (!status.ok()) {
      return status;
    }
    std::optional<PeerId> voted_for;
    if (hard_state.voted_for != 0) {
      voted_for = hard_state.voted_for;
    }
    return raft_->Restore(hard_state.current_term, voted_for, hard_state.commit_index,
                          hard_state.last_applied, std::move(entries));
  }
  if (status.code() == ErrorCode::kKeyNotFound) {
    return Status::Ok();
  }
  return status;
}

Status DistributedRegionNode::Put(const std::string& key, const std::string& value) {
  return ProposeAndReplicate(EncodePutCommand(key, value));
}

Status DistributedRegionNode::Delete(const std::string& key) {
  return ProposeAndReplicate(EncodeDeleteCommand(key));
}

Status DistributedRegionNode::Batch(const std::vector<KvMutation>& mutations) {
  if (mutations.empty()) {
    return Status::Ok();
  }
  return ProposeAndReplicate(EncodeBatchCommand(mutations));
}

Status DistributedRegionNode::Get(const std::string& key, Consistency consistency,
                                  std::string* value) const {
  (void)consistency;
  return engine_.Get(key, value);
}

Status DistributedRegionNode::TransferLeader(PeerId target_peer_id) {
  if (target_peer_id != local_peer_.peer_id) {
    return {ErrorCode::kNotLeader, "transfer request must be sent to target peer"};
  }
  region_.leader_peer_id = target_peer_id;
  return EnsureLeader();
}

RequestVoteResponse DistributedRegionNode::HandleRequestVote(const RequestVoteRequest& request) {
  RequestVoteResponse response;
  if (raft_ == nullptr) {
    return response;
  }
  response = raft_->HandleRequestVote(request);
  (void)PersistHardState();
  return response;
}

AppendEntriesResponse DistributedRegionNode::HandleAppendEntries(
    const AppendEntriesRequest& request) {
  AppendEntriesResponse response;
  if (raft_ == nullptr) {
    return response;
  }
  const LogIndex first_new_index = request.entries.empty() ? 0 : request.entries.front().index;
  response = raft_->HandleAppendEntries(request);
  if (!response.success) {
    (void)PersistHardState();
    return response;
  }
  if (first_new_index != 0) {
    (void)PersistLogFrom(first_new_index);
  }
  (void)ApplyCommitted();
  (void)PersistHardState();
  return response;
}

Status DistributedRegionNode::EnsureLeader() {
  if (raft_ == nullptr) {
    return Status::InvalidArgument("raft node is not initialized");
  }
  if (raft_->role() == RaftRole::kLeader) {
    return Status::Ok();
  }
  if (local_peer_.peer_id != region_.leader_peer_id) {
    return {ErrorCode::kNotLeader, "request must be sent to region leader"};
  }

  raft_->BecomeCandidate();
  auto status = PersistHardState();
  if (!status.ok()) {
    return status;
  }

  RequestVoteRequest request;
  request.region_id = region_.region_id;
  request.term = raft_->current_term();
  request.candidate_id = local_peer_.peer_id;
  request.last_log_index = raft_->last_log_index();
  request.last_log_term = raft_->last_log_term();

  int votes = 1;
  for (const auto& peer : region_.peers) {
    if (peer.peer_id == local_peer_.peer_id) {
      continue;
    }
    const StoreInfo* store = FindStoreByPeer(peer.peer_id);
    if (store == nullptr) {
      continue;
    }
    FieldMap fields;
    PutRequestVoteFields(&fields, request);
    const auto rpc_response = CallStore(*store, "store.RaftRequestVote", fields);
    if (!rpc_response.ok) {
      continue;
    }
    FieldMap response_fields;
    status = DecodeFields(rpc_response.payload, &response_fields);
    if (!status.ok()) {
      return status;
    }
    RequestVoteResponse vote_response;
    status = GetRequestVoteResponseFields(response_fields, &vote_response);
    if (!status.ok()) {
      return status;
    }
    if (vote_response.vote_granted) {
      ++votes;
    }
  }

  if (votes <= static_cast<int>(region_.peers.size() / 2)) {
    return Status::Internal("failed to elect distributed region leader");
  }
  raft_->BecomeLeader();
  return PersistHardState();
}

Status DistributedRegionNode::ProposeAndReplicate(const std::string& command) {
  auto status = EnsureLeader();
  if (!status.ok()) {
    return status;
  }

  LogEntry entry;
  status = raft_->Propose(EntryType::kNormal, command, &entry);
  if (!status.ok()) {
    return status;
  }
  status = raft_storage_->AppendLog(entry);
  if (!status.ok()) {
    return status;
  }

  int replicated = 1;
  for (const auto& peer : region_.peers) {
    if (peer.peer_id == local_peer_.peer_id) {
      continue;
    }
    const StoreInfo* store = FindStoreByPeer(peer.peer_id);
    if (store == nullptr) {
      continue;
    }
    bool follower_replicated = false;
    status = ReplicateToStore(*store, entry.index, raft_->commit_index(), &follower_replicated);
    if (!status.ok()) {
      return status;
    }
    if (follower_replicated) {
      ++replicated;
    }
  }

  if (replicated <= static_cast<int>(region_.peers.size() / 2)) {
    return Status::Internal("failed to replicate entry to majority");
  }

  raft_->SetCommitIndex(entry.index);
  status = ApplyCommitted();
  if (!status.ok()) {
    return status;
  }
  status = PersistHardState();
  if (!status.ok()) {
    return status;
  }
  return BroadcastCommit(entry.index);
}

Status DistributedRegionNode::BroadcastCommit(LogIndex commit_index) {
  for (const auto& peer : region_.peers) {
    if (peer.peer_id == local_peer_.peer_id) {
      continue;
    }
    const StoreInfo* store = FindStoreByPeer(peer.peer_id);
    if (store == nullptr) {
      continue;
    }
    bool replicated = false;
    (void)ReplicateToStore(*store, raft_->last_log_index() + 1, commit_index, &replicated);
  }
  return Status::Ok();
}

Status DistributedRegionNode::ReplicateToStore(const StoreInfo& store, LogIndex first_index,
                                               LogIndex leader_commit, bool* replicated) {
  if (replicated == nullptr) {
    return Status::InvalidArgument("replicated must not be null");
  }
  *replicated = false;
  if (raft_ == nullptr) {
    return Status::InvalidArgument("raft node is not initialized");
  }

  LogIndex next_index = first_index;
  for (int attempt = 0; attempt < 8; ++attempt) {
    AppendEntriesRequest request;
    request.region_id = region_.region_id;
    request.term = raft_->current_term();
    request.leader_id = local_peer_.peer_id;
    request.leader_commit = leader_commit;
    request.prev_log_index = next_index == 0 ? 0 : next_index - 1;
    request.prev_log_term =
        request.prev_log_index == 0 ? 0 : raft_->log()[request.prev_log_index - 1].term;
    for (const auto& entry : raft_->log()) {
      if (entry.index >= next_index) {
        request.entries.push_back(entry);
      }
    }

    FieldMap fields;
    PutAppendEntriesFields(&fields, request);
    const auto rpc_response = CallStore(store, "store.RaftAppendEntries", fields);
    if (!rpc_response.ok) {
      return Status::Ok();
    }
    FieldMap response_fields;
    auto status = DecodeFields(rpc_response.payload, &response_fields);
    if (!status.ok()) {
      return status;
    }
    AppendEntriesResponse append_response;
    status = GetAppendEntriesResponseFields(response_fields, &append_response);
    if (!status.ok()) {
      return status;
    }
    if (append_response.success) {
      *replicated = true;
      return Status::Ok();
    }
    if (append_response.match_index == 0) {
      next_index = 1;
    } else {
      next_index = append_response.match_index;
    }
  }
  return Status::Ok();
}

Status DistributedRegionNode::PersistHardState() {
  if (raft_storage_ == nullptr || raft_ == nullptr) {
    return Status::InvalidArgument("raft storage is not initialized");
  }
  HardState state;
  state.current_term = raft_->current_term();
  state.voted_for = raft_->voted_for().value_or(0);
  state.commit_index = raft_->commit_index();
  state.last_applied = raft_->last_applied();
  return raft_storage_->SaveHardState(region_.region_id, state);
}

Status DistributedRegionNode::PersistLogFrom(LogIndex first_index) {
  if (raft_storage_ == nullptr || raft_ == nullptr) {
    return Status::InvalidArgument("raft storage is not initialized");
  }
  auto status = raft_storage_->DeleteLogsFrom(region_.region_id, first_index);
  if (!status.ok()) {
    return status;
  }
  for (const auto& entry : raft_->log()) {
    if (entry.index >= first_index) {
      status = raft_storage_->AppendLog(entry);
      if (!status.ok()) {
        return status;
      }
    }
  }
  return Status::Ok();
}

Status DistributedRegionNode::ApplyCommitted() {
  for (const auto& entry : raft_->TakeCommittedEntries()) {
    if (entry.type == EntryType::kNoop) {
      continue;
    }
    auto status = ApplyKvCommand(&engine_, entry.command);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

RpcResponse DistributedRegionNode::CallStore(const StoreInfo& store, const std::string& method,
                                             const FieldMap& fields) {
  TcpRpcClient client(store.client_endpoint.host, store.client_endpoint.port);
  RpcRequest request;
  request.request_id = "store-raft-" + std::to_string(next_request_id_++);
  request.method = method;
  request.source = "store-" + std::to_string(local_store_.store_id);
  request.target = "store-" + std::to_string(store.store_id);
  request.payload = EncodeFields(fields);
  return client.Call(request);
}

const Peer* DistributedRegionNode::FindLocalPeer() const {
  for (const auto& peer : region_.peers) {
    if (peer.store_id == local_store_.store_id) {
      return &peer;
    }
  }
  return nullptr;
}

const StoreInfo* DistributedRegionNode::FindStoreByPeer(PeerId peer_id) const {
  const Peer* target_peer = nullptr;
  for (const auto& peer : region_.peers) {
    if (peer.peer_id == peer_id) {
      target_peer = &peer;
      break;
    }
  }
  if (target_peer == nullptr) {
    return nullptr;
  }
  for (const auto& store : stores_) {
    if (store.store_id == target_peer->store_id) {
      return &store;
    }
  }
  return nullptr;
}

void PutRequestVoteFields(FieldMap* fields, const RequestVoteRequest& request) {
  (*fields)["region_id"] = std::to_string(request.region_id);
  (*fields)["term"] = std::to_string(request.term);
  (*fields)["candidate_id"] = std::to_string(request.candidate_id);
  (*fields)["last_log_index"] = std::to_string(request.last_log_index);
  (*fields)["last_log_term"] = std::to_string(request.last_log_term);
}

Status GetRequestVoteFields(const FieldMap& fields, RequestVoteRequest* request) {
  if (request == nullptr) {
    return Status::InvalidArgument("request must not be null");
  }
  request->region_id = static_cast<RegionId>(std::stoull(fields.at("region_id")));
  request->term = static_cast<Term>(std::stoull(fields.at("term")));
  request->candidate_id = static_cast<PeerId>(std::stoull(fields.at("candidate_id")));
  request->last_log_index = static_cast<LogIndex>(std::stoull(fields.at("last_log_index")));
  request->last_log_term = static_cast<Term>(std::stoull(fields.at("last_log_term")));
  return Status::Ok();
}

void PutRequestVoteResponseFields(FieldMap* fields, const RequestVoteResponse& response) {
  (*fields)["term"] = std::to_string(response.term);
  (*fields)["vote_granted"] = response.vote_granted ? "1" : "0";
}

Status GetRequestVoteResponseFields(const FieldMap& fields, RequestVoteResponse* response) {
  if (response == nullptr) {
    return Status::InvalidArgument("response must not be null");
  }
  response->term = static_cast<Term>(std::stoull(fields.at("term")));
  response->vote_granted = GetOr(fields, "vote_granted", "0") == "1";
  return Status::Ok();
}

void PutAppendEntriesFields(FieldMap* fields, const AppendEntriesRequest& request) {
  (*fields)["region_id"] = std::to_string(request.region_id);
  (*fields)["term"] = std::to_string(request.term);
  (*fields)["leader_id"] = std::to_string(request.leader_id);
  (*fields)["prev_log_index"] = std::to_string(request.prev_log_index);
  (*fields)["prev_log_term"] = std::to_string(request.prev_log_term);
  (*fields)["leader_commit"] = std::to_string(request.leader_commit);
  (*fields)["entry_count"] = std::to_string(request.entries.size());
  for (std::size_t i = 0; i < request.entries.size(); ++i) {
    const auto prefix = "entry" + std::to_string(i) + ".";
    (*fields)[prefix + "region_id"] = std::to_string(request.entries[i].region_id);
    (*fields)[prefix + "index"] = std::to_string(request.entries[i].index);
    (*fields)[prefix + "term"] = std::to_string(request.entries[i].term);
    (*fields)[prefix + "type"] = std::to_string(EntryTypeToInt(request.entries[i].type));
    (*fields)[prefix + "command"] = HexEncode(request.entries[i].command);
  }
}

Status GetAppendEntriesFields(const FieldMap& fields, AppendEntriesRequest* request) {
  if (request == nullptr) {
    return Status::InvalidArgument("request must not be null");
  }
  request->region_id = static_cast<RegionId>(std::stoull(fields.at("region_id")));
  request->term = static_cast<Term>(std::stoull(fields.at("term")));
  request->leader_id = static_cast<PeerId>(std::stoull(fields.at("leader_id")));
  request->prev_log_index = static_cast<LogIndex>(std::stoull(fields.at("prev_log_index")));
  request->prev_log_term = static_cast<Term>(std::stoull(fields.at("prev_log_term")));
  request->leader_commit = static_cast<LogIndex>(std::stoull(fields.at("leader_commit")));
  const auto entry_count = static_cast<std::size_t>(std::stoull(fields.at("entry_count")));
  request->entries.clear();
  for (std::size_t i = 0; i < entry_count; ++i) {
    const auto prefix = "entry" + std::to_string(i) + ".";
    LogEntry entry;
    entry.region_id = static_cast<RegionId>(std::stoull(fields.at(prefix + "region_id")));
    entry.index = static_cast<LogIndex>(std::stoull(fields.at(prefix + "index")));
    entry.term = static_cast<Term>(std::stoull(fields.at(prefix + "term")));
    entry.type = EntryTypeFromInt(std::stoi(fields.at(prefix + "type")));
    auto status = HexDecode(fields.at(prefix + "command"), &entry.command);
    if (!status.ok()) {
      return status;
    }
    request->entries.push_back(entry);
  }
  return Status::Ok();
}

void PutAppendEntriesResponseFields(FieldMap* fields, const AppendEntriesResponse& response) {
  (*fields)["term"] = std::to_string(response.term);
  (*fields)["success"] = response.success ? "1" : "0";
  (*fields)["match_index"] = std::to_string(response.match_index);
}

Status GetAppendEntriesResponseFields(const FieldMap& fields, AppendEntriesResponse* response) {
  if (response == nullptr) {
    return Status::InvalidArgument("response must not be null");
  }
  response->term = static_cast<Term>(std::stoull(fields.at("term")));
  response->success = GetOr(fields, "success", "0") == "1";
  response->match_index = static_cast<LogIndex>(std::stoull(fields.at("match_index")));
  return Status::Ok();
}

}  // namespace rstone
