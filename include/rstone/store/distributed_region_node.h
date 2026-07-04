#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rstone/common/serialization.h"
#include "rstone/raft/raft_node.h"
#include "rstone/raft/raft_storage.h"
#include "rstone/rpc/rpc_client.h"
#include "rstone/storage/file_kv_engine.h"

namespace rstone {

class DistributedRegionNode {
 public:
  Status Bootstrap(const std::string& data_dir, const StoreInfo& local_store,
                   const std::vector<StoreInfo>& stores, const RegionInfo& region);

  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Batch(const std::vector<KvMutation>& mutations);
  Status Get(const std::string& key, Consistency consistency, std::string* value) const;
  Status TransferLeader(PeerId target_peer_id);

  RequestVoteResponse HandleRequestVote(const RequestVoteRequest& request);
  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request);

  StoreId local_store_id() const { return local_store_.store_id; }
  PeerId local_peer_id() const { return local_peer_.peer_id; }
  const RegionInfo& region() const { return region_; }
  const RaftNode* raft() const { return raft_.get(); }

 private:
  Status EnsureLeader();
  Status ProposeAndReplicate(const std::string& command);
  Status BroadcastCommit(LogIndex commit_index);
  Status ReplicateToStore(const StoreInfo& store, LogIndex first_index,
                          LogIndex leader_commit, bool* replicated);
  Status PersistHardState();
  Status PersistLogFrom(LogIndex first_index);
  Status ApplyCommitted();
  RpcResponse CallStore(const StoreInfo& store, const std::string& method,
                        const FieldMap& fields);
  const Peer* FindLocalPeer() const;
  const StoreInfo* FindStoreByPeer(PeerId peer_id) const;

  StoreInfo local_store_;
  std::vector<StoreInfo> stores_;
  RegionInfo region_;
  Peer local_peer_;
  std::unique_ptr<RaftNode> raft_;
  FileKvEngine engine_;
  std::unique_ptr<RaftStorage> raft_storage_;
  std::uint64_t next_request_id_ = 1;
};

void PutRequestVoteFields(FieldMap* fields, const RequestVoteRequest& request);
Status GetRequestVoteFields(const FieldMap& fields, RequestVoteRequest* request);
void PutRequestVoteResponseFields(FieldMap* fields, const RequestVoteResponse& response);
Status GetRequestVoteResponseFields(const FieldMap& fields, RequestVoteResponse* response);

void PutAppendEntriesFields(FieldMap* fields, const AppendEntriesRequest& request);
Status GetAppendEntriesFields(const FieldMap& fields, AppendEntriesRequest* request);
void PutAppendEntriesResponseFields(FieldMap* fields, const AppendEntriesResponse& response);
Status GetAppendEntriesResponseFields(const FieldMap& fields, AppendEntriesResponse* response);

}  // namespace rstone
