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
  // 分布式 Store 的本地 Peer。每个进程只持有自己的 Peer，通过 RPC 复制给其他 Store。
  Status Bootstrap(const std::string& data_dir, const StoreInfo& local_store,
                   const std::vector<StoreInfo>& stores, const RegionInfo& region);

  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Batch(const std::vector<KvMutation>& mutations);
  Status Get(const std::string& key, Consistency consistency, std::string* value) const;
  // 手动迁移 Leader：目标 Store 收到请求后发起一轮 RequestVote。
  Status TransferLeader(PeerId target_peer_id);

  // 以下两个方法是 Store 间 Raft RPC 的本地处理入口。
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
  // 将日志后缀复制到指定 Store；失败时根据 follower 的 match_index 回退重试。
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
