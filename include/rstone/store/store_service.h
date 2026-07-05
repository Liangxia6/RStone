#pragma once

#include "rstone/rpc/rpc_server.h"
#include "rstone/store/distributed_region_node.h"
#include "rstone/store/distributed_store_node.h"
#include "rstone/store/multi_region_cluster.h"
#include "rstone/store/single_region_cluster.h"

namespace rstone {

class StoreService {
 public:
  explicit StoreService(SingleRegionCluster* cluster);
  explicit StoreService(MultiRegionCluster* cluster);
  explicit StoreService(DistributedRegionNode* node);
  explicit StoreService(DistributedStoreNode* node);

  Status RegisterHandlers(RpcServer* server);

 private:
  RpcResponse HandlePut(const RpcRequest& request);
  RpcResponse HandleDelete(const RpcRequest& request);
  RpcResponse HandleGet(const RpcRequest& request);
  RpcResponse HandleBatch(const RpcRequest& request);
  RpcResponse HandleSplitRegion(const RpcRequest& request);
  RpcResponse HandleTransferLeader(const RpcRequest& request);
  RpcResponse HandleAddPeer(const RpcRequest& request);
  RpcResponse HandleRemovePeer(const RpcRequest& request);
  RpcResponse HandleStatus(const RpcRequest& request);
  RpcResponse HandleRaftRequestVote(const RpcRequest& request);
  RpcResponse HandleRaftAppendEntries(const RpcRequest& request);

  SingleRegionCluster* single_cluster_ = nullptr;
  MultiRegionCluster* multi_cluster_ = nullptr;
  DistributedRegionNode* distributed_node_ = nullptr;
  DistributedStoreNode* distributed_store_ = nullptr;
};

}  // namespace rstone
