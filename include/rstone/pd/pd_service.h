#pragma once

#include "rstone/pd/pd_server.h"
#include "rstone/rpc/rpc_server.h"

namespace rstone {

class PdService {
 public:
  explicit PdService(PdServer* pd);

  Status RegisterHandlers(RpcServer* server);

 private:
  RpcResponse HandleRegisterStore(const RpcRequest& request);
  RpcResponse HandleStoreHeartbeat(const RpcRequest& request);
  RpcResponse HandleBootstrapDefaultRegion(const RpcRequest& request);
  RpcResponse HandleSplitRegion(const RpcRequest& request);
  RpcResponse HandleTransferLeader(const RpcRequest& request);
  RpcResponse HandleAddPeer(const RpcRequest& request);
  RpcResponse HandleRemovePeer(const RpcRequest& request);
  RpcResponse HandleGetRegionByKey(const RpcRequest& request);
  RpcResponse HandleGetStore(const RpcRequest& request);
  RpcResponse HandleStatus(const RpcRequest& request);

  PdServer* pd_ = nullptr;
};

}  // namespace rstone
