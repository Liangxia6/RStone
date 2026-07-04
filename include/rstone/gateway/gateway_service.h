#pragma once

#include "rstone/gateway/rpc_gateway_client.h"
#include "rstone/rpc/rpc_server.h"

namespace rstone {

class GatewayService {
 public:
  explicit GatewayService(RpcGatewayClient* gateway);

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

  RpcGatewayClient* gateway_ = nullptr;
};

}  // namespace rstone
