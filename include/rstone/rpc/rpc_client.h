#pragma once

#include <memory>

#include "rstone/rpc/rpc_message.h"
#include "rstone/rpc/rpc_server.h"

namespace rstone {

class RpcClient {
 public:
  virtual ~RpcClient() = default;
  virtual RpcResponse Call(const RpcRequest& request) = 0;
};

class InProcessRpcClient final : public RpcClient {
 public:
  explicit InProcessRpcClient(std::shared_ptr<RpcServer> server);
  RpcResponse Call(const RpcRequest& request) override;

 private:
  std::shared_ptr<RpcServer> server_;
};

}  // namespace rstone
