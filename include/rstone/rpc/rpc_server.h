#pragma once

#include <functional>
#include <map>
#include <mutex>
#include <string>

#include "rstone/common/status.h"
#include "rstone/rpc/rpc_message.h"

namespace rstone {

using RpcHandler = std::function<RpcResponse(const RpcRequest&)>;

class RpcServer {
 public:
  Status RegisterHandler(const std::string& method, RpcHandler handler);
  RpcResponse Handle(const RpcRequest& request) const;

 private:
  mutable std::mutex mutex_;
  std::map<std::string, RpcHandler> handlers_;
};

}  // namespace rstone
