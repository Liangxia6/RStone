#include "rstone/rpc/rpc_client.h"

#include <utility>

#include "rstone/common/error_code.h"

namespace rstone {

RpcResponse MakeRpcOk(const RpcRequest& request, std::string payload) {
  RpcResponse response;
  response.request_id = request.request_id;
  response.ok = true;
  response.payload = std::move(payload);
  return response;
}

RpcResponse MakeRpcError(const RpcRequest& request, std::string error_code,
                         std::string error_message) {
  RpcResponse response;
  response.request_id = request.request_id;
  response.ok = false;
  response.error_code = std::move(error_code);
  response.error_message = std::move(error_message);
  return response;
}

Status RpcServer::RegisterHandler(const std::string& method, RpcHandler handler) {
  if (method.empty()) {
    return Status::InvalidArgument("rpc method must not be empty");
  }
  if (!handler) {
    return Status::InvalidArgument("rpc handler must not be empty");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  handlers_[method] = std::move(handler);
  return Status::Ok();
}

RpcResponse RpcServer::Handle(const RpcRequest& request) const {
  RpcHandler handler;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = handlers_.find(request.method);
    if (it == handlers_.end()) {
      return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kRpcError)),
                          "method not found: " + request.method);
    }
    handler = it->second;
  }
  return handler(request);
}

InProcessRpcClient::InProcessRpcClient(std::shared_ptr<RpcServer> server)
    : server_(std::move(server)) {}

RpcResponse InProcessRpcClient::Call(const RpcRequest& request) {
  if (!server_) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kRpcError)),
                        "rpc server is not connected");
  }
  return server_->Handle(request);
}

}  // namespace rstone
