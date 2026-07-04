#pragma once

#include <map>
#include <string>

namespace rstone {

struct RpcRequest {
  std::string request_id;
  std::string method;
  std::string source;
  std::string target;
  int deadline_ms = 3000;
  std::string payload;
  std::map<std::string, std::string> metadata;
};

struct RpcResponse {
  std::string request_id;
  bool ok = true;
  std::string error_code;
  std::string error_message;
  std::string payload;
  std::map<std::string, std::string> metadata;
};

RpcResponse MakeRpcOk(const RpcRequest& request, std::string payload);
RpcResponse MakeRpcError(const RpcRequest& request, std::string error_code,
                         std::string error_message);

}  // namespace rstone
