#pragma once

#include <string>

#include "rstone/common/status.h"
#include "rstone/rpc/rpc_message.h"

namespace rstone {

std::string EncodeRpcRequest(const RpcRequest& request);
Status DecodeRpcRequest(const std::string& encoded, RpcRequest* request);

std::string EncodeRpcResponse(const RpcResponse& response);
Status DecodeRpcResponse(const std::string& encoded, RpcResponse* response);

std::string EncodeFields(const std::map<std::string, std::string>& fields);
Status DecodeFields(const std::string& encoded, std::map<std::string, std::string>* fields);

}  // namespace rstone
