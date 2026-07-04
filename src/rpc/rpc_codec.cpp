#include "rstone/rpc/rpc_codec.h"

#include <sstream>

#include "rstone/storage/file_kv_engine.h"

namespace rstone {
namespace {

void PutField(std::map<std::string, std::string>* fields, const std::string& key,
              const std::string& value) {
  (*fields)[key] = value;
}

std::string GetFieldOr(const std::map<std::string, std::string>& fields,
                       const std::string& key, const std::string& fallback) {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return fallback;
  }
  return it->second;
}

int GetIntFieldOr(const std::map<std::string, std::string>& fields, const std::string& key,
                  int fallback) {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return fallback;
  }
  try {
    return std::stoi(it->second);
  } catch (...) {
    return fallback;
  }
}

}  // namespace

std::string EncodeFields(const std::map<std::string, std::string>& fields) {
  std::ostringstream output;
  for (const auto& [key, value] : fields) {
    output << HexEncode(key) << '=' << HexEncode(value) << '\n';
  }
  return output.str();
}

Status DecodeFields(const std::string& encoded, std::map<std::string, std::string>* fields) {
  if (fields == nullptr) {
    return Status::InvalidArgument("fields must not be null");
  }
  fields->clear();
  std::istringstream input(encoded);
  std::string line;
  while (std::getline(input, line)) {
    if (line.empty()) {
      continue;
    }
    const auto equal = line.find('=');
    if (equal == std::string::npos) {
      return Status::InvalidArgument("encoded field is missing '='");
    }
    std::string key;
    std::string value;
    auto status = HexDecode(line.substr(0, equal), &key);
    if (!status.ok()) {
      return status;
    }
    status = HexDecode(line.substr(equal + 1), &value);
    if (!status.ok()) {
      return status;
    }
    (*fields)[key] = value;
  }
  return Status::Ok();
}

std::string EncodeRpcRequest(const RpcRequest& request) {
  std::map<std::string, std::string> fields = request.metadata;
  PutField(&fields, "_request_id", request.request_id);
  PutField(&fields, "_method", request.method);
  PutField(&fields, "_source", request.source);
  PutField(&fields, "_target", request.target);
  PutField(&fields, "_deadline_ms", std::to_string(request.deadline_ms));
  PutField(&fields, "_payload", request.payload);
  return EncodeFields(fields);
}

Status DecodeRpcRequest(const std::string& encoded, RpcRequest* request) {
  if (request == nullptr) {
    return Status::InvalidArgument("request must not be null");
  }
  std::map<std::string, std::string> fields;
  auto status = DecodeFields(encoded, &fields);
  if (!status.ok()) {
    return status;
  }
  request->request_id = GetFieldOr(fields, "_request_id", "");
  request->method = GetFieldOr(fields, "_method", "");
  request->source = GetFieldOr(fields, "_source", "");
  request->target = GetFieldOr(fields, "_target", "");
  request->deadline_ms = GetIntFieldOr(fields, "_deadline_ms", 3000);
  request->payload = GetFieldOr(fields, "_payload", "");
  request->metadata.clear();
  for (const auto& [key, value] : fields) {
    if (!key.empty() && key.front() != '_') {
      request->metadata[key] = value;
    }
  }
  return Status::Ok();
}

std::string EncodeRpcResponse(const RpcResponse& response) {
  std::map<std::string, std::string> fields = response.metadata;
  PutField(&fields, "_request_id", response.request_id);
  PutField(&fields, "_ok", response.ok ? "1" : "0");
  PutField(&fields, "_error_code", response.error_code);
  PutField(&fields, "_error_message", response.error_message);
  PutField(&fields, "_payload", response.payload);
  return EncodeFields(fields);
}

Status DecodeRpcResponse(const std::string& encoded, RpcResponse* response) {
  if (response == nullptr) {
    return Status::InvalidArgument("response must not be null");
  }
  std::map<std::string, std::string> fields;
  auto status = DecodeFields(encoded, &fields);
  if (!status.ok()) {
    return status;
  }
  response->request_id = GetFieldOr(fields, "_request_id", "");
  response->ok = GetFieldOr(fields, "_ok", "0") == "1";
  response->error_code = GetFieldOr(fields, "_error_code", "");
  response->error_message = GetFieldOr(fields, "_error_message", "");
  response->payload = GetFieldOr(fields, "_payload", "");
  response->metadata.clear();
  for (const auto& [key, value] : fields) {
    if (!key.empty() && key.front() != '_') {
      response->metadata[key] = value;
    }
  }
  return Status::Ok();
}

}  // namespace rstone
