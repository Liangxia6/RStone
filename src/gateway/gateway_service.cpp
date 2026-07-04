#include "rstone/gateway/gateway_service.h"

#include "rstone/common/error_code.h"
#include "rstone/common/serialization.h"
#include "rstone/rpc/rpc_codec.h"

namespace rstone {
namespace {

RpcResponse StatusToRpcError(const RpcRequest& request, const Status& status) {
  return MakeRpcError(request, std::string(ErrorCodeName(status.code())), status.message());
}

std::string GetOr(const FieldMap& fields, const std::string& key, const std::string& fallback) {
  const auto it = fields.find(key);
  return it == fields.end() ? fallback : it->second;
}

Consistency ParseConsistency(const std::string& value) {
  return value == "eventual" ? Consistency::kEventual : Consistency::kLinearizable;
}

std::vector<KvMutation> DecodeBatchMutations(const FieldMap& fields) {
  std::vector<KvMutation> mutations;
  const auto count = static_cast<std::size_t>(std::stoull(GetOr(fields, "op_count", "0")));
  for (std::size_t i = 0; i < count; ++i) {
    const auto prefix = "op" + std::to_string(i) + ".";
    KvMutation mutation;
    mutation.type =
        GetOr(fields, prefix + "type", "put") == "delete" ? KvMutationType::kDelete
                                                           : KvMutationType::kPut;
    mutation.key = GetOr(fields, prefix + "key", "");
    mutation.value = GetOr(fields, prefix + "value", "");
    mutations.push_back(mutation);
  }
  return mutations;
}

}  // namespace

GatewayService::GatewayService(RpcGatewayClient* gateway) : gateway_(gateway) {}

Status GatewayService::RegisterHandlers(RpcServer* server) {
  if (server == nullptr) {
    return Status::InvalidArgument("rpc server must not be null");
  }
  auto status = server->RegisterHandler("kv.Put", [this](const RpcRequest& request) {
    return HandlePut(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("kv.Delete", [this](const RpcRequest& request) {
    return HandleDelete(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("kv.Get", [this](const RpcRequest& request) {
    return HandleGet(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("kv.Batch", [this](const RpcRequest& request) {
    return HandleBatch(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("cluster.SplitRegion", [this](const RpcRequest& request) {
    return HandleSplitRegion(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("cluster.TransferLeader", [this](const RpcRequest& request) {
    return HandleTransferLeader(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("cluster.AddPeer", [this](const RpcRequest& request) {
    return HandleAddPeer(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("cluster.RemovePeer", [this](const RpcRequest& request) {
    return HandleRemovePeer(request);
  });
  if (!status.ok()) {
    return status;
  }
  return server->RegisterHandler("cluster.Status", [this](const RpcRequest& request) {
    return HandleStatus(request);
  });
}

RpcResponse GatewayService::HandlePut(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = gateway_->Put(fields["key"], fields["value"]);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse GatewayService::HandleAddPeer(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = gateway_->AddPeer(static_cast<RegionId>(std::stoull(fields["region_id"])),
                             static_cast<StoreId>(std::stoull(fields["store_id"])));
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse GatewayService::HandleRemovePeer(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = gateway_->RemovePeer(static_cast<RegionId>(std::stoull(fields["region_id"])),
                                static_cast<PeerId>(std::stoull(fields["peer_id"])));
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse GatewayService::HandleStatus(const RpcRequest& request) {
  FieldMap fields;
  auto status = gateway_->GetStatus(&fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, EncodeFields(fields));
}

RpcResponse GatewayService::HandleTransferLeader(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = gateway_->TransferLeader(
      static_cast<RegionId>(std::stoull(fields["region_id"])),
      static_cast<PeerId>(std::stoull(fields["target_peer_id"])));
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse GatewayService::HandleSplitRegion(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = gateway_->SplitRegion(static_cast<RegionId>(std::stoull(fields["region_id"])),
                                 fields["split_key"]);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse GatewayService::HandleDelete(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = gateway_->Delete(fields["key"]);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse GatewayService::HandleGet(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  std::string value;
  status = gateway_->Get(fields["key"], ParseConsistency(GetOr(fields, "consistency", "")),
                         &value);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  FieldMap response_fields;
  response_fields["value"] = value;
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse GatewayService::HandleBatch(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = gateway_->Batch(DecodeBatchMutations(fields));
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

}  // namespace rstone
