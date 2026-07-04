#include "rstone/pd/pd_service.h"

#include "rstone/common/error_code.h"
#include "rstone/common/serialization.h"
#include "rstone/rpc/rpc_codec.h"

namespace rstone {
namespace {

RpcResponse StatusToRpcError(const RpcRequest& request, const Status& status) {
  return MakeRpcError(request, std::string(ErrorCodeName(status.code())), status.message());
}

}  // namespace

PdService::PdService(PdServer* pd) : pd_(pd) {}

Status PdService::RegisterHandlers(RpcServer* server) {
  if (server == nullptr) {
    return Status::InvalidArgument("rpc server must not be null");
  }
  auto status = server->RegisterHandler("pd.RegisterStore", [this](const RpcRequest& request) {
    return HandleRegisterStore(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("pd.StoreHeartbeat", [this](const RpcRequest& request) {
    return HandleStoreHeartbeat(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("pd.BootstrapDefaultRegion",
                                   [this](const RpcRequest& request) {
                                     return HandleBootstrapDefaultRegion(request);
                                   });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("pd.SplitRegion", [this](const RpcRequest& request) {
    return HandleSplitRegion(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("pd.TransferLeader", [this](const RpcRequest& request) {
    return HandleTransferLeader(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("pd.AddPeer", [this](const RpcRequest& request) {
    return HandleAddPeer(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("pd.RemovePeer", [this](const RpcRequest& request) {
    return HandleRemovePeer(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("pd.GetRegionByKey", [this](const RpcRequest& request) {
    return HandleGetRegionByKey(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("pd.GetStore", [this](const RpcRequest& request) {
    return HandleGetStore(request);
  });
  if (!status.ok()) {
    return status;
  }
  return server->RegisterHandler("pd.Status", [this](const RpcRequest& request) {
    return HandleStatus(request);
  });
}

RpcResponse PdService::HandleRegisterStore(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  StoreInfo store;
  status = GetStoreFields(fields, &store);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = pd_->RegisterStore(&store);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  FieldMap response_fields;
  PutStoreFields(&response_fields, store);
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse PdService::HandleTransferLeader(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = pd_->TransferLeader(static_cast<RegionId>(std::stoull(fields["region_id"])),
                               static_cast<PeerId>(std::stoull(fields["target_peer_id"])));
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse PdService::HandleAddPeer(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  Peer peer;
  status = pd_->AddPeer(static_cast<RegionId>(std::stoull(fields["region_id"])),
                        static_cast<StoreId>(std::stoull(fields["store_id"])), &peer);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  FieldMap response_fields;
  response_fields["peer_id"] = std::to_string(peer.peer_id);
  response_fields["store_id"] = std::to_string(peer.store_id);
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse PdService::HandleRemovePeer(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = pd_->RemovePeer(static_cast<RegionId>(std::stoull(fields["region_id"])),
                           static_cast<PeerId>(std::stoull(fields["peer_id"])));
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse PdService::HandleStoreHeartbeat(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto store_id = static_cast<StoreId>(std::stoull(fields["store_id"]));
  const auto now_ms = static_cast<std::int64_t>(std::stoll(fields["now_ms"]));
  status = pd_->StoreHeartbeat(store_id, now_ms);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse PdService::HandleSplitRegion(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  RegionInfo left;
  RegionInfo right;
  status = pd_->SplitRegion(static_cast<RegionId>(std::stoull(fields["region_id"])),
                            fields["split_key"], &left, &right);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  FieldMap response_fields;
  PutRegionFields(&response_fields, left, "left");
  PutRegionFields(&response_fields, right, "right");
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse PdService::HandleBootstrapDefaultRegion(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto count = static_cast<std::size_t>(std::stoull(fields["store_count"]));
  std::vector<StoreId> store_ids;
  for (std::size_t i = 0; i < count; ++i) {
    store_ids.push_back(static_cast<StoreId>(std::stoull(fields["store" + std::to_string(i)])));
  }
  status = pd_->BootstrapDefaultRegion(store_ids);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse PdService::HandleGetRegionByKey(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto key = fields["key"];
  auto region = pd_->GetRegionByKey(key);
  if (!region) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kRegionNotFound)),
                        "region not found");
  }
  FieldMap response_fields;
  PutRegionFields(&response_fields, *region, "region");
  auto leader_store = pd_->GetRegionLeaderStore(*region);
  if (leader_store) {
    PutStoreFields(&response_fields, *leader_store, "leader_store");
  }
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse PdService::HandleGetStore(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto store_id = static_cast<StoreId>(std::stoull(fields["store_id"]));
  auto store = pd_->GetStore(store_id);
  if (!store) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kStoreNotFound)),
                        "store not found");
  }
  FieldMap response_fields;
  PutStoreFields(&response_fields, *store);
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse PdService::HandleStatus(const RpcRequest& request) {
  FieldMap fields;
  const auto stores = pd_->metadata().ListStores();
  const auto regions = pd_->metadata().ListRegions();
  fields["store_count"] = std::to_string(stores.size());
  fields["region_count"] = std::to_string(regions.size());
  for (std::size_t i = 0; i < stores.size(); ++i) {
    PutStoreFields(&fields, stores[i], "store" + std::to_string(i));
  }
  for (std::size_t i = 0; i < regions.size(); ++i) {
    PutRegionFields(&fields, regions[i], "region" + std::to_string(i));
  }
  return MakeRpcOk(request, EncodeFields(fields));
}

}  // namespace rstone
