#include "rstone/gateway/rpc_gateway_client.h"

#include "rstone/common/serialization.h"
#include "rstone/rpc/rpc_codec.h"
#include "rstone/rpc/tcp_rpc.h"

namespace rstone {
namespace {

Status RpcToStatus(const RpcResponse& response) {
  if (response.ok) {
    return Status::Ok();
  }
  return {ErrorCode::kRpcError, response.error_code + ": " + response.error_message};
}

std::string ConsistencyName(Consistency consistency) {
  return consistency == Consistency::kEventual ? "eventual" : "linearizable";
}

}  // namespace

RpcGatewayClient::RpcGatewayClient(std::shared_ptr<RpcClient> pd_client,
                                   std::shared_ptr<RpcClient> store_client)
    : pd_client_(std::move(pd_client)), store_client_(std::move(store_client)) {}

RpcGatewayClient::RpcGatewayClient(std::shared_ptr<RpcClient> pd_client,
                                   std::shared_ptr<RpcClient> store_client,
                                   bool dynamic_store_routing)
    : pd_client_(std::move(pd_client)),
      store_client_(std::move(store_client)),
      dynamic_store_routing_(dynamic_store_routing) {}

Status RpcGatewayClient::Put(const std::string& key, const std::string& value) {
  RouteEntry route;
  // 先拿到 Region 路由，后续请求携带 region_id 让 Store 可以发现 stale route。
  auto status = ResolveRoute(key, &route);
  if (!status.ok()) {
    return status;
  }
  FieldMap fields;
  fields["key"] = key;
  fields["value"] = value;
  fields["region_id"] = std::to_string(route.region.region_id);
  return RpcToStatus(CallStoreWithRouteRetry("store.KvPut", key, fields));
}

Status RpcGatewayClient::Delete(const std::string& key) {
  RouteEntry route;
  auto status = ResolveRoute(key, &route);
  if (!status.ok()) {
    return status;
  }
  FieldMap fields;
  fields["key"] = key;
  fields["region_id"] = std::to_string(route.region.region_id);
  return RpcToStatus(CallStoreWithRouteRetry("store.KvDelete", key, fields));
}

Status RpcGatewayClient::Batch(const std::vector<KvMutation>& mutations) {
  if (mutations.empty()) {
    return Status::Ok();
  }
  RouteEntry route;
  auto status = ResolveRoute(mutations.front().key, &route);
  if (!status.ok()) {
    return status;
  }
  for (const auto& mutation : mutations) {
    RouteEntry mutation_route;
    status = ResolveRoute(mutation.key, &mutation_route);
    if (!status.ok()) {
      return status;
    }
    if (mutation_route.region.region_id != route.region.region_id) {
      return Status::InvalidArgument("batch must target a single region");
    }
  }

  FieldMap fields;
  fields["op_count"] = std::to_string(mutations.size());
  fields["region_id"] = std::to_string(route.region.region_id);
  for (std::size_t i = 0; i < mutations.size(); ++i) {
    const auto prefix = "op" + std::to_string(i) + ".";
    fields[prefix + "type"] =
        mutations[i].type == KvMutationType::kDelete ? "delete" : "put";
    fields[prefix + "key"] = mutations[i].key;
    fields[prefix + "value"] = mutations[i].value;
  }
  return RpcToStatus(CallStoreWithRouteRetry("store.KvBatch", mutations.front().key, fields));
}

Status RpcGatewayClient::Get(const std::string& key, Consistency consistency,
                             std::string* value) {
  RouteEntry route;
  auto status = ResolveRoute(key, &route);
  if (!status.ok()) {
    return status;
  }
  FieldMap fields;
  fields["key"] = key;
  fields["consistency"] = ConsistencyName(consistency);
  fields["region_id"] = std::to_string(route.region.region_id);
  const auto response = CallStoreWithRouteRetry("store.KvGet", key, fields);
  if (!response.ok) {
    return RpcToStatus(response);
  }
  FieldMap response_fields;
  status = DecodeFields(response.payload, &response_fields);
  if (!status.ok()) {
    return status;
  }
  *value = response_fields["value"];
  return Status::Ok();
}

Status RpcGatewayClient::SplitRegion(RegionId region_id, const std::string& split_key) {
  FieldMap fields;
  fields["region_id"] = std::to_string(region_id);
  fields["split_key"] = split_key;
  auto pd_response = CallPd("pd.SplitRegion", fields);
  if (!pd_response.ok) {
    return RpcToStatus(pd_response);
  }
  auto store_response = CallStore("store.SplitRegion", fields);
  if (!store_response.ok) {
    route_cache_.Clear();
    return RpcToStatus(store_response);
  }
  route_cache_.Clear();
  return Status::Ok();
}

Status RpcGatewayClient::TransferLeader(RegionId region_id, PeerId target_peer_id) {
  FieldMap fields;
  fields["region_id"] = std::to_string(region_id);
  fields["target_peer_id"] = std::to_string(target_peer_id);
  auto pd_response = CallPd("pd.TransferLeader", fields);
  if (!pd_response.ok) {
    return RpcToStatus(pd_response);
  }

  RpcResponse store_response;
  if (dynamic_store_routing_) {
    // 动态路由模式下，转主 RPC 必须发送给目标 Peer 所在 Store。
    FieldMap empty;
    auto status_response = CallPd("pd.Status", empty);
    if (!status_response.ok) {
      return RpcToStatus(status_response);
    }
    FieldMap status_fields;
    auto status = DecodeFields(status_response.payload, &status_fields);
    if (!status.ok()) {
      return status;
    }

    StoreId target_store_id = 0;
    const auto region_count = static_cast<std::size_t>(std::stoull(status_fields["region_count"]));
    for (std::size_t i = 0; i < region_count; ++i) {
      RegionInfo region;
      status = GetRegionFields(status_fields, &region, "region" + std::to_string(i));
      if (!status.ok()) {
        return status;
      }
      if (region.region_id != region_id) {
        continue;
      }
      for (const auto& peer : region.peers) {
        if (peer.peer_id == target_peer_id) {
          target_store_id = peer.store_id;
          break;
        }
      }
      break;
    }
    if (target_store_id == 0) {
      return Status::InvalidArgument("target peer store not found");
    }

    StoreInfo target_store;
    bool found_store = false;
    const auto store_count = static_cast<std::size_t>(std::stoull(status_fields["store_count"]));
    for (std::size_t i = 0; i < store_count; ++i) {
      StoreInfo store;
      status = GetStoreFields(status_fields, &store, "store" + std::to_string(i));
      if (!status.ok()) {
        return status;
      }
      if (store.store_id == target_store_id) {
        target_store = store;
        found_store = true;
        break;
      }
    }
    if (!found_store) {
      return Status::InvalidArgument("target store not found");
    }

    TcpRpcClient client(target_store.client_endpoint.host, target_store.client_endpoint.port);
    RpcRequest request;
    request.request_id = "gateway-store-" + std::to_string(next_request_id_++);
    request.method = "store.TransferLeader";
    request.source = "gateway";
    request.target = "store-" + std::to_string(target_store.store_id);
    request.payload = EncodeFields(fields);
    store_response = client.Call(request);
  } else {
    store_response = CallStore("store.TransferLeader", fields);
  }
  if (!store_response.ok) {
    route_cache_.Clear();
    return RpcToStatus(store_response);
  }
  route_cache_.Clear();
  return Status::Ok();
}

Status RpcGatewayClient::AddPeer(RegionId region_id, StoreId store_id) {
  FieldMap fields;
  fields["region_id"] = std::to_string(region_id);
  fields["store_id"] = std::to_string(store_id);
  auto pd_response = CallPd("pd.AddPeer", fields);
  if (!pd_response.ok) {
    return RpcToStatus(pd_response);
  }
  FieldMap response_fields;
  auto status = DecodeFields(pd_response.payload, &response_fields);
  if (!status.ok()) {
    return status;
  }
  fields["peer_id"] = response_fields["peer_id"];
  auto store_response = CallStore("store.AddPeer", fields);
  if (!store_response.ok) {
    route_cache_.Clear();
    return RpcToStatus(store_response);
  }
  route_cache_.Clear();
  return Status::Ok();
}

Status RpcGatewayClient::RemovePeer(RegionId region_id, PeerId peer_id) {
  FieldMap fields;
  fields["region_id"] = std::to_string(region_id);
  fields["peer_id"] = std::to_string(peer_id);
  auto pd_response = CallPd("pd.RemovePeer", fields);
  if (!pd_response.ok) {
    return RpcToStatus(pd_response);
  }
  auto store_response = CallStore("store.RemovePeer", fields);
  if (!store_response.ok) {
    route_cache_.Clear();
    return RpcToStatus(store_response);
  }
  route_cache_.Clear();
  return Status::Ok();
}

Status RpcGatewayClient::GetStatus(FieldMap* fields) {
  if (fields == nullptr) {
    return Status::InvalidArgument("fields must not be null");
  }
  fields->clear();
  (*fields)["gateway.route_cache_size"] = std::to_string(route_cache_.Size());

  FieldMap empty;
  auto pd_response = CallPd("pd.Status", empty);
  if (!pd_response.ok) {
    return RpcToStatus(pd_response);
  }
  FieldMap pd_fields;
  auto status = DecodeFields(pd_response.payload, &pd_fields);
  if (!status.ok()) {
    return status;
  }
  for (const auto& [key, value] : pd_fields) {
    (*fields)["pd." + key] = value;
  }

  auto store_response = CallStore("store.Status", empty);
  if (!store_response.ok) {
    return RpcToStatus(store_response);
  }
  FieldMap store_fields;
  status = DecodeFields(store_response.payload, &store_fields);
  if (!status.ok()) {
    return status;
  }
  for (const auto& [key, value] : store_fields) {
    (*fields)["store." + key] = value;
  }
  return Status::Ok();
}

Status RpcGatewayClient::ResolveRoute(const std::string& key, RouteEntry* route) {
  if (pd_client_ == nullptr || store_client_ == nullptr || route == nullptr) {
    return Status::InvalidArgument("gateway rpc clients are not initialized");
  }
  auto cached = route_cache_.GetByKey(key);
  if (cached) {
    *route = *cached;
    return Status::Ok();
  }

  // 缓存未命中时向 PD 查询，PD 会返回 Region 和当前 Leader Store。
  FieldMap fields;
  fields["key"] = key;
  const auto response = CallPd("pd.GetRegionByKey", fields);
  if (!response.ok) {
    return RpcToStatus(response);
  }

  FieldMap response_fields;
  auto status = DecodeFields(response.payload, &response_fields);
  if (!status.ok()) {
    return status;
  }
  RouteEntry entry;
  status = GetRegionFields(response_fields, &entry.region, "region");
  if (!status.ok()) {
    return status;
  }
  status = GetStoreFields(response_fields, &entry.leader_store, "leader_store");
  if (!status.ok()) {
    return status;
  }
  route_cache_.Put(entry);
  *route = entry;
  return Status::Ok();
}

RpcResponse RpcGatewayClient::CallPd(const std::string& method, const FieldMap& fields) {
  RpcRequest request;
  request.request_id = "gateway-pd-" + std::to_string(next_request_id_++);
  request.method = method;
  request.source = "gateway";
  request.target = "pd";
  request.payload = EncodeFields(fields);
  return pd_client_->Call(request);
}

RpcResponse RpcGatewayClient::CallStore(const std::string& method, const FieldMap& fields) {
  RpcRequest request;
  request.request_id = "gateway-store-" + std::to_string(next_request_id_++);
  request.method = method;
  request.source = "gateway";
  request.target = "store";
  request.payload = EncodeFields(fields);
  return store_client_->Call(request);
}

RpcResponse RpcGatewayClient::CallStoreWithRouteRetry(const std::string& method,
                                                      const std::string& key,
                                                      FieldMap fields) {
  auto call_for_key = [this, &method, &key](const FieldMap& request_fields) {
    if (!dynamic_store_routing_) {
      return CallStore(method, request_fields);
    }
    // 服务模式下按路由动态连接 Store；测试路径仍可使用注入的固定 RpcClient。
    RouteEntry route;
    auto status = ResolveRoute(key, &route);
    if (!status.ok()) {
      RpcRequest synthetic;
      synthetic.request_id = "gateway-route-" + std::to_string(next_request_id_++);
      return MakeRpcError(synthetic, std::string(ErrorCodeName(status.code())),
                          status.message());
    }
    TcpRpcClient client(route.leader_store.client_endpoint.host,
                        route.leader_store.client_endpoint.port);
    RpcRequest request;
    request.request_id = "gateway-store-" + std::to_string(next_request_id_++);
    request.method = method;
    request.source = "gateway";
    request.target = "store-" + std::to_string(route.leader_store.store_id);
    request.payload = EncodeFields(request_fields);
    return client.Call(request);
  };

  auto response = call_for_key(fields);
  if (!IsRouteStaleError(response)) {
    return response;
  }

  // Region split 或 leader 改变时，旧缓存会触发 stale/not leader，清空后重查 PD。
  route_cache_.Clear();
  RouteEntry refreshed;
  auto status = ResolveRoute(key, &refreshed);
  if (!status.ok()) {
    RpcRequest synthetic;
    synthetic.request_id = "gateway-refresh-" + std::to_string(next_request_id_++);
    return MakeRpcError(synthetic, std::string(ErrorCodeName(status.code())),
                        status.message());
  }
  fields["region_id"] = std::to_string(refreshed.region.region_id);
  return call_for_key(fields);
}

bool RpcGatewayClient::IsRouteStaleError(const RpcResponse& response) const {
  if (response.ok) {
    return false;
  }
  return response.error_code == "STALE_EPOCH" ||
         response.error_code == "REGION_NOT_FOUND" ||
         response.error_code == "NOT_LEADER";
}

}  // namespace rstone
