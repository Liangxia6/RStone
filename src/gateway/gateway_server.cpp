#include "rstone/gateway/gateway_server.h"

namespace rstone {

GatewayServer::GatewayServer(PdServer* pd, SingleRegionCluster* cluster)
    : pd_(pd), cluster_(cluster) {}

GatewayServer::GatewayServer(PdServer* pd, MultiRegionCluster* cluster)
    : pd_(pd), multi_cluster_(cluster) {}

Status GatewayServer::Put(const std::string& key, const std::string& value) {
  RouteEntry route;
  auto status = ResolveRoute(key, &route);
  if (!status.ok()) {
    return status;
  }
  (void)route;
  if (multi_cluster_ != nullptr) {
    return multi_cluster_->Put(key, value);
  }
  return cluster_->Put(key, value);
}

Status GatewayServer::Delete(const std::string& key) {
  RouteEntry route;
  auto status = ResolveRoute(key, &route);
  if (!status.ok()) {
    return status;
  }
  (void)route;
  if (multi_cluster_ != nullptr) {
    return multi_cluster_->Delete(key);
  }
  return cluster_->Delete(key);
}

Status GatewayServer::Batch(const std::vector<KvMutation>& mutations) {
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
  if (multi_cluster_ != nullptr) {
    return multi_cluster_->Batch(mutations);
  }
  return cluster_->Batch(mutations);
}

Status GatewayServer::Get(const std::string& key, Consistency consistency, std::string* value) {
  RouteEntry route;
  auto status = ResolveRoute(key, &route);
  if (!status.ok()) {
    return status;
  }

  if (multi_cluster_ != nullptr) {
    return multi_cluster_->Get(key, consistency, value);
  }

  if (consistency == Consistency::kLinearizable) {
    const auto* leader = cluster_->Leader();
    if (leader == nullptr) {
      return {ErrorCode::kNotLeader, "no leader"};
    }
    return leader->Get(key, value);
  }

  if (cluster_->stores().empty()) {
    return Status::Internal("no stores");
  }
  return cluster_->stores().front()->Get(key, value);
}

Status GatewayServer::ResolveRoute(const std::string& key, RouteEntry* route) {
  if (pd_ == nullptr || (cluster_ == nullptr && multi_cluster_ == nullptr) ||
      route == nullptr) {
    return Status::InvalidArgument("gateway is not initialized");
  }

  auto cached = route_cache_.GetByKey(key);
  if (cached) {
    *route = *cached;
    return Status::Ok();
  }

  auto region = pd_->GetRegionByKey(key);
  if (!region) {
    return {ErrorCode::kRegionNotFound, "region not found for key"};
  }
  auto leader_store = pd_->GetRegionLeaderStore(*region);
  if (!leader_store) {
    return {ErrorCode::kNotLeader, "region leader store not found"};
  }

  RouteEntry entry;
  entry.region = *region;
  entry.leader_store = *leader_store;
  route_cache_.Put(entry);
  *route = entry;
  return Status::Ok();
}

}  // namespace rstone
