#pragma once

#include <vector>

#include "rstone/common/status.h"
#include "rstone/common/types.h"
#include "rstone/gateway/route_cache.h"
#include "rstone/pd/pd_server.h"
#include "rstone/store/multi_region_cluster.h"
#include "rstone/store/single_region_cluster.h"

namespace rstone {

class GatewayServer {
 public:
  GatewayServer(PdServer* pd, SingleRegionCluster* cluster);
  GatewayServer(PdServer* pd, MultiRegionCluster* cluster);

  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Batch(const std::vector<KvMutation>& mutations);
  Status Get(const std::string& key, Consistency consistency, std::string* value);

  const RouteCache& route_cache() const { return route_cache_; }

 private:
  Status ResolveRoute(const std::string& key, RouteEntry* route);

  PdServer* pd_ = nullptr;
  SingleRegionCluster* cluster_ = nullptr;
  MultiRegionCluster* multi_cluster_ = nullptr;
  RouteCache route_cache_;
};

}  // namespace rstone
