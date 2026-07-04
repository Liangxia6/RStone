#pragma once

#include <memory>
#include <vector>

#include "rstone/common/status.h"
#include "rstone/common/serialization.h"
#include "rstone/common/types.h"
#include "rstone/gateway/route_cache.h"
#include "rstone/rpc/rpc_client.h"
#include "rstone/storage/kv_engine.h"

namespace rstone {

class RpcGatewayClient {
 public:
  RpcGatewayClient(std::shared_ptr<RpcClient> pd_client,
                   std::shared_ptr<RpcClient> store_client);
  RpcGatewayClient(std::shared_ptr<RpcClient> pd_client,
                   std::shared_ptr<RpcClient> store_client,
                   bool dynamic_store_routing);

  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Batch(const std::vector<KvMutation>& mutations);
  Status Get(const std::string& key, Consistency consistency, std::string* value);
  Status SplitRegion(RegionId region_id, const std::string& split_key);
  Status TransferLeader(RegionId region_id, PeerId target_peer_id);
  Status AddPeer(RegionId region_id, StoreId store_id);
  Status RemovePeer(RegionId region_id, PeerId peer_id);
  Status GetStatus(FieldMap* fields);

  const RouteCache& route_cache() const { return route_cache_; }

 private:
  Status ResolveRoute(const std::string& key, RouteEntry* route);
  RpcResponse CallPd(const std::string& method, const FieldMap& fields);
  RpcResponse CallStore(const std::string& method, const FieldMap& fields);
  RpcResponse CallStoreWithRouteRetry(const std::string& method, const std::string& key,
                                      FieldMap fields);
  bool IsRouteStaleError(const RpcResponse& response) const;

  std::shared_ptr<RpcClient> pd_client_;
  std::shared_ptr<RpcClient> store_client_;
  bool dynamic_store_routing_ = false;
  RouteCache route_cache_;
  std::uint64_t next_request_id_ = 1;
};

}  // namespace rstone
