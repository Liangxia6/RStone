#include "rstone/gateway/rpc_gateway_client.h"

#include <memory>

#include "rstone/pd/pd_service.h"
#include "rstone/rpc/rpc_client.h"
#include "rstone/store/store_service.h"
#include "test_assert.h"

namespace {

rstone::StoreInfo MakeRpcGatewayStore(int port) {
  rstone::StoreInfo store;
  store.client_endpoint.port = port;
  return store;
}

void TestRpcGatewayClientEndToEnd() {
  rstone::PdServer pd;
  auto s1 = MakeRpcGatewayStore(8101);
  auto s2 = MakeRpcGatewayStore(8102);
  auto s3 = MakeRpcGatewayStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  auto pd_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::PdService pd_service(&pd);
  RSTONE_ASSERT_TRUE(pd_service.RegisterHandlers(pd_rpc_server.get()).ok());

  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/rpc-gateway", 3).ok());
  auto store_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService store_service(&cluster);
  RSTONE_ASSERT_TRUE(store_service.RegisterHandlers(store_rpc_server.get()).ok());

  auto pd_client = std::make_shared<rstone::InProcessRpcClient>(pd_rpc_server);
  auto store_client = std::make_shared<rstone::InProcessRpcClient>(store_rpc_server);
  rstone::RpcGatewayClient gateway(pd_client, store_client);

  RSTONE_ASSERT_TRUE(gateway.Put("rpc-gateway:key", "value").ok());
  std::string value;
  RSTONE_ASSERT_TRUE(
      gateway.Get("rpc-gateway:key", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "value");
  RSTONE_ASSERT_TRUE(gateway.route_cache().Size() > 0);

  std::vector<rstone::KvMutation> batch;
  batch.push_back({rstone::KvMutationType::kPut, "rpc-gateway:a", "a"});
  batch.push_back({rstone::KvMutationType::kPut, "rpc-gateway:b", "b"});
  RSTONE_ASSERT_TRUE(gateway.Batch(batch).ok());
  RSTONE_ASSERT_TRUE(
      gateway.Get("rpc-gateway:a", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "a");
}

void TestRpcGatewayRefreshesStaleRoute() {
  rstone::PdServer pd;
  auto s1 = MakeRpcGatewayStore(8101);
  auto s2 = MakeRpcGatewayStore(8102);
  auto s3 = MakeRpcGatewayStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  auto pd_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::PdService pd_service(&pd);
  RSTONE_ASSERT_TRUE(pd_service.RegisterHandlers(pd_rpc_server.get()).ok());

  rstone::RegionInfo default_region;
  default_region.region_id = 1;
  rstone::MultiRegionCluster cluster;
  RSTONE_ASSERT_TRUE(
      cluster.Bootstrap("build/test-data/rpc-gateway-stale-route", 3, {default_region}).ok());
  auto store_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService store_service(&cluster);
  RSTONE_ASSERT_TRUE(store_service.RegisterHandlers(store_rpc_server.get()).ok());

  auto pd_client_a = std::make_shared<rstone::InProcessRpcClient>(pd_rpc_server);
  auto store_client_a = std::make_shared<rstone::InProcessRpcClient>(store_rpc_server);
  auto pd_client_b = std::make_shared<rstone::InProcessRpcClient>(pd_rpc_server);
  auto store_client_b = std::make_shared<rstone::InProcessRpcClient>(store_rpc_server);
  rstone::RpcGatewayClient gateway_a(pd_client_a, store_client_a);
  rstone::RpcGatewayClient gateway_b(pd_client_b, store_client_b);

  RSTONE_ASSERT_TRUE(gateway_a.Put("apple", "red").ok());
  RSTONE_ASSERT_TRUE(gateway_a.Put("zebra", "stripe").ok());
  RSTONE_ASSERT_EQ(gateway_a.route_cache().Size(), static_cast<std::size_t>(1));

  RSTONE_ASSERT_TRUE(gateway_b.SplitRegion(1, "m").ok());

  std::string value;
  RSTONE_ASSERT_TRUE(gateway_a.Get("zebra", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "stripe");
  RSTONE_ASSERT_EQ(gateway_a.route_cache().Size(), static_cast<std::size_t>(1));
}

}  // namespace

struct RpcGatewayClientTestRunner {
  RpcGatewayClientTestRunner() {
    TestRpcGatewayClientEndToEnd();
    TestRpcGatewayRefreshesStaleRoute();
  }
};

static RpcGatewayClientTestRunner rpc_gateway_client_test_runner;
