#include "rstone/gateway/gateway_server.h"

#include "test_assert.h"

namespace {

rstone::StoreInfo MakeStore(int port) {
  rstone::StoreInfo store;
  store.client_endpoint.host = "127.0.0.1";
  store.client_endpoint.port = port;
  return store;
}

void TestGatewayPutGet() {
  rstone::PdServer pd;
  auto s1 = MakeStore(8101);
  auto s2 = MakeStore(8102);
  auto s3 = MakeStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/gateway-cluster", 3).ok());

  rstone::GatewayServer gateway(&pd, &cluster);
  RSTONE_ASSERT_TRUE(gateway.Put("user:42", "bob").ok());

  std::string value;
  RSTONE_ASSERT_TRUE(
      gateway.Get("user:42", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "bob");
  RSTONE_ASSERT_TRUE(gateway.route_cache().Size() > 0);

  RSTONE_ASSERT_TRUE(gateway.Delete("user:42").ok());
  RSTONE_ASSERT_TRUE(
      !gateway.Get("user:42", rstone::Consistency::kLinearizable, &value).ok());
}

}  // namespace

struct GatewayServerTestRunner {
  GatewayServerTestRunner() { TestGatewayPutGet(); }
};

static GatewayServerTestRunner gateway_server_test_runner;
