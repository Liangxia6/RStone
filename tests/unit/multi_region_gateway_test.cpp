#include "rstone/gateway/gateway_server.h"

#include "test_assert.h"

namespace {

rstone::StoreInfo MakeMultiGatewayStore(int port) {
  rstone::StoreInfo store;
  store.client_endpoint.port = port;
  return store;
}

void TestMultiRegionGateway() {
  rstone::PdServer pd;
  auto s1 = MakeMultiGatewayStore(8101);
  auto s2 = MakeMultiGatewayStore(8102);
  auto s3 = MakeMultiGatewayStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());
  auto default_region = pd.GetRegionByKey("a");
  RSTONE_ASSERT_TRUE(default_region.has_value());
  rstone::RegionInfo left;
  rstone::RegionInfo right;
  RSTONE_ASSERT_TRUE(pd.SplitRegion(default_region->region_id, "m", &left, &right).ok());

  rstone::MultiRegionCluster cluster;
  RSTONE_ASSERT_TRUE(
      cluster.Bootstrap("build/test-data/multi-region-gateway", 3, pd.metadata().ListRegions())
          .ok());

  rstone::GatewayServer gateway(&pd, &cluster);
  RSTONE_ASSERT_TRUE(gateway.Put("apple", "red").ok());
  RSTONE_ASSERT_TRUE(gateway.Put("zebra", "stripe").ok());

  std::string value;
  RSTONE_ASSERT_TRUE(gateway.Get("apple", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "red");
  RSTONE_ASSERT_TRUE(gateway.Get("zebra", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "stripe");
  RSTONE_ASSERT_EQ(gateway.route_cache().Size(), static_cast<std::size_t>(2));
}

}  // namespace

struct MultiRegionGatewayTestRunner {
  MultiRegionGatewayTestRunner() { TestMultiRegionGateway(); }
};

static MultiRegionGatewayTestRunner multi_region_gateway_test_runner;
