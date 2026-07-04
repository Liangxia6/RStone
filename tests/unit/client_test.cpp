#include "rstone/client/rstone_client.h"

#include "test_assert.h"

namespace {

rstone::StoreInfo MakeClientTestStore(int port) {
  rstone::StoreInfo store;
  store.client_endpoint.port = port;
  return store;
}

void TestClientApi() {
  rstone::PdServer pd;
  auto s1 = MakeClientTestStore(8101);
  auto s2 = MakeClientTestStore(8102);
  auto s3 = MakeClientTestStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/client-cluster", 3).ok());
  rstone::GatewayServer gateway(&pd, &cluster);
  rstone::RStoneClient client(&gateway);

  RSTONE_ASSERT_TRUE(client.Put("client:key", "value").ok());
  std::string value;
  RSTONE_ASSERT_TRUE(client.Get("client:key", &value).ok());
  RSTONE_ASSERT_EQ(value, "value");

  std::vector<rstone::KvMutation> batch;
  batch.push_back({rstone::KvMutationType::kPut, "client:a", "1"});
  batch.push_back({rstone::KvMutationType::kPut, "client:b", "2"});
  RSTONE_ASSERT_TRUE(client.Batch(batch).ok());
  RSTONE_ASSERT_TRUE(client.Get("client:a", &value).ok());
  RSTONE_ASSERT_EQ(value, "1");

  RSTONE_ASSERT_TRUE(client.Delete("client:key").ok());
  RSTONE_ASSERT_TRUE(!client.Get("client:key", &value).ok());
}

}  // namespace

struct ClientTestRunner {
  ClientTestRunner() { TestClientApi(); }
};

static ClientTestRunner client_test_runner;
