#include "rstone/gateway/rpc_gateway_client.h"

#include <chrono>
#include <memory>
#include <thread>

#include "rstone/pd/pd_service.h"
#include "rstone/rpc/tcp_rpc.h"
#include "rstone/store/store_service.h"
#include "test_assert.h"

namespace {

rstone::StoreInfo MakeTcpE2eStore(int port) {
  rstone::StoreInfo store;
  store.client_endpoint.port = port;
  return store;
}

void TestTcpGatewayEndToEnd() {
  rstone::PdServer pd;
  auto s1 = MakeTcpE2eStore(8101);
  auto s2 = MakeTcpE2eStore(8102);
  auto s3 = MakeTcpE2eStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  auto pd_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::PdService pd_service(&pd);
  RSTONE_ASSERT_TRUE(pd_service.RegisterHandlers(pd_rpc_server.get()).ok());
  rstone::TcpRpcServer pd_tcp(pd_rpc_server);
  RSTONE_ASSERT_TRUE(pd_tcp.Start("127.0.0.1", 0).ok());

  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/tcp-gateway-e2e", 3).ok());
  auto store_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService store_service(&cluster);
  RSTONE_ASSERT_TRUE(store_service.RegisterHandlers(store_rpc_server.get()).ok());
  rstone::TcpRpcServer store_tcp(store_rpc_server);
  RSTONE_ASSERT_TRUE(store_tcp.Start("127.0.0.1", 0).ok());

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  auto pd_client = std::make_shared<rstone::TcpRpcClient>("127.0.0.1", pd_tcp.bound_port());
  auto store_client =
      std::make_shared<rstone::TcpRpcClient>("127.0.0.1", store_tcp.bound_port());
  rstone::RpcGatewayClient gateway(pd_client, store_client);

  RSTONE_ASSERT_TRUE(gateway.Put("tcp:key", "tcp-value").ok());
  std::string value;
  RSTONE_ASSERT_TRUE(gateway.Get("tcp:key", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "tcp-value");

  store_tcp.Stop();
  pd_tcp.Stop();
}

}  // namespace

struct TcpGatewayE2eTestRunner {
  TcpGatewayE2eTestRunner() { TestTcpGatewayEndToEnd(); }
};

static TcpGatewayE2eTestRunner tcp_gateway_e2e_test_runner;
