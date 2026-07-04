#include "rstone/gateway/gateway_service.h"

#include <memory>

#include "rstone/common/serialization.h"
#include "rstone/pd/pd_service.h"
#include "rstone/rpc/rpc_client.h"
#include "rstone/rpc/rpc_codec.h"
#include "rstone/store/multi_region_cluster.h"
#include "rstone/store/store_service.h"
#include "test_assert.h"

namespace {

rstone::StoreInfo MakeGatewayServiceStore(int port) {
  rstone::StoreInfo store;
  store.client_endpoint.port = port;
  return store;
}

void TestGatewayServiceRpc() {
  rstone::PdServer pd;
  auto s1 = MakeGatewayServiceStore(8101);
  auto s2 = MakeGatewayServiceStore(8102);
  auto s3 = MakeGatewayServiceStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  auto pd_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::PdService pd_service(&pd);
  RSTONE_ASSERT_TRUE(pd_service.RegisterHandlers(pd_rpc_server.get()).ok());

  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/gateway-service", 3).ok());
  auto store_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService store_service(&cluster);
  RSTONE_ASSERT_TRUE(store_service.RegisterHandlers(store_rpc_server.get()).ok());

  auto pd_client = std::make_shared<rstone::InProcessRpcClient>(pd_rpc_server);
  auto store_client = std::make_shared<rstone::InProcessRpcClient>(store_rpc_server);
  rstone::RpcGatewayClient rpc_gateway(pd_client, store_client);

  auto gateway_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::GatewayService gateway_service(&rpc_gateway);
  RSTONE_ASSERT_TRUE(gateway_service.RegisterHandlers(gateway_rpc_server.get()).ok());
  rstone::InProcessRpcClient client(gateway_rpc_server);

  rstone::FieldMap fields;
  fields["key"] = "gateway-service:key";
  fields["value"] = "value";
  rstone::RpcRequest request;
  request.request_id = "1";
  request.method = "kv.Put";
  request.payload = rstone::EncodeFields(fields);
  auto response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);

  fields.clear();
  fields["key"] = "gateway-service:key";
  request.request_id = "2";
  request.method = "kv.Get";
  request.payload = rstone::EncodeFields(fields);
  response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
  fields.clear();
  RSTONE_ASSERT_TRUE(rstone::DecodeFields(response.payload, &fields).ok());
  RSTONE_ASSERT_EQ(fields["value"], "value");
}

void TestGatewayServiceSplitRpc() {
  rstone::PdServer pd;
  auto s1 = MakeGatewayServiceStore(8101);
  auto s2 = MakeGatewayServiceStore(8102);
  auto s3 = MakeGatewayServiceStore(8103);
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
      cluster.Bootstrap("build/test-data/gateway-service-split", 3, {default_region}).ok());
  auto store_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService store_service(&cluster);
  RSTONE_ASSERT_TRUE(store_service.RegisterHandlers(store_rpc_server.get()).ok());

  auto pd_client = std::make_shared<rstone::InProcessRpcClient>(pd_rpc_server);
  auto store_client = std::make_shared<rstone::InProcessRpcClient>(store_rpc_server);
  rstone::RpcGatewayClient rpc_gateway(pd_client, store_client);
  auto gateway_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::GatewayService gateway_service(&rpc_gateway);
  RSTONE_ASSERT_TRUE(gateway_service.RegisterHandlers(gateway_rpc_server.get()).ok());
  rstone::InProcessRpcClient client(gateway_rpc_server);

  rstone::FieldMap fields;
  fields["key"] = "apple";
  fields["value"] = "red";
  rstone::RpcRequest request;
  request.request_id = "split-put-left";
  request.method = "kv.Put";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);
  fields["key"] = "zebra";
  fields["value"] = "stripe";
  request.request_id = "split-put-right";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);

  fields.clear();
  fields["region_id"] = "1";
  fields["split_key"] = "m";
  request.request_id = "split";
  request.method = "cluster.SplitRegion";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);

  fields.clear();
  fields["key"] = "zebra";
  request.request_id = "split-get-right";
  request.method = "kv.Get";
  request.payload = rstone::EncodeFields(fields);
  auto response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
  fields.clear();
  RSTONE_ASSERT_TRUE(rstone::DecodeFields(response.payload, &fields).ok());
  RSTONE_ASSERT_EQ(fields["value"], "stripe");
}

void TestGatewayServiceTransferLeaderRpc() {
  rstone::PdServer pd;
  auto s1 = MakeGatewayServiceStore(8101);
  auto s2 = MakeGatewayServiceStore(8102);
  auto s3 = MakeGatewayServiceStore(8103);
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
      cluster.Bootstrap("build/test-data/gateway-service-transfer", 3, {default_region}).ok());
  auto store_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService store_service(&cluster);
  RSTONE_ASSERT_TRUE(store_service.RegisterHandlers(store_rpc_server.get()).ok());

  auto pd_client = std::make_shared<rstone::InProcessRpcClient>(pd_rpc_server);
  auto store_client = std::make_shared<rstone::InProcessRpcClient>(store_rpc_server);
  rstone::RpcGatewayClient rpc_gateway(pd_client, store_client);
  auto gateway_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::GatewayService gateway_service(&rpc_gateway);
  RSTONE_ASSERT_TRUE(gateway_service.RegisterHandlers(gateway_rpc_server.get()).ok());
  rstone::InProcessRpcClient client(gateway_rpc_server);

  rstone::FieldMap fields;
  fields["region_id"] = "1";
  fields["target_peer_id"] = "2";
  rstone::RpcRequest request;
  request.request_id = "transfer";
  request.method = "cluster.TransferLeader";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);

  auto* region_cluster = cluster.FindRegionCluster(1);
  RSTONE_ASSERT_TRUE(region_cluster != nullptr);
  RSTONE_ASSERT_TRUE(region_cluster->Leader() != nullptr);
  RSTONE_ASSERT_EQ(region_cluster->Leader()->peer_id(), static_cast<rstone::PeerId>(2));

  fields.clear();
  fields["key"] = "transfer-rpc:key";
  fields["value"] = "ok";
  request.request_id = "transfer-put";
  request.method = "kv.Put";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);
  std::string value;
  RSTONE_ASSERT_TRUE(region_cluster->GetFromStore(1, "transfer-rpc:key", &value).ok());
  RSTONE_ASSERT_EQ(value, "ok");
}

void TestGatewayServiceAddRemovePeerRpc() {
  rstone::PdServer pd;
  auto s1 = MakeGatewayServiceStore(8101);
  auto s2 = MakeGatewayServiceStore(8102);
  auto s3 = MakeGatewayServiceStore(8103);
  auto s4 = MakeGatewayServiceStore(8104);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s4).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  auto pd_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::PdService pd_service(&pd);
  RSTONE_ASSERT_TRUE(pd_service.RegisterHandlers(pd_rpc_server.get()).ok());

  rstone::RegionInfo default_region;
  default_region.region_id = 1;
  rstone::MultiRegionCluster cluster;
  RSTONE_ASSERT_TRUE(
      cluster.Bootstrap("build/test-data/gateway-service-membership", 3, {default_region}).ok());
  auto store_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService store_service(&cluster);
  RSTONE_ASSERT_TRUE(store_service.RegisterHandlers(store_rpc_server.get()).ok());

  auto pd_client = std::make_shared<rstone::InProcessRpcClient>(pd_rpc_server);
  auto store_client = std::make_shared<rstone::InProcessRpcClient>(store_rpc_server);
  rstone::RpcGatewayClient rpc_gateway(pd_client, store_client);
  auto gateway_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::GatewayService gateway_service(&rpc_gateway);
  RSTONE_ASSERT_TRUE(gateway_service.RegisterHandlers(gateway_rpc_server.get()).ok());
  rstone::InProcessRpcClient client(gateway_rpc_server);

  rstone::FieldMap fields;
  fields["key"] = "membership:key";
  fields["value"] = "before";
  rstone::RpcRequest request;
  request.request_id = "membership-put";
  request.method = "kv.Put";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);

  fields.clear();
  fields["region_id"] = "1";
  fields["store_id"] = std::to_string(s4.store_id);
  request.request_id = "membership-add";
  request.method = "cluster.AddPeer";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);
  auto* region_cluster = cluster.FindRegionCluster(1);
  RSTONE_ASSERT_TRUE(region_cluster != nullptr);
  std::string value;
  RSTONE_ASSERT_TRUE(region_cluster->GetFromStore(s4.store_id, "membership:key", &value).ok());
  RSTONE_ASSERT_EQ(value, "before");

  fields.clear();
  fields["region_id"] = "1";
  fields["peer_id"] = "2";
  request.request_id = "membership-remove";
  request.method = "cluster.RemovePeer";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);

  fields.clear();
  fields["key"] = "membership:after";
  fields["value"] = "after";
  request.request_id = "membership-put-after";
  request.method = "kv.Put";
  request.payload = rstone::EncodeFields(fields);
  RSTONE_ASSERT_TRUE(client.Call(request).ok);
  RSTONE_ASSERT_TRUE(region_cluster->GetFromStore(s4.store_id, "membership:after", &value).ok());
  RSTONE_ASSERT_EQ(value, "after");
}

void TestGatewayServiceStatusRpc() {
  rstone::PdServer pd;
  auto s1 = MakeGatewayServiceStore(8101);
  auto s2 = MakeGatewayServiceStore(8102);
  auto s3 = MakeGatewayServiceStore(8103);
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
      cluster.Bootstrap("build/test-data/gateway-service-status", 3, {default_region}).ok());
  auto store_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService store_service(&cluster);
  RSTONE_ASSERT_TRUE(store_service.RegisterHandlers(store_rpc_server.get()).ok());

  auto pd_client = std::make_shared<rstone::InProcessRpcClient>(pd_rpc_server);
  auto store_client = std::make_shared<rstone::InProcessRpcClient>(store_rpc_server);
  rstone::RpcGatewayClient rpc_gateway(pd_client, store_client);
  auto gateway_rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::GatewayService gateway_service(&rpc_gateway);
  RSTONE_ASSERT_TRUE(gateway_service.RegisterHandlers(gateway_rpc_server.get()).ok());
  rstone::InProcessRpcClient client(gateway_rpc_server);

  rstone::RpcRequest request;
  request.request_id = "status";
  request.method = "cluster.Status";
  auto response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
  rstone::FieldMap fields;
  RSTONE_ASSERT_TRUE(rstone::DecodeFields(response.payload, &fields).ok());
  RSTONE_ASSERT_EQ(fields["pd.store_count"], "3");
  RSTONE_ASSERT_EQ(fields["pd.region_count"], "1");
  RSTONE_ASSERT_EQ(fields["store.region_count"], "1");
  RSTONE_ASSERT_EQ(fields["gateway.route_cache_size"], "0");
}

}  // namespace

struct GatewayServiceTestRunner {
  GatewayServiceTestRunner() {
    TestGatewayServiceRpc();
    TestGatewayServiceSplitRpc();
    TestGatewayServiceTransferLeaderRpc();
    TestGatewayServiceAddRemovePeerRpc();
    TestGatewayServiceStatusRpc();
  }
};

static GatewayServiceTestRunner gateway_service_test_runner;
