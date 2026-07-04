#include "rstone/pd/pd_service.h"

#include <memory>

#include "rstone/common/serialization.h"
#include "rstone/rpc/rpc_client.h"
#include "rstone/rpc/rpc_codec.h"
#include "test_assert.h"

namespace {

void TestPdServiceRpc() {
  rstone::PdServer pd;
  auto rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::PdService service(&pd);
  RSTONE_ASSERT_TRUE(service.RegisterHandlers(rpc_server.get()).ok());
  rstone::InProcessRpcClient client(rpc_server);

  rstone::StoreInfo store;
  store.client_endpoint.port = 8101;
  rstone::FieldMap fields;
  rstone::PutStoreFields(&fields, store);
  rstone::RpcRequest request;
  request.request_id = "1";
  request.method = "pd.RegisterStore";
  request.payload = rstone::EncodeFields(fields);
  auto response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
  fields.clear();
  RSTONE_ASSERT_TRUE(rstone::DecodeFields(response.payload, &fields).ok());
  RSTONE_ASSERT_EQ(fields["store_id"], "1");

  auto s2 = store;
  s2.client_endpoint.port = 8102;
  fields.clear();
  rstone::PutStoreFields(&fields, s2);
  request.request_id = "2";
  request.payload = rstone::EncodeFields(fields);
  response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);

  auto s3 = store;
  s3.client_endpoint.port = 8103;
  fields.clear();
  rstone::PutStoreFields(&fields, s3);
  request.request_id = "3";
  request.payload = rstone::EncodeFields(fields);
  response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);

  fields.clear();
  fields["store_count"] = "3";
  fields["store0"] = "1";
  fields["store1"] = "2";
  fields["store2"] = "3";
  request.request_id = "bootstrap";
  request.method = "pd.BootstrapDefaultRegion";
  request.payload = rstone::EncodeFields(fields);
  response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);

  fields.clear();
  fields["key"] = "user:1";
  request.request_id = "4";
  request.method = "pd.GetRegionByKey";
  request.payload = rstone::EncodeFields(fields);
  response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
  fields.clear();
  RSTONE_ASSERT_TRUE(rstone::DecodeFields(response.payload, &fields).ok());
  rstone::RegionInfo region;
  RSTONE_ASSERT_TRUE(rstone::GetRegionFields(fields, &region, "region").ok());
  RSTONE_ASSERT_EQ(region.region_id, static_cast<rstone::RegionId>(1));
}

}  // namespace

struct PdServiceTestRunner {
  PdServiceTestRunner() { TestPdServiceRpc(); }
};

static PdServiceTestRunner pd_service_test_runner;
