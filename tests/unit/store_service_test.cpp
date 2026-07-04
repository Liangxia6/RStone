#include "rstone/store/store_service.h"

#include <memory>

#include "rstone/common/serialization.h"
#include "rstone/rpc/rpc_client.h"
#include "rstone/rpc/rpc_codec.h"
#include "test_assert.h"

namespace {

void TestStoreServiceRpc() {
  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/store-service", 3).ok());

  auto rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService service(&cluster);
  RSTONE_ASSERT_TRUE(service.RegisterHandlers(rpc_server.get()).ok());
  rstone::InProcessRpcClient client(rpc_server);

  rstone::FieldMap fields;
  fields["key"] = "rpc:key";
  fields["value"] = "rpc-value";
  rstone::RpcRequest request;
  request.request_id = "1";
  request.method = "store.KvPut";
  request.payload = rstone::EncodeFields(fields);
  auto response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);

  fields.clear();
  fields["key"] = "rpc:key";
  fields["consistency"] = "linearizable";
  request.request_id = "2";
  request.method = "store.KvGet";
  request.payload = rstone::EncodeFields(fields);
  response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
  fields.clear();
  RSTONE_ASSERT_TRUE(rstone::DecodeFields(response.payload, &fields).ok());
  RSTONE_ASSERT_EQ(fields["value"], "rpc-value");

  fields.clear();
  fields["op_count"] = "2";
  fields["op0.type"] = "put";
  fields["op0.key"] = "batch:a";
  fields["op0.value"] = "a";
  fields["op1.type"] = "put";
  fields["op1.key"] = "batch:b";
  fields["op1.value"] = "b";
  request.request_id = "3";
  request.method = "store.KvBatch";
  request.payload = rstone::EncodeFields(fields);
  response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
}

}  // namespace

struct StoreServiceTestRunner {
  StoreServiceTestRunner() { TestStoreServiceRpc(); }
};

static StoreServiceTestRunner store_service_test_runner;
