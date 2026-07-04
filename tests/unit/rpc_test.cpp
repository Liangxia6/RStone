#include "rstone/rpc/rpc_client.h"

#include <memory>

#include "test_assert.h"

namespace {

void TestInProcessRpc() {
  auto server = std::make_shared<rstone::RpcServer>();
  RSTONE_ASSERT_TRUE(server->RegisterHandler(
                              "cluster.Ping",
                              [](const rstone::RpcRequest& request) {
                                return rstone::MakeRpcOk(request, "pong");
                              })
                         .ok());

  rstone::InProcessRpcClient client(server);
  rstone::RpcRequest request;
  request.request_id = "1";
  request.method = "cluster.Ping";
  auto response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
  RSTONE_ASSERT_EQ(response.payload, "pong");

  request.method = "missing";
  response = client.Call(request);
  RSTONE_ASSERT_TRUE(!response.ok);
  RSTONE_ASSERT_EQ(response.error_code, "RPC_ERROR");
}

}  // namespace

struct RpcTestRunner {
  RpcTestRunner() { TestInProcessRpc(); }
};

static RpcTestRunner rpc_test_runner;
