#include "rstone/rpc/tcp_rpc.h"

#include <chrono>
#include <memory>
#include <thread>

#include "test_assert.h"

namespace {

void TestTcpRpc() {
  auto rpc_server = std::make_shared<rstone::RpcServer>();
  RSTONE_ASSERT_TRUE(rpc_server->RegisterHandler(
                                   "cluster.Ping",
                                   [](const rstone::RpcRequest& request) {
                                     return rstone::MakeRpcOk(request, "pong");
                                   })
                         .ok());

  rstone::TcpRpcServer tcp_server(rpc_server);
  RSTONE_ASSERT_TRUE(tcp_server.Start("127.0.0.1", 0).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  rstone::TcpRpcClient client("127.0.0.1", tcp_server.bound_port());
  rstone::RpcRequest request;
  request.request_id = "tcp-1";
  request.method = "cluster.Ping";
  const auto response = client.Call(request);
  RSTONE_ASSERT_TRUE(response.ok);
  RSTONE_ASSERT_EQ(response.payload, "pong");
  tcp_server.Stop();
}

}  // namespace

struct TcpRpcTestRunner {
  TcpRpcTestRunner() { TestTcpRpc(); }
};

static TcpRpcTestRunner tcp_rpc_test_runner;
