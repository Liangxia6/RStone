#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "rstone/rpc/rpc_client.h"
#include "rstone/rpc/rpc_server.h"

namespace rstone {

class TcpRpcServer {
 public:
  explicit TcpRpcServer(std::shared_ptr<RpcServer> server);
  ~TcpRpcServer();

  Status Start(const std::string& host, int port);
  void Stop();
  int bound_port() const { return bound_port_; }

 private:
  void AcceptLoop();
  void HandleClient(int client_fd);

  std::shared_ptr<RpcServer> server_;
  std::atomic<bool> running_{false};
  int listen_fd_ = -1;
  int bound_port_ = 0;
  std::thread accept_thread_;
};

class TcpRpcClient final : public RpcClient {
 public:
  TcpRpcClient(std::string host, int port);
  RpcResponse Call(const RpcRequest& request) override;

 private:
  std::string host_;
  int port_ = 0;
};

}  // namespace rstone
