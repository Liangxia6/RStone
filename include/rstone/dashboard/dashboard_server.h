#pragma once

#include <atomic>
#include <string>
#include <thread>

#include "rstone/common/status.h"
#include "rstone/gateway/rpc_gateway_client.h"

namespace rstone {

class DashboardServer {
 public:
  explicit DashboardServer(RpcGatewayClient* gateway);
  ~DashboardServer();

  Status Start(const std::string& host, int port);
  void Stop();
  int bound_port() const { return bound_port_; }

 private:
  void AcceptLoop();
  void HandleClient(int client_fd);
  std::string HandleRequest(const std::string& request);
  std::string BuildStatusResponse();
  std::string ServeFile(const std::string& path, const std::string& content_type);
  std::string HttpResponse(int status_code, const std::string& status_text,
                           const std::string& content_type, const std::string& body);

  RpcGatewayClient* gateway_ = nullptr;
  std::atomic<bool> running_{false};
  int listen_fd_ = -1;
  int bound_port_ = 0;
  std::thread accept_thread_;
};

}  // namespace rstone
