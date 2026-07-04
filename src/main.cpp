#include <iostream>
#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rstone/common/serialization.h"
#include "rstone/common/config.h"
#include "rstone/common/logging.h"
#include "rstone/gateway/gateway_service.h"
#include "rstone/gateway/gateway_server.h"
#include "rstone/gateway/rpc_gateway_client.h"
#include "rstone/pd/pd_server.h"
#include "rstone/pd/pd_service.h"
#include "rstone/rpc/rpc_codec.h"
#include "rstone/rpc/tcp_rpc.h"
#include "rstone/store/distributed_region_node.h"
#include "rstone/store/single_region_cluster.h"
#include "rstone/store/multi_region_cluster.h"
#include "rstone/store/store_service.h"

namespace {

void PrintUsage(const char* program) {
  std::cerr << "Usage: " << program
            << " --role <pd|gateway|store> --config <path> [--demo] [--serve]\n";
}

rstone::StoreInfo MakeDemoStore(int client_port) {
  rstone::StoreInfo store;
  store.client_endpoint.host = "127.0.0.1";
  store.client_endpoint.port = client_port;
  return store;
}

int RunGatewayDemo() {
  rstone::PdServer pd;
  auto s1 = MakeDemoStore(8101);
  auto s2 = MakeDemoStore(8102);
  auto s3 = MakeDemoStore(8103);
  auto status = pd.RegisterStore(&s1);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  status = pd.RegisterStore(&s2);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  status = pd.RegisterStore(&s3);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  status = pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id});
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  rstone::SingleRegionCluster cluster;
  status = cluster.Bootstrap("build/demo-cluster", 3);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  rstone::GatewayServer gateway(&pd, &cluster);
  status = gateway.Put("demo:user:1", "alice");
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  std::string value;
  status = gateway.Get("demo:user:1", rstone::Consistency::kLinearizable, &value);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  RSTONE_LOG_INFO("gateway demo read demo:user:1=" + value);
  return value == "alice" ? 0 : 1;
}

rstone::Endpoint ParseEndpoint(const std::string& value, int fallback_port) {
  rstone::Endpoint endpoint;
  const auto colon = value.find(':');
  if (colon == std::string::npos) {
    endpoint.host = value.empty() ? "127.0.0.1" : value;
    endpoint.port = fallback_port;
    return endpoint;
  }
  endpoint.host = value.substr(0, colon);
  endpoint.port = std::stoi(value.substr(colon + 1));
  return endpoint;
}

void WaitForever() {
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(24));
  }
}

int RunPdService(const rstone::Config& config) {
  auto pd = std::make_unique<rstone::PdServer>();
  auto status = pd->Open(config.GetStringOr("pd.data_dir", "./data/pd1"));
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  auto rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::PdService service(pd.get());
  status = service.RegisterHandlers(rpc_server.get());
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  const auto host = config.GetStringOr("pd.host", "127.0.0.1");
  const int port = config.GetIntOr("pd.client_port", 7000);
  rstone::TcpRpcServer tcp_server(rpc_server);
  status = tcp_server.Start(host, port);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  RSTONE_LOG_INFO("PD TCP service listening on " + host + ":" +
                  std::to_string(tcp_server.bound_port()));
  WaitForever();
  return 0;
}

rstone::RpcResponse CallRpc(rstone::RpcClient* client, const std::string& method,
                            const rstone::FieldMap& fields, const std::string& request_id) {
  rstone::RpcRequest request;
  request.request_id = request_id;
  request.method = method;
  request.source = "rstone-server";
  request.target = "remote";
  request.payload = rstone::EncodeFields(fields);
  return client->Call(request);
}

bool DecodePdStatus(const rstone::RpcResponse& response, rstone::FieldMap* fields,
                    std::vector<rstone::StoreInfo>* stores,
                    std::vector<rstone::RegionInfo>* regions) {
  if (!response.ok || fields == nullptr || stores == nullptr || regions == nullptr) {
    return false;
  }
  auto status = rstone::DecodeFields(response.payload, fields);
  if (!status.ok()) {
    return false;
  }
  stores->clear();
  regions->clear();
  const auto store_count =
      static_cast<std::size_t>(std::stoull((*fields)["store_count"]));
  for (std::size_t i = 0; i < store_count; ++i) {
    rstone::StoreInfo store;
    status = rstone::GetStoreFields(*fields, &store, "store" + std::to_string(i));
    if (!status.ok()) {
      return false;
    }
    stores->push_back(store);
  }
  const auto region_count =
      static_cast<std::size_t>(std::stoull((*fields)["region_count"]));
  for (std::size_t i = 0; i < region_count; ++i) {
    rstone::RegionInfo region;
    status = rstone::GetRegionFields(*fields, &region, "region" + std::to_string(i));
    if (!status.ok()) {
      return false;
    }
    regions->push_back(region);
  }
  return true;
}

int RunDistributedStoreService(const rstone::Config& config) {
  // 分布式模式：每个 Store 进程只注册自己，不再在一个进程里模拟三个 Store。
  const auto pd_endpoint = ParseEndpoint(config.GetStringOr("pd.endpoints", "127.0.0.1:7000"),
                                         7000);
  rstone::TcpRpcClient pd_client(pd_endpoint.host, pd_endpoint.port);

  rstone::StoreInfo local_store;
  local_store.store_id = static_cast<rstone::StoreId>(config.GetIntOr("store.id", 0));
  local_store.client_endpoint.host = config.GetStringOr("store.host", "127.0.0.1");
  local_store.client_endpoint.port = config.GetIntOr("store.client_port", 8101);
  local_store.raft_endpoint.host = config.GetStringOr("store.host", "127.0.0.1");
  local_store.raft_endpoint.port = config.GetIntOr("store.raft_port", 7101);

  rstone::FieldMap fields;
  rstone::PutStoreFields(&fields, local_store);
  rstone::RpcResponse response;
  for (int attempt = 0; attempt < 100; ++attempt) {
    // PD 刚监听时 TCP 连接可能还没完全稳定，注册 Store 做短重试。
    response = CallRpc(&pd_client, "pd.RegisterStore", fields,
                       "store-register-" + std::to_string(local_store.store_id) + "-" +
                           std::to_string(attempt));
    if (response.ok) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (!response.ok) {
    RSTONE_LOG_ERROR("failed to register store: " + response.error_message);
    return 1;
  }
  fields.clear();
  auto status = rstone::DecodeFields(response.payload, &fields);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  status = rstone::GetStoreFields(fields, &local_store);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  const int expected_store_count = config.GetIntOr("cluster.store_count", 3);
  std::vector<rstone::StoreInfo> stores;
  std::vector<rstone::RegionInfo> regions;
  for (int attempt = 0; attempt < 200; ++attempt) {
    // 等待所有 Store 注册完成后再 bootstrap Region，否则会形成不完整副本集。
    rstone::FieldMap empty;
    response = CallRpc(&pd_client, "pd.Status", empty, "store-pd-status-" + std::to_string(attempt));
    rstone::FieldMap status_fields;
    if (DecodePdStatus(response, &status_fields, &stores, &regions) &&
        static_cast<int>(stores.size()) >= expected_store_count) {
      if (regions.empty()) {
        rstone::FieldMap bootstrap_fields;
        bootstrap_fields["store_count"] = std::to_string(stores.size());
        for (std::size_t i = 0; i < stores.size(); ++i) {
          bootstrap_fields["store" + std::to_string(i)] =
              std::to_string(stores[i].store_id);
        }
        response = CallRpc(&pd_client, "pd.BootstrapDefaultRegion", bootstrap_fields,
                           "store-bootstrap-region");
        if (!response.ok) {
          RSTONE_LOG_ERROR("failed to bootstrap default region: " + response.error_message);
          return 1;
        }
      } else {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (regions.empty()) {
    rstone::FieldMap empty;
    response = CallRpc(&pd_client, "pd.Status", empty, "store-pd-status-final");
    rstone::FieldMap status_fields;
    (void)DecodePdStatus(response, &status_fields, &stores, &regions);
  }
  if (regions.empty()) {
    RSTONE_LOG_ERROR("default region was not bootstrapped");
    return 1;
  }

  auto node = std::make_unique<rstone::DistributedRegionNode>();
  // 使用 PD 中的 Region/Peer 元数据启动本地 Peer，并从本地数据目录恢复 Raft 状态。
  status = node->Bootstrap(config.GetStringOr("store.data_dir", "./data/store1"),
                           local_store, stores, regions.front());
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  auto rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService service(node.get());
  status = service.RegisterHandlers(rpc_server.get());
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  rstone::TcpRpcServer tcp_server(rpc_server);
  status = tcp_server.Start(local_store.client_endpoint.host, local_store.client_endpoint.port);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  RSTONE_LOG_INFO("Store TCP service listening on " + local_store.client_endpoint.host + ":" +
                  std::to_string(tcp_server.bound_port()));
  WaitForever();
  return 0;
}

int RunStoreService(const rstone::Config& config) {
  if (config.GetStringOr("store.mode", "") == "distributed") {
    return RunDistributedStoreService(config);
  }

  const auto pd_endpoint = ParseEndpoint(config.GetStringOr("pd.endpoints", "127.0.0.1:7000"),
                                         7000);
  rstone::TcpRpcClient pd_client(pd_endpoint.host, pd_endpoint.port);

  std::vector<rstone::StoreInfo> stores;
  for (int i = 0; i < 3; ++i) {
    rstone::StoreInfo store;
    store.client_endpoint.host = config.GetStringOr("store.host", "127.0.0.1");
    store.client_endpoint.port = config.GetIntOr("store.client_port", 8101) + i;
    rstone::FieldMap fields;
    rstone::PutStoreFields(&fields, store);
    auto response =
        CallRpc(&pd_client, "pd.RegisterStore", fields, "store-register-" + std::to_string(i));
    if (!response.ok) {
      RSTONE_LOG_ERROR("failed to register store: " + response.error_message);
      return 1;
    }
    fields.clear();
    auto status = rstone::DecodeFields(response.payload, &fields);
    if (!status.ok()) {
      RSTONE_LOG_ERROR(status.ToString());
      return 1;
    }
    status = rstone::GetStoreFields(fields, &store);
    if (!status.ok()) {
      RSTONE_LOG_ERROR(status.ToString());
      return 1;
    }
    stores.push_back(store);
  }

  rstone::FieldMap bootstrap_fields;
  bootstrap_fields["store_count"] = std::to_string(stores.size());
  for (std::size_t i = 0; i < stores.size(); ++i) {
    bootstrap_fields["store" + std::to_string(i)] = std::to_string(stores[i].store_id);
  }
  auto response =
      CallRpc(&pd_client, "pd.BootstrapDefaultRegion", bootstrap_fields, "store-bootstrap");
  if (!response.ok) {
    RSTONE_LOG_ERROR("failed to bootstrap default region: " + response.error_message);
    return 1;
  }

  rstone::RegionInfo default_region;
  default_region.region_id = 1;
  default_region.start_key = "";
  default_region.end_key = "";
  auto cluster = std::make_unique<rstone::MultiRegionCluster>();
  auto status = cluster->Bootstrap(config.GetStringOr("store.data_dir", "./data/store1"), 3,
                                   {default_region}, false);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  auto rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::StoreService service(cluster.get());
  status = service.RegisterHandlers(rpc_server.get());
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  const auto host = config.GetStringOr("store.host", "127.0.0.1");
  const int port = config.GetIntOr("store.client_port", 8101);
  rstone::TcpRpcServer tcp_server(rpc_server);
  status = tcp_server.Start(host, port);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  RSTONE_LOG_INFO("Store TCP service listening on " + host + ":" +
                  std::to_string(tcp_server.bound_port()));
  WaitForever();
  return 0;
}

int RunGatewayService(const rstone::Config& config) {
  const auto pd_endpoint = ParseEndpoint(config.GetStringOr("pd.endpoints", "127.0.0.1:7000"),
                                         7000);
  const auto store_endpoint =
      ParseEndpoint(config.GetStringOr("store.endpoint", "127.0.0.1:8101"), 8101);

  auto pd_client = std::make_shared<rstone::TcpRpcClient>(pd_endpoint.host, pd_endpoint.port);
  auto store_client =
      std::make_shared<rstone::TcpRpcClient>(store_endpoint.host, store_endpoint.port);
  const bool dynamic_store_routing =
      config.GetStringOr("gateway.store_routing", "") == "dynamic";
  auto gateway = std::make_unique<rstone::RpcGatewayClient>(pd_client, store_client,
                                                            dynamic_store_routing);

  auto rpc_server = std::make_shared<rstone::RpcServer>();
  rstone::GatewayService service(gateway.get());
  auto status = service.RegisterHandlers(rpc_server.get());
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  const auto host = config.GetStringOr("gateway.host", "127.0.0.1");
  const int port = config.GetIntOr("gateway.http_port", 8081);
  rstone::TcpRpcServer tcp_server(rpc_server);
  status = tcp_server.Start(host, port);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }
  RSTONE_LOG_INFO("Gateway TCP service listening on " + host + ":" +
                  std::to_string(tcp_server.bound_port()));
  WaitForever();
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  std::string role_arg;
  std::string config_path;
  bool demo = false;
  bool serve = false;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--role" && i + 1 < argc) {
      role_arg = argv[++i];
    } else if (arg == "--config" && i + 1 < argc) {
      config_path = argv[++i];
    } else if (arg == "--demo") {
      demo = true;
    } else if (arg == "--serve") {
      serve = true;
    } else if (arg == "--help" || arg == "-h") {
      PrintUsage(argv[0]);
      return 0;
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      PrintUsage(argv[0]);
      return 2;
    }
  }

  if (config_path.empty()) {
    PrintUsage(argv[0]);
    return 2;
  }

  rstone::Config config;
  auto status = config.LoadFromFile(config_path);
  if (!status.ok()) {
    RSTONE_LOG_ERROR(status.ToString());
    return 1;
  }

  rstone::Role role = role_arg.empty() ? config.GetRole() : rstone::ParseRole(role_arg);
  if (role == rstone::Role::kUnknown) {
    RSTONE_LOG_ERROR("unknown or missing role");
    return 2;
  }

  RSTONE_LOG_INFO("starting rstone-server role=" + rstone::RoleName(role) +
                  " config=" + config_path);

  switch (role) {
    case rstone::Role::kPd:
      RSTONE_LOG_INFO("PD service skeleton initialized");
      if (serve) {
        return RunPdService(config);
      }
      break;
    case rstone::Role::kGateway:
      RSTONE_LOG_INFO("Gateway service skeleton initialized");
      if (demo) {
        return RunGatewayDemo();
      }
      if (serve) {
        return RunGatewayService(config);
      }
      break;
    case rstone::Role::kStore:
      RSTONE_LOG_INFO("Store service skeleton initialized");
      if (serve) {
        return RunStoreService(config);
      }
      break;
    case rstone::Role::kUnknown:
      break;
  }

  return 0;
}
