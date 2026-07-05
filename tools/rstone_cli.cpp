#include <iostream>
#include <string>

#include "rstone/common/serialization.h"
#include "rstone/rpc/rpc_codec.h"
#include "rstone/rpc/tcp_rpc.h"

namespace {

void Usage(const char* program) {
  std::cerr << "Usage:\n"
            << "  " << program << " --endpoint host:port put <key> <value>\n"
            << "  " << program << " --endpoint host:port get <key> [linearizable|eventual]\n"
            << "  " << program << " --endpoint host:port store-get <key> [linearizable|eventual]\n"
            << "  " << program << " --endpoint host:port delete <key>\n"
            << "  " << program << " --endpoint host:port split <region_id> <split_key>\n"
            << "  " << program
            << " --endpoint host:port transfer-leader <region_id> <peer_id>\n"
            << "  " << program << " --endpoint host:port add-peer <region_id> <store_id>\n"
            << "  " << program << " --endpoint host:port remove-peer <region_id> <peer_id>\n"
            << "  " << program << " --endpoint host:port status\n";
}

rstone::Endpoint ParseEndpoint(const std::string& value) {
  rstone::Endpoint endpoint;
  const auto colon = value.find(':');
  endpoint.host = colon == std::string::npos ? value : value.substr(0, colon);
  endpoint.port = colon == std::string::npos ? 18080 : std::stoi(value.substr(colon + 1));
  return endpoint;
}

rstone::RpcResponse Call(rstone::RpcClient* client, const std::string& method,
                         const rstone::FieldMap& fields) {
  rstone::RpcRequest request;
  request.request_id = "cli-1";
  request.method = method;
  request.source = "cli";
  request.target = "gateway";
  request.payload = rstone::EncodeFields(fields);
  return client->Call(request);
}

}  // namespace

int main(int argc, char** argv) {
  std::string endpoint_arg = "127.0.0.1:18080";
  int index = 1;
  if (argc > 2 && std::string(argv[1]) == "--endpoint") {
    endpoint_arg = argv[2];
    index = 3;
  }
  if (index >= argc) {
    Usage(argv[0]);
    return 2;
  }

  const auto endpoint = ParseEndpoint(endpoint_arg);
  rstone::TcpRpcClient client(endpoint.host, endpoint.port);
  const std::string command = argv[index++];
  rstone::FieldMap fields;
  rstone::RpcResponse response;

  if (command == "put") {
    if (index + 1 >= argc) {
      Usage(argv[0]);
      return 2;
    }
    fields["key"] = argv[index++];
    fields["value"] = argv[index++];
    response = Call(&client, "kv.Put", fields);
  } else if (command == "get") {
    if (index >= argc) {
      Usage(argv[0]);
      return 2;
    }
    fields["key"] = argv[index++];
    fields["consistency"] = index < argc ? argv[index++] : "linearizable";
    response = Call(&client, "kv.Get", fields);
  } else if (command == "store-get") {
    if (index >= argc) {
      Usage(argv[0]);
      return 2;
    }
    fields["key"] = argv[index++];
    fields["consistency"] = index < argc ? argv[index++] : "linearizable";
    response = Call(&client, "store.KvGet", fields);
  } else if (command == "delete") {
    if (index >= argc) {
      Usage(argv[0]);
      return 2;
    }
    fields["key"] = argv[index++];
    response = Call(&client, "kv.Delete", fields);
  } else if (command == "split") {
    if (index + 1 >= argc) {
      Usage(argv[0]);
      return 2;
    }
    fields["region_id"] = argv[index++];
    fields["split_key"] = argv[index++];
    response = Call(&client, "cluster.SplitRegion", fields);
  } else if (command == "transfer-leader") {
    if (index + 1 >= argc) {
      Usage(argv[0]);
      return 2;
    }
    fields["region_id"] = argv[index++];
    fields["target_peer_id"] = argv[index++];
    response = Call(&client, "cluster.TransferLeader", fields);
  } else if (command == "add-peer") {
    if (index + 1 >= argc) {
      Usage(argv[0]);
      return 2;
    }
    fields["region_id"] = argv[index++];
    fields["store_id"] = argv[index++];
    response = Call(&client, "cluster.AddPeer", fields);
  } else if (command == "remove-peer") {
    if (index + 1 >= argc) {
      Usage(argv[0]);
      return 2;
    }
    fields["region_id"] = argv[index++];
    fields["peer_id"] = argv[index++];
    response = Call(&client, "cluster.RemovePeer", fields);
  } else if (command == "status") {
    response = Call(&client, "cluster.Status", fields);
  } else {
    Usage(argv[0]);
    return 2;
  }

  if (!response.ok) {
    std::cerr << response.error_code << ": " << response.error_message << "\n";
    return 1;
  }

  if (command == "get" || command == "store-get") {
    rstone::FieldMap response_fields;
    auto status = rstone::DecodeFields(response.payload, &response_fields);
    if (!status.ok()) {
      std::cerr << status << "\n";
      return 1;
    }
    std::cout << response_fields["value"] << "\n";
  } else if (command == "status") {
    rstone::FieldMap response_fields;
    auto status = rstone::DecodeFields(response.payload, &response_fields);
    if (!status.ok()) {
      std::cerr << status << "\n";
      return 1;
    }
    for (const auto& [key, value] : response_fields) {
      std::cout << key << "=" << value << "\n";
    }
  } else {
    std::cout << "OK\n";
  }
  return 0;
}
