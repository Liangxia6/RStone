#pragma once

#include <string>
#include <vector>

#include "rstone/common/status.h"
#include "rstone/common/types.h"
#include "rstone/gateway/gateway_server.h"

namespace rstone {

class RStoneClient {
 public:
  explicit RStoneClient(GatewayServer* gateway);

  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Batch(const std::vector<KvMutation>& mutations);
  Status Get(const std::string& key, std::string* value);
  Status Get(const std::string& key, Consistency consistency, std::string* value);

 private:
  GatewayServer* gateway_ = nullptr;
};

}  // namespace rstone
