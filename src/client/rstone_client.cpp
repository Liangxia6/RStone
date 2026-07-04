#include "rstone/client/rstone_client.h"

namespace rstone {

RStoneClient::RStoneClient(GatewayServer* gateway) : gateway_(gateway) {}

Status RStoneClient::Put(const std::string& key, const std::string& value) {
  if (gateway_ == nullptr) {
    return Status::InvalidArgument("client has no gateway");
  }
  return gateway_->Put(key, value);
}

Status RStoneClient::Delete(const std::string& key) {
  if (gateway_ == nullptr) {
    return Status::InvalidArgument("client has no gateway");
  }
  return gateway_->Delete(key);
}

Status RStoneClient::Batch(const std::vector<KvMutation>& mutations) {
  if (gateway_ == nullptr) {
    return Status::InvalidArgument("client has no gateway");
  }
  return gateway_->Batch(mutations);
}

Status RStoneClient::Get(const std::string& key, std::string* value) {
  return Get(key, Consistency::kLinearizable, value);
}

Status RStoneClient::Get(const std::string& key, Consistency consistency, std::string* value) {
  if (gateway_ == nullptr) {
    return Status::InvalidArgument("client has no gateway");
  }
  return gateway_->Get(key, consistency, value);
}

}  // namespace rstone
