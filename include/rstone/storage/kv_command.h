#pragma once

#include <string>
#include <vector>

#include "rstone/storage/kv_engine.h"

namespace rstone {

std::string EncodePutCommand(const std::string& key, const std::string& value);
std::string EncodeDeleteCommand(const std::string& key);
std::string EncodeBatchCommand(const std::vector<KvMutation>& mutations);
Status ApplyKvCommand(KvEngine* engine, const std::string& command);

}  // namespace rstone
