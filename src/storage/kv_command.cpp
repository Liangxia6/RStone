#include "rstone/storage/kv_command.h"

#include <sstream>

#include "rstone/storage/file_kv_engine.h"

namespace rstone {

std::string EncodePutCommand(const std::string& key, const std::string& value) {
  return "PUT " + HexEncode(key) + " " + HexEncode(value);
}

std::string EncodeDeleteCommand(const std::string& key) {
  return "DELETE " + HexEncode(key);
}

std::string EncodeBatchCommand(const std::vector<KvMutation>& mutations) {
  std::ostringstream output;
  output << "BATCH " << mutations.size();
  for (const auto& mutation : mutations) {
    const std::string value_hex = mutation.value.empty() ? "-" : HexEncode(mutation.value);
    output << ' ' << (mutation.type == KvMutationType::kPut ? "P" : "D") << ' '
           << HexEncode(mutation.key) << ' ' << value_hex;
  }
  return output.str();
}

Status ApplyKvCommand(KvEngine* engine, const std::string& command) {
  if (engine == nullptr) {
    return Status::InvalidArgument("engine must not be null");
  }

  std::istringstream input(command);
  std::string op;
  input >> op;
  if (op == "PUT") {
    std::string key_hex;
    std::string value_hex;
    input >> key_hex >> value_hex;
    std::string key;
    std::string value;
    auto status = HexDecode(key_hex, &key);
    if (!status.ok()) {
      return status;
    }
    status = HexDecode(value_hex, &value);
    if (!status.ok()) {
      return status;
    }
    return engine->Put(key, value);
  }

  if (op == "DELETE") {
    std::string key_hex;
    input >> key_hex;
    std::string key;
    auto status = HexDecode(key_hex, &key);
    if (!status.ok()) {
      return status;
    }
    return engine->Delete(key);
  }

  if (op == "BATCH") {
    std::size_t count = 0;
    input >> count;
    std::vector<KvMutation> mutations;
    mutations.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      std::string mutation_type;
      std::string key_hex;
      std::string value_hex;
      input >> mutation_type >> key_hex >> value_hex;
      std::string key;
      std::string value;
      auto status = HexDecode(key_hex, &key);
      if (!status.ok()) {
        return status;
      }
      if (value_hex != "-") {
        status = HexDecode(value_hex, &value);
        if (!status.ok()) {
          return status;
        }
      }
      mutations.push_back(KvMutation{mutation_type == "P" ? KvMutationType::kPut
                                                           : KvMutationType::kDelete,
                                     key, value});
    }
    return engine->WriteBatch(mutations);
  }

  return Status::InvalidArgument("unknown kv command: " + op);
}

}  // namespace rstone
