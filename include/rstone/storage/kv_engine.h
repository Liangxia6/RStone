#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "rstone/common/status.h"

namespace rstone {

enum class KvMutationType {
  kPut,
  kDelete,
};

struct KvMutation {
  KvMutationType type = KvMutationType::kPut;
  std::string key;
  std::string value;
};

class KvEngine {
 public:
  virtual ~KvEngine() = default;

  virtual Status Open(const std::string& path) = 0;
  virtual Status Put(const std::string& key, const std::string& value) = 0;
  virtual Status Delete(const std::string& key) = 0;
  virtual Status Get(const std::string& key, std::string* value) const = 0;
  virtual Status WriteBatch(const std::vector<KvMutation>& mutations) = 0;
  virtual std::vector<std::pair<std::string, std::string>> ScanPrefix(
      const std::string& prefix) const = 0;
  virtual std::vector<std::pair<std::string, std::string>> ScanRange(
      const std::string& start_key, const std::string& end_key) const = 0;
};

}  // namespace rstone
