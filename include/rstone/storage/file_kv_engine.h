#pragma once

#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "rstone/storage/kv_engine.h"

namespace rstone {

class FileKvEngine final : public KvEngine {
 public:
  Status Open(const std::string& path) override;
  Status Put(const std::string& key, const std::string& value) override;
  Status Delete(const std::string& key) override;
  Status Get(const std::string& key, std::string* value) const override;
  Status WriteBatch(const std::vector<KvMutation>& mutations) override;
  std::vector<std::pair<std::string, std::string>> ScanPrefix(
      const std::string& prefix) const override;
  std::vector<std::pair<std::string, std::string>> ScanRange(
      const std::string& start_key, const std::string& end_key) const override;

 private:
  Status AppendMutation(const KvMutation& mutation);
  Status Replay();

  mutable std::mutex mutex_;
  std::string directory_;
  std::string log_path_;
  std::map<std::string, std::string> data_;
};

std::string HexEncode(const std::string& input);
Status HexDecode(const std::string& input, std::string* output);

}  // namespace rstone
