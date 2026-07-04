#include "rstone/storage/file_kv_engine.h"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace rstone {
namespace {

int HexValue(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return ch - 'a' + 10;
  }
  if (ch >= 'A' && ch <= 'F') {
    return ch - 'A' + 10;
  }
  return -1;
}

}  // namespace

std::string HexEncode(const std::string& input) {
  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  for (unsigned char ch : input) {
    oss << std::setw(2) << static_cast<int>(ch);
  }
  return oss.str();
}

Status HexDecode(const std::string& input, std::string* output) {
  if (input.size() % 2 != 0) {
    return Status::InvalidArgument("hex input has odd length");
  }
  std::string decoded;
  decoded.reserve(input.size() / 2);
  for (std::size_t i = 0; i < input.size(); i += 2) {
    const int hi = HexValue(input[i]);
    const int lo = HexValue(input[i + 1]);
    if (hi < 0 || lo < 0) {
      return Status::InvalidArgument("hex input contains invalid character");
    }
    decoded.push_back(static_cast<char>((hi << 4) | lo));
  }
  *output = std::move(decoded);
  return Status::Ok();
}

Status FileKvEngine::Open(const std::string& path) {
  std::lock_guard<std::mutex> lock(mutex_);
  directory_ = path;
  log_path_ = (std::filesystem::path(directory_) / "kv.log").string();

  std::error_code ec;
  std::filesystem::create_directories(directory_, ec);
  if (ec) {
    return Status::IoError("failed to create data directory: " + ec.message());
  }

  std::ofstream create(log_path_, std::ios::app);
  if (!create) {
    return Status::IoError("failed to open kv log: " + log_path_);
  }
  create.close();

  return Replay();
}

Status FileKvEngine::Put(const std::string& key, const std::string& value) {
  return WriteBatch({KvMutation{KvMutationType::kPut, key, value}});
}

Status FileKvEngine::Delete(const std::string& key) {
  return WriteBatch({KvMutation{KvMutationType::kDelete, key, ""}});
}

Status FileKvEngine::Get(const std::string& key, std::string* value) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = data_.find(key);
  if (it == data_.end()) {
    return Status::KeyNotFound("key not found: " + key);
  }
  *value = it->second;
  return Status::Ok();
}

Status FileKvEngine::WriteBatch(const std::vector<KvMutation>& mutations) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& mutation : mutations) {
    auto status = AppendMutation(mutation);
    if (!status.ok()) {
      return status;
    }
  }
  for (const auto& mutation : mutations) {
    if (mutation.type == KvMutationType::kPut) {
      data_[mutation.key] = mutation.value;
    } else {
      data_.erase(mutation.key);
    }
  }
  return Status::Ok();
}

std::vector<std::pair<std::string, std::string>> FileKvEngine::ScanPrefix(
    const std::string& prefix) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> result;
  auto it = data_.lower_bound(prefix);
  while (it != data_.end() && it->first.rfind(prefix, 0) == 0) {
    result.push_back(*it);
    ++it;
  }
  return result;
}

std::vector<std::pair<std::string, std::string>> FileKvEngine::ScanRange(
    const std::string& start_key, const std::string& end_key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> result;
  auto it = start_key.empty() ? data_.begin() : data_.lower_bound(start_key);
  while (it != data_.end() && (end_key.empty() || it->first < end_key)) {
    result.push_back(*it);
    ++it;
  }
  return result;
}

Status FileKvEngine::AppendMutation(const KvMutation& mutation) {
  std::ofstream output(log_path_, std::ios::app);
  if (!output) {
    return Status::IoError("failed to append kv log: " + log_path_);
  }
  const std::string value_hex = mutation.value.empty() ? "-" : HexEncode(mutation.value);
  output << (mutation.type == KvMutationType::kPut ? 'P' : 'D') << ' '
         << HexEncode(mutation.key) << ' ' << value_hex << '\n';
  if (!output) {
    return Status::IoError("failed to write kv log: " + log_path_);
  }
  return Status::Ok();
}

Status FileKvEngine::Replay() {
  data_.clear();
  std::ifstream input(log_path_);
  if (!input) {
    return Status::IoError("failed to replay kv log: " + log_path_);
  }

  std::string line;
  while (std::getline(input, line)) {
    if (line.empty()) {
      continue;
    }
    std::istringstream record(line);
    char op = '\0';
    std::string key_hex;
    std::string value_hex;
    record >> op >> key_hex >> value_hex;
    if (!record && value_hex.empty()) {
      value_hex = "-";
    }
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
    if (op == 'P') {
      data_[key] = value;
    } else if (op == 'D') {
      data_.erase(key);
    } else {
      return Status::InvalidArgument("invalid kv log operation");
    }
  }
  return Status::Ok();
}

}  // namespace rstone
