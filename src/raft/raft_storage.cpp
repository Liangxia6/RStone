#include "rstone/raft/raft_storage.h"

#include <iomanip>
#include <sstream>

#include "rstone/storage/file_kv_engine.h"

namespace rstone {
namespace {

int EntryTypeToInt(EntryType type) {
  switch (type) {
    case EntryType::kNormal:
      return 0;
    case EntryType::kConfigChange:
      return 1;
    case EntryType::kSplit:
      return 2;
    case EntryType::kNoop:
      return 3;
  }
  return 0;
}

EntryType EntryTypeFromInt(int value) {
  switch (value) {
    case 1:
      return EntryType::kConfigChange;
    case 2:
      return EntryType::kSplit;
    case 3:
      return EntryType::kNoop;
    case 0:
    default:
      return EntryType::kNormal;
  }
}

std::string SerializeLogEntry(const LogEntry& entry) {
  std::ostringstream output;
  output << entry.region_id << ' ' << entry.index << ' ' << entry.term << ' '
         << EntryTypeToInt(entry.type) << ' ' << HexEncode(entry.command);
  return output.str();
}

Status ParseLogEntry(const std::string& value, LogEntry* entry) {
  std::istringstream input(value);
  int type = 0;
  std::string command_hex;
  input >> entry->region_id >> entry->index >> entry->term >> type >> command_hex;
  if (!input) {
    return Status::InvalidArgument("invalid serialized log entry");
  }
  entry->type = EntryTypeFromInt(type);
  return HexDecode(command_hex, &entry->command);
}

}  // namespace

RaftStorage::RaftStorage(KvEngine* engine) : engine_(engine) {}

Status RaftStorage::SaveHardState(RegionId region_id, const HardState& state) {
  if (engine_ == nullptr) {
    return Status::InvalidArgument("engine must not be null");
  }
  std::vector<KvMutation> mutations;
  mutations.push_back(
      {KvMutationType::kPut, RaftMetaKey(region_id, "current_term"),
       std::to_string(state.current_term)});
  mutations.push_back(
      {KvMutationType::kPut, RaftMetaKey(region_id, "voted_for"),
       std::to_string(state.voted_for)});
  mutations.push_back(
      {KvMutationType::kPut, RaftMetaKey(region_id, "commit_index"),
       std::to_string(state.commit_index)});
  mutations.push_back(
      {KvMutationType::kPut, RaftMetaKey(region_id, "last_applied"),
       std::to_string(state.last_applied)});
  return engine_->WriteBatch(mutations);
}

Status RaftStorage::LoadHardState(RegionId region_id, HardState* state) const {
  if (engine_ == nullptr || state == nullptr) {
    return Status::InvalidArgument("engine/state must not be null");
  }
  std::string value;
  auto status = engine_->Get(RaftMetaKey(region_id, "current_term"), &value);
  if (!status.ok()) {
    return status;
  }
  state->current_term = static_cast<Term>(std::stoull(value));
  status = engine_->Get(RaftMetaKey(region_id, "voted_for"), &value);
  if (!status.ok()) {
    return status;
  }
  state->voted_for = static_cast<PeerId>(std::stoull(value));
  status = engine_->Get(RaftMetaKey(region_id, "commit_index"), &value);
  if (!status.ok()) {
    return status;
  }
  state->commit_index = static_cast<LogIndex>(std::stoull(value));
  status = engine_->Get(RaftMetaKey(region_id, "last_applied"), &value);
  if (!status.ok()) {
    return status;
  }
  state->last_applied = static_cast<LogIndex>(std::stoull(value));
  return Status::Ok();
}

Status RaftStorage::AppendLog(const LogEntry& entry) {
  if (engine_ == nullptr) {
    return Status::InvalidArgument("engine must not be null");
  }
  return engine_->Put(RaftLogKey(entry.region_id, entry.index), SerializeLogEntry(entry));
}

Status RaftStorage::LoadLog(RegionId region_id, std::vector<LogEntry>* entries) const {
  if (engine_ == nullptr || entries == nullptr) {
    return Status::InvalidArgument("engine/entries must not be null");
  }
  entries->clear();
  const auto records = engine_->ScanPrefix("raft/log/" + std::to_string(region_id) + "/");
  for (const auto& [unused_key, value] : records) {
    (void)unused_key;
    LogEntry entry;
    auto status = ParseLogEntry(value, &entry);
    if (!status.ok()) {
      return status;
    }
    entries->push_back(entry);
  }
  return Status::Ok();
}

Status RaftStorage::DeleteLogsFrom(RegionId region_id, LogIndex first_index) {
  if (engine_ == nullptr) {
    return Status::InvalidArgument("engine must not be null");
  }
  std::vector<KvMutation> mutations;
  const auto records = engine_->ScanPrefix("raft/log/" + std::to_string(region_id) + "/");
  for (const auto& [key, value] : records) {
    (void)value;
    const auto slash = key.find_last_of('/');
    if (slash == std::string::npos) {
      continue;
    }
    const auto index = static_cast<LogIndex>(std::stoull(key.substr(slash + 1)));
    if (index >= first_index) {
      mutations.push_back({KvMutationType::kDelete, key, ""});
    }
  }
  return engine_->WriteBatch(mutations);
}

std::string RaftMetaKey(RegionId region_id, const std::string& name) {
  return "raft/meta/" + std::to_string(region_id) + "/" + name;
}

std::string RaftLogKey(RegionId region_id, LogIndex index) {
  std::ostringstream output;
  output << "raft/log/" << region_id << "/" << std::setw(20) << std::setfill('0') << index;
  return output.str();
}

}  // namespace rstone
