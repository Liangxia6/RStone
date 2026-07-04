#pragma once

#include <optional>
#include <vector>

#include "rstone/raft/raft_node.h"
#include "rstone/storage/kv_engine.h"

namespace rstone {

struct HardState {
  Term current_term = 0;
  PeerId voted_for = 0;
  LogIndex commit_index = 0;
  LogIndex last_applied = 0;
};

class RaftStorage {
 public:
  explicit RaftStorage(KvEngine* engine);

  Status SaveHardState(RegionId region_id, const HardState& state);
  Status LoadHardState(RegionId region_id, HardState* state) const;

  Status AppendLog(const LogEntry& entry);
  Status LoadLog(RegionId region_id, std::vector<LogEntry>* entries) const;
  Status DeleteLogsFrom(RegionId region_id, LogIndex first_index);

 private:
  KvEngine* engine_ = nullptr;
};

std::string RaftMetaKey(RegionId region_id, const std::string& name);
std::string RaftLogKey(RegionId region_id, LogIndex index);

}  // namespace rstone
