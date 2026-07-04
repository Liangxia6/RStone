#pragma once

#include <string>

#include "rstone/common/types.h"
#include "rstone/storage/kv_engine.h"

namespace rstone {

struct SnapshotMeta {
  RegionId region_id = 0;
  LogIndex last_included_index = 0;
  Term last_included_term = 0;
  std::string prefix;
};

class Snapshotter {
 public:
  Status Create(const KvEngine& engine, const SnapshotMeta& meta,
                const std::string& snapshot_path) const;
  Status Restore(KvEngine* engine, const std::string& snapshot_path,
                 SnapshotMeta* meta) const;
};

}  // namespace rstone
