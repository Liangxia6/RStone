#pragma once

#include <map>
#include <optional>
#include <string>

#include "rstone/common/types.h"

namespace rstone {

struct RouteEntry {
  RegionInfo region;
  StoreInfo leader_store;
  std::int64_t last_update_ms = 0;
};

class RouteCache {
 public:
  void Put(const RouteEntry& entry);
  std::optional<RouteEntry> GetByKey(const std::string& key) const;
  void Invalidate(RegionId region_id);
  void Clear();
  std::size_t Size() const { return entries_.size(); }

 private:
  std::map<RegionId, RouteEntry> entries_;
};

}  // namespace rstone
