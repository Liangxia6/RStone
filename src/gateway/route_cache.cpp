#include "rstone/gateway/route_cache.h"

#include "rstone/region/region.h"

namespace rstone {

void RouteCache::Put(const RouteEntry& entry) {
  entries_[entry.region.region_id] = entry;
}

std::optional<RouteEntry> RouteCache::GetByKey(const std::string& key) const {
  for (const auto& [unused_region_id, entry] : entries_) {
    (void)unused_region_id;
    if (ContainsKey(entry.region, key)) {
      return entry;
    }
  }
  return std::nullopt;
}

void RouteCache::Invalidate(RegionId region_id) {
  entries_.erase(region_id);
}

void RouteCache::Clear() {
  entries_.clear();
}

}  // namespace rstone
