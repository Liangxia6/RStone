#pragma once

#include <map>
#include <string>

#include "rstone/common/status.h"
#include "rstone/common/types.h"

namespace rstone {

using FieldMap = std::map<std::string, std::string>;

void PutStoreFields(FieldMap* fields, const StoreInfo& store, const std::string& prefix = "");
Status GetStoreFields(const FieldMap& fields, StoreInfo* store, const std::string& prefix = "");

void PutRegionFields(FieldMap* fields, const RegionInfo& region, const std::string& prefix = "");
Status GetRegionFields(const FieldMap& fields, RegionInfo* region, const std::string& prefix = "");

std::string PeerRoleName(PeerRole role);
PeerRole ParsePeerRole(const std::string& value);

}  // namespace rstone
