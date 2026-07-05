#pragma once

#include <cstdint>
#include <string>

#include "rstone/common/serialization.h"

namespace rstone {

std::string JsonEscape(const std::string& value);

std::string BuildDashboardStatusJson(const FieldMap& fields, bool ok,
                                     const std::string& error_message,
                                     std::int64_t refresh_cost_ms,
                                     std::int64_t timestamp_ms);

}  // namespace rstone
