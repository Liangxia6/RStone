#include "rstone/dashboard/status_json.h"

#include <sstream>
#include <vector>

namespace rstone {
namespace {

std::string GetOr(const FieldMap& fields, const std::string& key, const std::string& fallback) {
  const auto it = fields.find(key);
  return it == fields.end() ? fallback : it->second;
}

std::size_t GetSizeOr(const FieldMap& fields, const std::string& key, std::size_t fallback) {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return fallback;
  }
  try {
    return static_cast<std::size_t>(std::stoull(it->second));
  } catch (...) {
    return fallback;
  }
}

void AppendJsonString(std::ostringstream* output, const std::string& value) {
  *output << "\"" << JsonEscape(value) << "\"";
}

void AppendJsonField(std::ostringstream* output, const std::string& name,
                     const std::string& value, bool comma = true) {
  AppendJsonString(output, name);
  *output << ":";
  AppendJsonString(output, value);
  if (comma) {
    *output << ",";
  }
}

void AppendWarningArray(std::ostringstream* output, const std::vector<std::string>& warnings) {
  *output << "\"warnings\":[";
  for (std::size_t i = 0; i < warnings.size(); ++i) {
    if (i != 0) {
      *output << ",";
    }
    AppendJsonString(output, warnings[i]);
  }
  *output << "]";
}

std::vector<std::string> BuildWarnings(const FieldMap& fields) {
  std::vector<std::string> warnings;
  const auto store_count = GetSizeOr(fields, "pd.store_count", 0);
  const auto region_count = GetSizeOr(fields, "pd.region_count", 0);
  if (store_count == 0) {
    warnings.push_back("PD has no registered Store");
  } else if (store_count < 3) {
    warnings.push_back("Store count is below the recommended replication factor");
  }
  if (region_count == 0) {
    warnings.push_back("PD has no Region metadata");
  }
  for (std::size_t i = 0; i < region_count; ++i) {
    const auto prefix = "pd.region" + std::to_string(i) + ".";
    if (GetOr(fields, prefix + "leader_peer_id", "0") == "0") {
      warnings.push_back("Region " + GetOr(fields, prefix + "region_id", "?") +
                         " has no leader");
    }
  }
  const auto runtime_region_count = GetSizeOr(fields, "store.region_count", 0);
  for (std::size_t i = 0; i < runtime_region_count; ++i) {
    const auto prefix = "store.region" + std::to_string(i) + ".";
    const auto commit = GetOr(fields, prefix + "runtime_commit_index", "");
    const auto applied = GetOr(fields, prefix + "runtime_last_applied", "");
    if (!commit.empty() && !applied.empty() && commit != applied) {
      warnings.push_back("Region " + GetOr(fields, prefix + "region_id", "?") +
                         " has unapplied committed logs");
    }
  }
  return warnings;
}

void AppendStores(std::ostringstream* output, const FieldMap& fields) {
  const auto count = GetSizeOr(fields, "pd.store_count", 0);
  *output << "\"stores\":[";
  for (std::size_t i = 0; i < count; ++i) {
    if (i != 0) {
      *output << ",";
    }
    const auto prefix = "pd.store" + std::to_string(i) + ".";
    *output << "{";
    AppendJsonField(output, "store_id", GetOr(fields, prefix + "store_id", "0"));
    AppendJsonField(output, "client_endpoint",
                    GetOr(fields, prefix + "client_host", "127.0.0.1") + ":" +
                        GetOr(fields, prefix + "client_port", "0"));
    AppendJsonField(output, "raft_endpoint",
                    GetOr(fields, prefix + "raft_host", "127.0.0.1") + ":" +
                        GetOr(fields, prefix + "raft_port", "0"));
    AppendJsonField(output, "state", GetOr(fields, prefix + "state", "Unknown"));
    AppendJsonField(output, "last_heartbeat_ms",
                    GetOr(fields, prefix + "last_heartbeat_ms", "0"), false);
    *output << "}";
  }
  *output << "]";
}

void AppendRegions(std::ostringstream* output, const FieldMap& fields) {
  const auto count = GetSizeOr(fields, "pd.region_count", 0);
  *output << "\"regions\":[";
  for (std::size_t i = 0; i < count; ++i) {
    if (i != 0) {
      *output << ",";
    }
    const auto prefix = "pd.region" + std::to_string(i) + ".";
    *output << "{";
    AppendJsonField(output, "region_id", GetOr(fields, prefix + "region_id", "0"));
    AppendJsonField(output, "start_key", GetOr(fields, prefix + "start_key", ""));
    AppendJsonField(output, "end_key", GetOr(fields, prefix + "end_key", ""));
    AppendJsonField(output, "conf_ver", GetOr(fields, prefix + "conf_ver", "1"));
    AppendJsonField(output, "version", GetOr(fields, prefix + "version", "1"));
    AppendJsonField(output, "leader_peer_id", GetOr(fields, prefix + "leader_peer_id", "0"));
    const auto peer_count = GetSizeOr(fields, prefix + "peer_count", 0);
    *output << "\"peers\":[";
    for (std::size_t peer_index = 0; peer_index < peer_count; ++peer_index) {
      if (peer_index != 0) {
        *output << ",";
      }
      const auto peer_prefix = prefix + "peer" + std::to_string(peer_index) + ".";
      *output << "{";
      AppendJsonField(output, "peer_id", GetOr(fields, peer_prefix + "peer_id", "0"));
      AppendJsonField(output, "store_id", GetOr(fields, peer_prefix + "store_id", "0"));
      AppendJsonField(output, "role", GetOr(fields, peer_prefix + "role", "Voter"), false);
      *output << "}";
    }
    *output << "]}";
  }
  *output << "]";
}

void AppendRuntimeRegions(std::ostringstream* output, const FieldMap& fields) {
  const auto count = GetSizeOr(fields, "store.region_count", 0);
  *output << "\"runtime_regions\":[";
  for (std::size_t i = 0; i < count; ++i) {
    if (i != 0) {
      *output << ",";
    }
    const auto prefix = "store.region" + std::to_string(i) + ".";
    *output << "{";
    AppendJsonField(output, "region_id", GetOr(fields, prefix + "region_id", "0"));
    AppendJsonField(output, "local_store_id", GetOr(fields, prefix + "local_store_id", ""));
    AppendJsonField(output, "local_peer_id", GetOr(fields, prefix + "local_peer_id", ""));
    AppendJsonField(output, "runtime_role", GetOr(fields, prefix + "runtime_role", ""));
    AppendJsonField(output, "runtime_leader_peer_id",
                    GetOr(fields, prefix + "runtime_leader_peer_id", ""));
    AppendJsonField(output, "runtime_peer_count",
                    GetOr(fields, prefix + "runtime_peer_count", ""));
    AppendJsonField(output, "runtime_commit_index",
                    GetOr(fields, prefix + "runtime_commit_index", ""));
    AppendJsonField(output, "runtime_last_applied",
                    GetOr(fields, prefix + "runtime_last_applied", ""));
    AppendJsonField(output, "runtime_last_log_index",
                    GetOr(fields, prefix + "runtime_last_log_index", ""), false);
    *output << "}";
  }
  *output << "]";
}

void AppendRaw(std::ostringstream* output, const FieldMap& fields) {
  *output << "\"raw\":{";
  bool first = true;
  for (const auto& [key, value] : fields) {
    if (!first) {
      *output << ",";
    }
    first = false;
    AppendJsonString(output, key);
    *output << ":";
    AppendJsonString(output, value);
  }
  *output << "}";
}

}  // namespace

std::string JsonEscape(const std::string& value) {
  std::ostringstream output;
  for (const char ch : value) {
    switch (ch) {
      case '\\':
        output << "\\\\";
        break;
      case '"':
        output << "\\\"";
        break;
      case '\n':
        output << "\\n";
        break;
      case '\r':
        output << "\\r";
        break;
      case '\t':
        output << "\\t";
        break;
      default:
        output << ch;
        break;
    }
  }
  return output.str();
}

std::string BuildDashboardStatusJson(const FieldMap& fields, bool ok,
                                     const std::string& error_message,
                                     std::int64_t refresh_cost_ms,
                                     std::int64_t timestamp_ms) {
  const auto warnings = ok ? BuildWarnings(fields) : std::vector<std::string>{error_message};
  const auto store_count = GetOr(fields, "pd.store_count", "0");
  const auto region_count = GetOr(fields, "pd.region_count", "0");
  const auto route_cache_size = GetOr(fields, "gateway.route_cache_size", "0");

  std::ostringstream output;
  output << "{";
  output << "\"ok\":" << (ok ? "true" : "false") << ",";
  output << "\"timestamp_ms\":" << timestamp_ms << ",";
  output << "\"refresh_cost_ms\":" << refresh_cost_ms << ",";
  AppendJsonField(&output, "error", ok ? "" : error_message);

  output << "\"summary\":{";
  output << "\"healthy\":" << (ok && warnings.empty() ? "true" : "false") << ",";
  AppendJsonField(&output, "store_count", store_count);
  AppendJsonField(&output, "region_count", region_count);
  AppendJsonField(&output, "route_cache_size", route_cache_size);
  AppendWarningArray(&output, warnings);
  output << "},";

  output << "\"gateway\":{";
  AppendJsonField(&output, "route_cache_size", route_cache_size, false);
  output << "},";

  output << "\"pd\":{";
  AppendJsonField(&output, "store_count", store_count);
  AppendJsonField(&output, "region_count", region_count);
  AppendStores(&output, fields);
  output << ",";
  AppendRegions(&output, fields);
  output << "},";

  output << "\"store\":{";
  AppendJsonField(&output, "region_count", GetOr(fields, "store.region_count", "0"));
  AppendRuntimeRegions(&output, fields);
  output << "},";

  AppendRaw(&output, fields);
  output << "}";
  return output.str();
}

}  // namespace rstone
