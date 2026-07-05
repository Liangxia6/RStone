#include "rstone/dashboard/status_json.h"

#include "test_assert.h"

namespace {

void TestStatusJsonIncludesSummaryAndStores() {
  rstone::FieldMap fields;
  fields["gateway.route_cache_size"] = "2";
  fields["pd.store_count"] = "1";
  fields["pd.store0.store_id"] = "1";
  fields["pd.store0.client_host"] = "127.0.0.1";
  fields["pd.store0.client_port"] = "8101";
  fields["pd.store0.raft_host"] = "127.0.0.1";
  fields["pd.store0.raft_port"] = "7101";
  fields["pd.store0.state"] = "Up";
  fields["pd.store0.last_heartbeat_ms"] = "0";
  fields["pd.region_count"] = "1";
  fields["pd.region0.region_id"] = "1";
  fields["pd.region0.start_key"] = "";
  fields["pd.region0.end_key"] = "";
  fields["pd.region0.conf_ver"] = "1";
  fields["pd.region0.version"] = "1";
  fields["pd.region0.leader_peer_id"] = "1";
  fields["pd.region0.peer_count"] = "1";
  fields["pd.region0.peer0.peer_id"] = "1";
  fields["pd.region0.peer0.store_id"] = "1";
  fields["pd.region0.peer0.role"] = "Voter";
  fields["store.region_count"] = "1";
  fields["store.region0.region_id"] = "1";
  fields["store.region0.runtime_role"] = "Leader";
  fields["store.region0.runtime_commit_index"] = "3";
  fields["store.region0.runtime_last_applied"] = "3";
  fields["store.region0.runtime_last_log_index"] = "3";

  const auto json = rstone::BuildDashboardStatusJson(fields, true, "", 5, 1000);
  RSTONE_ASSERT_TRUE(json.find("\"ok\":true") != std::string::npos);
  RSTONE_ASSERT_TRUE(json.find("\"store_count\":\"1\"") != std::string::npos);
  RSTONE_ASSERT_TRUE(json.find("\"client_endpoint\":\"127.0.0.1:8101\"") !=
                     std::string::npos);
  RSTONE_ASSERT_TRUE(json.find("\"runtime_role\":\"Leader\"") != std::string::npos);
}

void TestJsonEscape() {
  RSTONE_ASSERT_EQ(rstone::JsonEscape("a\"b\\c"), "a\\\"b\\\\c");
}

}  // namespace

struct StatusJsonTestRunner {
  StatusJsonTestRunner() {
    TestStatusJsonIncludesSummaryAndStores();
    TestJsonEscape();
  }
};

static StatusJsonTestRunner status_json_test_runner;
