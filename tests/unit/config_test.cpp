#include "rstone/common/config.h"

#include "test_assert.h"

namespace {

void TestConfigLoad() {
  rstone::Config config;
  const auto status = config.LoadFromFile("config/store1.yaml");
  RSTONE_ASSERT_TRUE(status.ok());
  RSTONE_ASSERT_EQ(config.GetRole(), rstone::Role::kStore);
  RSTONE_ASSERT_EQ(config.GetStringOr("store.host", ""), "127.0.0.1");
  RSTONE_ASSERT_EQ(config.GetIntOr("store.raft_port", 0), 7101);
}

}  // namespace

struct ConfigTestRunner {
  ConfigTestRunner() { TestConfigLoad(); }
};

static ConfigTestRunner config_test_runner;
