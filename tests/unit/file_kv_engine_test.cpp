#include "rstone/storage/file_kv_engine.h"

#include <filesystem>

#include "test_assert.h"

namespace {

void TestPutGetRestartAndScan() {
  const std::string path = "build/test-data/file-kv-engine";
  std::filesystem::remove_all(path);

  rstone::FileKvEngine engine;
  RSTONE_ASSERT_TRUE(engine.Open(path).ok());
  RSTONE_ASSERT_TRUE(engine.Put("kv/a", "1").ok());
  RSTONE_ASSERT_TRUE(engine.Put("kv/b", "2").ok());
  RSTONE_ASSERT_TRUE(engine.Put("meta/a", "3").ok());

  std::string value;
  RSTONE_ASSERT_TRUE(engine.Get("kv/a", &value).ok());
  RSTONE_ASSERT_EQ(value, "1");

  const auto scan = engine.ScanPrefix("kv/");
  RSTONE_ASSERT_EQ(scan.size(), static_cast<std::size_t>(2));
  const auto range = engine.ScanRange("kv/a", "kv/c");
  RSTONE_ASSERT_EQ(range.size(), static_cast<std::size_t>(2));

  rstone::FileKvEngine restarted;
  RSTONE_ASSERT_TRUE(restarted.Open(path).ok());
  RSTONE_ASSERT_TRUE(restarted.Get("kv/b", &value).ok());
  RSTONE_ASSERT_EQ(value, "2");

  RSTONE_ASSERT_TRUE(restarted.Delete("kv/b").ok());
  RSTONE_ASSERT_TRUE(!restarted.Get("kv/b", &value).ok());

  rstone::FileKvEngine after_delete_restart;
  RSTONE_ASSERT_TRUE(after_delete_restart.Open(path).ok());
  RSTONE_ASSERT_TRUE(!after_delete_restart.Get("kv/b", &value).ok());
  RSTONE_ASSERT_TRUE(after_delete_restart.Get("kv/a", &value).ok());
  RSTONE_ASSERT_EQ(value, "1");

  std::vector<rstone::KvMutation> batch_delete;
  batch_delete.push_back({rstone::KvMutationType::kDelete, "kv/a", ""});
  RSTONE_ASSERT_TRUE(after_delete_restart.WriteBatch(batch_delete).ok());
}

}  // namespace

struct FileKvEngineTestRunner {
  FileKvEngineTestRunner() { TestPutGetRestartAndScan(); }
};

static FileKvEngineTestRunner file_kv_engine_test_runner;
