#include "rstone/raft/raft_storage.h"

#include <filesystem>

#include "rstone/storage/file_kv_engine.h"
#include "test_assert.h"

namespace {

void TestHardStateAndLogPersistence() {
  const std::string path = "build/test-data/raft-storage";
  std::filesystem::remove_all(path);

  rstone::FileKvEngine engine;
  RSTONE_ASSERT_TRUE(engine.Open(path).ok());
  rstone::RaftStorage storage(&engine);

  rstone::HardState state;
  state.current_term = 3;
  state.voted_for = 2;
  state.commit_index = 7;
  state.last_applied = 6;
  RSTONE_ASSERT_TRUE(storage.SaveHardState(1, state).ok());

  rstone::HardState loaded;
  RSTONE_ASSERT_TRUE(storage.LoadHardState(1, &loaded).ok());
  RSTONE_ASSERT_EQ(loaded.current_term, static_cast<rstone::Term>(3));
  RSTONE_ASSERT_EQ(loaded.voted_for, static_cast<rstone::PeerId>(2));

  rstone::LogEntry entry;
  entry.region_id = 1;
  entry.index = 1;
  entry.term = 3;
  entry.type = rstone::EntryType::kNormal;
  entry.command = "PUT";
  RSTONE_ASSERT_TRUE(storage.AppendLog(entry).ok());

  std::vector<rstone::LogEntry> entries;
  RSTONE_ASSERT_TRUE(storage.LoadLog(1, &entries).ok());
  RSTONE_ASSERT_EQ(entries.size(), static_cast<std::size_t>(1));
  RSTONE_ASSERT_EQ(entries.front().command, "PUT");

  RSTONE_ASSERT_TRUE(storage.DeleteLogsFrom(1, 1).ok());
  RSTONE_ASSERT_TRUE(storage.LoadLog(1, &entries).ok());
  RSTONE_ASSERT_TRUE(entries.empty());
}

}  // namespace

struct RaftStorageTestRunner {
  RaftStorageTestRunner() { TestHardStateAndLogPersistence(); }
};

static RaftStorageTestRunner raft_storage_test_runner;
