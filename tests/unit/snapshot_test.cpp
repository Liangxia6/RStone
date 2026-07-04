#include "rstone/raft/snapshot.h"

#include <filesystem>

#include "rstone/storage/file_kv_engine.h"
#include "test_assert.h"

namespace {

void TestSnapshotCreateRestore() {
  const std::string base = "build/test-data/snapshot";
  std::filesystem::remove_all(base);

  rstone::FileKvEngine source;
  RSTONE_ASSERT_TRUE(source.Open(base + "/source").ok());
  RSTONE_ASSERT_TRUE(source.Put("kv/1/a", "a").ok());
  RSTONE_ASSERT_TRUE(source.Put("kv/1/b", "b").ok());
  RSTONE_ASSERT_TRUE(source.Put("kv/2/c", "c").ok());

  rstone::Snapshotter snapshotter;
  rstone::SnapshotMeta meta;
  meta.region_id = 1;
  meta.last_included_index = 10;
  meta.last_included_term = 2;
  meta.prefix = "kv/1/";
  RSTONE_ASSERT_TRUE(snapshotter.Create(source, meta, base + "/snapshot.dat").ok());

  rstone::FileKvEngine target;
  RSTONE_ASSERT_TRUE(target.Open(base + "/target").ok());
  rstone::SnapshotMeta restored_meta;
  RSTONE_ASSERT_TRUE(snapshotter.Restore(&target, base + "/snapshot.dat", &restored_meta).ok());
  RSTONE_ASSERT_EQ(restored_meta.region_id, static_cast<rstone::RegionId>(1));

  std::string value;
  RSTONE_ASSERT_TRUE(target.Get("kv/1/a", &value).ok());
  RSTONE_ASSERT_EQ(value, "a");
  RSTONE_ASSERT_TRUE(!target.Get("kv/2/c", &value).ok());
}

}  // namespace

struct SnapshotTestRunner {
  SnapshotTestRunner() { TestSnapshotCreateRestore(); }
};

static SnapshotTestRunner snapshot_test_runner;
