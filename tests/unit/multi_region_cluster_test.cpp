#include "rstone/store/multi_region_cluster.h"

#include "test_assert.h"

namespace {

rstone::RegionInfo MakeRegion(rstone::RegionId id, std::string start, std::string end) {
  rstone::RegionInfo region;
  region.region_id = id;
  region.start_key = std::move(start);
  region.end_key = std::move(end);
  return region;
}

void TestMultiRegionRouting() {
  rstone::MultiRegionCluster cluster;
  std::vector<rstone::RegionInfo> regions;
  regions.push_back(MakeRegion(1, "", "m"));
  regions.push_back(MakeRegion(2, "m", ""));
  RSTONE_ASSERT_TRUE(
      cluster.Bootstrap("build/test-data/multi-region-cluster", 3, regions).ok());
  RSTONE_ASSERT_EQ(cluster.RegionCount(), static_cast<std::size_t>(2));

  RSTONE_ASSERT_TRUE(cluster.Put("apple", "red").ok());
  RSTONE_ASSERT_TRUE(cluster.Put("zebra", "stripe").ok());

  std::string value;
  RSTONE_ASSERT_TRUE(cluster.Get("apple", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "red");
  RSTONE_ASSERT_TRUE(cluster.Get("zebra", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "stripe");

  const auto left = cluster.FindRegionByKey("apple");
  const auto right = cluster.FindRegionByKey("zebra");
  RSTONE_ASSERT_TRUE(left.has_value());
  RSTONE_ASSERT_TRUE(right.has_value());
  RSTONE_ASSERT_EQ(left->region_id, static_cast<rstone::RegionId>(1));
  RSTONE_ASSERT_EQ(right->region_id, static_cast<rstone::RegionId>(2));

  std::vector<rstone::KvMutation> batch;
  batch.push_back({rstone::KvMutationType::kPut, "ant", "small"});
  batch.push_back({rstone::KvMutationType::kPut, "bee", "busy"});
  RSTONE_ASSERT_TRUE(cluster.Batch(batch).ok());
  RSTONE_ASSERT_TRUE(cluster.Get("ant", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "small");

  batch.push_back({rstone::KvMutationType::kPut, "yak", "large"});
  RSTONE_ASSERT_TRUE(!cluster.Batch(batch).ok());
}

void TestSplitRegionMigratesData() {
  rstone::MultiRegionCluster cluster;
  std::vector<rstone::RegionInfo> regions;
  regions.push_back(MakeRegion(10, "", ""));
  RSTONE_ASSERT_TRUE(
      cluster.Bootstrap("build/test-data/multi-region-split", 3, regions).ok());

  RSTONE_ASSERT_TRUE(cluster.Put("apple", "red").ok());
  RSTONE_ASSERT_TRUE(cluster.Put("zebra", "stripe").ok());

  rstone::RegionInfo left;
  rstone::RegionInfo right;
  RSTONE_ASSERT_TRUE(cluster.SplitRegion(10, "m", &left, &right).ok());
  RSTONE_ASSERT_EQ(cluster.RegionCount(), static_cast<std::size_t>(2));
  RSTONE_ASSERT_EQ(left.end_key, "m");
  RSTONE_ASSERT_EQ(right.start_key, "m");

  std::string value;
  RSTONE_ASSERT_TRUE(cluster.Get("apple", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "red");
  RSTONE_ASSERT_TRUE(cluster.Get("zebra", rstone::Consistency::kLinearizable, &value).ok());
  RSTONE_ASSERT_EQ(value, "stripe");

  const auto* left_cluster = cluster.FindRegionCluster(left.region_id);
  const auto* right_cluster = cluster.FindRegionCluster(right.region_id);
  RSTONE_ASSERT_TRUE(left_cluster != nullptr);
  RSTONE_ASSERT_TRUE(right_cluster != nullptr);
  RSTONE_ASSERT_TRUE(left_cluster->GetFromStore(1, "apple", &value).ok());
  RSTONE_ASSERT_TRUE(!left_cluster->GetFromStore(1, "zebra", &value).ok());
  RSTONE_ASSERT_TRUE(right_cluster->GetFromStore(1, "zebra", &value).ok());

  RSTONE_ASSERT_TRUE(cluster.Put("ant", "tiny").ok());
  RSTONE_ASSERT_TRUE(cluster.Put("yak", "large").ok());
  RSTONE_ASSERT_TRUE(left_cluster->GetFromStore(2, "ant", &value).ok());
  RSTONE_ASSERT_TRUE(right_cluster->GetFromStore(2, "yak", &value).ok());
}

void TestMultiRegionTransferLeader() {
  rstone::MultiRegionCluster cluster;
  std::vector<rstone::RegionInfo> regions;
  regions.push_back(MakeRegion(1, "", "m"));
  regions.push_back(MakeRegion(2, "m", ""));
  RSTONE_ASSERT_TRUE(
      cluster.Bootstrap("build/test-data/multi-region-transfer", 3, regions).ok());
  auto* right = cluster.FindRegionCluster(2);
  RSTONE_ASSERT_TRUE(right != nullptr);
  RSTONE_ASSERT_TRUE(right->Leader() != nullptr);
  RSTONE_ASSERT_EQ(right->Leader()->peer_id(), static_cast<rstone::PeerId>(1));
  RSTONE_ASSERT_TRUE(cluster.TransferLeader(2, 3).ok());
  RSTONE_ASSERT_TRUE(right->Leader() != nullptr);
  RSTONE_ASSERT_EQ(right->Leader()->peer_id(), static_cast<rstone::PeerId>(3));
  RSTONE_ASSERT_TRUE(cluster.Put("zulu", "leader3").ok());
  std::string value;
  RSTONE_ASSERT_TRUE(right->GetFromStore(1, "zulu", &value).ok());
  RSTONE_ASSERT_EQ(value, "leader3");
}

void TestMultiRegionAddRemovePeer() {
  rstone::MultiRegionCluster cluster;
  std::vector<rstone::RegionInfo> regions;
  regions.push_back(MakeRegion(1, "", ""));
  RSTONE_ASSERT_TRUE(
      cluster.Bootstrap("build/test-data/multi-region-membership", 3, regions).ok());
  RSTONE_ASSERT_TRUE(cluster.Put("member:key", "before").ok());
  RSTONE_ASSERT_TRUE(cluster.AddPeer(1, 4, 4).ok());
  auto* region = cluster.FindRegionCluster(1);
  RSTONE_ASSERT_TRUE(region != nullptr);
  std::string value;
  RSTONE_ASSERT_TRUE(region->GetFromStore(4, "member:key", &value).ok());
  RSTONE_ASSERT_EQ(value, "before");
  RSTONE_ASSERT_TRUE(cluster.RemovePeer(1, 2).ok());
  RSTONE_ASSERT_TRUE(cluster.Put("member:after", "after").ok());
  RSTONE_ASSERT_TRUE(region->GetFromStore(4, "member:after", &value).ok());
  RSTONE_ASSERT_EQ(value, "after");
}

void TestSplitRestartRecovery() {
  const std::string path = "build/test-data/multi-region-split-recovery";
  {
    rstone::MultiRegionCluster cluster;
    std::vector<rstone::RegionInfo> regions;
    regions.push_back(MakeRegion(1, "", ""));
    RSTONE_ASSERT_TRUE(cluster.Bootstrap(path, 3, regions, true).ok());
    RSTONE_ASSERT_TRUE(cluster.Put("apple", "red").ok());
    RSTONE_ASSERT_TRUE(cluster.Put("zebra", "stripe").ok());
    rstone::RegionInfo left;
    rstone::RegionInfo right;
    RSTONE_ASSERT_TRUE(cluster.SplitRegion(1, "m", &left, &right).ok());
    RSTONE_ASSERT_EQ(cluster.RegionCount(), static_cast<std::size_t>(2));
  }

  {
    rstone::MultiRegionCluster cluster;
    std::vector<rstone::RegionInfo> seed;
    seed.push_back(MakeRegion(1, "", ""));
    RSTONE_ASSERT_TRUE(cluster.Bootstrap(path, 3, seed, false).ok());
    RSTONE_ASSERT_EQ(cluster.RegionCount(), static_cast<std::size_t>(2));

    std::string value;
    RSTONE_ASSERT_TRUE(cluster.Get("apple", rstone::Consistency::kLinearizable, &value).ok());
    RSTONE_ASSERT_EQ(value, "red");
    RSTONE_ASSERT_TRUE(cluster.Get("zebra", rstone::Consistency::kLinearizable, &value).ok());
    RSTONE_ASSERT_EQ(value, "stripe");

    RSTONE_ASSERT_TRUE(cluster.Put("yak", "large").ok());
    RSTONE_ASSERT_TRUE(cluster.Get("yak", rstone::Consistency::kLinearizable, &value).ok());
    RSTONE_ASSERT_EQ(value, "large");
  }
}

}  // namespace

struct MultiRegionClusterTestRunner {
  MultiRegionClusterTestRunner() {
    TestMultiRegionRouting();
    TestSplitRegionMigratesData();
    TestMultiRegionTransferLeader();
    TestMultiRegionAddRemovePeer();
    TestSplitRestartRecovery();
  }
};

static MultiRegionClusterTestRunner multi_region_cluster_test_runner;
