#include "rstone/store/single_region_cluster.h"

#include "test_assert.h"

namespace {

void TestReplicatedPutDelete() {
  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/single-region-cluster", 3).ok());
  RSTONE_ASSERT_TRUE(cluster.Leader() != nullptr);
  RSTONE_ASSERT_EQ(cluster.Leader()->peer_id(), static_cast<rstone::PeerId>(1));

  RSTONE_ASSERT_TRUE(cluster.Put("user:1", "alice").ok());

  std::string value;
  RSTONE_ASSERT_TRUE(cluster.GetFromStore(1, "user:1", &value).ok());
  RSTONE_ASSERT_EQ(value, "alice");
  RSTONE_ASSERT_TRUE(cluster.GetFromStore(2, "user:1", &value).ok());
  RSTONE_ASSERT_EQ(value, "alice");
  RSTONE_ASSERT_TRUE(cluster.GetFromStore(3, "user:1", &value).ok());
  RSTONE_ASSERT_EQ(value, "alice");

  auto* leader = cluster.Leader();
  RSTONE_ASSERT_TRUE(leader != nullptr);
  const auto raft_logs = leader->engine().ScanPrefix("raft/log/1/");
  RSTONE_ASSERT_TRUE(!raft_logs.empty());

  RSTONE_ASSERT_TRUE(cluster.Delete("user:1").ok());
  RSTONE_ASSERT_TRUE(!cluster.GetFromStore(1, "user:1", &value).ok());
  RSTONE_ASSERT_TRUE(!cluster.GetFromStore(2, "user:1", &value).ok());
  RSTONE_ASSERT_TRUE(!cluster.GetFromStore(3, "user:1", &value).ok());

  std::vector<rstone::KvMutation> batch;
  batch.push_back({rstone::KvMutationType::kPut, "user:2", "bob"});
  batch.push_back({rstone::KvMutationType::kPut, "user:3", "carol"});
  RSTONE_ASSERT_TRUE(cluster.Batch(batch).ok());
  RSTONE_ASSERT_TRUE(cluster.GetFromStore(2, "user:2", &value).ok());
  RSTONE_ASSERT_EQ(value, "bob");
  RSTONE_ASSERT_TRUE(cluster.GetFromStore(3, "user:3", &value).ok());
  RSTONE_ASSERT_EQ(value, "carol");

  std::vector<rstone::KvMutation> batch_delete;
  batch_delete.push_back({rstone::KvMutationType::kDelete, "user:2", ""});
  RSTONE_ASSERT_TRUE(cluster.Batch(batch_delete).ok());
  RSTONE_ASSERT_TRUE(!cluster.GetFromStore(1, "user:2", &value).ok());
}

void TestRestartRecovery() {
  const std::string path = "build/test-data/single-region-recovery";
  {
    rstone::SingleRegionCluster cluster;
    RSTONE_ASSERT_TRUE(cluster.Bootstrap(path, 3, 1, true).ok());
    RSTONE_ASSERT_TRUE(cluster.Put("recover:1", "before").ok());
    auto* leader = cluster.Leader();
    RSTONE_ASSERT_TRUE(leader != nullptr);
    RSTONE_ASSERT_EQ(leader->raft().last_log_index(), static_cast<rstone::LogIndex>(1));
  }

  {
    rstone::SingleRegionCluster cluster;
    RSTONE_ASSERT_TRUE(cluster.Bootstrap(path, 3, 1, false).ok());
    std::string value;
    RSTONE_ASSERT_TRUE(cluster.GetFromStore(1, "recover:1", &value).ok());
    RSTONE_ASSERT_EQ(value, "before");
    auto* leader = cluster.Leader();
    RSTONE_ASSERT_TRUE(leader != nullptr);
    RSTONE_ASSERT_EQ(leader->raft().last_log_index(), static_cast<rstone::LogIndex>(1));

    RSTONE_ASSERT_TRUE(cluster.Put("recover:2", "after").ok());
    RSTONE_ASSERT_TRUE(cluster.GetFromStore(2, "recover:2", &value).ok());
    RSTONE_ASSERT_EQ(value, "after");
    RSTONE_ASSERT_TRUE(cluster.Leader() != nullptr);
    RSTONE_ASSERT_EQ(cluster.Leader()->raft().last_log_index(),
                     static_cast<rstone::LogIndex>(2));
  }
}

void TestTransferLeader() {
  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/single-region-transfer", 3).ok());
  RSTONE_ASSERT_TRUE(cluster.Leader() != nullptr);
  RSTONE_ASSERT_EQ(cluster.Leader()->peer_id(), static_cast<rstone::PeerId>(1));
  RSTONE_ASSERT_TRUE(cluster.TransferLeader(2).ok());
  RSTONE_ASSERT_TRUE(cluster.Leader() != nullptr);
  RSTONE_ASSERT_EQ(cluster.Leader()->peer_id(), static_cast<rstone::PeerId>(2));
  RSTONE_ASSERT_TRUE(cluster.Put("transfer:key", "ok").ok());
  std::string value;
  RSTONE_ASSERT_TRUE(cluster.GetFromStore(3, "transfer:key", &value).ok());
  RSTONE_ASSERT_EQ(value, "ok");
}

void TestAddRemovePeer() {
  rstone::SingleRegionCluster cluster;
  RSTONE_ASSERT_TRUE(cluster.Bootstrap("build/test-data/single-region-membership", 3).ok());
  RSTONE_ASSERT_TRUE(cluster.Put("member:key", "before").ok());
  RSTONE_ASSERT_TRUE(cluster.AddPeer(4, 4).ok());
  std::string value;
  RSTONE_ASSERT_TRUE(cluster.GetFromStore(4, "member:key", &value).ok());
  RSTONE_ASSERT_EQ(value, "before");
  RSTONE_ASSERT_TRUE(cluster.RemovePeer(2).ok());
  RSTONE_ASSERT_TRUE(cluster.Put("member:after", "after").ok());
  RSTONE_ASSERT_TRUE(cluster.GetFromStore(4, "member:after", &value).ok());
  RSTONE_ASSERT_EQ(value, "after");
}

}  // namespace

struct SingleRegionClusterTestRunner {
  SingleRegionClusterTestRunner() {
    TestReplicatedPutDelete();
    TestRestartRecovery();
    TestTransferLeader();
    TestAddRemovePeer();
  }
};

static SingleRegionClusterTestRunner single_region_cluster_test_runner;
