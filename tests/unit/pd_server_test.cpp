#include "rstone/pd/pd_server.h"

#include <filesystem>

#include "test_assert.h"

namespace {

rstone::StoreInfo MakeStore(int client_port) {
  rstone::StoreInfo store;
  store.client_endpoint.host = "127.0.0.1";
  store.client_endpoint.port = client_port;
  return store;
}

void TestRegisterAndRoute() {
  rstone::PdServer pd;
  auto s1 = MakeStore(8101);
  auto s2 = MakeStore(8102);
  auto s3 = MakeStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(s1.store_id != 0);
  RSTONE_ASSERT_TRUE(s2.store_id != 0);
  RSTONE_ASSERT_TRUE(s3.store_id != 0);

  RSTONE_ASSERT_TRUE(pd.StoreHeartbeat(s1.store_id, 100).ok());
  auto stored = pd.GetStore(s1.store_id);
  RSTONE_ASSERT_TRUE(stored.has_value());
  RSTONE_ASSERT_EQ(stored->last_heartbeat_ms, static_cast<std::int64_t>(100));

  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());
  const auto region = pd.GetRegionByKey("user:1");
  RSTONE_ASSERT_TRUE(region.has_value());
  RSTONE_ASSERT_EQ(region->peers.size(), static_cast<std::size_t>(3));
  const auto leader_store = pd.GetRegionLeaderStore(*region);
  RSTONE_ASSERT_TRUE(leader_store.has_value());
  RSTONE_ASSERT_EQ(leader_store->store_id, s1.store_id);
}

void TestSplitRegion() {
  rstone::PdServer pd;
  auto s1 = MakeStore(8101);
  auto s2 = MakeStore(8102);
  auto s3 = MakeStore(8103);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  auto region = pd.GetRegionByKey("abc");
  RSTONE_ASSERT_TRUE(region.has_value());

  rstone::RegionInfo left;
  rstone::RegionInfo right;
  RSTONE_ASSERT_TRUE(pd.SplitRegion(region->region_id, "m", &left, &right).ok());
  RSTONE_ASSERT_EQ(left.end_key, "m");
  RSTONE_ASSERT_EQ(right.start_key, "m");
  RSTONE_ASSERT_TRUE(pd.GetRegionByKey("apple").has_value());
  RSTONE_ASSERT_EQ(pd.GetRegionByKey("apple")->region_id, left.region_id);
  RSTONE_ASSERT_TRUE(pd.GetRegionByKey("zebra").has_value());
  RSTONE_ASSERT_EQ(pd.GetRegionByKey("zebra")->region_id, right.region_id);
}

void TestPeerChanges() {
  rstone::PdServer pd;
  auto s1 = MakeStore(8101);
  auto s2 = MakeStore(8102);
  auto s3 = MakeStore(8103);
  auto s4 = MakeStore(8104);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s4).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());

  auto region = pd.GetRegionByKey("k");
  RSTONE_ASSERT_TRUE(region.has_value());
  const auto old_conf_ver = region->epoch.conf_ver;

  rstone::Peer added;
  RSTONE_ASSERT_TRUE(pd.AddPeer(region->region_id, s4.store_id, &added).ok());
  region = pd.GetRegionByKey("k");
  RSTONE_ASSERT_TRUE(region.has_value());
  RSTONE_ASSERT_EQ(region->peers.size(), static_cast<std::size_t>(4));
  RSTONE_ASSERT_EQ(region->epoch.conf_ver, old_conf_ver + 1);

  RSTONE_ASSERT_TRUE(pd.TransferLeader(region->region_id, added.peer_id).ok());
  region = pd.GetRegionByKey("k");
  RSTONE_ASSERT_TRUE(region.has_value());
  RSTONE_ASSERT_EQ(region->leader_peer_id, added.peer_id);

  RSTONE_ASSERT_TRUE(pd.RemovePeer(region->region_id, added.peer_id).ok());
  region = pd.GetRegionByKey("k");
  RSTONE_ASSERT_TRUE(region.has_value());
  RSTONE_ASSERT_EQ(region->peers.size(), static_cast<std::size_t>(3));
  RSTONE_ASSERT_TRUE(region->leader_peer_id != added.peer_id);
}

void TestPersistenceRecovery() {
  const std::string path = "build/test-data/pd-recovery";
  std::filesystem::remove_all(path);

  rstone::Peer added;
  {
    rstone::PdServer pd;
    RSTONE_ASSERT_TRUE(pd.Open(path).ok());
    auto s1 = MakeStore(8101);
    auto s2 = MakeStore(8102);
    auto s3 = MakeStore(8103);
    auto s4 = MakeStore(8104);
    RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
    RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
    RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
    RSTONE_ASSERT_TRUE(pd.RegisterStore(&s4).ok());
    RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());
    auto region = pd.GetRegionByKey("apple");
    RSTONE_ASSERT_TRUE(region.has_value());
    rstone::RegionInfo left;
    rstone::RegionInfo right;
    RSTONE_ASSERT_TRUE(pd.SplitRegion(region->region_id, "m", &left, &right).ok());
    RSTONE_ASSERT_TRUE(pd.AddPeer(right.region_id, s4.store_id, &added).ok());
    RSTONE_ASSERT_TRUE(pd.TransferLeader(right.region_id, added.peer_id).ok());
  }

  {
    rstone::PdServer pd;
    RSTONE_ASSERT_TRUE(pd.Open(path).ok());
    auto left = pd.GetRegionByKey("apple");
    auto right = pd.GetRegionByKey("zebra");
    RSTONE_ASSERT_TRUE(left.has_value());
    RSTONE_ASSERT_TRUE(right.has_value());
    RSTONE_ASSERT_TRUE(left->region_id != right->region_id);
    RSTONE_ASSERT_EQ(right->leader_peer_id, added.peer_id);
    RSTONE_ASSERT_EQ(right->peers.size(), static_cast<std::size_t>(4));

    rstone::StoreInfo new_store = MakeStore(8105);
    RSTONE_ASSERT_TRUE(pd.RegisterStore(&new_store).ok());
    RSTONE_ASSERT_TRUE(new_store.store_id > 4);
  }
}

}  // namespace

struct PdServerTestRunner {
  PdServerTestRunner() {
    TestRegisterAndRoute();
    TestSplitRegion();
    TestPeerChanges();
    TestPersistenceRecovery();
  }
};

static PdServerTestRunner pd_server_test_runner;
