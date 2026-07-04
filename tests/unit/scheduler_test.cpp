#include "rstone/pd/scheduler.h"

#include "rstone/pd/pd_server.h"
#include "test_assert.h"

namespace {

rstone::StoreInfo MakeSchedulerStore(int port) {
  rstone::StoreInfo store;
  store.client_endpoint.port = port;
  return store;
}

void TestSchedulerOperators() {
  rstone::PdServer pd;
  auto s1 = MakeSchedulerStore(8101);
  auto s2 = MakeSchedulerStore(8102);
  auto s3 = MakeSchedulerStore(8103);
  auto s4 = MakeSchedulerStore(8104);
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s1).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s2).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s3).ok());
  RSTONE_ASSERT_TRUE(pd.RegisterStore(&s4).ok());
  RSTONE_ASSERT_TRUE(pd.BootstrapDefaultRegion({s1.store_id, s2.store_id, s3.store_id}).ok());
  const auto region = pd.GetRegionByKey("key");
  RSTONE_ASSERT_TRUE(region.has_value());

  rstone::Scheduler scheduler(&pd.metadata());
  const auto transfer = scheduler.MakeTransferLeaderOperator(region->region_id, s2.store_id);
  RSTONE_ASSERT_TRUE(transfer.has_value());
  RSTONE_ASSERT_EQ(transfer->type, rstone::OperatorType::kTransferLeader);

  const auto add = scheduler.MakeAddPeerOperator(region->region_id, s4.store_id);
  RSTONE_ASSERT_TRUE(add.has_value());
  RSTONE_ASSERT_EQ(add->type, rstone::OperatorType::kAddPeer);

  const auto remove = scheduler.MakeRemovePeerOperator(region->region_id,
                                                       region->peers.back().peer_id);
  RSTONE_ASSERT_TRUE(remove.has_value());
  RSTONE_ASSERT_EQ(remove->type, rstone::OperatorType::kRemovePeer);
}

}  // namespace

struct SchedulerTestRunner {
  SchedulerTestRunner() { TestSchedulerOperators(); }
};

static SchedulerTestRunner scheduler_test_runner;
