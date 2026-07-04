#include "rstone/pd/metadata_store.h"

#include "test_assert.h"

namespace {

void TestStoreAndRegionLookup() {
  rstone::PdMetadataStore store;
  rstone::StoreInfo store_info;
  store_info.store_id = store.AllocStoreId();
  store_info.client_endpoint.port = 8101;
  RSTONE_ASSERT_TRUE(store.PutStore(store_info).ok());
  RSTONE_ASSERT_TRUE(store.GetStore(store_info.store_id).has_value());

  rstone::RegionInfo region;
  region.region_id = store.AllocRegionId();
  region.start_key = "a";
  region.end_key = "m";
  region.peers.push_back(rstone::Peer{store.AllocPeerId(), store_info.store_id,
                                      rstone::PeerRole::kVoter});
  region.leader_peer_id = region.peers.front().peer_id;
  RSTONE_ASSERT_TRUE(store.PutRegion(region).ok());

  auto found = store.GetRegionByKey("b");
  RSTONE_ASSERT_TRUE(found.has_value());
  RSTONE_ASSERT_EQ(found->region_id, region.region_id);
  RSTONE_ASSERT_TRUE(!store.GetRegionByKey("z").has_value());
}

}  // namespace

struct PdMetadataStoreTestRunner {
  PdMetadataStoreTestRunner() { TestStoreAndRegionLookup(); }
};

static PdMetadataStoreTestRunner pd_metadata_store_test_runner;
