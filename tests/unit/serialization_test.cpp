#include "rstone/common/serialization.h"

#include "test_assert.h"

namespace {

void TestRegionRoundTrip() {
  rstone::RegionInfo region;
  region.region_id = 7;
  region.start_key = "a";
  region.end_key = "z";
  region.epoch.conf_ver = 2;
  region.epoch.version = 3;
  region.peers.push_back({11, 1, rstone::PeerRole::kVoter});
  region.peers.push_back({12, 2, rstone::PeerRole::kLearner});
  region.leader_peer_id = 11;

  rstone::FieldMap fields;
  rstone::PutRegionFields(&fields, region, "region");
  rstone::RegionInfo decoded;
  RSTONE_ASSERT_TRUE(rstone::GetRegionFields(fields, &decoded, "region").ok());
  RSTONE_ASSERT_EQ(decoded.region_id, region.region_id);
  RSTONE_ASSERT_EQ(decoded.peers.size(), static_cast<std::size_t>(2));
  RSTONE_ASSERT_EQ(decoded.peers[1].role, rstone::PeerRole::kLearner);
}

}  // namespace

struct SerializationTestRunner {
  SerializationTestRunner() { TestRegionRoundTrip(); }
};

static SerializationTestRunner serialization_test_runner;
