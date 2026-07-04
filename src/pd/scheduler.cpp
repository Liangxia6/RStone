#include "rstone/pd/scheduler.h"

#include "rstone/region/region.h"

namespace rstone {

Scheduler::Scheduler(const PdMetadataStore* metadata) : metadata_(metadata) {}

std::optional<Operator> Scheduler::MakeTransferLeaderOperator(
    RegionId region_id, StoreId target_store_id) const {
  if (metadata_ == nullptr) {
    return std::nullopt;
  }
  auto region = metadata_->GetRegion(region_id);
  if (!region) {
    return std::nullopt;
  }
  const Peer* peer = FindPeerByStore(*region, target_store_id);
  if (peer == nullptr || peer->peer_id == region->leader_peer_id) {
    return std::nullopt;
  }
  Operator op;
  op.type = OperatorType::kTransferLeader;
  op.region_id = region_id;
  op.target_store_id = target_store_id;
  op.target_peer_id = peer->peer_id;
  return op;
}

std::optional<Operator> Scheduler::MakeAddPeerOperator(RegionId region_id,
                                                       StoreId target_store_id) const {
  if (metadata_ == nullptr) {
    return std::nullopt;
  }
  auto region = metadata_->GetRegion(region_id);
  if (!region || !metadata_->GetStore(target_store_id)) {
    return std::nullopt;
  }
  if (FindPeerByStore(*region, target_store_id) != nullptr) {
    return std::nullopt;
  }
  Operator op;
  op.type = OperatorType::kAddPeer;
  op.region_id = region_id;
  op.target_store_id = target_store_id;
  return op;
}

std::optional<Operator> Scheduler::MakeRemovePeerOperator(RegionId region_id,
                                                          PeerId peer_id) const {
  if (metadata_ == nullptr) {
    return std::nullopt;
  }
  auto region = metadata_->GetRegion(region_id);
  if (!region || FindPeer(*region, peer_id) == nullptr || region->peers.size() <= 1) {
    return std::nullopt;
  }
  Operator op;
  op.type = OperatorType::kRemovePeer;
  op.region_id = region_id;
  op.remove_peer_id = peer_id;
  return op;
}

}  // namespace rstone
