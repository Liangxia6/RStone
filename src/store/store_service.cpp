#include "rstone/store/store_service.h"

#include "rstone/common/error_code.h"
#include "rstone/common/serialization.h"
#include "rstone/rpc/rpc_codec.h"

namespace rstone {
namespace {

RpcResponse StatusToRpcError(const RpcRequest& request, const Status& status) {
  return MakeRpcError(request, std::string(ErrorCodeName(status.code())), status.message());
}

Consistency ParseConsistency(const std::string& value) {
  return value == "eventual" ? Consistency::kEventual : Consistency::kLinearizable;
}

std::string GetOr(const FieldMap& fields, const std::string& key, const std::string& fallback) {
  const auto it = fields.find(key);
  return it == fields.end() ? fallback : it->second;
}

Status ValidateRoute(MultiRegionCluster* cluster, const FieldMap& fields,
                     const std::string& key) {
  if (cluster == nullptr) {
    return Status::Ok();
  }
  const auto expected = GetOr(fields, "region_id", "");
  if (expected.empty()) {
    return Status::Ok();
  }
  const auto actual_region = cluster->FindRegionByKey(key);
  if (!actual_region) {
    return {ErrorCode::kRegionNotFound, "region not found for key"};
  }
  const auto expected_region_id = static_cast<RegionId>(std::stoull(expected));
  if (actual_region->region_id != expected_region_id) {
    return {ErrorCode::kStaleEpoch, "request route is stale"};
  }
  return Status::Ok();
}

}  // namespace

StoreService::StoreService(SingleRegionCluster* cluster) : single_cluster_(cluster) {}

StoreService::StoreService(MultiRegionCluster* cluster) : multi_cluster_(cluster) {}

StoreService::StoreService(DistributedRegionNode* node) : distributed_node_(node) {}

StoreService::StoreService(DistributedStoreNode* node) : distributed_store_(node) {}

Status StoreService::RegisterHandlers(RpcServer* server) {
  if (server == nullptr) {
    return Status::InvalidArgument("rpc server must not be null");
  }
  auto status = server->RegisterHandler("store.KvPut", [this](const RpcRequest& request) {
    return HandlePut(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.KvDelete", [this](const RpcRequest& request) {
    return HandleDelete(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.KvGet", [this](const RpcRequest& request) {
    return HandleGet(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.KvBatch", [this](const RpcRequest& request) {
    return HandleBatch(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.SplitRegion", [this](const RpcRequest& request) {
    return HandleSplitRegion(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.TransferLeader", [this](const RpcRequest& request) {
    return HandleTransferLeader(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.AddPeer", [this](const RpcRequest& request) {
    return HandleAddPeer(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.RemovePeer", [this](const RpcRequest& request) {
    return HandleRemovePeer(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.Status", [this](const RpcRequest& request) {
    return HandleStatus(request);
  });
  if (!status.ok()) {
    return status;
  }
  status = server->RegisterHandler("store.RaftRequestVote", [this](const RpcRequest& request) {
    return HandleRaftRequestVote(request);
  });
  if (!status.ok()) {
    return status;
  }
  return server->RegisterHandler("store.RaftAppendEntries", [this](const RpcRequest& request) {
    return HandleRaftAppendEntries(request);
  });
}

RpcResponse StoreService::HandlePut(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = ValidateRoute(multi_cluster_, fields, fields["key"]);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto region_id = static_cast<RegionId>(std::stoull(GetOr(fields, "region_id", "0")));
  if (distributed_store_ != nullptr) {
    status = distributed_store_->Put(region_id, fields["key"], fields["value"]);
  } else if (distributed_node_ != nullptr) {
    status = distributed_node_->Put(fields["key"], fields["value"]);
  } else if (multi_cluster_ != nullptr) {
    status = multi_cluster_->Put(fields["key"], fields["value"]);
  } else {
    status = single_cluster_->Put(fields["key"], fields["value"]);
  }
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse StoreService::HandleAddPeer(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto region_id = static_cast<RegionId>(std::stoull(fields["region_id"]));
  const auto store_id = static_cast<StoreId>(std::stoull(fields["store_id"]));
  const auto peer_id = static_cast<PeerId>(std::stoull(fields["peer_id"]));
  if (distributed_store_ != nullptr) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "distributed store add peer is not implemented yet");
  } else if (distributed_node_ != nullptr) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "distributed store add peer is not implemented yet");
  } else if (multi_cluster_ != nullptr) {
    status = multi_cluster_->AddPeer(region_id, store_id, peer_id);
  } else {
    status = single_cluster_->AddPeer(store_id, peer_id);
  }
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse StoreService::HandleRemovePeer(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto region_id = static_cast<RegionId>(std::stoull(fields["region_id"]));
  const auto peer_id = static_cast<PeerId>(std::stoull(fields["peer_id"]));
  if (distributed_store_ != nullptr) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "distributed store remove peer is not implemented yet");
  } else if (distributed_node_ != nullptr) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "distributed store remove peer is not implemented yet");
  } else if (multi_cluster_ != nullptr) {
    status = multi_cluster_->RemovePeer(region_id, peer_id);
  } else {
    status = single_cluster_->RemovePeer(peer_id);
  }
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse StoreService::HandleStatus(const RpcRequest& request) {
  FieldMap fields;
  if (distributed_store_ != nullptr) {
    const auto regions = distributed_store_->ListRegions();
    fields["region_count"] = std::to_string(regions.size());
    for (std::size_t i = 0; i < regions.size(); ++i) {
      const auto prefix = "region" + std::to_string(i);
      PutRegionFields(&fields, regions[i], prefix);
      const auto* node = distributed_store_->FindRegion(regions[i].region_id);
      if (node != nullptr) {
        fields[prefix + ".local_store_id"] = std::to_string(node->local_store_id());
        fields[prefix + ".local_peer_id"] = std::to_string(node->local_peer_id());
        if (node->raft() != nullptr) {
          fields[prefix + ".runtime_role"] = RaftRoleName(node->raft()->role());
          fields[prefix + ".runtime_commit_index"] =
              std::to_string(node->raft()->commit_index());
          fields[prefix + ".runtime_last_applied"] =
              std::to_string(node->raft()->last_applied());
          fields[prefix + ".runtime_last_log_index"] =
              std::to_string(node->raft()->last_log_index());
        }
      }
    }
  } else if (distributed_node_ != nullptr) {
    fields["region_count"] = "1";
    PutRegionFields(&fields, distributed_node_->region(), "region0");
    fields["region0.local_store_id"] = std::to_string(distributed_node_->local_store_id());
    fields["region0.local_peer_id"] = std::to_string(distributed_node_->local_peer_id());
    if (distributed_node_->raft() != nullptr) {
      fields["region0.runtime_role"] = RaftRoleName(distributed_node_->raft()->role());
      fields["region0.runtime_commit_index"] =
          std::to_string(distributed_node_->raft()->commit_index());
      fields["region0.runtime_last_applied"] =
          std::to_string(distributed_node_->raft()->last_applied());
      fields["region0.runtime_last_log_index"] =
          std::to_string(distributed_node_->raft()->last_log_index());
    }
  } else if (multi_cluster_ != nullptr) {
    const auto regions = multi_cluster_->ListRegions();
    fields["region_count"] = std::to_string(regions.size());
    for (std::size_t i = 0; i < regions.size(); ++i) {
      PutRegionFields(&fields, regions[i], "region" + std::to_string(i));
      const auto* cluster = multi_cluster_->FindRegionCluster(regions[i].region_id);
      if (cluster != nullptr && cluster->Leader() != nullptr) {
        fields["region" + std::to_string(i) + ".runtime_leader_peer_id"] =
            std::to_string(cluster->Leader()->peer_id());
        fields["region" + std::to_string(i) + ".runtime_peer_count"] =
            std::to_string(cluster->stores().size());
      }
    }
  } else {
    fields["region_count"] = "1";
    fields["region0.region_id"] = single_cluster_ ? "1" : "0";
    if (single_cluster_ != nullptr && single_cluster_->Leader() != nullptr) {
      fields["region0.runtime_leader_peer_id"] =
          std::to_string(single_cluster_->Leader()->peer_id());
      fields["region0.runtime_peer_count"] = std::to_string(single_cluster_->stores().size());
    }
  }
  return MakeRpcOk(request, EncodeFields(fields));
}

RpcResponse StoreService::HandleSplitRegion(const RpcRequest& request) {
  if (distributed_store_ != nullptr || distributed_node_ != nullptr) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "distributed split is not implemented yet");
  }
  if (multi_cluster_ == nullptr) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "split requires multi-region cluster");
  }
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  RegionInfo left;
  RegionInfo right;
  status = multi_cluster_->SplitRegion(static_cast<RegionId>(std::stoull(fields["region_id"])),
                                       fields["split_key"], &left, &right);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  FieldMap response_fields;
  PutRegionFields(&response_fields, left, "left");
  PutRegionFields(&response_fields, right, "right");
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse StoreService::HandleTransferLeader(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto region_id = static_cast<RegionId>(std::stoull(fields["region_id"]));
  const auto target_peer_id = static_cast<PeerId>(std::stoull(fields["target_peer_id"]));
  if (distributed_store_ != nullptr) {
    status = distributed_store_->TransferLeader(region_id, target_peer_id);
  } else if (distributed_node_ != nullptr) {
    status = distributed_node_->TransferLeader(target_peer_id);
  } else if (multi_cluster_ != nullptr) {
    status = multi_cluster_->TransferLeader(region_id, target_peer_id);
  } else {
    status = single_cluster_->TransferLeader(target_peer_id);
  }
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse StoreService::HandleDelete(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = ValidateRoute(multi_cluster_, fields, fields["key"]);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto region_id = static_cast<RegionId>(std::stoull(GetOr(fields, "region_id", "0")));
  if (distributed_store_ != nullptr) {
    status = distributed_store_->Delete(region_id, fields["key"]);
  } else if (distributed_node_ != nullptr) {
    status = distributed_node_->Delete(fields["key"]);
  } else if (multi_cluster_ != nullptr) {
    status = multi_cluster_->Delete(fields["key"]);
  } else {
    status = single_cluster_->Delete(fields["key"]);
  }
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse StoreService::HandleGet(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  status = ValidateRoute(multi_cluster_, fields, fields["key"]);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  std::string value;
  const auto region_id = static_cast<RegionId>(std::stoull(GetOr(fields, "region_id", "0")));
  if (distributed_store_ != nullptr) {
    status = distributed_store_->Get(region_id, fields["key"],
                                     ParseConsistency(GetOr(fields, "consistency", "")), &value);
  } else if (distributed_node_ != nullptr) {
    status = distributed_node_->Get(fields["key"],
                                    ParseConsistency(GetOr(fields, "consistency", "")), &value);
  } else if (multi_cluster_ != nullptr) {
    status = multi_cluster_->Get(fields["key"], ParseConsistency(GetOr(fields, "consistency", "")),
                                 &value);
  } else {
    const auto* leader = single_cluster_->Leader();
    if (leader == nullptr) {
      status = {ErrorCode::kNotLeader, "no leader"};
    } else {
      status = leader->Get(fields["key"], &value);
    }
  }
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  FieldMap response_fields;
  response_fields["value"] = value;
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse StoreService::HandleBatch(const RpcRequest& request) {
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  if (fields.find("op_count") != fields.end() && fields["op_count"] != "0") {
    status = ValidateRoute(multi_cluster_, fields, fields["op0.key"]);
    if (!status.ok()) {
      return StatusToRpcError(request, status);
    }
  }
  std::vector<KvMutation> mutations;
  const auto count = static_cast<std::size_t>(std::stoull(GetOr(fields, "op_count", "0")));
  for (std::size_t i = 0; i < count; ++i) {
    const auto prefix = "op" + std::to_string(i) + ".";
    KvMutation mutation;
    mutation.type =
        GetOr(fields, prefix + "type", "put") == "delete" ? KvMutationType::kDelete
                                                           : KvMutationType::kPut;
    mutation.key = fields[prefix + "key"];
    mutation.value = GetOr(fields, prefix + "value", "");
    mutations.push_back(mutation);
  }
  const auto region_id = static_cast<RegionId>(std::stoull(GetOr(fields, "region_id", "0")));
  if (distributed_store_ != nullptr) {
    status = distributed_store_->Batch(region_id, mutations);
  } else if (distributed_node_ != nullptr) {
    status = distributed_node_->Batch(mutations);
  } else if (multi_cluster_ != nullptr) {
    status = multi_cluster_->Batch(mutations);
  } else {
    status = single_cluster_->Batch(mutations);
  }
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  return MakeRpcOk(request, "");
}

RpcResponse StoreService::HandleRaftRequestVote(const RpcRequest& request) {
  if (distributed_store_ == nullptr && distributed_node_ == nullptr) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "raft request vote requires distributed store node");
  }
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  RequestVoteRequest vote_request;
  status = GetRequestVoteFields(fields, &vote_request);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto vote_response = distributed_store_ != nullptr
                                 ? distributed_store_->HandleRequestVote(vote_request)
                                 : distributed_node_->HandleRequestVote(vote_request);
  FieldMap response_fields;
  PutRequestVoteResponseFields(&response_fields, vote_response);
  return MakeRpcOk(request, EncodeFields(response_fields));
}

RpcResponse StoreService::HandleRaftAppendEntries(const RpcRequest& request) {
  if (distributed_store_ == nullptr && distributed_node_ == nullptr) {
    return MakeRpcError(request, std::string(ErrorCodeName(ErrorCode::kInvalidArgument)),
                        "raft append entries requires distributed store node");
  }
  FieldMap fields;
  auto status = DecodeFields(request.payload, &fields);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  AppendEntriesRequest append_request;
  status = GetAppendEntriesFields(fields, &append_request);
  if (!status.ok()) {
    return StatusToRpcError(request, status);
  }
  const auto append_response = distributed_store_ != nullptr
                                   ? distributed_store_->HandleAppendEntries(append_request)
                                   : distributed_node_->HandleAppendEntries(append_request);
  FieldMap response_fields;
  PutAppendEntriesResponseFields(&response_fields, append_response);
  return MakeRpcOk(request, EncodeFields(response_fields));
}

}  // namespace rstone
