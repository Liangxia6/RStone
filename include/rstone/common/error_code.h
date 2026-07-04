#pragma once

#include <string_view>

namespace rstone {

enum class ErrorCode {
  kOk = 0,
  kInvalidArgument,
  kKeyNotFound,
  kCasFailed,
  kNotLeader,
  kRegionNotFound,
  kStaleEpoch,
  kEpochNotMatch,
  kStoreNotFound,
  kPdUnavailable,
  kRaftNotReady,
  kTermOutdated,
  kLogConflict,
  kSnapshotRequired,
  kRpcTimeout,
  kRpcError,
  kStorageError,
  kConfigChangeInProgress,
  kIoError,
  kInternal,
};

inline std::string_view ErrorCodeName(ErrorCode code) {
  switch (code) {
    case ErrorCode::kOk:
      return "OK";
    case ErrorCode::kInvalidArgument:
      return "INVALID_ARGUMENT";
    case ErrorCode::kKeyNotFound:
      return "KEY_NOT_FOUND";
    case ErrorCode::kCasFailed:
      return "CAS_FAILED";
    case ErrorCode::kNotLeader:
      return "NOT_LEADER";
    case ErrorCode::kRegionNotFound:
      return "REGION_NOT_FOUND";
    case ErrorCode::kStaleEpoch:
      return "STALE_EPOCH";
    case ErrorCode::kEpochNotMatch:
      return "EPOCH_NOT_MATCH";
    case ErrorCode::kStoreNotFound:
      return "STORE_NOT_FOUND";
    case ErrorCode::kPdUnavailable:
      return "PD_UNAVAILABLE";
    case ErrorCode::kRaftNotReady:
      return "RAFT_NOT_READY";
    case ErrorCode::kTermOutdated:
      return "TERM_OUTDATED";
    case ErrorCode::kLogConflict:
      return "LOG_CONFLICT";
    case ErrorCode::kSnapshotRequired:
      return "SNAPSHOT_REQUIRED";
    case ErrorCode::kRpcTimeout:
      return "RPC_TIMEOUT";
    case ErrorCode::kRpcError:
      return "RPC_ERROR";
    case ErrorCode::kStorageError:
      return "STORAGE_ERROR";
    case ErrorCode::kConfigChangeInProgress:
      return "CONFIG_CHANGE_IN_PROGRESS";
    case ErrorCode::kIoError:
      return "IO_ERROR";
    case ErrorCode::kInternal:
      return "INTERNAL";
  }
  return "UNKNOWN";
}

}  // namespace rstone
