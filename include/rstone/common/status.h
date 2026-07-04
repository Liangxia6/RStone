#pragma once

#include <ostream>
#include <string>
#include <utility>

#include "rstone/common/error_code.h"

namespace rstone {

class Status {
 public:
  Status() = default;
  Status(ErrorCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  static Status Ok() { return {}; }
  static Status InvalidArgument(std::string message) {
    return {ErrorCode::kInvalidArgument, std::move(message)};
  }
  static Status KeyNotFound(std::string message) {
    return {ErrorCode::kKeyNotFound, std::move(message)};
  }
  static Status StorageError(std::string message) {
    return {ErrorCode::kStorageError, std::move(message)};
  }
  static Status IoError(std::string message) {
    return {ErrorCode::kIoError, std::move(message)};
  }
  static Status Internal(std::string message) {
    return {ErrorCode::kInternal, std::move(message)};
  }

  bool ok() const { return code_ == ErrorCode::kOk; }
  ErrorCode code() const { return code_; }
  const std::string& message() const { return message_; }

  std::string ToString() const {
    if (ok()) {
      return std::string(ErrorCodeName(code_));
    }
    return std::string(ErrorCodeName(code_)) + ": " + message_;
  }

 private:
  ErrorCode code_ = ErrorCode::kOk;
  std::string message_;
};

inline std::ostream& operator<<(std::ostream& os, const Status& status) {
  os << status.ToString();
  return os;
}

}  // namespace rstone
