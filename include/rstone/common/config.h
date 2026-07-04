#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "rstone/common/status.h"
#include "rstone/common/types.h"

namespace rstone {

class Config {
 public:
  Status LoadFromFile(const std::string& path);

  std::optional<std::string> GetString(const std::string& key) const;
  std::string GetStringOr(const std::string& key, std::string fallback) const;
  int GetIntOr(const std::string& key, int fallback) const;
  std::vector<std::string> GetList(const std::string& key) const;

  Role GetRole() const;
  const std::map<std::string, std::string>& values() const { return values_; }

 private:
  std::map<std::string, std::string> values_;
};

Role ParseRole(const std::string& value);
std::string RoleName(Role role);

}  // namespace rstone
