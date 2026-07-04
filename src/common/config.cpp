#include "rstone/common/config.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>

namespace rstone {
namespace {

std::string Trim(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

std::string StripQuotes(std::string value) {
  if (value.size() >= 2 && ((value.front() == '"' && value.back() == '"') ||
                            (value.front() == '\'' && value.back() == '\''))) {
    return value.substr(1, value.size() - 2);
  }
  return value;
}

}  // namespace

Status Config::LoadFromFile(const std::string& path) {
  std::ifstream input(path);
  if (!input) {
    return Status::IoError("failed to open config file: " + path);
  }

  values_.clear();
  std::vector<std::string> sections;
  std::string line;
  while (std::getline(input, line)) {
    const auto comment = line.find('#');
    if (comment != std::string::npos) {
      line = line.substr(0, comment);
    }
    if (Trim(line).empty()) {
      continue;
    }

    const std::size_t indent = line.find_first_not_of(' ');
    const std::string trimmed = Trim(line);
    const std::size_t depth = indent == std::string::npos ? 0 : indent / 2;

    if (trimmed.back() == ':' && trimmed.find(':') == trimmed.size() - 1) {
      if (sections.size() > depth) {
        sections.resize(depth);
      }
      if (sections.size() == depth) {
        sections.push_back(trimmed.substr(0, trimmed.size() - 1));
      } else {
        sections[depth] = trimmed.substr(0, trimmed.size() - 1);
      }
      continue;
    }

    const auto colon = trimmed.find(':');
    if (colon == std::string::npos) {
      return Status::InvalidArgument("invalid config line: " + trimmed);
    }

    if (sections.size() > depth) {
      sections.resize(depth);
    }
    std::ostringstream key;
    for (const auto& section : sections) {
      if (!section.empty()) {
        if (key.tellp() > 0) {
          key << '.';
        }
        key << section;
      }
    }
    if (key.tellp() > 0) {
      key << '.';
    }
    key << Trim(trimmed.substr(0, colon));
    values_[key.str()] = StripQuotes(Trim(trimmed.substr(colon + 1)));
  }

  return Status::Ok();
}

std::optional<std::string> Config::GetString(const std::string& key) const {
  const auto it = values_.find(key);
  if (it == values_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::string Config::GetStringOr(const std::string& key, std::string fallback) const {
  auto value = GetString(key);
  return value ? *value : std::move(fallback);
}

int Config::GetIntOr(const std::string& key, int fallback) const {
  auto value = GetString(key);
  if (!value) {
    return fallback;
  }
  try {
    return std::stoi(*value);
  } catch (...) {
    return fallback;
  }
}

std::vector<std::string> Config::GetList(const std::string& key) const {
  std::vector<std::string> values;
  auto value = GetString(key);
  if (!value) {
    return values;
  }
  std::stringstream ss(*value);
  std::string item;
  while (std::getline(ss, item, ',')) {
    values.push_back(Trim(item));
  }
  return values;
}

Role Config::GetRole() const {
  return ParseRole(GetStringOr("role", ""));
}

Role ParseRole(const std::string& value) {
  if (value == "pd") {
    return Role::kPd;
  }
  if (value == "gateway") {
    return Role::kGateway;
  }
  if (value == "store") {
    return Role::kStore;
  }
  return Role::kUnknown;
}

std::string RoleName(Role role) {
  switch (role) {
    case Role::kPd:
      return "pd";
    case Role::kGateway:
      return "gateway";
    case Role::kStore:
      return "store";
    case Role::kUnknown:
      return "unknown";
  }
  return "unknown";
}

}  // namespace rstone
