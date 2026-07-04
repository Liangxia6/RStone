#pragma once

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace rstone {

inline std::string NowTimestamp() {
  const auto now = std::chrono::system_clock::now();
  const auto time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
#if defined(_WIN32)
  localtime_s(&tm, &time);
#else
  localtime_r(&time, &tm);
#endif
  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return oss.str();
}

inline void LogLine(const char* level, const std::string& message) {
  std::cerr << NowTimestamp() << " [" << level << "] " << message << '\n';
}

}  // namespace rstone

#define RSTONE_LOG_INFO(message) ::rstone::LogLine("INFO", (message))
#define RSTONE_LOG_WARN(message) ::rstone::LogLine("WARN", (message))
#define RSTONE_LOG_ERROR(message) ::rstone::LogLine("ERROR", (message))
