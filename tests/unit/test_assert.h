#pragma once

#include <cstdlib>
#include <iostream>

#define RSTONE_ASSERT_TRUE(expr)                                                   \
  do {                                                                             \
    if (!(expr)) {                                                                 \
      std::cerr << "ASSERT_TRUE failed at " << __FILE__ << ":" << __LINE__       \
                << ": " #expr << "\n";                                           \
      std::exit(1);                                                                \
    }                                                                              \
  } while (false)

#define RSTONE_ASSERT_EQ(lhs, rhs)                                                 \
  do {                                                                             \
    const auto lhs_value = (lhs);                                                   \
    const auto rhs_value = (rhs);                                                   \
    if (!(lhs_value == rhs_value)) {                                                \
      std::cerr << "ASSERT_EQ failed at " << __FILE__ << ":" << __LINE__         \
                << ": " #lhs " != " #rhs << "\n";                               \
      std::exit(1);                                                                \
    }                                                                              \
  } while (false)
