#include "rstone/common/status.h"

#include "test_assert.h"

namespace {

void TestOk() {
  const auto status = rstone::Status::Ok();
  RSTONE_ASSERT_TRUE(status.ok());
  RSTONE_ASSERT_EQ(status.ToString(), "OK");
}

void TestError() {
  const auto status = rstone::Status::InvalidArgument("bad input");
  RSTONE_ASSERT_TRUE(!status.ok());
  RSTONE_ASSERT_EQ(status.ToString(), "INVALID_ARGUMENT: bad input");
}

}  // namespace

struct StatusTestRunner {
  StatusTestRunner() {
    TestOk();
    TestError();
  }
};

static StatusTestRunner status_test_runner;
