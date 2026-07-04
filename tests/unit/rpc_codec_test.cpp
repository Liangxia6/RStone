#include "rstone/rpc/rpc_codec.h"

#include "test_assert.h"

namespace {

void TestRpcRequestRoundTrip() {
  rstone::RpcRequest request;
  request.request_id = "req-1";
  request.method = "pd.GetRegionByKey";
  request.source = "gateway";
  request.target = "pd";
  request.deadline_ms = 1234;
  request.payload = "key=user:1\nvalue=alice";
  request.metadata["trace"] = "abc";

  const auto encoded = rstone::EncodeRpcRequest(request);
  rstone::RpcRequest decoded;
  RSTONE_ASSERT_TRUE(rstone::DecodeRpcRequest(encoded, &decoded).ok());
  RSTONE_ASSERT_EQ(decoded.request_id, request.request_id);
  RSTONE_ASSERT_EQ(decoded.method, request.method);
  RSTONE_ASSERT_EQ(decoded.deadline_ms, request.deadline_ms);
  RSTONE_ASSERT_EQ(decoded.payload, request.payload);
  RSTONE_ASSERT_EQ(decoded.metadata["trace"], "abc");
}

void TestRpcResponseRoundTrip() {
  rstone::RpcResponse response;
  response.request_id = "req-2";
  response.ok = false;
  response.error_code = "NOT_LEADER";
  response.error_message = "try leader";
  response.payload = "leader=store2";
  response.metadata["region"] = "10";

  const auto encoded = rstone::EncodeRpcResponse(response);
  rstone::RpcResponse decoded;
  RSTONE_ASSERT_TRUE(rstone::DecodeRpcResponse(encoded, &decoded).ok());
  RSTONE_ASSERT_TRUE(!decoded.ok);
  RSTONE_ASSERT_EQ(decoded.error_code, "NOT_LEADER");
  RSTONE_ASSERT_EQ(decoded.payload, response.payload);
  RSTONE_ASSERT_EQ(decoded.metadata["region"], "10");
}

}  // namespace

struct RpcCodecTestRunner {
  RpcCodecTestRunner() {
    TestRpcRequestRoundTrip();
    TestRpcResponseRoundTrip();
  }
};

static RpcCodecTestRunner rpc_codec_test_runner;
