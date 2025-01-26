// Copyright (c) 2023-present, arana-db Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#include <gtest/gtest.h>
#include <string>

#include "../resp2_parse.h"

class Resp2ParseTest : public ::testing::Test {
 protected:
  Resp2Parse parser;
};

TEST_F(Resp2ParseTest, ParsesCorrectly) {
  RespResult result = parser.Parse("+OK\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "OK");
}

TEST_F(Resp2ParseTest, ParseError) {
  RespResult result = parser.Parse("-Error message\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "Error message");
}

TEST_F(Resp2ParseTest, ParseInteger) {
  RespResult result = parser.Parse(":1000\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "1000");
}

TEST_F(Resp2ParseTest, ParseInline) {
  RespResult result = parser.Parse("ping\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "ping");

  result = parser.Parse("PING\r\n\r\n");
  EXPECT_EQ(result, RespResult::OK);
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "PING");

  result = parser.Parse("PING\n");
  EXPECT_EQ(result, RespResult::OK);
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "PING");

  result = parser.Parse("PING\n\n");
  EXPECT_EQ(result, RespResult::OK);
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "PING");

  result = parser.Parse("PING\r\n\n");
  EXPECT_EQ(result, RespResult::OK);
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "PING");
}

TEST_F(Resp2ParseTest, ParseInlineParams) {
  RespResult result = parser.Parse("hmget fruit apple banana watermelon\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 5);
  EXPECT_EQ(params[0][0], "hmget");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
  EXPECT_EQ(params[0][4], "watermelon");
}

TEST_F(Resp2ParseTest, ParseMultipleInline) {
  RespResult result = parser.Parse("ping\r\nhmget fruit apple banana watermelon\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 2);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "ping");
  ASSERT_EQ(params[1].size(), 5);
  EXPECT_EQ(params[1][0], "hmget");
  EXPECT_EQ(params[1][1], "fruit");
  EXPECT_EQ(params[1][2], "apple");
  EXPECT_EQ(params[1][3], "banana");
  EXPECT_EQ(params[1][4], "watermelon");
}

TEST_F(Resp2ParseTest, ParseBulkString) {
  RespResult result = parser.Parse("$6\r\nfoobar\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "foobar");
}

TEST_F(Resp2ParseTest, ParseArray) {
  RespResult result = parser.Parse("*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$-1\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 3);
  EXPECT_EQ(params[0][0], "foo");
  EXPECT_EQ(params[0][1], "bar");
  EXPECT_EQ(params[0][2], "");
}

TEST_F(Resp2ParseTest, ParseArrayRestSwap) {
  RespResult result = parser.Parse("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
  EXPECT_EQ(result, RespResult::OK);
  RespParams params;
  parser.GetParams(params);
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 2);
  EXPECT_EQ(params[0][0], "foo");
  EXPECT_EQ(params[0][1], "bar");

  auto oldParams = parser.GetParams();
  ASSERT_EQ(oldParams.size(), 0);
}

TEST_F(Resp2ParseTest, ParseEmptyBulkString) {
  RespResult result = parser.Parse("$0\r\n\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "");
}

TEST_F(Resp2ParseTest, ParseEmptyArray) {
  RespResult result = parser.Parse("*0\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 0);
}

TEST_F(Resp2ParseTest, ParseIncomplete) {
  RespResult result = parser.Parse("$10\r\nfoobar");
  ASSERT_EQ(result, RespResult::WAIT);
}

TEST_F(Resp2ParseTest, ParseFragment) {
  RespResult result = parser.Parse("*5\r\n$5\r\nhmget\r\n$5\r\nfruit\r\n$5\r\napple\r\n");
  ASSERT_EQ(result, RespResult::WAIT);
  result = parser.Parse("$6\r\nbanana\r\n$10\r\nwatermelon\r\n");
  ASSERT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 5);
  EXPECT_EQ(params[0][0], "hmget");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
  EXPECT_EQ(params[0][4], "watermelon");
}

TEST_F(Resp2ParseTest, ParseFragment2) {
  RespResult result = parser.Parse("*5\r\n$5\r\nhmget\r\n$5\r\nfrui");
  EXPECT_EQ(result, RespResult::WAIT);
  result = parser.Parse("t\r\n$5\r\napple\r\n$6\r");
  EXPECT_EQ(result, RespResult::WAIT);
  result = parser.Parse("\nbanana\r\n$10\r\nwatermelon\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 5);
  EXPECT_EQ(params[0][0], "hmget");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
  EXPECT_EQ(params[0][4], "watermelon");
}

TEST_F(Resp2ParseTest, ParseFragment3) {
  RespResult result = parser.Parse("*5\r\n$5\r\nhmget\r\n$5");
  EXPECT_EQ(result, RespResult::WAIT);
  result = parser.Parse("\r\nfruit\r\n$5\r\napple\r\n$");
  EXPECT_EQ(result, RespResult::WAIT);
  result = parser.Parse("6\r\nbanana\r");
  EXPECT_EQ(result, RespResult::WAIT);
  result = parser.Parse("\n$10\r\nwatermelon\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 5);
  EXPECT_EQ(params[0][0], "hmget");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
  EXPECT_EQ(params[0][4], "watermelon");
}

// TEST_F pipeline cmd parse
TEST_F(Resp2ParseTest, ParsePipeline) {
  RespResult result = parser.Parse("*5\r\n$5\r\nhmget\r\n$5\r\nfruit\r\n$5\r\napple\r\n$6\r\nbanana\r");
  EXPECT_EQ(result, RespResult::WAIT);
  result = parser.Parse("\n$10\r\nwatermelon\r\n");
  EXPECT_EQ(result, RespResult::OK);

  // second cmd
  result = parser.Parse("*3\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 2);
  ASSERT_EQ(params[0].size(), 5);
  EXPECT_EQ(params[0][0], "hmget");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
  EXPECT_EQ(params[0][4], "watermelon");

  ASSERT_EQ(params[1].size(), 3);
  EXPECT_EQ(params[1][0], "set");
  EXPECT_EQ(params[1][1], "fruit");
  EXPECT_EQ(params[1][2], "apple");
}

TEST_F(Resp2ParseTest, ParsePipeline2) {
  RespResult result = parser.Parse("*5\r\n$5\r\nhmget\r\n$5\r\nfruit\r\n$5\r\napple\r\n$6\r\nbanana\r");
  EXPECT_EQ(result, RespResult::WAIT);
  // second cmd data in first cmd data
  result = parser.Parse("\n$10\r\nwatermelon\r\n*3\r\n$3\r\nset\r\n$5\r");
  EXPECT_EQ(result, RespResult::WAIT);

  result = parser.Parse("\nfruit\r\n$5\r\napple\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 2);
  ASSERT_EQ(params[0].size(), 5);
  EXPECT_EQ(params[0][0], "hmget");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
  EXPECT_EQ(params[0][4], "watermelon");

  ASSERT_EQ(params[1].size(), 3);
  EXPECT_EQ(params[1][0], "set");
  EXPECT_EQ(params[1][1], "fruit");
  EXPECT_EQ(params[1][2], "apple");
}

TEST_F(Resp2ParseTest, ParseMultiplexing) {
  parser.Parse("*3\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n");
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 3);
  EXPECT_EQ(params[0][0], "set");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");

  parser.Parse("*4\r\n$4\r\nhset\r\n$5\r\nfruit\r\n$5\r\napple\r\n$6\r\nbanana\r\n");
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 4);
  EXPECT_EQ(params[0][0], "hset");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
}

TEST_F(Resp2ParseTest, ParseMultiplexingInline) {
  parser.Parse("*3\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n");
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 3);
  EXPECT_EQ(params[0][0], "set");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");

  RespResult result = parser.Parse("ping\r\n");
  ASSERT_EQ(result, RespResult::OK);
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "ping");

  parser.Parse("*4\r\n$4\r\nhset\r\n$5\r\nfruit\r\n$5\r\napple\r\n$6\r\nbanana\r\n");
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 4);
  EXPECT_EQ(params[0][0], "hset");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
}

TEST_F(Resp2ParseTest, ParseMultiplexingPipeline) {
  RespResult result = parser.Parse("*5\r\n$5\r\nhmget\r\n$5\r\nfruit\r\n$5\r\napple\r\n$6\r\nbanana\r");
  EXPECT_EQ(result, RespResult::WAIT);
  // second cmd data in first cmd data
  result = parser.Parse("\n$10\r\nwatermelon\r\n*3\r\n$3\r\nset\r\n$5\r");
  EXPECT_EQ(result, RespResult::WAIT);

  result = parser.Parse("\nfruit\r\n$5\r\napple\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 2);
  ASSERT_EQ(params[0].size(), 5);
  EXPECT_EQ(params[0][0], "hmget");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
  EXPECT_EQ(params[0][4], "watermelon");

  ASSERT_EQ(params[1].size(), 3);
  EXPECT_EQ(params[1][0], "set");
  EXPECT_EQ(params[1][1], "fruit");
  EXPECT_EQ(params[1][2], "apple");

  parser.Parse("*3\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n");
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 3);
  EXPECT_EQ(params[0][0], "set");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");

  result = parser.Parse("*5\r\n$5\r\nhmget\r\n$5\r\nfruit\r\n$5\r\napple\r\n$6\r\nbanana\r");
  EXPECT_EQ(result, RespResult::WAIT);
  // second cmd data in first cmd data
  result = parser.Parse("\n$10\r\nwatermelon\r\n*3\r\n$3\r\nset\r\n$5\r");
  EXPECT_EQ(result, RespResult::WAIT);

  result = parser.Parse("\nfruit\r\n$5\r\napple\r\n");
  EXPECT_EQ(result, RespResult::OK);
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 2);
  ASSERT_EQ(params[0].size(), 5);
  EXPECT_EQ(params[0][0], "hmget");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
  EXPECT_EQ(params[0][4], "watermelon");

  ASSERT_EQ(params[1].size(), 3);
  EXPECT_EQ(params[1][0], "set");
  EXPECT_EQ(params[1][1], "fruit");
  EXPECT_EQ(params[1][2], "apple");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
