// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#include <gtest/gtest.h>
#include <string>

#include "resp2_parse.h"

TEST(ParseSimpleString, ParsesCorrectly) {
  Resp2Parse parser;
  RespResult result = parser.Parse("+OK\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "OK");
}

TEST(ParseError, ParsesCorrectly) {
  Resp2Parse parser;
  RespResult result = parser.Parse("-Error message\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "Error message");
}

TEST(ParseInteger, ParsesCorrectly) {
  Resp2Parse parser;
  RespResult result = parser.Parse(":1000\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "1000");
}

TEST(ParseBulkString, ParsesCorrectly) {
  Resp2Parse parser;
  RespResult result = parser.Parse("$6\r\nfoobar\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "foobar");
}

TEST(ParseArray, ParsesCorrectly) {
  Resp2Parse parser;
  RespResult result = parser.Parse("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 2);
  EXPECT_EQ(params[0][0], "foo");
  EXPECT_EQ(params[0][1], "bar");
}

TEST(ParseArrayRestSwap, ParsesCorrectly) {
  Resp2Parse parser;
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

TEST(ParseEmptyBulkString, ParsesCorrectly) {
  Resp2Parse parser;
  RespResult result = parser.Parse("$0\r\n\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 1);
  EXPECT_EQ(params[0][0], "");
}

TEST(ParseEmptyArray, ParsesCorrectly) {
  Resp2Parse parser;
  RespResult result = parser.Parse("*0\r\n");
  EXPECT_EQ(result, RespResult::OK);
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 0);
}

TEST(ParseInvalidInput, ReturnsError) {
  Resp2Parse parser;
  RespResult result = parser.Parse("invalid\r\n");
  ASSERT_EQ(result, RespResult::ERROR);
}

TEST(ParseIncomplete, ReturnsWait) {
  Resp2Parse parser;
  RespResult result = parser.Parse("$10\r\nfoobar");
  ASSERT_EQ(result, RespResult::WAIT);
}

TEST(ParseFragment, ParsesCorrectly) {
  Resp2Parse parser;
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

TEST(ParseFragment2, ParsesCorrectly) {
  Resp2Parse parser;
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

TEST(ParseFragment3, ParsesCorrectly) {
  Resp2Parse parser;
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

// test pipeline cmd parse
TEST(ParsePipeline, ParsesCorrectly) {
  Resp2Parse parser;
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

TEST(ParsePipeline2, ParsesCorrectly) {
  Resp2Parse parser;
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

TEST(ParseMultiplexing, ParsesCorrectly) {
  Resp2Parse parser;
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

TEST(ParseMultiplexingInputInvalid, ParsesCorrectly) {
  Resp2Parse parser;
  parser.Parse("*3\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n");
  auto params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 3);
  EXPECT_EQ(params[0][0], "set");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");

  RespResult result = parser.Parse("invalid\r\n");
  ASSERT_EQ(result, RespResult::ERROR);

  parser.Parse("*4\r\n$4\r\nhset\r\n$5\r\nfruit\r\n$5\r\napple\r\n$6\r\nbanana\r\n");
  params = parser.GetParams();
  ASSERT_EQ(params.size(), 1);
  ASSERT_EQ(params[0].size(), 4);
  EXPECT_EQ(params[0][0], "hset");
  EXPECT_EQ(params[0][1], "fruit");
  EXPECT_EQ(params[0][2], "apple");
  EXPECT_EQ(params[0][3], "banana");
}

TEST(ParseMultiplexingPipeline, ParsesCorrectly) {
  Resp2Parse parser;
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
