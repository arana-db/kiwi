// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#include <gtest/gtest.h>
#include <string>

#include "../resp2_encode.h"

class Resp2EncodeTest : public ::testing::Test {
 protected:
  Resp2Encode encoder;
};

TEST_F(Resp2EncodeTest, AppendArrayLen) {
  encoder.AppendArrayLen(5);
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "*5\r\n");
}

TEST_F(Resp2EncodeTest, AppendInteger) {
  encoder.AppendInteger(42);
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, ":42\r\n");
}

TEST_F(Resp2EncodeTest, AppendStringRaw) {
  encoder.AppendStringRaw("test");
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "test");
}

TEST_F(Resp2EncodeTest, AppendSimpleString) {
  encoder.AppendSimpleString("OK");
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "+OK\r\n");
}

TEST_F(Resp2EncodeTest, AppendString) {
  encoder.AppendString("test");
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "$4\r\ntest\r\n");
}

TEST_F(Resp2EncodeTest, AppendStringWithSize) {
  const char* value = "test";
  encoder.AppendString(value, 4);
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "$4\r\ntest\r\n");
}

TEST_F(Resp2EncodeTest, AppendStringVector) {
  std::vector<std::string> strArray = {"one", "two", "three"};
  encoder.AppendStringVector(strArray);
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n");
}

TEST_F(Resp2EncodeTest, SetLineString) {
  std::string value = "line";
  encoder.SetLineString(value);
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "line\r\n");
}

TEST_F(Resp2EncodeTest, SetRes) {
  encoder.SetRes(CmdRes::kOK);
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "+OK\r\n");

  encoder.SetRes(CmdRes::kWrongNum, "hmget");
  reply.clear();
  encoder.Reply(reply);
  ASSERT_EQ(reply, "-ERR wrong number of arguments for 'hmget' command\r\n");
}

TEST_F(Resp2EncodeTest, ClearReply) {
  encoder.AppendSimpleString("OK");
  encoder.ClearReply();
  std::string reply;
  encoder.Reply(reply);
  ASSERT_EQ(reply, "");
}

TEST_F(Resp2EncodeTest, Array) {
  encoder.AppendArrayLen(3);
  encoder.AppendStringVector({"array1_1", "array1_2", "array1_3"});
  encoder.AppendSimpleString("simpleString");
  encoder.AppendInteger(666);
  encoder.AppendStringVector({"array3_1", "array3_2", "array3_3"});

  std::string reply;
  encoder.Reply(reply);

  std::string expected =
      "*3\r\n$8\r\narray1_1\r\n$8\r\narray1_2\r\n$8\r\narray1_3\r\n+simpleString\r\n:666\r\n*3\r\n$8\r\narray3_1\r\n$"
      "8\r\narray3_2\r\n$8\r\narray3_3\r\n";
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
