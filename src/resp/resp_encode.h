// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#pragma once

#include <cstdint>
#include <string>

#include "pstd_string.h"

enum class CmdRes : std::int8_t {
  kNone = 0,
  kOK,
  kPong,
  kSyntaxErr,
  kInvalidInt,
  kInvalidBitInt,
  kInvalidBitOffsetInt,
  kInvalidFloat,
  kOverFlow,
  kNotFound,
  kOutOfRange,
  kInvalidPwd,
  kNoneBgsave,
  kPurgeExist,
  kInvalidParameter,
  kWrongNum,
  kInvalidIndex,
  kInvalidDbType,
  kInvalidDB,
  kInconsistentHashTag,
  kErrOther,
  kUnknownCmd,
  kUnknownSubCmd,
  KIncrByOverFlow,
  kInvalidCursor,
  kWrongLeader,
  kMultiKey,
  kNoAuth,
};

constexpr char CRLF[] = "\r\n";

class RespEncode {
 public:
  virtual ~RespEncode() = default;

  static void AppendBulkString(std::string& str, const std::string& value);
  static void SetBulkStringLen(std::string& str, int64_t ori, const std::string& prefix);

  void Reply(std::string& str) { str.swap(reply_); }

  virtual void SetRes(CmdRes ret, const std::string& content = "") = 0;
  virtual void AppendArrayLen(int64_t ori) = 0;
  virtual void AppendInteger(int64_t ori) = 0;
  virtual void AppendStringRaw(const std::string& value) = 0;
  virtual void AppendSimpleString(const std::string& value) = 0;
  virtual void AppendString(const std::string& value) = 0;
  virtual void AppendString(const char* value, int64_t size) = 0;
  virtual void AppendStringVector(const std::vector<std::string>& strArray) = 0;
  virtual void SetLineString(const std::string& value) = 0;
  virtual void ClearReply() = 0;

 protected:
  std::string reply_;
  CmdRes ret_ = CmdRes::kNone;
};
