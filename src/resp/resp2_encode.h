// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#pragma once

#include "resp_encode.h"

class Resp2Encode : public RespEncode {
 public:
  void SetRes(CmdRes ret, const std::string& content = "") override;
  void AppendArrayLen(int64_t ori) override { SetBulkStringLen(reply_, ori, "*"); }
  void AppendInteger(int64_t ori) override { SetBulkStringLen(reply_, ori, ":"); }
  void AppendStringRaw(const std::string& value) override { reply_.append(value); }
  void AppendSimpleString(const std::string& value) override;
  void AppendString(const std::string& value) override { AppendBulkString(reply_, value); }
  void AppendString(const char* value, int64_t size) override;
  void AppendStringVector(const std::vector<std::string>& strArray) override;
  void SetLineString(const std::string& value) override { reply_.append(value + CRLF); }
  void ClearReply() override { reply_.clear(); }
};
