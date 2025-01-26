// Copyright (c) 2023-present, arana-db Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#pragma once

#include <string>
#include <vector>

enum class RespType { SimpleString, Error, Integer, BulkString, Array, Inline };

enum class RespResult { OK, ERROR, WAIT };

using RespParams = std::vector<std::vector<std::string>>;

class RespParse {
 public:
  virtual RespResult Parse(std::string&& data) = 0;
  virtual RespParams GetParams() = 0;
  virtual void GetParams(RespParams& params) = 0;
  virtual ~RespParse() = default;
};
