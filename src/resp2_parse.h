// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#pragma once

#include "resp_parse_base.h"

class Resp2Parse : public RespParse {
 public:
  explicit Resp2Parse() = default;

  inline RespResult Parse(std::string&& data) override {
    data_.append(data);
    return ParsePipeline();
  };

  RespParams GetParams() override {
    Reset();
    auto result = std::move(params_);
    ClearParams();
    return result;
  }

  inline void GetParams(RespParams& params) override {
    Reset();
    params.swap(params_);
    ClearParams();
  }

 private:
  inline void Reset() {
    pos_ = 0;
    data_.clear();
  };

  inline void ClearParams() {
    params_.resize(0);
    singleParams_.resize(0);
    singleParamsSize_ = -1;
  }

  RespType PetRespType(char prefix);
  RespResult ParsePipeline();
  std::pair<std::string, RespResult> ReadLine();
  RespResult ParseSimpleString();
  RespResult parseError();
  RespResult ParseInteger();
  RespResult ParseBulkString();
  RespResult ParseArray();
  RespResult ParseResp();
  inline void AppendParams(const std::string& param) { singleParams_.emplace_back(param); };
  inline void MergeParams() {
    if (singleParamsSize_ < 0 || singleParamsSize_ == singleParams_.size()) {
      params_.emplace_back(std::move(singleParams_));
      singleParams_.clear();
    }
  };

 private:
  std::string data_;
  size_t pos_ = 0;
  std::vector<std::string> singleParams_;
  // -1 means not array, 0 means null array
  int singleParamsSize_ = -1;
  RespParams params_;
};