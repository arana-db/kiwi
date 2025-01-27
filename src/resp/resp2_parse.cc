// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#include "resp2_parse.h"
#include "log.h"
#include "std_string.h"

RespType Resp2Parse::PetRespType(char prefix) {
  switch (prefix) {
    case '+':
      return RespType::SimpleString;
    case '-':
      return RespType::Error;
    case ':':
      return RespType::Integer;
    case '$':
      return RespType::BulkString;
    case '*':
      return RespType::Array;
    default:
      return RespType::Inline;
  }
}

RespResult Resp2Parse::ParsePipeline() {
  while (pos_ < data_.size()) {
    auto result = ParseResp();
    if (result == RespResult::ERROR) {
      Reset();
      ClearParams();
      return result;
    } else if (result == RespResult::WAIT) {
      MergeParams();
      return result;
    }
    MergeParams();
  }
  return RespResult::OK;
}

std::pair<std::string, RespResult> Resp2Parse::ReadLine() {
  size_t end = data_.find('\n', pos_);
  if (end == std::string::npos) {
    // No \n found
    pos_ -= 1;  // Move back to the last character
    return {"", RespResult::WAIT};
  }
  if (end == pos_) {
    ++pos_;  // first char is \n, skip \n
    return {"", RespResult::OK};
  }
  size_t offset = 1;
  if (data_[end - 1] == '\r') {  // Determine whether the terminator is \r\n
    offset = 2;
    --end;
  }
  std::string line;
  if (end > pos_) {
    line = data_.substr(pos_, end - pos_);
  }
  pos_ = end + offset;  // Move past \r\n
  return {line, RespResult::OK};
}

RespResult Resp2Parse::ParseInline() {
  auto [line, result] = ReadLine();
  if (result != RespResult::OK) {
    return result;
  }
  if (line.empty()) {  // inline command empty string cant append
    singleParamsSize_ = 0;
    return RespResult::OK;
  }
  kstd::StringSplit(line, ' ', singleParams_);
  return RespResult::OK;
}

RespResult Resp2Parse::ParseSimpleString() {
  auto [line, result] = ReadLine();
  if (result != RespResult::OK) {
    return result;
  }
  AppendParams(line);
  return RespResult::OK;
}

RespResult Resp2Parse::parseError() {
  auto [line, result] = ReadLine();
  if (result != RespResult::OK) {
    return result;
  }
  AppendParams(line);
  return RespResult::OK;
}

RespResult Resp2Parse::ParseInteger() {
  auto [line, result] = ReadLine();
  if (result != RespResult::OK) {
    return result;
  }
  AppendParams(line);
  return RespResult::OK;
}

RespResult Resp2Parse::ParseBulkString() {
  auto [line, result] = ReadLine();
  if (result == RespResult::WAIT) {
    return RespResult::WAIT;
  }
  int length;
  if (!kstd::String2int(line, &length)) {
    return RespResult::ERROR;
  }

  if (length <= 0) {
    pos_ += 2;  // Move past \r\n
    AppendParams("");
    return RespResult::OK;  // Null bulk string
  }

  if (pos_ + length + 1 >= data_.size()) {
    // The current index + length +1 exceeds the data length，
    // +1 is to ensure that the last character is \n
    // If the last digit is not \n, then the data is incomplete
    pos_ -= 4;  // Move back to \r\n
    return RespResult::WAIT;
  }
  std::string bulkString = data_.substr(pos_, length);
  pos_ += length + 2;  // Move past the bulk string and \r\n
  AppendParams(bulkString);

  return RespResult::OK;
}

RespResult Resp2Parse::ParseArray() {
  auto [line, result] = ReadLine();
  int count;
  if (!kstd::String2int(line, &count)) {
    return RespResult::ERROR;
  }
  if (count <= 0) {  // Null array
    return RespResult::OK;
  }

  singleParamsSize_ = count;
  for (int i = 0; i < count; ++i) {
    if (result = ParseResp(); result != RespResult::OK) {
      return result;
    }
  }
  return RespResult::OK;
}

RespResult Resp2Parse::ParseResp() {
  if (pos_ >= data_.size()) {
    return RespResult::WAIT;
  }
  char prefix = data_[pos_++];
  switch (PetRespType(prefix)) {
    case RespType::SimpleString:
      return ParseSimpleString();
    case RespType::Error:
      return parseError();
    case RespType::Integer:
      return ParseInteger();
    case RespType::BulkString:
      return ParseBulkString();
    case RespType::Array:
      return ParseArray();
    case RespType::Inline:
      pos_--;  // Move back to the first character of the inline command
      return ParseInline();
    default:
      return RespResult::ERROR;
  }
}

void Resp2Parse::MergeParams() {
  if (singleParamsSize_ < 0 || (singleParamsSize_ != 0 && singleParamsSize_ == singleParams_.size())) {
    params_.emplace_back(std::move(singleParams_));
    singleParams_.clear();
  }
}
