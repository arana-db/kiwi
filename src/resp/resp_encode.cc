// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#include "resp_encode.h"

void RespEncode::AppendBulkString(std::string& str, const std::string& value) {
  SetBulkStringLen(str, static_cast<int64_t>(value.size()), "$");
  str.append(value.data(), value.size());
  str.append(CRLF);
}

void RespEncode::SetBulkStringLen(std::string& str, int64_t ori, const std::string& prefix) {
  str.append(prefix);
  str.append(pstd::Int2string(ori));
  str.append(CRLF);
}
