// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#include <fmt/format.h>

#include "resp2_encode.h"

void Resp2Encode::SetRes(CmdRes ret, const std::string& content) {
  ret_ = ret;
  switch (ret_) {
    case CmdRes::kOK:
      SetLineString("+OK");
      break;
    case CmdRes::kPong:
      SetLineString("+PONG");
      break;
    case CmdRes::kSyntaxErr:
      SetLineString("-ERR syntax error");
      break;
    case CmdRes::kUnknownCmd:
      AppendStringRaw(fmt::format("-ERR unknown command '{}'\r\n", content));
      break;
    case CmdRes::kUnknownSubCmd:
      AppendStringRaw(fmt::format("-ERR unknown sub command '{}'\r\n", content));
      break;
    case CmdRes::kInvalidInt:
      SetLineString("-ERR value is not an integer or out of range");
      break;
    case CmdRes::kInvalidBitInt:
      SetLineString("-ERR bit is not an integer or out of range");
      break;
    case CmdRes::kInvalidBitOffsetInt:
      SetLineString("-ERR bit offset is not an integer or out of range");
      break;
    case CmdRes::kInvalidBitPosArgument:
      SetLineString("-ERR The bit argument must be 1 or 0.");
      break;
    case CmdRes::kWrongBitOpNotNum:
      SetLineString("-ERR BITOP NOT must be called with a single source key.");
      break;
    case CmdRes::kInvalidFloat:
      SetLineString("-ERR value is not a valid float");
      break;
    case CmdRes::kOverFlow:
      SetLineString("-ERR increment or decrement would overflow");
      break;
    case CmdRes::kNotFound:
      SetLineString("-ERR no such key");
      break;
    case CmdRes::kOutOfRange:
      SetLineString("-ERR index out of range");
      break;
    case CmdRes::kInvalidPwd:
      SetLineString("-ERR invalid password");
      break;
    case CmdRes::kNoneBgsave:
      SetLineString("-ERR No BGSave Works now");
      break;
    case CmdRes::kPurgeExist:
      SetLineString("-ERR binlog already in purging...");
      break;
    case CmdRes::kInvalidParameter:
      SetLineString("-ERR Invalid Argument");
      break;
    case CmdRes::kWrongNum:
      AppendStringRaw(fmt::format("-ERR wrong number of arguments for '{}' command\r\n", content));
      break;
    case CmdRes::kInvalidIndex:
      AppendStringRaw(fmt::format("-ERR invalid DB index for '{}'\r\n", content));
      break;
    case CmdRes::kInvalidDbType:
      AppendStringRaw(fmt::format("-ERR invalid DB for '{}'\r\n", content));
      break;
    case CmdRes::kInconsistentHashTag:
      SetLineString("-ERR parameters hashtag is inconsistent");
    case CmdRes::kInvalidDB:
      AppendStringRaw(fmt::format("-ERR invalid DB for '{}'\r\n", content));
      break;
    case CmdRes::kErrOther:
      AppendStringRaw(fmt::format("-ERR {}\r\n", content));
      break;
    case CmdRes::KIncrByOverFlow:
      AppendStringRaw(fmt::format("-ERR increment would produce NaN or Infinity {}\r\n", content));
      break;
    case CmdRes::kInvalidCursor:
      AppendStringRaw("-ERR invalid cursor");
      break;
    case CmdRes::kWrongLeader:
      AppendStringRaw(fmt::format("-ERR wrong leader {}\r\n", content));
      break;
    case CmdRes::kMultiKey:
      AppendStringRaw(
          fmt::format("-WRONGTYPE Operation against a key holding the wrong kind of value {}\r\n", content));
      break;
    case CmdRes::kDirtyExec:
      AppendStringRaw("-ERR EXECABORT Transaction discarded because of previous errors.");
      AppendStringRaw(CRLF);
      break;
    case CmdRes::kPErrorWatch:
      AppendStringRaw("-ERR WATCH inside MULTI is not allowed");
      AppendStringRaw(CRLF);
      break;
    case CmdRes::kQueued:
      AppendStringRaw("+QUEUED");
      AppendStringRaw(CRLF);
      break;
    case CmdRes::kNoAuth:
      SetLineString("-NOAUTH Authentication required");
      break;
    default:
      break;
  }
}

void Resp2Encode::AppendStringVector(const std::vector<std::string>& strArray) {
  if (strArray.empty()) {
    AppendArrayLen(int64_t(0));
    return;
  }
  AppendArrayLen(static_cast<int64_t>(strArray.size()));
  for (const auto& item : strArray) {
    AppendString(item);
  }
}

void Resp2Encode::AppendString(const char* value, int64_t size) {
  SetBulkStringLen(reply_, size, "$");
  reply_.append(value, size);
  reply_.append(CRLF);
}

void Resp2Encode::AppendSimpleString(const std::string& value) { reply_.append(fmt::format("+{}\r\n", value)); }
