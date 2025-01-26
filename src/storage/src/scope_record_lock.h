//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "src/lock_mgr.h"
#include "std/scope_record_lock.h"
#include "storage/storage.h"

namespace storage {

using ScopeRecordLock = kstd::lock::ScopeRecordLock;
using MultiScopeRecordLock = kstd::lock::MultiScopeRecordLock;

}  // namespace storage
