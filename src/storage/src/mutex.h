//  Copyright (c) 2017-present, Arana/Kiwi Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <memory>

#include "rocksdb/status.h"

#include "std/mutex.h"

namespace storage {

using Status = rocksdb::Status;

using Mutex = kstd::lock::Mutex;
using CondVar = kstd::lock::CondVar;
using MutexFactory = kstd::lock::MutexFactory;

}  // namespace storage
