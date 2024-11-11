// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef kiwi_SLOT_H_
#define kiwi_SLOT_H_

#include <cstdint>
#include <memory>
#include <string>

// get db instance number of the key
uint32_t GetSlotID(const std::string& str);

// get db instance number of the key
uint32_t GetSlotsID(const std::string& str, uint32_t* pcrc, int* phastag);

#endif
