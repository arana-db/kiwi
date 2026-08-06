// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use bytes::Bytes;
use resp::{
    RespData, RespVersion,
    encode::{RespEncode, RespEncoder},
};

struct CountingAllocator;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn count_allocations(f: impl FnOnce()) -> usize {
    ALLOCATION_COUNT.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
    // Ensure the global counting flag is always reset, even if the measured
    // closure panics, so it cannot leak into later tests in the binary.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    match result {
        Ok(()) => ALLOCATION_COUNT.load(Ordering::Relaxed),
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[test]
fn resp2_arrays_encode_without_normalization_and_downgrade_nested_resp3_values() {
    let data = RespData::Array(Some(vec![
        RespData::Integer(1),
        RespData::BulkString(Some(Bytes::from_static(b"value"))),
    ]));
    let mut encoder = RespEncoder::new(RespVersion::RESP2);

    // Warm the output buffer so the measured encode does not need to grow it.
    encoder.encode_resp_data(&data).clear();

    let allocations = count_allocations(|| {
        encoder.encode_resp_data(&data);
    });

    assert_eq!(
        allocations, 0,
        "RESP2-compatible aggregates should encode without a normalized copy"
    );
    assert_eq!(
        encoder.get_response(),
        Bytes::from_static(b"*2\r\n:1\r\n$5\r\nvalue\r\n")
    );

    let data = RespData::Array(Some(vec![RespData::Boolean(true)]));
    encoder.clear().encode_resp_data(&data);

    assert_eq!(encoder.get_response(), Bytes::from_static(b"*1\r\n:1\r\n"));
}
