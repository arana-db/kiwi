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

//! Benchmark suite for ZSet score key encoding/decoding performance.
//!
//! This benchmark measures the performance improvements from optimized capacity
//! estimation in the ZSetsScoreKey implementation.
//!
//! ## Running Benchmarks
//!
//! ```bash
//! cargo bench --bench zset_score_key_benchmark -- --verbose
//! ```
//!
//! The results will show performance metrics for:
//! - Small, medium, and large key/member combinations
//! - Keys with null bytes (special encoding)
//! - Edge cases (empty keys/members)
//! - Special float values (infinity, NaN)
//! - Seek key operations
//! - Round-trip encode/decode operations

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use storage::zset_score_key_format::{ZSetsScoreKey, ParsedZSetsScoreKey};

/// Benchmark ZSetsScoreKey encoding with various key and member sizes.
fn bench_zset_score_key_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("zset_score_key_encode");

    // Test different key/member sizes
    let test_cases = vec![
        ("small", b"key".as_slice(), b"member".as_slice()),
        ("medium", b"user:12345:profile:settings:key".as_slice(), 
         b"john.doe@example.com:extra:data".as_slice()),
        ("key_with_nulls", b"key\x00with\x00null\x00bytes".as_slice(), b"member".as_slice()),
        ("empty", b"".as_slice(), b"".as_slice()),
    ];

    for (name, key, member) in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &(key, member), |b, &(key, member)| {
            b.iter(|| {
                let score_key = ZSetsScoreKey::new(
                    black_box(key),
                    black_box(1),
                    black_box(3.14),
                    black_box(member),
                );
                score_key.encode()
            })
        });
    }

    // Large key and member benchmark
    group.bench_function("large_key_member", |b| {
        let large_key = vec![b'k'; 256];
        let large_member = vec![b'm'; 512];
        b.iter(|| {
            let score_key = ZSetsScoreKey::new(
                black_box(&large_key),
                black_box(1),
                black_box(999.999),
                black_box(&large_member),
            );
            score_key.encode()
        })
    });

    // Special float values
    let special_cases = vec![
        ("infinity", f64::INFINITY),
        ("neg_infinity", f64::NEG_INFINITY),
        ("nan", f64::NAN),
        ("zero", 0.0),
    ];

    for (name, score) in special_cases {
        group.bench_with_input(BenchmarkId::new("special_score", name), &score, |b, &score| {
            b.iter(|| {
                let key = ZSetsScoreKey::new(
                    black_box(b"key"),
                    black_box(1),
                    black_box(score),
                    black_box(b"member"),
                );
                key.encode()
            })
        });
    }

    group.finish();
}

/// Benchmark ZSetsScoreKey seek key encoding.
fn bench_zset_score_key_encode_seek(c: &mut Criterion) {
    let mut group = c.benchmark_group("zset_score_key_encode_seek");

    let test_cases = vec![
        ("small", b"key".as_slice()),
        ("medium", b"user:12345:profile:settings:key".as_slice()),
        ("large", &vec![b'k'; 256][..]),
    ];

    for (name, key) in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &key, |b, &key| {
            b.iter(|| {
                let score_key = ZSetsScoreKey::new(
                    black_box(key),
                    black_box(1),
                    black_box(42.5),
                    black_box(b"member"),
                );
                score_key.encode_seek_key()
            })
        });
    }

    group.finish();
}

/// Benchmark parsing of encoded ZSet score keys.
fn bench_zset_score_key_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("zset_score_key_parse");

    // Setup: create and encode test keys of different sizes
    let test_cases = vec![
        ("small", ZSetsScoreKey::new(b"key", 42, 3.14159, b"member")),
        ("medium", ZSetsScoreKey::new(b"user:12345:profile:settings:key", 42, 3.14159, 
                                       b"john.doe@example.com:extra:data")),
    ];

    for (name, score_key) in test_cases {
        let encoded = score_key.encode().unwrap();
        group.bench_with_input(BenchmarkId::new("parse", name), &encoded, |b, encoded| {
            b.iter(|| ParsedZSetsScoreKey::new(black_box(encoded)))
        });
    }

    group.finish();
}

/// Benchmark encode-then-parse roundtrip.
fn bench_zset_score_key_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("zset_score_key_roundtrip");

    group.bench_function("small_roundtrip", |b| {
        b.iter(|| {
            let key = ZSetsScoreKey::new(
                black_box(b"key"),
                black_box(1),
                black_box(2.71828),
                black_box(b"member"),
            );
            let encoded = key.encode().unwrap();
            ParsedZSetsScoreKey::new(&encoded)
        })
    });

    group.bench_function("large_roundtrip", |b| {
        let large_key = vec![b'k'; 256];
        let large_member = vec![b'm'; 512];
        b.iter(|| {
            let key = ZSetsScoreKey::new(
                black_box(&large_key),
                black_box(1),
                black_box(2.71828),
                black_box(&large_member),
            );
            let encoded = key.encode().unwrap();
            ParsedZSetsScoreKey::new(&encoded)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_zset_score_key_encode,
    bench_zset_score_key_encode_seek,
    bench_zset_score_key_parse,
    bench_zset_score_key_roundtrip,
);

criterion_main!(benches);
