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

//! K-way merge over the per-instance key streams that back the SCAN family
//! (issue #142).
//!
//! Kiwi shards the keyspace across several independent RocksDB instances, each
//! of which iterates its own column family in sorted order. Merging those
//! already-sorted streams yields a single globally ordered stream, so keyspace
//! commands can present deterministic, RocksDB-order results instead of a
//! per-instance concatenation.

use std::iter::Peekable;

/// Merges several individually sorted `Vec<u8>` streams into one sorted stream.
///
/// Every input stream must already be ordered — ascending for a forward merge,
/// descending for a reverse merge. The merge is stable: when two streams expose
/// an equal head, the lower-indexed stream is emitted first.
pub(crate) struct MergingIterator<I: Iterator<Item = Vec<u8>>> {
    streams: Vec<Peekable<I>>,
    reverse: bool,
}

impl<I: Iterator<Item = Vec<u8>>> MergingIterator<I> {
    /// Build a merge over `streams`. `reverse` selects descending order and
    /// requires each input stream to be descending.
    pub(crate) fn new(streams: Vec<I>, reverse: bool) -> Self {
        Self {
            streams: streams.into_iter().map(Iterator::peekable).collect(),
            reverse,
        }
    }
}

impl<I: Iterator<Item = Vec<u8>>> Iterator for MergingIterator<I> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // Pick the stream whose current head sorts first (ascending) or last
        // (descending). Heads are cloned so only one stream is borrowed at a
        // time; with a handful of instances this is cheap.
        let mut chosen: Option<(usize, Vec<u8>)> = None;
        for index in 0..self.streams.len() {
            let Some(head) = self.streams[index].peek() else {
                continue;
            };
            let is_better = match &chosen {
                None => true,
                Some((_, best)) => {
                    if self.reverse {
                        head > best
                    } else {
                        head < best
                    }
                }
            };
            if is_better {
                chosen = Some((index, head.clone()));
            }
        }

        match chosen {
            Some((index, _)) => self.streams[index].next(),
            None => None,
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn merge(streams: Vec<Vec<&[u8]>>, reverse: bool) -> Vec<Vec<u8>> {
        let streams = streams
            .into_iter()
            .map(|stream| {
                stream
                    .into_iter()
                    .map(<[u8]>::to_vec)
                    .collect::<Vec<_>>()
                    .into_iter()
            })
            .collect();
        MergingIterator::new(streams, reverse).collect()
    }

    #[test]
    fn forward_merge_interleaves_in_sorted_order() {
        let out = merge(vec![vec![b"a", b"c", b"e"], vec![b"b", b"d", b"f"]], false);
        assert_eq!(
            out,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec(),
            ]
        );
    }

    #[test]
    fn reverse_merge_interleaves_in_descending_order() {
        let out = merge(vec![vec![b"e", b"c", b"a"], vec![b"f", b"d", b"b"]], true);
        assert_eq!(
            out,
            vec![
                b"f".to_vec(),
                b"e".to_vec(),
                b"d".to_vec(),
                b"c".to_vec(),
                b"b".to_vec(),
                b"a".to_vec(),
            ]
        );
    }

    #[test]
    fn merges_three_uneven_streams() {
        let out = merge(
            vec![
                vec![b"a", b"m", b"z"],
                vec![b"b"],
                vec![b"c", b"d", b"n", b"y"],
            ],
            false,
        );
        assert_eq!(
            out,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"m".to_vec(),
                b"n".to_vec(),
                b"y".to_vec(),
                b"z".to_vec(),
            ]
        );
    }

    #[test]
    fn empty_and_single_streams() {
        assert!(merge(vec![], false).is_empty());
        assert!(merge(vec![vec![], vec![]], false).is_empty());
        assert_eq!(
            merge(vec![vec![], vec![b"x"], vec![]], false),
            vec![b"x".to_vec()]
        );
    }

    #[test]
    fn equal_heads_keep_every_element() {
        // Duplicate keys across streams must all be emitted (the merge does not
        // deduplicate), lower-indexed stream first.
        let out = merge(vec![vec![b"k", b"k"], vec![b"k"]], false);
        assert_eq!(out, vec![b"k".to_vec(), b"k".to_vec(), b"k".to_vec()]);
    }
}
