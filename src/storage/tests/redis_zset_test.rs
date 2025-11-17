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

mod redis_zset_test {
    use kstd::lock_mgr::LockMgr;
    use std::sync::Arc;
    use storage::{
        BgTaskHandler, Redis, ScoreMember, StorageOptions, safe_cleanup_test_db,
        unique_test_db_path,
    };

    fn create_test_redis() -> Redis {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 0, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        redis
    }

    #[test]
    fn test_zscore_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with scores
        let score_members = vec![
            ScoreMember::new(1.5, b"member1".to_vec()),
            ScoreMember::new(2.5, b"member2".to_vec()),
            ScoreMember::new(3.5, b"member3".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 3);

        // Test existing members
        let mut score = None;
        redis.zscore(key, b"member1", &mut score).unwrap();
        assert!(score.is_some());
        let score_str = String::from_utf8(score.unwrap()).unwrap();
        assert_eq!(score_str, "1.5");

        let mut score = None;
        redis.zscore(key, b"member2", &mut score).unwrap();
        assert!(score.is_some());
        let score_str = String::from_utf8(score.unwrap()).unwrap();
        assert_eq!(score_str, "2.5");

        // Test non-existing member
        let mut score = None;
        redis.zscore(key, b"nonexistent", &mut score).unwrap();
        assert!(score.is_none());
    }

    #[test]
    fn test_zscore_nonexistent_key() {
        let redis = create_test_redis();
        let key = b"nonexistent_key";

        let mut score = None;
        redis.zscore(key, b"member", &mut score).unwrap();
        assert!(score.is_none());
    }

    #[test]
    fn test_zscan_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with scores
        let score_members = vec![
            ScoreMember::new(1.0, b"apple".to_vec()),
            ScoreMember::new(2.0, b"banana".to_vec()),
            ScoreMember::new(3.0, b"cherry".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 3);

        // Scan all members
        let (next_cursor, results) = redis.zscan(key, 0, None, Some(10)).unwrap();
        println!(
            "Next cursor: {}, Results count: {}",
            next_cursor,
            results.len()
        );
        println!("Results: {:?}", results);

        // For now, just check that we get some results
        assert!(results.len() > 0, "Should return at least some results");
    }

    #[test]
    fn test_zscan_with_pattern() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with scores
        let score_members = vec![
            ScoreMember::new(1.0, b"apple".to_vec()),
            ScoreMember::new(2.0, b"apricot".to_vec()),
            ScoreMember::new(3.0, b"banana".to_vec()),
            ScoreMember::new(4.0, b"cherry".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 4);

        // Scan with pattern matching "ap*"
        let (next_cursor, results) = redis.zscan(key, 0, Some("ap*"), Some(10)).unwrap();
        assert_eq!(next_cursor, 0);
        assert_eq!(results.len(), 2);

        // Should match "apple" and "apricot"
        assert!(results.iter().any(|(member, _)| member == "apple"));
        assert!(results.iter().any(|(member, _)| member == "apricot"));
    }

    #[test]
    fn test_zscan_nonexistent_key() {
        let redis = create_test_redis();
        let key = b"nonexistent_key";

        let (next_cursor, results) = redis.zscan(key, 0, None, Some(10)).unwrap();
        assert_eq!(next_cursor, 0);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_zrank_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with different scores
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
            ScoreMember::new(3.0, b"member3".to_vec()),
            ScoreMember::new(4.0, b"member4".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 4);

        // Test ranks
        let mut rank = None;
        redis.zrank(key, b"member1", &mut rank).unwrap();
        assert_eq!(rank, Some(0));

        let mut rank = None;
        redis.zrank(key, b"member2", &mut rank).unwrap();
        assert_eq!(rank, Some(1));

        let mut rank = None;
        redis.zrank(key, b"member3", &mut rank).unwrap();
        assert_eq!(rank, Some(2));

        let mut rank = None;
        redis.zrank(key, b"member4", &mut rank).unwrap();
        assert_eq!(rank, Some(3));
    }

    #[test]
    fn test_zrank_nonexistent_member() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Test non-existent member
        let mut rank = None;
        redis.zrank(key, b"nonexistent", &mut rank).unwrap();
        assert_eq!(rank, None);
    }

    #[test]
    fn test_zrank_nonexistent_key() {
        let redis = create_test_redis();
        let key = b"nonexistent_key";

        let mut rank = None;
        redis.zrank(key, b"member", &mut rank).unwrap();
        assert_eq!(rank, None);
    }

    #[test]
    fn test_zrem_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
            ScoreMember::new(3.0, b"member3".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 3);

        // Remove one member
        let mut removed = 0;
        redis
            .zrem(key, &[b"member2".to_vec()], &mut removed)
            .unwrap();
        assert_eq!(removed, 1);

        // Check cardinality
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);

        // Verify member is removed
        let mut score = None;
        redis.zscore(key, b"member2", &mut score).unwrap();
        assert_eq!(score, None);
    }

    #[test]
    fn test_zrem_multiple_members() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
            ScoreMember::new(3.0, b"member3".to_vec()),
            ScoreMember::new(4.0, b"member4".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 4);

        // Remove multiple members
        let mut removed = 0;
        redis
            .zrem(
                key,
                &[b"member1".to_vec(), b"member3".to_vec()],
                &mut removed,
            )
            .unwrap();
        assert_eq!(removed, 2);

        // Check cardinality
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);
    }

    #[test]
    fn test_zrem_nonexistent_member() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![ScoreMember::new(1.0, b"member1".to_vec())];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Try to remove non-existent member
        let mut removed = 0;
        redis
            .zrem(key, &[b"nonexistent".to_vec()], &mut removed)
            .unwrap();
        assert_eq!(removed, 0);

        // Check cardinality unchanged
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 1);
    }

    #[test]
    fn test_zrem_nonexistent_key() {
        let redis = create_test_redis();
        let key = b"nonexistent_key";

        let mut removed = 0;
        redis
            .zrem(key, &[b"member".to_vec()], &mut removed)
            .unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_zrange_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with scores
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
            ScoreMember::new(3.0, b"member3".to_vec()),
            ScoreMember::new(4.0, b"member4".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 4);

        // Get all members
        let mut result = Vec::new();
        redis.zrange(key, 0, -1, false, &mut result).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], b"member1");
        assert_eq!(result[1], b"member2");
        assert_eq!(result[2], b"member3");
        assert_eq!(result[3], b"member4");
    }

    #[test]
    fn test_zrange_with_scores() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with scores
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
            ScoreMember::new(3.0, b"member3".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get all members with scores
        let mut result = Vec::new();
        redis.zrange(key, 0, -1, true, &mut result).unwrap();
        assert_eq!(result.len(), 6); // 3 members * 2 (member + score)
        assert_eq!(result[0], b"member1");
        assert_eq!(result[1], b"1");
        assert_eq!(result[2], b"member2");
        assert_eq!(result[3], b"2");
        assert_eq!(result[4], b"member3");
        assert_eq!(result[5], b"3");
    }

    #[test]
    fn test_zrange_partial() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
            ScoreMember::new(3.0, b"member3".to_vec()),
            ScoreMember::new(4.0, b"member4".to_vec()),
            ScoreMember::new(5.0, b"member5".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get members from index 1 to 3
        let mut result = Vec::new();
        redis.zrange(key, 1, 3, false, &mut result).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"member2");
        assert_eq!(result[1], b"member3");
        assert_eq!(result[2], b"member4");
    }

    #[test]
    fn test_zrange_negative_indices() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
            ScoreMember::new(3.0, b"member3".to_vec()),
            ScoreMember::new(4.0, b"member4".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get last two members
        let mut result = Vec::new();
        redis.zrange(key, -2, -1, false, &mut result).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"member3");
        assert_eq!(result[1], b"member4");
    }

    #[test]
    fn test_zrange_nonexistent_key() {
        let redis = create_test_redis();
        let key = b"nonexistent_key";

        let mut result = Vec::new();
        redis.zrange(key, 0, -1, false, &mut result).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_zrange_empty_range() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get empty range (start > stop)
        let mut result = Vec::new();
        redis.zrange(key, 5, 3, false, &mut result).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_zlexcount_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with the same score for lexicographical ordering
        let score_members = vec![
            ScoreMember::new(0.0, b"a".to_vec()),
            ScoreMember::new(0.0, b"b".to_vec()),
            ScoreMember::new(0.0, b"c".to_vec()),
            ScoreMember::new(0.0, b"d".to_vec()),
            ScoreMember::new(0.0, b"e".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 5);

        // Count all members from - to +
        let mut count = 0;
        redis.zlexcount(key, b"-", b"+", &mut count).unwrap();
        assert_eq!(count, 5);

        // Count members from [b to [d (inclusive)
        let mut count = 0;
        redis.zlexcount(key, b"[b", b"[d", &mut count).unwrap();
        assert_eq!(count, 3); // b, c, d

        // Count members from (b to (d (exclusive)
        let mut count = 0;
        redis.zlexcount(key, b"(b", b"(d", &mut count).unwrap();
        assert_eq!(count, 1); // only c
    }

    #[test]
    fn test_zlexcount_inclusive_exclusive() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(0.0, b"alpha".to_vec()),
            ScoreMember::new(0.0, b"beta".to_vec()),
            ScoreMember::new(0.0, b"gamma".to_vec()),
            ScoreMember::new(0.0, b"delta".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Inclusive min, exclusive max: [beta to (delta
        let mut count = 0;
        redis
            .zlexcount(key, b"[beta", b"(delta", &mut count)
            .unwrap();
        assert_eq!(count, 1); // beta (gamma > delta lexicographically)

        // Exclusive min, inclusive max: (beta to [delta
        let mut count = 0;
        redis
            .zlexcount(key, b"(beta", b"[delta", &mut count)
            .unwrap();
        assert_eq!(count, 1); // delta (gamma > delta lexicographically)

        // Both exclusive: (alpha to (gamma
        let mut count = 0;
        redis
            .zlexcount(key, b"(alpha", b"(gamma", &mut count)
            .unwrap();
        assert_eq!(count, 2); // beta, delta

        // Both inclusive: [beta to [gamma
        let mut count = 0;
        redis
            .zlexcount(key, b"[beta", b"[gamma", &mut count)
            .unwrap();
        assert_eq!(count, 3); // beta, delta, gamma
    }

    #[test]
    fn test_zlexcount_infinity() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(0.0, b"apple".to_vec()),
            ScoreMember::new(0.0, b"banana".to_vec()),
            ScoreMember::new(0.0, b"cherry".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // From negative infinity to [banana
        let mut count = 0;
        redis.zlexcount(key, b"-", b"[banana", &mut count).unwrap();
        assert_eq!(count, 2); // apple, banana

        // From [banana to positive infinity
        let mut count = 0;
        redis.zlexcount(key, b"[banana", b"+", &mut count).unwrap();
        assert_eq!(count, 2); // banana, cherry

        // From (banana to positive infinity
        let mut count = 0;
        redis.zlexcount(key, b"(banana", b"+", &mut count).unwrap();
        assert_eq!(count, 1); // cherry
    }

    #[test]
    fn test_zlexcount_no_matches() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(0.0, b"a".to_vec()),
            ScoreMember::new(0.0, b"b".to_vec()),
            ScoreMember::new(0.0, b"c".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Range with no matches
        let mut count = 0;
        redis.zlexcount(key, b"[x", b"[z", &mut count).unwrap();
        assert_eq!(count, 0);

        // Inverted range (min > max)
        let mut count = 0;
        redis.zlexcount(key, b"[c", b"[a", &mut count).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_zlexcount_nonexistent_key() {
        let redis = create_test_redis();
        let key = b"nonexistent_key";

        let mut count = 0;
        redis.zlexcount(key, b"-", b"+", &mut count).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_zlexcount_single_member() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add single member
        let score_members = vec![ScoreMember::new(0.0, b"only".to_vec())];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Exact match inclusive
        let mut count = 0;
        redis
            .zlexcount(key, b"[only", b"[only", &mut count)
            .unwrap();
        assert_eq!(count, 1);

        // Exact match exclusive
        let mut count = 0;
        redis
            .zlexcount(key, b"(only", b"(only", &mut count)
            .unwrap();
        assert_eq!(count, 0);

        // All members
        let mut count = 0;
        redis.zlexcount(key, b"-", b"+", &mut count).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_zlexcount_with_different_scores() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with different scores (lexicographical ordering still applies within same score)
        let score_members = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(1.0, b"b".to_vec()),
            ScoreMember::new(2.0, b"c".to_vec()),
            ScoreMember::new(2.0, b"d".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Count all members - should work across different scores
        let mut count = 0;
        redis.zlexcount(key, b"-", b"+", &mut count).unwrap();
        assert_eq!(count, 4);

        // Count specific range
        let mut count = 0;
        redis.zlexcount(key, b"[b", b"[c", &mut count).unwrap();
        assert_eq!(count, 2); // b, c
    }

    #[test]
    fn test_zlexcount_invalid_range() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![ScoreMember::new(0.0, b"a".to_vec())];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Invalid range format (missing bracket/paren)
        let mut count = 0;
        let result = redis.zlexcount(key, b"invalid", b"+", &mut count);
        assert!(result.is_err());

        // Empty range specifier
        let mut count = 0;
        let result = redis.zlexcount(key, b"", b"+", &mut count);
        assert!(result.is_err());
    }

    #[test]
    fn test_zinterstore_basic() {
        let redis = create_test_redis();

        // Create two sorted sets
        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
            ScoreMember::new(3.0, b"c".to_vec()),
        ];

        let score_members2 = vec![
            ScoreMember::new(2.0, b"b".to_vec()),
            ScoreMember::new(3.0, b"c".to_vec()),
            ScoreMember::new(4.0, b"d".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute intersection
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zinterstore(dest, &keys, &[], "SUM", &mut count)
            .unwrap();

        // Should have 2 members (b and c)
        assert_eq!(count, 2);

        // Check scores (should be sum)
        let mut score = None;
        redis.zscore(dest, b"b", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "4"); // 2 + 2

        let mut score = None;
        redis.zscore(dest, b"c", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "6"); // 3 + 3
    }

    #[test]
    fn test_zinterstore_with_weights() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let score_members2 = vec![
            ScoreMember::new(2.0, b"a".to_vec()),
            ScoreMember::new(3.0, b"b".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute intersection with weights
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let weights = vec![2.0, 3.0];
        let mut count = 0;
        redis
            .zinterstore(dest, &keys, &weights, "SUM", &mut count)
            .unwrap();

        assert_eq!(count, 2);

        // Check scores: a = 1*2 + 2*3 = 8, b = 2*2 + 3*3 = 13
        let mut score = None;
        redis.zscore(dest, b"a", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "8");

        let mut score = None;
        redis.zscore(dest, b"b", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "13");
    }

    #[test]
    fn test_zinterstore_aggregate_min() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(5.0, b"b".to_vec()),
        ];

        let score_members2 = vec![
            ScoreMember::new(3.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute intersection with MIN aggregate
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zinterstore(dest, &keys, &[], "MIN", &mut count)
            .unwrap();

        assert_eq!(count, 2);

        // Check scores: a = min(1, 3) = 1, b = min(5, 2) = 2
        let mut score = None;
        redis.zscore(dest, b"a", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "1");

        let mut score = None;
        redis.zscore(dest, b"b", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "2");
    }

    #[test]
    fn test_zinterstore_aggregate_max() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(5.0, b"b".to_vec()),
        ];

        let score_members2 = vec![
            ScoreMember::new(3.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute intersection with MAX aggregate
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zinterstore(dest, &keys, &[], "MAX", &mut count)
            .unwrap();

        assert_eq!(count, 2);

        // Check scores: a = max(1, 3) = 3, b = max(5, 2) = 5
        let mut score = None;
        redis.zscore(dest, b"a", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "3");

        let mut score = None;
        redis.zscore(dest, b"b", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "5");
    }

    #[test]
    fn test_zinterstore_empty_result() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let score_members2 = vec![
            ScoreMember::new(3.0, b"c".to_vec()),
            ScoreMember::new(4.0, b"d".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute intersection (no common members)
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zinterstore(dest, &keys, &[], "SUM", &mut count)
            .unwrap();

        assert_eq!(count, 0);

        // Destination should be empty
        let mut card = 0;
        redis.zcard(dest, &mut card).unwrap();
        assert_eq!(card, 0);
    }

    #[test]
    fn test_zunionstore_basic() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let score_members2 = vec![
            ScoreMember::new(2.0, b"b".to_vec()),
            ScoreMember::new(3.0, b"c".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute union
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zunionstore(dest, &keys, &[], "SUM", &mut count)
            .unwrap();

        // Should have 3 members (a, b, c)
        assert_eq!(count, 3);

        // Check scores
        let mut score = None;
        redis.zscore(dest, b"a", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "1");

        let mut score = None;
        redis.zscore(dest, b"b", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "4"); // 2 + 2

        let mut score = None;
        redis.zscore(dest, b"c", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "3");
    }

    #[test]
    fn test_zunionstore_with_weights() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let score_members2 = vec![
            ScoreMember::new(2.0, b"b".to_vec()),
            ScoreMember::new(3.0, b"c".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute union with weights
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let weights = vec![2.0, 0.5];
        let mut count = 0;
        redis
            .zunionstore(dest, &keys, &weights, "SUM", &mut count)
            .unwrap();

        assert_eq!(count, 3);

        // Check scores: a = 1*2 = 2, b = 2*2 + 2*0.5 = 5, c = 3*0.5 = 1.5
        let mut score = None;
        redis.zscore(dest, b"a", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "2");

        let mut score = None;
        redis.zscore(dest, b"b", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "5");

        let mut score = None;
        redis.zscore(dest, b"c", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "1.5");
    }

    #[test]
    fn test_zunionstore_aggregate_min() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(5.0, b"b".to_vec()),
        ];

        let score_members2 = vec![
            ScoreMember::new(3.0, b"b".to_vec()),
            ScoreMember::new(2.0, b"c".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute union with MIN aggregate
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zunionstore(dest, &keys, &[], "MIN", &mut count)
            .unwrap();

        assert_eq!(count, 3);

        // Check scores: a = 1 (only in key1), b = min(5, 3) = 3, c = 2 (only in key2)
        let mut score = None;
        redis.zscore(dest, b"a", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "1");

        let mut score = None;
        redis.zscore(dest, b"b", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "3");

        let mut score = None;
        redis.zscore(dest, b"c", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "2");
    }

    #[test]
    fn test_zinterstore_nonexistent_key() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"nonexistent";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();

        // Compute intersection with nonexistent key
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zinterstore(dest, &keys, &[], "SUM", &mut count)
            .unwrap();

        // Result should be empty
        assert_eq!(count, 0);
    }

    #[test]
    fn test_zunionstore_nonexistent_key() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"nonexistent";
        let dest = b"dest";

        let score_members1 = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key1, &score_members1, &mut ret).unwrap();

        // Compute union with nonexistent key
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zunionstore(dest, &keys, &[], "SUM", &mut count)
            .unwrap();

        // Result should contain only members from key1
        assert_eq!(count, 2);

        let mut score = None;
        redis.zscore(dest, b"a", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "1");
    }

    #[test]
    fn test_zinterstore_overwrites_destination() {
        let redis = create_test_redis();

        let key1 = b"zset1";
        let key2 = b"zset2";
        let dest = b"dest";

        // Create destination with initial data
        let dest_members = vec![
            ScoreMember::new(10.0, b"x".to_vec()),
            ScoreMember::new(20.0, b"y".to_vec()),
        ];
        let mut ret = 0;
        redis.zadd(dest, &dest_members, &mut ret).unwrap();

        // Create source sets
        let score_members1 = vec![ScoreMember::new(1.0, b"a".to_vec())];
        let score_members2 = vec![ScoreMember::new(2.0, b"a".to_vec())];

        redis.zadd(key1, &score_members1, &mut ret).unwrap();
        redis.zadd(key2, &score_members2, &mut ret).unwrap();

        // Compute intersection (should overwrite dest)
        let keys = vec![key1.to_vec(), key2.to_vec()];
        let mut count = 0;
        redis
            .zinterstore(dest, &keys, &[], "SUM", &mut count)
            .unwrap();

        assert_eq!(count, 1);

        // Old members should be gone
        let mut score = None;
        redis.zscore(dest, b"x", &mut score).unwrap();
        assert!(score.is_none());

        // New member should exist
        let mut score = None;
        redis.zscore(dest, b"a", &mut score).unwrap();
        assert_eq!(String::from_utf8(score.unwrap()).unwrap(), "3");
    }

    #[test]
    fn test_zrevrank_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with different scores
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
            ScoreMember::new(3.0, b"member3".to_vec()),
            ScoreMember::new(4.0, b"member4".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();
        assert_eq!(ret, 4);

        // Test reverse ranks (highest score = rank 0)
        let mut rank = None;
        redis.zrevrank(key, b"member4", &mut rank).unwrap();
        assert_eq!(rank, Some(0));

        let mut rank = None;
        redis.zrevrank(key, b"member3", &mut rank).unwrap();
        assert_eq!(rank, Some(1));

        let mut rank = None;
        redis.zrevrank(key, b"member2", &mut rank).unwrap();
        assert_eq!(rank, Some(2));

        let mut rank = None;
        redis.zrevrank(key, b"member1", &mut rank).unwrap();
        assert_eq!(rank, Some(3));
    }

    #[test]
    fn test_zrevrank_nonexistent_member() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(1.0, b"member1".to_vec()),
            ScoreMember::new(2.0, b"member2".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Test non-existent member
        let mut rank = None;
        redis.zrevrank(key, b"nonexistent", &mut rank).unwrap();
        assert_eq!(rank, None);
    }

    #[test]
    fn test_zrevrank_nonexistent_key() {
        let redis = create_test_redis();
        let key = b"nonexistent_key";

        let mut rank = None;
        redis.zrevrank(key, b"member", &mut rank).unwrap();
        assert_eq!(rank, None);
    }

    #[test]
    fn test_zrevrank_single_member() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add single member
        let score_members = vec![ScoreMember::new(5.0, b"only".to_vec())];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Single member should have rank 0
        let mut rank = None;
        redis.zrevrank(key, b"only", &mut rank).unwrap();
        assert_eq!(rank, Some(0));
    }

    #[test]
    fn test_zrevrank_same_scores() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with same scores (lexicographical ordering applies)
        let score_members = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(1.0, b"b".to_vec()),
            ScoreMember::new(1.0, b"c".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // All should have different ranks based on lexicographical order
        let mut rank_a = None;
        redis.zrevrank(key, b"a", &mut rank_a).unwrap();

        let mut rank_b = None;
        redis.zrevrank(key, b"b", &mut rank_b).unwrap();

        let mut rank_c = None;
        redis.zrevrank(key, b"c", &mut rank_c).unwrap();

        // With same scores, reverse order means c > b > a
        assert!(rank_a.is_some());
        assert!(rank_b.is_some());
        assert!(rank_c.is_some());
        assert_eq!(rank_c, Some(0));
        assert_eq!(rank_b, Some(1));
        assert_eq!(rank_a, Some(2));
    }

    #[test]
    fn test_zrank_vs_zrevrank() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members
        let score_members = vec![
            ScoreMember::new(10.0, b"low".to_vec()),
            ScoreMember::new(20.0, b"mid".to_vec()),
            ScoreMember::new(30.0, b"high".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get both ranks for comparison
        let mut zrank = None;
        redis.zrank(key, b"low", &mut zrank).unwrap();

        let mut zrevrank = None;
        redis.zrevrank(key, b"low", &mut zrevrank).unwrap();

        // For "low": zrank should be 0, zrevrank should be 2
        assert_eq!(zrank, Some(0));
        assert_eq!(zrevrank, Some(2));

        // For "high": zrank should be 2, zrevrank should be 0
        let mut zrank = None;
        redis.zrank(key, b"high", &mut zrank).unwrap();

        let mut zrevrank = None;
        redis.zrevrank(key, b"high", &mut zrevrank).unwrap();

        assert_eq!(zrank, Some(2));
        assert_eq!(zrevrank, Some(0));
    }

    #[test]
    fn test_zrangebylex_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        // Add members with same score for lexicographical ordering
        let score_members = vec![
            ScoreMember::new(0.0, b"a".to_vec()),
            ScoreMember::new(0.0, b"b".to_vec()),
            ScoreMember::new(0.0, b"c".to_vec()),
            ScoreMember::new(0.0, b"d".to_vec()),
            ScoreMember::new(0.0, b"e".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get all members
        let mut result = Vec::new();
        redis
            .zrangebylex(key, b"-", b"+", None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], b"a");
        assert_eq!(result[4], b"e");

        // Get range [b to [d
        let mut result = Vec::new();
        redis
            .zrangebylex(key, b"[b", b"[d", None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"b");
        assert_eq!(result[2], b"d");

        // Get range with LIMIT
        let mut result = Vec::new();
        redis
            .zrangebylex(key, b"-", b"+", Some(1), Some(2), &mut result)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"b");
        assert_eq!(result[1], b"c");
    }

    #[test]
    fn test_zrangebyscore_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(1.0, b"one".to_vec()),
            ScoreMember::new(2.0, b"two".to_vec()),
            ScoreMember::new(3.0, b"three".to_vec()),
            ScoreMember::new(4.0, b"four".to_vec()),
            ScoreMember::new(5.0, b"five".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get all members
        let mut result = Vec::new();
        redis
            .zrangebyscore(key, 0.0, 10.0, false, None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 5);

        // Get range 2.0 to 4.0
        let mut result = Vec::new();
        redis
            .zrangebyscore(key, 2.0, 4.0, false, None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"two");
        assert_eq!(result[2], b"four");

        // Get with scores
        let mut result = Vec::new();
        redis
            .zrangebyscore(key, 2.0, 3.0, true, None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 4); // 2 members * 2 (member + score)
        assert_eq!(result[0], b"two");
        assert_eq!(result[1], b"2");
        assert_eq!(result[2], b"three");
        assert_eq!(result[3], b"3");

        // Get with LIMIT
        let mut result = Vec::new();
        redis
            .zrangebyscore(key, 0.0, 10.0, false, Some(1), Some(2), &mut result)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"two");
        assert_eq!(result[1], b"three");
    }

    #[test]
    fn test_zremrangebylex_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(0.0, b"a".to_vec()),
            ScoreMember::new(0.0, b"b".to_vec()),
            ScoreMember::new(0.0, b"c".to_vec()),
            ScoreMember::new(0.0, b"d".to_vec()),
            ScoreMember::new(0.0, b"e".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Remove range [b to [d
        let mut removed = 0;
        redis
            .zremrangebylex(key, b"[b", b"[d", &mut removed)
            .unwrap();
        assert_eq!(removed, 3);

        // Check remaining members
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);

        // Verify a and e remain
        let mut result = Vec::new();
        redis
            .zrangebylex(key, b"-", b"+", None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"a");
        assert_eq!(result[1], b"e");
    }

    #[test]
    fn test_zremrangebyrank_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(1.0, b"one".to_vec()),
            ScoreMember::new(2.0, b"two".to_vec()),
            ScoreMember::new(3.0, b"three".to_vec()),
            ScoreMember::new(4.0, b"four".to_vec()),
            ScoreMember::new(5.0, b"five".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Remove ranks 1 to 3 (two, three, four)
        let mut removed = 0;
        redis.zremrangebyrank(key, 1, 3, &mut removed).unwrap();
        assert_eq!(removed, 3);

        // Check remaining members
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);

        // Verify one and five remain
        let mut result = Vec::new();
        redis.zrange(key, 0, -1, false, &mut result).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"one");
        assert_eq!(result[1], b"five");
    }

    #[test]
    fn test_zremrangebyrank_negative_indices() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
            ScoreMember::new(3.0, b"c".to_vec()),
            ScoreMember::new(4.0, b"d".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Remove last two members using negative indices
        let mut removed = 0;
        redis.zremrangebyrank(key, -2, -1, &mut removed).unwrap();
        assert_eq!(removed, 2);

        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);
    }

    #[test]
    fn test_zremrangebyscore_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(1.0, b"one".to_vec()),
            ScoreMember::new(2.0, b"two".to_vec()),
            ScoreMember::new(3.0, b"three".to_vec()),
            ScoreMember::new(4.0, b"four".to_vec()),
            ScoreMember::new(5.0, b"five".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Remove scores 2.0 to 4.0
        let mut removed = 0;
        redis.zremrangebyscore(key, 2.0, 4.0, &mut removed).unwrap();
        assert_eq!(removed, 3);

        // Check remaining members
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);

        // Verify one and five remain
        let mut result = Vec::new();
        redis.zrange(key, 0, -1, false, &mut result).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"one");
        assert_eq!(result[1], b"five");
    }

    #[test]
    fn test_zrevrange_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(1.0, b"one".to_vec()),
            ScoreMember::new(2.0, b"two".to_vec()),
            ScoreMember::new(3.0, b"three".to_vec()),
            ScoreMember::new(4.0, b"four".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get all members in reverse order
        let mut result = Vec::new();
        redis.zrevrange(key, 0, -1, false, &mut result).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], b"four");
        assert_eq!(result[1], b"three");
        assert_eq!(result[2], b"two");
        assert_eq!(result[3], b"one");

        // Get with scores
        let mut result = Vec::new();
        redis.zrevrange(key, 0, 1, true, &mut result).unwrap();
        assert_eq!(result.len(), 4); // 2 members * 2
        assert_eq!(result[0], b"four");
        assert_eq!(result[1], b"4");
        assert_eq!(result[2], b"three");
        assert_eq!(result[3], b"3");
    }

    #[test]
    fn test_zrevrangebyscore_basic() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(1.0, b"one".to_vec()),
            ScoreMember::new(2.0, b"two".to_vec()),
            ScoreMember::new(3.0, b"three".to_vec()),
            ScoreMember::new(4.0, b"four".to_vec()),
            ScoreMember::new(5.0, b"five".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Get all members in reverse order
        let mut result = Vec::new();
        redis
            .zrevrangebyscore(key, 10.0, 0.0, false, None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0], b"five");
        assert_eq!(result[4], b"one");

        // Get range 4.0 to 2.0 (max to min)
        let mut result = Vec::new();
        redis
            .zrevrangebyscore(key, 4.0, 2.0, false, None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"four");
        assert_eq!(result[2], b"two");

        // Get with scores
        let mut result = Vec::new();
        redis
            .zrevrangebyscore(key, 3.0, 2.0, true, None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 4); // 2 members * 2
        assert_eq!(result[0], b"three");
        assert_eq!(result[1], b"3");
        assert_eq!(result[2], b"two");
        assert_eq!(result[3], b"2");

        // Get with LIMIT
        let mut result = Vec::new();
        redis
            .zrevrangebyscore(key, 10.0, 0.0, false, Some(1), Some(2), &mut result)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"four");
        assert_eq!(result[1], b"three");
    }

    #[test]
    fn test_zrangebylex_nonexistent_key() {
        let redis = create_test_redis();
        let key = b"nonexistent";

        let mut result = Vec::new();
        redis
            .zrangebylex(key, b"-", b"+", None, None, &mut result)
            .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_zremrangebylex_empty_range() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(0.0, b"a".to_vec()),
            ScoreMember::new(0.0, b"b".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Remove non-existent range
        let mut removed = 0;
        redis
            .zremrangebylex(key, b"[x", b"[z", &mut removed)
            .unwrap();
        assert_eq!(removed, 0);

        // Verify nothing was removed
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);
    }

    #[test]
    fn test_zremrangebyrank_out_of_range() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Try to remove out of range
        let mut removed = 0;
        redis.zremrangebyrank(key, 10, 20, &mut removed).unwrap();
        assert_eq!(removed, 0);

        // Verify nothing was removed
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);
    }

    #[test]
    fn test_zremrangebyscore_no_matches() {
        let redis = create_test_redis();
        let key = b"test_zset";

        let score_members = vec![
            ScoreMember::new(1.0, b"a".to_vec()),
            ScoreMember::new(2.0, b"b".to_vec()),
        ];

        let mut ret = 0;
        redis.zadd(key, &score_members, &mut ret).unwrap();

        // Remove non-existent score range
        let mut removed = 0;
        redis
            .zremrangebyscore(key, 10.0, 20.0, &mut removed)
            .unwrap();
        assert_eq!(removed, 0);

        // Verify nothing was removed
        let mut card = 0;
        redis.zcard(key, &mut card).unwrap();
        assert_eq!(card, 2);
    }
}
