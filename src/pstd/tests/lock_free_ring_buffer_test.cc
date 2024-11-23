// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <gtest/gtest.h>

#include "../lock_free_ring_buffer.h"

class LockFreeRingBufferTest : public ::testing::Test {};

TEST_F(LockFreeRingBufferTest, Empty) {
  LockFreeRingBuffer<int> ring_buffer(10);
  ASSERT_EQ(ring_buffer.Empty(), true);
}

TEST_F(LockFreeRingBufferTest, Full) {
  LockFreeRingBuffer<int> ring_buffer(10);
  for (int i = 0; i < 20; ++i) {
    ring_buffer.Push(i);
    if (i <= 13) {
      ASSERT_EQ(ring_buffer.Full(), false);
    } else {
      ASSERT_EQ(ring_buffer.Full(), true);
    }
  }
}

TEST_F(LockFreeRingBufferTest, SingleThread) {
  LockFreeRingBuffer<int> ring_buffer(10);
  int pushSize = 0;
  for (int i = 0; i < 20; ++i) {
    if (!ring_buffer.Push(i)) {
      break;
    }
    ++pushSize;
  }
  ASSERT_EQ(pushSize, 15);
  ASSERT_EQ(ring_buffer.Full(), true);
  for (int i = 0; i < pushSize; ++i) {
    int item;
    ASSERT_EQ(ring_buffer.Pop(item), true);
    ASSERT_EQ(item, i);
  }
  ASSERT_EQ(ring_buffer.Empty(), true);
}

TEST_F(LockFreeRingBufferTest, MultipleThread) {
  LockFreeRingBuffer<int> ring_buffer(10);
  const int SIZE = 1000;
  std::thread producer([&ring_buffer]() {
    for (int i = 0; i < SIZE; ++i) {
      while (!ring_buffer.Push(i)) {
      }
    }
  });

  std::thread consumer([&ring_buffer]() {
    int i = 0;
    while (i < SIZE) {
      int item;
      if (ring_buffer.Pop(item)) {
        ASSERT_EQ(item, i);
        ++i;
      }
    }
  });
  producer.join();
  consumer.join();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
