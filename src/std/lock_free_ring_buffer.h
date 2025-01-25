// Copyright (c) 2023-present, arana-db Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <atomic>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// calculate the capacity of the ring buffer,
// which is the smallest power of 2 that is greater than or equal to size
// e.g. size = 10, return 16
static size_t calculateRingBuffCapacity(size_t size) {
  // if num is already a power of 2, return it
  if ((size & (size - 1)) == 0) {
    return size;
  }

  size_t result = size;
  result |= (result >> 1);
  result |= (result >> 2);
  result |= (result >> 4);
  result |= (result >> 8);
  result |= (result >> 16);
  result += 1;

  return result;
}

template <typename T>
class LockFreeRingBuffer {
 public:
  explicit LockFreeRingBuffer(size_t capacity)
      : capacity_(calculateRingBuffCapacity(capacity)), head_(0), tail_(0), buffer_(capacity_) {}

  // push an item into the queue
  template <typename U>
  bool Push(U&& item) {
    size_t current_tail = tail_.load(std::memory_order_relaxed);
    size_t next_tail = (current_tail + 1) & (capacity_ - 1);
    if (next_tail == head_.load(std::memory_order_acquire)) {
      // queue is full
      return false;
    }
    buffer_[current_tail] = std::forward<U>(item);
    tail_.store(next_tail, std::memory_order_release);
    return true;
  }

  // pop an item from the queue, if the queue is empty, return false
  bool Pop(T& item) {
    size_t current_head = head_.load(std::memory_order_relaxed);
    if (current_head == tail_.load(std::memory_order_acquire)) {
      // queue is empty
      return false;
    }

    item = std::move(buffer_[current_head]);
    head_.store((current_head + 1) & (capacity_ - 1), std::memory_order_release);
    return true;
  }

  bool Empty() { return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire); }
  bool Full() {
    return ((tail_.load(std::memory_order_acquire) + 1) & (capacity_ - 1)) == head_.load(std::memory_order_acquire);
  }

 private:
  const size_t capacity_;
  std::atomic<size_t> head_;
  std::atomic<size_t> tail_;
  std::vector<T> buffer_;
};
