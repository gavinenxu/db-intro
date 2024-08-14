//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);

  size_t inf = std::numeric_limits<size_t>::max();
  int victim_id = INVALID_FRAME_ID;
  size_t max_distance = 0;

  for (auto &[fid, node]: node_store_) {
    if (!node.is_evictable_) {
      continue;
    }

    size_t distance = node.history_.size() < k_ ? inf : current_timestamp_ - node.history_.front();
    if (victim_id == INVALID_FRAME_ID
        || max_distance < distance
        || (max_distance == distance && node.history_.front() < node_store_[victim_id].history_.front())) {
      max_distance = distance;
      victim_id = node.fid_;
    }
  }

  if (victim_id != INVALID_FRAME_ID) {
    *frame_id = victim_id;
    node_store_.erase(*frame_id);
    curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Frame id out of range");
  }

  current_timestamp_++;

  auto &node = node_store_[frame_id];
  node.fid_ = frame_id;
  node.history_.push_back(current_timestamp_);
  // do not update evictable

  while (node.history_.size() > k_) {
    node.history_.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Frame id out of range");
  }

  if (node_store_.count(frame_id) == 0) {
    return;
  }

  auto& node = node_store_[frame_id];
  if (node.is_evictable_ != set_evictable) {
    node.is_evictable_ = set_evictable;
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Frame id out of range");
  }

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (!it->second.is_evictable_) {
    throw Exception(ExceptionType::INVALID, "can't remove non-evictable frame");
  }

  node_store_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub
