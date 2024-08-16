//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

// remove a frame and assign it to input frame_id 
auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    std::lock_guard lock{latch_};
    if (frame_dll_.empty()) {
        return false;
    }
    *frame_id = frame_dll_.back();
    frame_dll_.pop_back();
    frame_table_.erase(*frame_id);
    return true; 
}

// remove frame
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard lock{latch_};
    auto it = frame_table_.find(frame_id);
    if (it == frame_table_.end()) {
        return;
    }
    frame_dll_.erase(it->second);
    frame_table_.erase(it);
}

// add frame
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard lock{latch_};
    if (frame_dll_.size() >= num_pages_) {
        return;
    }

    auto it = frame_table_.find(frame_id);
    if (it != frame_table_.end()) {
        return;
    }
    frame_dll_.push_front(frame_id);
    frame_table_[frame_id] = frame_dll_.begin();

}

auto LRUReplacer::Size() -> size_t {
    std::lock_guard lock{latch_};
    return frame_dll_.size(); 
}

}  // namespace bustub
