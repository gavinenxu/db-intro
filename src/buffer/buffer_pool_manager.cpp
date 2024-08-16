//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard lock(latch_);

  frame_id_t frame_id = RequestFrameId();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }

  // check current buffer pool is dirty, flush dirty page, remove prev page meta and reset memory data
  // flush dirty page in current buffer pool
  Page* page = &pages_[frame_id];
  if (page->IsDirty() && strlen(page->GetData()) > 0) {
    DiskSchedule({true, page->GetData(), page->GetPageId(), disk_scheduler_->CreatePromise()});
  }
  // delete prev page from buffer pool
  page_table_.erase(page->GetPageId());
  page->ResetMemory();

  *page_id = next_page_id_;
  InitPageOnBufferPool(page, *page_id, frame_id);
  AllocatePage();

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard lock(latch_);

  if (page_table_.count(page_id) != 0) {
    auto frame_id = page_table_[page_id];
    Page* page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    return page;
  }

  frame_id_t frame_id = RequestFrameId();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }

  // clear prev page from buffer pool
  Page* page = &pages_[frame_id];
  if (page->IsDirty() && strlen(page->GetData()) > 0) {
    DiskSchedule({true, page->GetData(), page->GetPageId(), disk_scheduler_->CreatePromise()});
  }
  page_table_.erase(page->GetPageId());

  page->ResetMemory();
  InitPageOnBufferPool(page, page_id, frame_id);

  // Read current page from disk
  DiskSchedule({false, page->GetData(), page->GetPageId(), disk_scheduler_->CreatePromise()});

  return page;
}

/*
 * Pinning a Page means incrementing a reference count or flag associated with a page
 * in the buffer pool to indicate that the page is currently being used
 * and should not be evicted from memory.
 *
 * Unpinning a Page means decrementing the reference count or flag associated with
 * the page in the buffer pool, indicating that the page is no longer
 * needed by the current process.
 */
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard lock(latch_);

  // page not in buffer pool
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];
  // no reference
  if (page->GetPinCount() <= 0) {
    return false;
  }

  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  page->is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];
  DiskSchedule({true, page->GetData(), page->GetPageId(), disk_scheduler_->CreatePromise()});
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard lock(latch_);

  for (auto &[_, frame_id]: page_table_) {
    Page* page = &pages_[frame_id];
    DiskSchedule({true, page->GetData(), page->GetPageId(), disk_scheduler_->CreatePromise()});
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return true;
  }

  auto frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];

  if (page->GetPinCount() > 0) {
    return false;
  }

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page* page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page* page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  return {this, NewPage(page_id)};
}

frame_id_t BufferPoolManager::RequestFrameId() {
  frame_id_t frame_id = INVALID_FRAME_ID;
  if (!free_list_.empty()) {
    // find from the free list front
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
  }
  return frame_id;
}

void BufferPoolManager::InitPageOnBufferPool(Page* page, page_id_t page_id, frame_id_t frame_id) {
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  // pin the frame
  page->pin_count_ = 1;
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  page_table_[page_id] = frame_id;
}

}  // namespace bustub
