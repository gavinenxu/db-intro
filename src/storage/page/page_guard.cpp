#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

// BasicPageGuard
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  // The move constructor transfers ownership from that to the current instance.
  // The original that instance should be left in a state where it can't be used.

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
    bpm_ = nullptr;
    is_dirty_ = false;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // The move assignment operator should handle self-assignment, drop the currently held page,
  // and then transfer ownership from that to the current instance.
  if (this != &that) {
    Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { return {bpm_, page_}; }

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { return {bpm_, page_}; }

// ReadPageGuard
ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
    guard_.page_->RUnlatch();
    guard_.page_ = nullptr;
    guard_.bpm_ = nullptr;
  }
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}  // NOLINT

// WritePageGuard
WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
    guard_.page_->WUnlatch();
    guard_.page_ = nullptr;
    guard_.bpm_ = nullptr;
  }
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace bustub
