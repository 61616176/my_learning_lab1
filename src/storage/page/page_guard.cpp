#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  //如何删除that的bpm和page
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  // std::cout << "move a basic guard of page" << PageId() << std::endl;
}

void BasicPageGuard::Drop() {
  if (bpm_ && page_) {
    // std::cout << "drop a basic guard of page " << PageId() << std::endl;
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
    bpm_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  //如何删除that的bpm和page
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  // std::cout << "move a basic guard of page" << PageId() << std::endl;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // std::cout << "upgrade to read guard of page " << PageId() << std::endl;
  page_->RLatch();
  ReadPageGuard new_read_guard(bpm_, page_);
  page_ = nullptr;
  bpm_ = nullptr;
  return new_read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // std::cout << "upgrade to write guard of page " << PageId() << std::endl;
  page_->WLatch();
  WritePageGuard new_write_guard(bpm_, page_);
  page_ = nullptr;
  bpm_ = nullptr;
  return new_write_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  Drop();
  guard_ = std::move(that.guard_);
  // std::cout << "move a read guard of page" << PageId() << std::endl;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  // std::cout << "move a read guard of page" << PageId() << std::endl;
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ && guard_.bpm_) {
    // std::cout << "drop a read guard of page " << PageId() << std::endl;
    guard_.bpm_->UnpinPage(guard_.PageId(), false);
    guard_.page_->RUnlatch();
    guard_.bpm_ = nullptr;
    guard_.page_ = nullptr;
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  Drop();
  guard_ = std::move(that.guard_);
  // std::cout << "move a write guard of page" << PageId() << std::endl;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  // td::cout << "move a write guard of page" << PageId() << std::endl;
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ && guard_.bpm_) {
    // std::cout << "drop a write guard of page" << PageId() << std::endl;
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), true);
    guard_.page_->WUnlatch();
    guard_.page_ = nullptr;
    guard_.bpm_ = nullptr;
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
