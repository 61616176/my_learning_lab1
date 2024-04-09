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

#include "fmt/core.h"
#include "fmt/std.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  // std::cout << "pool size: " << pool_size << std::endl;
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::FindFreeFrame(frame_id_t &frame_id) -> bool {
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {  //删除一个page后应当把相应的内容清空或置零
    Page *evicted_page = &pages_[frame_id];
    page_id_t page_id;
    std::any_of(page_table_.begin(), page_table_.end(), [&](auto const &iter) -> bool {
      if (iter.second == frame_id) {
        page_id = iter.first;
        return true;
      }
      return false;
    });
    // fmt::print(stderr, "start to evict page {}, frame {}\n", page_id, frame_id);
    if (evicted_page->IsDirty()) {
      FlushPage(page_id);
    }
    pages_[frame_id].ResetMemory();
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    page_table_.erase(page_id);
  } else {
    return false;
  }
  return true;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  frame_id_t frame_id;
  if (!FindFreeFrame(frame_id)) {
    // std::cout << 1<<std::endl;
    // std::cout << "didn't find empty frame" << std::endl;
    latch_.unlock();
    return nullptr;
  }
  *page_id = AllocatePage();
  page_table_.insert(std::make_pair(*page_id, frame_id));
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = *page_id;  // frame_id做下标
  pages_[frame_id].pin_count_++;
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  // std::cout << "new page " << *page_id << std::endl;
  latch_.unlock();
  return &pages_[frame_id];
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *cur_page = NewPage(page_id);
  // BasicPageGuard guard = new BasicPageGuard(this, cur_page);
  BasicPageGuard guard(this, cur_page);
  return guard;
}

void BufferPoolManager::ShowPage(page_id_t page_id) {
  latch_.lock();
  frame_id_t frame_id = page_table_[page_id];
  fmt::print("page_id: {} frame_id: {} pin_count: {}\n", page_id, frame_id, pages_[frame_id].pin_count_);
  latch_.unlock();
}

void BufferPoolManager::ShowPages() {
  for (auto [page_id, frame_id] : page_table_) {
    // std::cout << "page_id: " << page_id << "frame_id: " << frame_id << "pin_count: " << pages_[frame_id].pin_count_
    //          << std::endl;
    fmt::print("page_id: {} frame_id: {} pin_count: {}\n", page_id, frame_id, pages_[frame_id].pin_count_);
  }
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  // std::cout << "show pages\n";
  // ShowPages();
  // std::cout << "fetch page " << page_id << std::endl;
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    pages_[iter->second].pin_count_++;
    replacer_->SetEvictable(iter->second, false);
    replacer_->RecordAccess(iter->second);
    latch_.unlock();
    return &pages_[iter->second];
  }
  if (frame_id_t frame_id; FindFreeFrame(frame_id)) {  //;可以这么用吗,可以
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, pages_[frame_id].data_, page_id, std::move(promise)});
    future.get();
    pages_[frame_id].pin_count_++;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].page_id_ = page_id;
    page_table_.insert(std::make_pair(page_id, frame_id));
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    latch_.unlock();
    return &pages_[frame_id];
  }
  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // std::cout << "fetch page basic" << page_id << std::endl;
  Page *cur_page = FetchPage(page_id);
  BasicPageGuard guard(this, cur_page);
  return guard;
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // std::cout << "fetch page read " << page_id << std::endl;
  ReadPageGuard read_guard = FetchPageBasic(page_id).UpgradeRead();
  return read_guard;
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // std::cout << "fetch page write " << page_id << std::endl;
  WritePageGuard write_guard = FetchPageBasic(page_id).UpgradeWrite();
  return write_guard;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  if (auto const &iter = page_table_.find(page_id); iter != page_table_.end()) {
    const frame_id_t frame_id = iter->second;
    Page *current_page = &pages_[frame_id];
    if (current_page->pin_count_ != 0) {
      current_page->pin_count_--;
      if (is_dirty) {
        current_page->is_dirty_ = is_dirty;
      }
      if (current_page->pin_count_ == 0) {
        replacer_->SetEvictable(frame_id, true);
      }
      latch_.unlock();
      return true;
    }
    latch_.unlock();
    return false;
  }
  latch_.unlock();
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id != INVALID_PAGE_ID) {
    if (auto const &iter = page_table_.find(page_id); iter != page_table_.end()) {
      const frame_id_t frame_id = iter->second;
      Page *current_page = &pages_[frame_id];
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule(
          {true, current_page->GetData(), page_id, std::move(promise)});  //是否应该等待futrue变为true？
      future.get();
      current_page->is_dirty_ = false;
      return true;
    }
    return false;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (auto [page_id, _] : page_table_) {
    FlushPage(page_id);
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    auto frame_id = iter->second;
    Page *pointer_of_page = &pages_[frame_id];
    if (pointer_of_page->pin_count_ == 0) {
      page_table_.erase(page_id);
      replacer_->Remove(frame_id);
      free_list_.push_back(frame_id);
      DeallocatePage(page_id);
      latch_.unlock();
      return true;
    }
    latch_.unlock();
    return false;
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub