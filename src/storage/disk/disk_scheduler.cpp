//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  pool_.Put(false);
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    pool_.Get();
    pool_.Put(true);
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  std::optional<DiskRequest> optional_r(std::move(r));
  request_queue_.Put(std::move(optional_r));
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    std::optional<DiskRequest> request = request_queue_.Get();
    if (request.has_value()) {
      request->is_write_ ? disk_manager_->WritePage(request->page_id_, request->data_)
                         : disk_manager_->ReadPage(request->page_id_, request->data_);
      request->callback_.set_value(true);
    }
    if (!pool_.Get()) {
      pool_.Put(false);
    } else {
      break;
    }
  }
}
}  // namespace bustub