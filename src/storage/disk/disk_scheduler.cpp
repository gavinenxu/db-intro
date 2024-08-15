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
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
  while (true)
  {
    auto disk_request = request_queue_.Get();

    // receive nullopt signal to stop
    if (!disk_request.has_value())
    {
      break;
    }

    if (disk_request->is_write_)
    {
      disk_manager_->WritePage(disk_request->page_id_, disk_request->data_);
    }
    else
    {
      disk_manager_->ReadPage(disk_request->page_id_, disk_request->data_);
    }

    // signal callback, IO has complete
    disk_request->callback_.set_value(true);
  }
}

}  // namespace bustub
