//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  max_depth_ = max_depth;
  global_depth_ = 0;
  memset(bucket_page_ids_, -1, sizeof(bucket_page_ids_));
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  uint32_t mask = GetGlobalDepthMask();
  // std::cout << "hash to bucket in directory idx: " << (hash & mask);
  return hash & mask;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  uint32_t mask = (1 << global_depth_) - 1;
  return mask;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  uint32_t mask = (1 << local_depths_[bucket_idx]) - 1;
  return mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  std::vector<uint32_t> pointers = GetPointersToBucket(bucket_idx);
  for (auto i : pointers) {
    bucket_page_ids_[i] = bucket_page_id;
  }
}

//**********************not implemented
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  /*split image index example: lp:3 110 -- 010 ; lp:3 1010 -- 1110
  **方法：判断 local depth 那一bit的值（0或1），那一bit取反
  **local depth bit == 1 << (local depth -1)
  **  eg. 1000 lp:4  1000 = 1 << (3)
  **      11   lp:0
  */
  uint32_t local_depth = local_depths_[bucket_idx];
  uint32_t parent_bucket_idx;
  uint32_t check_bits = 1 << (local_depth - 1);
  if ((check_bits & bucket_idx) == check_bits) {
    parent_bucket_idx = bucket_idx - check_bits;
  } else {
    parent_bucket_idx = bucket_idx + check_bits;
  }
  return parent_bucket_idx;
}

// auto ExtendibleHTableDirectoryPage::Get

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  global_depth_++;
  Growing();
}

auto ExtendibleHTableDirectoryPage::CanGrow() -> bool { return global_depth_ != max_depth_; }

void ExtendibleHTableDirectoryPage::Growing() {
  // for (uint32_t old_idx = 0, idx = size_; old_idx < size_ ; old_idx++, idx++) {
  //  local_depths_[idx] = local_depths_[old_idx];
  //  bucket_page_ids_[idx] = bucket_page_ids_[old_idx];
  //}
  uint32_t current_size = Size();
  // std::cout << "size: " << current_size << std::endl;
  memcpy(&local_depths_[current_size], local_depths_, current_size * sizeof(uint8_t));
  memcpy(&bucket_page_ids_[current_size], bucket_page_ids_, current_size * sizeof(page_id_t));
  for (uint32_t idx = 0; idx < 2 * current_size; idx++) {
    // std::cout << "local_depth: " << local_depths_[idx] << ' ' << "page id: " << bucket_page_ids_[idx] << '\n';
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  Shrinking();
  global_depth_--;
}

void ExtendibleHTableDirectoryPage::Shrinking() {
  uint32_t current_size = Size();
  memset(&local_depths_[current_size / 2], 0, (current_size / 2) * sizeof(local_depths_[0]));
  memset(&bucket_page_ids_[current_size / 2], -1, (current_size / 2) * sizeof(bucket_page_ids_[0]));
}

//***********************not implemented
auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }
  for (uint32_t local_depth : local_depths_) {
    if (local_depth == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  uint32_t i = 0;
  for (; bucket_page_ids_[i] != -1; i++)
    ;
  return i;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  std::vector<uint32_t> pointers = GetPointersToBucket(bucket_idx);
  for (auto i : pointers) {
    local_depths_[i] = local_depth;
  }
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  std::vector<uint32_t> pointers = GetPointersToBucket(bucket_idx);
  for (auto i : pointers) {
    local_depths_[i]++;
  }
}

auto ExtendibleHTableDirectoryPage::GetPointersToBucket(uint32_t bucket_idx) -> std::vector<uint32_t> {
  /*get brother buckets
  **eg. lp = 3 1010
  **    brother buckets: 0010
  **    lp = 2 1010
  **    brother buckets: 1110 0110 0010
  **    lp = 1 1
  **    brother buckets: 0
  */
  uint32_t local_depth = local_depths_[bucket_idx];
  uint32_t n_pointers_to_bucket = 1 << (global_depth_ - local_depth);
  uint32_t min_idx_of_bucket = bucket_idx << (32 - local_depth) >> (32 - local_depth);
  // uint32_t pointers[n_pointers_to_bucket];
  std::vector<uint32_t> pointers;
  for (uint32_t idx = min_idx_of_bucket, i = 0; i < n_pointers_to_bucket; i++) {
    pointers.push_back(idx);
    idx += 1 << local_depth;
  }
  return pointers;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  auto pointers = GetPointersToBucket(bucket_idx);
  for (uint32_t i : pointers) {
    local_depths_[i]--;
  }
}
}  // namespace bustub
