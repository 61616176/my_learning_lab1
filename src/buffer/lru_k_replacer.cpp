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

#include <chrono>

namespace bustub {

auto LRUKNode::FindFrameID() const -> frame_id_t { return fid_; }

void LRUKNode::SetEvictable(bool set_evictable) { is_evictable_ = set_evictable; }

auto LRUKNode::CheckInf() const -> bool { return is_inf_; }

auto LRUKNode::CheckEvictable() const -> bool { return is_evictable_; }

auto LRUKNode::FindEarlyTime() const -> size_t { return history_.front(); }

auto LRUKNode::ComputeKDistance(size_t current_timestamp) const -> size_t {
  size_t res = 0;
  for (auto const &iter : history_) {
    res += current_timestamp - iter;
  }
  return res;
}

void LRUKNode::Record(size_t current_timestamp) {
  history_.push_back(current_timestamp);
  if (history_.size() == k_) {
    is_inf_ = false;
  } else if (history_.size() > k_) {
    history_.pop_front();
  }
}

}  // namespace bustub
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  frame_id_t res_frame_id = -1;
  size_t max_k_distance = 0;
  size_t early_timestamp = 4294967295;
  bool if_has_inf = false;
  for (auto const &iterator : node_store_) {
    if (!iterator.second.CheckEvictable()) {
      continue;
    }
    if (res_frame_id == -1) {
      res_frame_id = iterator.first;
      const LRUKNode &node = iterator.second;
      if (node.CheckInf()) {
        if_has_inf = true;
        early_timestamp = node.FindEarlyTime();
      } else {
        max_k_distance = node.ComputeKDistance(current_timestamp_);
      }
    } else {
      const LRUKNode &node = iterator.second;
      if (node.CheckInf()) {
        if_has_inf = true;
        if (early_timestamp > node.FindEarlyTime()) {
          early_timestamp = node.FindEarlyTime();
          res_frame_id = iterator.first;
        }
      } else if (if_has_inf) {
        continue;
      } else {
        if (max_k_distance < node.ComputeKDistance(current_timestamp_)) {
          max_k_distance = node.ComputeKDistance(current_timestamp_);
          res_frame_id = iterator.first;
        }
      }
    }
  }
  if (res_frame_id != -1) {
    *frame_id = res_frame_id;
    curr_size_--;
    node_store_.erase(res_frame_id);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    return;
  }
  if (auto iter = node_store_.find(frame_id); iter == node_store_.end()) {
    LRUKNode new_node(frame_id, k_);
    new_node.Record(current_timestamp_);
    node_store_.insert(std::make_pair(frame_id, new_node));
  } else {
    LRUKNode &node = iter->second;
    node.Record(current_timestamp_);
  }
  SetCurrentTime();
  latch_.unlock();
}

void LRUKReplacer::SetCurrentTime() { current_timestamp_++; }

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    return;
  }
  if (auto iter = node_store_.find(frame_id); iter != node_store_.end()) {
    LRUKNode &node = iter->second;
    if (set_evictable != node.CheckEvictable()) {
      node.SetEvictable(set_evictable);
      set_evictable ? curr_size_++ : curr_size_--;
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (auto iter = node_store_.find(frame_id); iter != node_store_.end()) {
    LRUKNode &node = iter->second;
    if (!node.CheckEvictable()) {
      return;
    }
    node_store_.erase(frame_id);
    curr_size_--;
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub