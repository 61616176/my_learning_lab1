//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <optional>
#include <utility>

#include "common/exception.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/std.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  size_ = 0;
  max_size_ = max_size;
  memset(array_, -1, sizeof(array_));
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for (uint32_t idx = 0; idx < size_; idx++) {
    if (!cmp(key, array_[idx].first)) {
      value = array_[idx].second;
      // std::cout << "found the key\n";
      // fmt::print(stderr, "found key\n");
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (size_ == max_size_) {
    // std::cout << "full \n";
    fmt::print(stderr, "full\n");
    return false;
  }
  V tmp_value;
  if (Lookup(key, tmp_value, cmp)) {
    // std::cout << "have the key\n";
    return false;
  }
  array_[size_++] = {key, value};
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  bool found = false;
  uint32_t current_size = Size();
  // std::cout << "try to remove : " << key << std::endl;
  for (uint32_t cur_idx = 0; cur_idx < current_size; cur_idx++) {
    if (!cmp(array_[cur_idx].first, key)) {
      found = true;
      // std::cout << "0\n";
      RemoveAt(cur_idx);
      break;
    }
    // std::cout << "1\n";
  }
  return found;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::ShowAllKey() {
  for (int idx = 0; idx < 10; idx++) {
    std::cout << idx << "_th key:" << array_[idx].first << '\n';
  }
  fmt::print(stderr, "\n");
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  uint32_t current_size = Size();
  for (uint32_t cur_idx = bucket_idx; cur_idx < current_size - 1; cur_idx++) {
    // std::cout << "cur_size: " << current_size;
    array_[cur_idx] = array_[cur_idx + 1];
  }
  // std::cout << "4\n";
  memset(&array_[current_size - 1], -1, sizeof(array_[0]));
  // std::cout << "3\n";
  size_--;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
