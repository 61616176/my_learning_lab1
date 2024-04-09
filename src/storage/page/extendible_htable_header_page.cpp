//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstring>

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

//为什么要删掉构造函数、析构函数来专用init等函数来保证内存安全？
//析构函数应该时是调用默认析构函数
//********ExtendibleHTableHeaderPage是从page中的data_部分reinterpret_cast转化过来的，每个page的data_在create时都会初始化为0；那么我们只需要在必要的不为零的变量初始化即可
void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  max_depth_ = max_depth;
  memset(directory_page_ids_, -1, sizeof(directory_page_ids_));
  // directory_page_ids_需要初始化吗？
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  uint32_t hash_key = hash >> (32 - max_depth_);
  uint32_t index = hash_key & GetDepthMask();
  // std::cout <<  "hash key: " << hash_key <<"hash to directory page in header index: " << index << std::endl;
  return index;
}

auto ExtendibleHTableHeaderPage::GetDepthMask() const -> uint32_t {
  uint32_t mask = 1 << max_depth_;
  mask--;
  // std::cout << "header mask: " << mask ;
  return mask;
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return max_depth_; }

}  // namespace bustub
