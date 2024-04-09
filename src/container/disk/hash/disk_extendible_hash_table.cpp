//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cmath>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // std::cout << "create hash table " << header_max_depth << " " << directory_max_depth << " " << bucket_max_size
  //          << std::endl;
  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  WritePageGuard header_write_guard = header_guard.UpgradeWrite();
  ExtendibleHTableHeaderPage *header = header_write_guard.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result,
                                                 [[maybe_unused]] Transaction *transaction) -> bool {
  // std::cout << "get value " << key << std::endl;
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  const ExtendibleHTableHeaderPage *header = header_guard.As<ExtendibleHTableHeaderPage>();

  uint32_t hash_key = Hash(key);
  page_id_t directory_page_id = GetDirectoryPageIdFromHeader(hash_key, header);

  if (directory_page_id == -1) {
    return false;
  }
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  const ExtendibleHTableDirectoryPage *directory = directory_guard.As<ExtendibleHTableDirectoryPage>();

  page_id_t bucket_page_id = GetBucketPageIdFromDirectory(hash_key, directory);

  if (bucket_page_id == -1) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  const ExtendibleHTableBucketPage<K, V, KC> *bucket = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  // std::cout << "get value " << key << " from directory : " << directory_page_id << " bucket: " << bucket_page_id
  //          << std::endl;
  bool res = GetValueFromBucket(key, result, bucket);
  return res;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetDirectoryPageIdFromHeader(uint32_t hash_key,
                                                                     const ExtendibleHTableHeaderPage *header)
    -> page_id_t {
  page_id_t directory_page_id = header->GetDirectoryPageId(header->HashToDirectoryIndex(hash_key));
  return directory_page_id;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetBucketPageIdFromDirectory(uint32_t hash_key,
                                                                     const ExtendibleHTableDirectoryPage *directory)
    -> page_id_t {
  page_id_t bucket_page_id = directory->GetBucketPageId(directory->HashToBucketIndex(hash_key));
  return bucket_page_id;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValueFromBucket(const K &key, std::vector<V> *result,
                                                           const ExtendibleHTableBucketPage<K, V, KC> *bucket) -> bool {
  V value;
  if (bucket->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, [[maybe_unused]] Transaction *transaction)
    -> bool {
  // std::cout << "insert key " << key << std::endl;
  bool res;
  uint32_t hash_key = Hash(key);
  // std::cout << "key : " << key << " hash key : " << hash_key << std::endl;
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  ExtendibleHTableHeaderPage *header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  page_id_t directory_page_id = InsertToHeader(hash_key, header);
  header_guard.Drop();
  // std::cout << "directory page id : " << directory_page_id << std::endl;
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  ExtendibleHTableDirectoryPage *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  page_id_t bucket_page_id = InsertToDirectory(hash_key, directory);
  directory_guard.Drop();

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  // std::cout << "insert key " << key << " to directory : " << directory_page_id << " bucket : " << bucket_page_id
  //          << std::endl;
  ExtendibleHTableBucketPage<K, V, KC> *bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket->IsFull()) {
    // std::cout << "full" << std::endl;
    if (page_id_t now_bucket_page_id = SplitAndRemap(hash_key, bucket_page_id, directory_page_id, bucket);
        now_bucket_page_id != -1) {
      // std::cout << "4\n";
      if (now_bucket_page_id == bucket_page_id) {
        res = InsertToBucket(key, value, bucket);
      } else {
        bucket_guard.Drop();
        auto now_bucket = bpm_->FetchPageWrite(now_bucket_page_id).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        res = InsertToBucket(key, value, now_bucket);
      }
    } else {
      res = false;
    }
  } else {
    res = InsertToBucket(key, value, bucket);
  }
  // PrintHT();
  // std::cout << "global depth: " << directory->GetGlobalDepth() << std::endl;
  return res;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToHeader(uint32_t hash_key, ExtendibleHTableHeaderPage *header)
    -> page_id_t {
  uint32_t directory_page_idx = header->HashToDirectoryIndex(hash_key);
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_page_idx);
  if (directory_page_id == -1) {
    CreateNewDirectoryPage(&directory_page_id);

    header->SetDirectoryPageId(directory_page_idx, directory_page_id);
  }
  return directory_page_id;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToDirectory(uint32_t hash_key, ExtendibleHTableDirectoryPage *directory)
    -> page_id_t {
  uint32_t bucket_idx = directory->HashToBucketIndex(hash_key);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == -1) {
    CreateNewBucket(&bucket_page_id);

    directory->SetBucketPageId(bucket_idx, bucket_page_id);
  }
  return bucket_page_id;
  //<< "bucket page: " << bucket_page_id << std::endl;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitAndRemap(uint32_t hash_key, page_id_t bucket_page_id,
                                                      page_id_t directory_page_id,
                                                      ExtendibleHTableBucketPage<K, V, KC> *bucket) -> page_id_t {
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  ExtendibleHTableDirectoryPage *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  page_id_t new_bucket_page_id;
  CreateNewBucket(&new_bucket_page_id);
  uint32_t bucket_idx = directory->HashToBucketIndex(hash_key);
  if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
    if (!directory->CanGrow()) {
      return -1;
    }
    // add a new bucket and remap the whole directory
    // example : gp=2 lp=2 01 page 2 --> gp=3 lp=3 101 page2 001 page3
    // first: lp->3 01 page2
    // second: gp->3 001 101 page2
    // third: 001 page2 101 page3
    directory->IncrLocalDepth(bucket_idx);
    directory->IncrGlobalDepth();
    directory->SetBucketPageId(directory->GetSplitImageIndex(bucket_idx), new_bucket_page_id);
  } else {
    // since the local depth < global depth, we don't need to remap the whole directory but just move the entry between
    // old and new remaop corresponding idx
    // example : gp =3 lp=2 101 001 page 2 --> gp=3 lp=3 101 page2 001 page3
    directory->IncrLocalDepth(bucket_idx);
    uint32_t oppsite_idx = directory->GetSplitImageIndex(bucket_idx);
    directory->SetBucketPageId(oppsite_idx, new_bucket_page_id);
  }
  // move the entry in bucket to new bucket
  auto new_bucket = bpm_->FetchPageWrite(new_bucket_page_id).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // bucket->PrintBucket();
  // new_bucket->PrintBucket();
  // move some (k, v) between two bucket.
  //因为每次remove都会调整

  for (uint32_t entry_in_bucket_idx = 0; entry_in_bucket_idx < bucket->Size();) {
    const std::pair<K, V> entry = bucket->EntryAt(entry_in_bucket_idx);
    uint32_t hash_key = Hash(entry.first);
    uint32_t new_bucket_in_directory_idx = directory->HashToBucketIndex(hash_key);
    page_id_t current_bucket_page_id = directory->GetBucketPageId(new_bucket_in_directory_idx);
    // std::cout << "key : " << entry.first << " current page id : " << current_bucket_page_id
    //          << "entry idx : " << entry_in_bucket_idx << std::endl;
    if (current_bucket_page_id == bucket_page_id) {
      // std::cout << "value " << entry.first << " keep in " << bucket_page_id << std::endl;
      entry_in_bucket_idx++;
    } else {
      bucket->RemoveAt(entry_in_bucket_idx);
      InsertToBucket(entry.first, entry.second, new_bucket);
      // std::cout << "value " << entry.first << " go to " << new_bucket_page_id << std::endl;
    }
    // bucket->PrintBucket();
    // new_bucket->PrintBucket();
  }
  page_id_t now_bucket_page_id = directory->GetBucketPageId(directory->HashToBucketIndex(hash_key));
  return now_bucket_page_id;
}

//判断和split交给directory，InsertTobucket只负责插入；插入要在direcory保证bucket不会溢出
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToBucket(const K &key, const V &value,
                                                       ExtendibleHTableBucketPage<K, V, KC> *bucket) -> bool {
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::CreateNewDirectoryPage(page_id_t *page_id) {
  auto directory_guard = bpm_->NewPageGuarded(page_id);
  ExtendibleHTableDirectoryPage *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::CreateNewBucket(page_id_t *page_id) {
  // std::cout << "create new bucket\n";
  auto bucket_guard = bpm_->NewPageGuarded(page_id);
  ExtendibleHTableBucketPage<K, V, KC> *bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, [[maybe_unused]] Transaction *transaction) -> bool {
  // std::cout << "remove key " << key << std::endl;
  uint32_t hash_key = Hash(key);
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  const ExtendibleHTableHeaderPage *header = header_guard.As<ExtendibleHTableHeaderPage>();
  header_guard.Drop();

  uint32_t directory_idx = header->HashToDirectoryIndex(hash_key);
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == -1) {
    return false;
  }
  auto directory = bpm_->FetchPageWrite(directory_page_id).AsMut<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_idx = directory->HashToBucketIndex(hash_key);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == -1) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // std::cout << "remove key " << key << " in directory : " << directory_page_id << " bucket: " << bucket_page_id
  //          << "bucket idx in directory: " << bucket_idx << std::endl;
  if (!bucket->Remove(key, cmp_)) {
    return false;
  }
  bucket_guard.Drop();
  if (bucket->IsEmpty()) {
    BucketMerge(directory, bucket_page_id, bucket_idx);
  }

  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::CheckBucketMerge(ExtendibleHTableDirectoryPage *directory,
                                                         const ExtendibleHTableBucketPage<K, V, KC> *bucket,
                                                         const ExtendibleHTableBucketPage<K, V, KC> *split_image_bucket,
                                                         uint32_t bucket_idx, uint32_t split_image_bucket_idx)
    -> const ExtendibleHTableBucketPage<K, V, KC> * {
  //可以merge，return 非空的bucket；否则返回nullptr
  if (directory->GetLocalDepth(bucket_idx) != directory->GetLocalDepth(split_image_bucket_idx)) {
    return nullptr;
  }
  if (!bucket->IsEmpty() && !split_image_bucket->IsEmpty()) {
    return nullptr;
  }
  if (bucket->IsEmpty()) {
    return split_image_bucket;
  }
  return bucket;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::BucketMerge(ExtendibleHTableDirectoryPage *directory, page_id_t bucket_page_id,
                                                    uint32_t bucket_idx) {
  /*bucket merge 规则：不让directory指向空的bucket
  **bucket为空时，与它的同depth的split image bucket融合
  **recursive merge:当融合完成后，检查是否能继续和 split image融合
  */

  if (!directory->GetGlobalDepth()) {
    return;
  }

  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  const ExtendibleHTableBucketPage<K, V, KC> *bucket = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  uint32_t brother_bucket_idx = directory->GetSplitImageIndex(bucket_idx);

  // std::cout << "bucket_idx and brother bucket idx: " << bucket_idx << " " << brother_bucket_idx << std::endl;
  page_id_t brother_bucket_page_id = directory->GetBucketPageId(brother_bucket_idx);
  auto brother_bucket_guard = bpm_->FetchPageRead(brother_bucket_page_id);
  const ExtendibleHTableBucketPage<K, V, KC> *brother_bucket =
      brother_bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  if (auto left_bucket = CheckBucketMerge(directory, bucket, brother_bucket, bucket_idx, brother_bucket_idx);
      left_bucket != nullptr) {
    // std::cout << "merge\n";
    page_id_t left_bucket_page = (left_bucket == bucket ? bucket_page_id : brother_bucket_page_id);
    uint32_t left_bucket_idx = (left_bucket == bucket ? bucket_idx : brother_bucket_idx);
    // page_id_t deleted_page = (left_bucket != bucket ? bucket_page_id : brother_bucket_page_id);
    uint32_t deleted_bucket_idx = (left_bucket != bucket ? bucket_idx : brother_bucket_idx);
    //修改bucket_page_ids_[]
    // example: gp=2 lp=1
    directory->SetBucketPageId(deleted_bucket_idx, left_bucket_page);
    directory->DecrLocalDepth(bucket_idx);
    directory->DecrLocalDepth(brother_bucket_idx);
    bpm_->DeletePage(left_bucket_page);
    bucket_guard.Drop();
    brother_bucket_guard.Drop();
    //这一步是防止directory shrink后，原来的left_bucket_idx越界
    left_bucket_idx = (deleted_bucket_idx < left_bucket_idx ? deleted_bucket_idx : left_bucket_idx);
    while (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    }
    PrintHT();
    BucketMerge(directory, left_bucket_page, left_bucket_idx);
    // std::cout << "bucket_idx depth : " << directory->GetLocalDepth(bucket_idx) << std::endl;
  }
  // check if it's needed to shrink
  // std::cout << "wrong here\n";
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::ShrinkDirectoryOneTime(ExtendibleHTableDirectoryPage *directory) {
  directory->DecrGlobalDepth();
  uint32_t global_depth = directory->GetGlobalDepth();
  for (uint32_t idx = 1 << global_depth; idx < (1 << (global_depth + 1)); idx++) {
    directory->SetBucketPageId(idx, 0);
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
