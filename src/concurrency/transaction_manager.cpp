//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();
  // watermark
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  timestamp_t commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  for (auto &t : txn->GetWriteSets()) {
    auto t_id = t.first;
    std::cout << "t_id: " << t_id << std::endl;
    auto table_info = catalog_->GetTable(t_id);
    for (auto rid : t.second) {
      auto is_delete = table_info->table_->GetTupleMeta(rid).is_deleted_;
      table_info->table_->UpdateTupleMeta({commit_ts, is_delete}, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  last_commit_ts_++;
  txn->commit_ts_ = last_commit_ts_.load();

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  auto watermark = GetWatermark(); 
  // 遍历base table
  auto tablenames = catalog_->GetTableNames();
  for (auto tablename : tablenames) {
    TableInfo *table = catalog_->GetTable(tablename);
    auto table_iter = table->table_->MakeIterator();

    std::vector<txn_id_t> tail_txn;

    while (!table_iter.IsEnd()) {
      RID rid = table_iter.GetRID();
      auto optional_undo_link = GetUndoLink(rid);
      auto undo_link = *optional_undo_link;
      bool begin_delete{false};
      while (undo_link.IsValid()) {
        auto undo_log = GetUndoLog(undo_link);

        if (undo_log.ts_ == watermark) {
          // 标记
          begin_delete = true;
          tail_txn.push_back(undo_link.prev_txn_);
        } else if (undo_log.ts_ < watermark){
          if (!begin_delete) {
            begin_delete = true;            
            tail_txn.push_back(undo_link.prev_txn_);
          } else {
            // 删除
            undo_link.prev_txn_ = INVALID_TXN_ID;

          }
        }
        undo_link = undo_log.prev_version_;
      }      

      ++table_iter;
    }

    std::sort(tail_txn.begin(), tail_txn.end());
    auto last = std::unique(tail_txn.begin(), tail_txn.end());
    tail_txn.erase(last, tail_txn.end());

    for (auto &i : txn_map_) {
      if (std::find(tail_txn.cbegin(), tail_txn.cend(), i.first) == tail_txn.cend()) {
        txn_map_.erase(i.first);
      }
    }
  }
}

}  // namespace bustub
