//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  const table_oid_t scanned_table_oid = plan_->GetTableOid();
  Catalog *catalog_ptr = exec_ctx_->GetCatalog();
  // std::unique_ptr<TableHeap> scanned_table = std::move(catalog_ptr->GetTable(scanned_table_oid)->table_);
  BUSTUB_ASSERT(catalog_ptr->GetTable(scanned_table_oid)->table_ != nullptr, "table is empty!");
  iter_ptr_ = std::make_unique<TableIterator>(catalog_ptr->GetTable(scanned_table_oid)->table_->MakeIterator());
}

//从上下文中拿到bpm，从plan中拿到table_oid
/* scan得到一个没有被删除的tuple才返回，返回true
 * 当遍历完了返回false
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // fmt::print("start scan\n");
  while (!iter_ptr_->IsEnd()) {
    std::pair<TupleMeta, Tuple> scanned_tuple = iter_ptr_->GetTuple();

    if (auto filter_expr = plan_->filter_predicate_; filter_expr != nullptr) {
      // std::cout << "right here 1" << std::endl;
      Value value = filter_expr->Evaluate(&(scanned_tuple.second), GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++(*iter_ptr_);
        // return true;
        continue;
      }
      fmt::print(stderr, "let's see\n");
    }

    // 判断是否删除应该放在重建tuple 的 部分完成
    //if (scanned_tuple.first.is_deleted_) {
    //  ++(*iter_ptr_);
    //  continue;
    //}

    if (scanned_tuple.first.ts_ > exec_ctx_->GetTransaction()->GetReadTs()/*当前事务的时间戳*/) {
      // 此时应该重建 之前版本的tuple
      // 找到合适的 undo log 调用 ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
      //                                          const std::vector<UndoLog> &undo_logs)
      fmt::println(stderr, "timestamp: {} vs {}", scanned_tuple.first.ts_, exec_ctx_->GetTransaction()->GetReadTs());
      auto optional_undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(scanned_tuple.second.GetRid());
      if (optional_undo_link.has_value()) {
        auto undo_link = *optional_undo_link;
        std::vector<UndoLog> logs;
        
        bool is_arrived{false};
        // 找到第一个小于等于当前ts的log
        do {
          auto undo_log = exec_ctx_->GetTransactionManager()->GetUndoLog(undo_link);
          
            logs.push_back(undo_log);
          if (undo_log.ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
            is_arrived = true;
            break;
          }
          undo_link = undo_log.prev_version_;
        } while(undo_link.IsValid());

        if (!is_arrived) {
          ++(*iter_ptr_);
          continue;
        }

        //for (auto iter : *optional_undo_link) {
        //  if (iter->ts_ > exec_ctx_->GetTransaction()->GetReadTs()) {
        //    logs.push_back(*iter);
        // }
        //}

        auto reconstruct_tuple = ReconstructTuple(&GetOutputSchema(), scanned_tuple.second, scanned_tuple.first, logs);
        if (reconstruct_tuple.has_value()) {
          *tuple = *reconstruct_tuple;
        } else {
          ++(*iter_ptr_);
          continue;
        }
      } 
    } else {
      *tuple = scanned_tuple.second;
    }
    
    *rid = iter_ptr_->GetRID();
    ++(*iter_ptr_);
    return true;
  }
  fmt::print(stderr, "it's end\n");
  return false;
}

}  // namespace bustub
