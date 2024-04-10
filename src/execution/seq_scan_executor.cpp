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

    if (scanned_tuple.first.is_deleted_) {
      ++(*iter_ptr_);
      // return true;
      // std::cout << "deleted" << std::endl;
      continue;
    }

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

    *tuple = scanned_tuple.second;
    *rid = iter_ptr_->GetRID();
    ++(*iter_ptr_);
    return true;
  }
  fmt::print(stderr, "it's end\n");
  return false;
}

}  // namespace bustub
