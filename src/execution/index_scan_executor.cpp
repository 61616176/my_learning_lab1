//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { status = true; }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!status) {
    return false;
  }
  Catalog *catalog_ptr = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog_ptr->GetTable(plan_->table_oid_);
  IndexInfo *index_info = catalog_ptr->GetIndex(plan_->GetIndexOid());

  // index to get rid
  Value key = plan_->pred_key_->Evaluate(nullptr, index_info->key_schema_);
  // std::cout << key.ToString() << std::endl;
  std::vector<RID> result = {};
  Schema &key_schema = index_info->key_schema_;
  index_info->index_->ScanKey({{key}, &key_schema}, &result, nullptr);  // Value become Tuple is ok?
  if (result.empty()) {
    return false;
  }
  *rid = result[0];

  // get tuple from table by rid
  // std::pair<TupleMeta, Tuple> res = table_info->table_->GetTuple(*rid);
  auto [index_meta, index_tuple] = table_info->table_->GetTuple(*rid);

  if (!index_meta.is_deleted_) {
    *tuple = index_tuple;
  } else {
    return false;
  }

  status = false;
  return true;
}

}  // namespace bustub
