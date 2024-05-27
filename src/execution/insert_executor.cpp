//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

/*
 *一次只能插一个值，还是一次插多个值 --> 应该是多个
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // std::cout<< "0" << std::endl;

  Catalog *catalog_ptr = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog_ptr->GetTable(plan_->GetTableOid());
  std::vector<IndexInfo *> index_infoes = catalog_ptr->GetTableIndexes(table_info->name_);

  IndexInfo *pk_index_info;
  for (auto p : index_infoes) {
    if (p->is_primary_key_) {
      pk_index_info = p;
    }
  }

  Tuple child_tuple{};
  int32_t inserted_tuple_number{0};
  // Get the next tuple
  // std::cout<< "1" << std::endl;

  if (!status) {
    return false;
  }

  while (child_executor_->Next(&child_tuple, rid)) {
    // if (child_tuple.GetData() == nullptr) {
    //  continue;
    //}

    // get the key
    auto key_schema = pk_index_info->index_->GetMetadata()->GetKeySchema();
    auto key_attrs = pk_index_info->index_->GetMetadata()->GetKeyAttrs();
    Tuple key = child_tuple.KeyFromTuple(GetOutputSchema(), *key_schema, key_attrs);
    // check if there is same tuple in index 
    std::vector<RID> res;
    if (pk_index_info->index_->ScanKey(key, &res, exec_ctx_->GetTransaction()); res.size() != 0) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw bustub::ExecutionException("tainted\n");
    }

    timestamp_t ts = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    std::optional<RID> optional_res = table_info->table_->InsertTuple({ts, false}, child_tuple);
    BUSTUB_ASSERT(optional_res.has_value(), "fail to insert to tableheap");
    exec_ctx_->GetTransactionManager()->UpdateVersionLink(*optional_res, std::nullopt);
    exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), *optional_res);

    Schema &tuple_schema = table_info->schema_;
    std::vector<Column> tuple_columns = tuple_schema.GetColumns();

    // check if there is the same tuple in index
    if (pk_index_info->index_->ScanKey(key, &res, exec_ctx_->GetTransaction()); res.size() != 0) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw bustub::ExecutionException("tainted\n");
    }
    // modify
    std::unique_lock<std::mutex> pk_index_lck(exec_ctx_->GetTransactionManager()->pk_index_mutex_);
    for (IndexInfo *index_info : index_infoes) {
      // get the index column value from tuple
      std::vector<Value> vals;

      Schema &index_schema = index_info->key_schema_;
      std::vector<Column> index_columns = index_schema.GetColumns();
      for (uint32_t index_idx = 0; index_idx < index_schema.GetColumnCount(); index_idx++) {
        for (uint32_t tuple_idx = 0; tuple_idx < tuple_schema.GetColumnCount(); tuple_idx++) {
          if (index_columns[index_idx].GetName() == tuple_columns[tuple_idx].GetName()) {
            vals.push_back(child_tuple.GetValue(&tuple_schema, tuple_idx));
            break;
          }
        }
      }
      Tuple key(vals, &index_schema);
      BUSTUB_ASSERT(index_info->index_->InsertEntry(key, *optional_res, nullptr), "fail to insert to index");
    }
    pk_index_lck.unlock();
    inserted_tuple_number++;
    // child_tuple = Tuple::Empty();
  }

  // std::cout<< "2" << std::endl;
  Value val(INTEGER, inserted_tuple_number);
  *tuple = Tuple({val}, &GetOutputSchema());
  status = false;
  return true;
}

}  // namespace bustub
