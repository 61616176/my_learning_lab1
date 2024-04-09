//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Catalog *catalog_ptr = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog_ptr->GetTable(plan_->GetTableOid());
  std::vector<IndexInfo *> index_infoes = catalog_ptr->GetTableIndexes(table_info->name_);

  Tuple child_tuple{};
  RID child_tuple_rid{0};
  int32_t updated_tuple_number = 0;

  if (!status) {
    return false;
  }

  const Schema &tuple_schema = child_executor_->GetOutputSchema();

  while (child_executor_->Next(&child_tuple, &child_tuple_rid)) {
    // delete the tuple by setting is_deleted_ = true
    table_info->table_->UpdateTupleMeta({0, true}, child_tuple_rid);

    // change the tuple
    std::vector<Value> new_values = {};
    for (auto i : plan_->target_expressions_) {
      new_values.push_back(i->Evaluate(&child_tuple, tuple_schema));
    }

    Tuple new_tuple(new_values, &tuple_schema);

    // insert tuple to table
    std::optional<RID> optional_res = table_info->table_->InsertTuple({0, false}, new_tuple);
    BUSTUB_ASSERT(optional_res.has_value(), "fail to insert to tableheap");

    Schema &tuple_schema = table_info->schema_;
    std::vector<Column> tuple_columns = tuple_schema.GetColumns();

    for (IndexInfo *index_info : index_infoes) {
      // get the index column value from tuple
      std::vector<Value> new_key_vals;
      std::vector<Value> deleted_key_vals;

      Schema &index_schema = index_info->key_schema_;
      std::vector<Column> index_columns = index_schema.GetColumns();
      for (uint32_t index_idx = 0; index_idx < index_schema.GetColumnCount(); index_idx++) {
        for (uint32_t tuple_idx = 0; tuple_idx < tuple_schema.GetColumnCount(); tuple_idx++) {
          if (index_columns[index_idx].GetName() == tuple_columns[tuple_idx].GetName()) {
            new_key_vals.push_back(new_tuple.GetValue(&tuple_schema, tuple_idx));
            deleted_key_vals.push_back(child_tuple.GetValue(&tuple_schema, tuple_idx));
            break;
          }
        }
      }

      Tuple new_key(new_key_vals, &index_schema);
      Tuple deleted_key(deleted_key_vals, &index_schema);
      index_info->index_->DeleteEntry(deleted_key, child_tuple_rid, nullptr);
      BUSTUB_ASSERT(index_info->index_->InsertEntry(new_key, *optional_res, nullptr), "fail to insert to index");
    }

    updated_tuple_number++;
  }

  Value val(INTEGER, updated_tuple_number);
  *tuple = Tuple({val}, &GetOutputSchema());

  status = false;
  return true;
}

}  // namespace bustub
