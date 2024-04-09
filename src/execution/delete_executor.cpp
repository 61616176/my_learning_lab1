//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  std::cout << "begin delete\n";
  if (status) {
    return false;
  }

  Catalog *catalog_ptr = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog_ptr->GetTable(plan_->GetTableOid());
  std::vector<IndexInfo *> index_infoes = catalog_ptr->GetTableIndexes(table_info->name_);

  Tuple child_tuple{};
  int32_t deleted_tuple_number{0};
  RID child_tuple_rid{};

  while (child_executor_->Next(&child_tuple, &child_tuple_rid)) {
    table_info->table_->UpdateTupleMeta({0, true}, child_tuple_rid);

    Schema &tuple_schema = table_info->schema_;
    std::vector<Column> tuple_columns = tuple_schema.GetColumns();

    for (IndexInfo *index_info : index_infoes) {
      // get the index column value from tuple
      std::vector<Value> deleted_key_vals;

      Schema &index_schema = index_info->key_schema_;
      std::vector<Column> index_columns = index_schema.GetColumns();
      for (uint32_t index_idx = 0; index_idx < index_schema.GetColumnCount(); index_idx++) {
        for (uint32_t tuple_idx = 0; tuple_idx < tuple_schema.GetColumnCount(); tuple_idx++) {
          if (index_columns[index_idx].GetName() == tuple_columns[tuple_idx].GetName()) {
            deleted_key_vals.push_back(child_tuple.GetValue(&tuple_schema, tuple_idx));
            break;
          }
        }
      }

      Tuple deleted_key(deleted_key_vals, &index_schema);
      index_info->index_->DeleteEntry(deleted_key, child_tuple_rid, nullptr);
    }

    deleted_tuple_number++;
  }
  std::cout << "deleted number is : " << deleted_tuple_number << std::endl;
  Value val(INTEGER, deleted_tuple_number);
  *tuple = Tuple({val}, &GetOutputSchema());

  status = true;
  return true;
}

}  // namespace bustub
