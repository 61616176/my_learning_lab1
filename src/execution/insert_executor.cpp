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
  //std::cout<< "0" << std::endl;

  Catalog *catalog_ptr = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog_ptr->GetTable(plan_->GetTableOid());
  std::vector<IndexInfo *> index_infoes = catalog_ptr->GetTableIndexes(table_info->name_);

  Tuple child_tuple{};
  int32_t inserted_tuple_number{0};
  // Get the next tuple
  //std::cout<< "1" << std::endl;

  if (!status) {
    return false;
  }

  while (child_executor_->Next(&child_tuple, rid)) {
    // if (child_tuple.GetData() == nullptr) {
    //  continue;
    //}
    std::optional<RID> optional_res = table_info->table_->InsertTuple({0, false}, child_tuple);
    BUSTUB_ASSERT(optional_res.has_value(), "fail to insert to tableheap");

    Schema &tuple_schema = table_info->schema_;
    std::vector<Column> tuple_columns = tuple_schema.GetColumns();

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

    inserted_tuple_number++;
    // child_tuple = Tuple::Empty();
  }

  //std::cout<< "2" << std::endl;
  Value val(INTEGER, inserted_tuple_number);
  *tuple = Tuple({val}, &GetOutputSchema());
  status = false;
  return true;
}

}  // namespace bustub
