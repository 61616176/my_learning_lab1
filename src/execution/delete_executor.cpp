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
  const Schema &tuple_schema = child_executor_->GetOutputSchema();

  while (child_executor_->Next(&child_tuple, &child_tuple_rid)) {
    // 检查时间戳判定是不是self modification
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    timestamp_t ts = table_info->table_->GetTupleMeta(child_tuple_rid).ts_;
    timestamp_t tmp_ts = exec_ctx_->GetTransaction()->GetTransactionTempTs();
    if (txn_mgr->txn_map_.find(ts) == txn_mgr->txn_map_.end() && ts > exec_ctx_->GetTransaction()->GetReadTs()) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw bustub::ExecutionException("tainted\n");
    }

    if (txn_mgr->txn_map_.find(ts) != txn_mgr->txn_map_.end() && ts != tmp_ts) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw bustub::ExecutionException("tainted\n");
    }

    std::vector<bool> modified_fields;
    modified_fields.insert(modified_fields.end(), tuple_schema.GetColumns().size(), true);

    if (ts == tmp_ts) {
      // 检查是否是新插入tuple
      fmt::println(stderr, "self modification");
      auto undo_link = txn_mgr->GetUndoLink(child_tuple_rid);
      if (std::nullopt != undo_link) {
        // delete 的schema 会是原本tuple的schema， 因为tuple全部被修改
        fmt::println(stderr, "has undo link");
        fmt::println(stderr, "begin update undo log");
        auto undo_log = txn_mgr->GetUndoLog(*undo_link);
        // get undo log schema
        std::vector<Value> log_vals;
        std::vector<bool> log_modified = undo_log.modified_fields_;
        std::vector<uint32_t> log_cols;
        
        for (uint32_t idx = 0; idx < undo_log.modified_fields_.size(); idx++) {
          if (undo_log.modified_fields_[idx]) {
            log_cols.push_back(idx);
          }
        }

        auto log_schema = Schema::CopySchema(&tuple_schema, log_cols);
        fmt::print(stderr, "log val: \n");
        for (uint32_t idx = 0; idx < log_schema.GetColumns().size(); idx++) {
          log_vals.push_back(undo_log.tuple_.GetValue(&log_schema, idx));
          fmt::print(stderr, "{} ", undo_log.tuple_.GetValue(&log_schema, idx).ToString());
        }

        std::vector<Value> modified_value;
        for (uint32_t idx=0; idx < tuple_schema.GetColumns().size(); idx++) {
          modified_value.push_back(child_tuple.GetValue(&tuple_schema, idx));
        }

        // 修改filed、value、cols
        uint32_t col_to_val{0};
        for (uint32_t idx = 0; idx<modified_fields.size(); idx++) {
          if (modified_fields[idx] == true && log_modified[idx] == false) {
            // 插入
            // modified_cols 和 modified_values 一一对应
            fmt::print(stderr, "插入新的 {}", idx);
            log_modified[idx] = true;
            log_vals.push_back(modified_value[col_to_val]);
          } 
          if (modified_fields[idx]) {
            col_to_val++;
          } 
        }
        Tuple modified(log_vals, &tuple_schema);
        UndoLog new_undo_log{undo_log.is_deleted_, modified_fields, modified, undo_log.ts_, undo_log.prev_version_};

        exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
      }
    } else {
      // 创建新的undo log
      Tuple &modified = child_tuple;
      auto is_undo_link = txn_mgr->GetUndoLink(child_tuple_rid);
      UndoLink prev_version;
      if (is_undo_link.has_value()) {
        prev_version = *is_undo_link;
      }
      // 这个false可能出问题，不过先不改它

      fmt::println(stderr, "delted tuple is : {}", modified.ToString(&tuple_schema));

      UndoLog undo_log{false, modified_fields, modified, ts, prev_version};

      UndoLink undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
      // 修改version link
      // 如果上传的undolink ， 那么是如何去保存undo log 的呢
      if (!txn_mgr->UpdateUndoLink(child_tuple_rid, std::optional(std::move(undo_link)))) {
        return false;
      }
    }

    // 这个是修改table heap ， 不确定需不需要改进
    table_info->table_->UpdateTupleMeta({tmp_ts, true}, child_tuple_rid);

    exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), child_tuple_rid);

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
