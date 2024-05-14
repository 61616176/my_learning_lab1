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
    fmt::print(stderr, "begin\n");
    // delete the tuple by setting is_deleted_ = true
    // table_info->table_->UpdateTupleMeta({0, true}, child_tuple_rid);
    // change the tuple
    std::vector<Value> new_values = {};
    for (auto i : plan_->target_expressions_) {
      new_values.push_back(i->Evaluate(&child_tuple, tuple_schema));
    }
    Tuple new_tuple(new_values, &tuple_schema);

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

    // 判断当前修改了的内容
    std::vector<Value> old_values = {};
    for (uint32_t idx = 0; idx < tuple_schema.GetColumnCount(); idx++) {
      old_values.push_back(child_tuple.GetValue(&tuple_schema, idx));
    }
    std::vector<bool> modified_fields;
    std::vector<Value> modified_value;
    std::vector<uint32_t> modified_cols;
    uint32_t col_idx(0);
    for (auto iter_new = new_values.cbegin(), iter_old = old_values.cbegin(); iter_new != new_values.cend();
         iter_new++, iter_old++) {
      if (iter_new->CompareNotEquals(*iter_old) == CmpBool::CmpTrue) {
        modified_fields.push_back(true);
        modified_value.push_back(*iter_old);
        modified_cols.push_back(col_idx);
      } else {
        //modified_value.emplace(TypeId:INVALID);
        modified_fields.push_back(false);
      }
      col_idx++;
    }
      for (auto i : modified_cols) {
          fmt::print(stderr, " {}", i);
        }
        fmt::println(stderr, "size: {}", modified_cols.size());
        std::cerr << std::endl;
    fmt::print(stderr, "succeed foing here\n");


    if (ts == tmp_ts) {
      fmt::print(stderr, "self modification\n");
      // 检查是否是新插入tuple
      auto undo_link = txn_mgr->GetUndoLink(child_tuple_rid);
      if (std::nullopt != undo_link) {
        fmt::println(stderr, "begin update undo log");
        auto undo_log = txn_mgr->GetUndoLog(*undo_link);
        // get undo log schema
        std::vector<uint32_t> log_cols;
        std::vector<Value> log_vals;
        std::vector<bool> log_modified = undo_log.modified_fields_;
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
        // 修改filed、value、cols
        uint32_t col_to_val{0};
        for (uint32_t idx = 0; idx<modified_fields.size(); idx++) {
          if (modified_fields[idx] == true && log_modified[idx] == false) {
            // 插入
            // modified_cols 和 modified_values 一一对应
            fmt::print(stderr, "插入新的 {}", idx);
            log_modified[idx] = true;
            log_cols.push_back(idx);
            log_vals.push_back(modified_value[col_to_val]);
          } 
          if (modified_fields[idx]) {
            col_to_val++;
          } 
        }
        
        for (auto i : log_cols) {
          fmt::print(stderr, " {}", i);
        }
        std::cerr << std::endl;

        fmt::println(stderr, "begin copy schema");
        auto modified_schema = Schema::CopySchema(&tuple_schema, log_cols);
        fmt::println(stderr, "finish copy schema. schema : {}", modified_schema.ToString());
        Tuple modified(log_vals, &modified_schema);
        UndoLog new_undo_log{undo_log.is_deleted_, log_modified, modified, undo_log.ts_, undo_log.prev_version_};
        fmt::println(stderr, "update undo log");
        exec_ctx_->GetTransaction()->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
        fmt::println(stderr, "finish update undo log");
      }
    } else {
      // 创建新的undo log
      fmt::print(stderr, "create new log\n");
      auto modified_schema = Schema::CopySchema(&tuple_schema, modified_cols);
      Tuple modified(modified_value, &modified_schema);
      auto is_undo_link = txn_mgr->GetUndoLink(child_tuple_rid);
      UndoLink prev_version;
      if (is_undo_link.has_value()) {
        prev_version = *is_undo_link;
      }
      UndoLog undo_log{false, modified_fields, modified, ts, prev_version};

      UndoLink undo_link = exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
      // 修改version link
      // 如果上传的undolink ， 那么是如何去保存undo log 的呢
      if (!txn_mgr->UpdateUndoLink(child_tuple_rid, std::optional(std::move(undo_link)))) {
        return false;
      }
    }

    if (!table_info->table_->UpdateTupleInPlace({tmp_ts, 0}, new_tuple, child_tuple_rid)) {
      return false;
    }

    exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), child_tuple_rid);
    // 下面的应该是要被删除的
    // insert tuple to table
    // std::optional<RID> optional_res = table_info->table_->InsertTuple({0, false}, new_tuple);
    // BUSTUB_ASSERT(optional_res.has_value(), "fail to insert to tableheap");

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
      BUSTUB_ASSERT(index_info->index_->InsertEntry(new_key, child_tuple_rid, nullptr), "fail to insert to index");
    }

    updated_tuple_number++;
  }

  Value val(INTEGER, updated_tuple_number);
  *tuple = Tuple({val}, &GetOutputSchema());

  status = false;
  return true;
}

}  // namespace bustub
