#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

// 还没有解决delete的问题
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::optional<Tuple> res;
  std::vector<Value> tuple_content;
  tuple_content.reserve(schema->GetColumns().size());
  for (std::size_t i = 0; i < schema->GetColumns().size(); ++i) {
    auto tmp = base_tuple.GetValue(schema, i);
    tuple_content.push_back(tmp);
  }
  if (!base_meta.is_deleted_) {
    res.emplace(tuple_content, schema);
  }

  for (auto log : undo_logs) {
    fmt::print(stderr, "construct\n");
    if (log.is_deleted_) {
      res.reset();
      continue;
    }

    uint32_t idx = 0;
    std::vector<Column> cols;
    for (std::size_t i = 0; i < tuple_content.size(); i++) {
      if (log.modified_fields_[i]) {
        cols.push_back(schema->GetColumn(i));
      }
    }
    Schema log_schema(cols);  // 本地的log schema 怎么保存的？后期tuple不会读不出来吗？
    for (std::size_t i = 0; i < tuple_content.size(); i++) {
      if (log.modified_fields_[i]) {
        tuple_content[i] = log.tuple_.GetValue(&log_schema, idx++);
      }
    }
    res.emplace(tuple_content, schema);
  }

  return res;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  auto iter = table_heap->MakeIterator();

  while (!iter.IsEnd()) {
    fmt::print(stderr, "hhhhh\n");
    fmt::print(stderr, "RID: {}", iter.GetRID().ToString());
    auto tuple_pair = iter.GetTuple();
    fmt::println(stderr, " ts={} tuple={} is_delete={}", tuple_pair.first.ts_,
                 tuple_pair.second.ToString(&table_info->schema_), tuple_pair.first.is_deleted_);

    auto optional_undo_link = txn_mgr->GetUndoLink(iter.GetRID());
    // if 在这里似乎没有起作用
    if (optional_undo_link.has_value()) {
      auto undo_link = *optional_undo_link;
      std::vector<UndoLog> logs;

      // 找到第一个小于等于当前ts的log
      do {
        if (!undo_link.IsValid()) {
          break;
        }
        if (txn_mgr->txn_map_.find(undo_link.prev_txn_) == txn_mgr->txn_map_.cend()) {
          fmt::println(stderr, "there is no undo log");
          break;
        }
        fmt::print(stderr, " txn{}@ ", undo_link.prev_txn_);
        
        auto undo_log = txn_mgr->GetUndoLog(undo_link);

        std::vector<uint32_t> modified_cols;
        for (uint32_t idx = 0; idx < undo_log.modified_fields_.size(); idx++) {
          if (undo_log.modified_fields_[idx]) {
            modified_cols.push_back(idx);
          }
        }
        auto part_of_schema = Schema::CopySchema(&table_info->schema_, modified_cols);

        fmt::print(stderr, "is deleted:{} ", undo_log.is_deleted_);
        fmt::print(stderr, "ts:{} ", undo_log.ts_);
        fmt::print(stderr, "tuple:{}\n", undo_log.tuple_.ToString(&part_of_schema));

        undo_link = undo_log.prev_version_;
      } while (true);

    } else {
      fmt::println(stderr, "no undo log");
    }
    ++iter;
    fmt::print(stderr, "\n");
  }

  /// fmt::println(
  //    stderr,
  //    "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //    "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example
  // output of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
