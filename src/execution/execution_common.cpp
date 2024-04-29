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
  std::vector<Value> tuple_content;
  tuple_content.reserve(schema->GetColumns().size());
  fmt::println(stderr, "base tuple size: {}", base_tuple.GetLength());
  std::cout << base_tuple.ToString(schema) << std::endl;
  fmt::println(stderr, "in reconstrusction, here is right up");
  for (std::size_t i = 0; i < schema->GetColumns().size(); ++i)
  {
    fmt::println(stderr, "the {}th times", i);
    auto tmp = base_tuple.GetValue(schema, i);
    fmt::println(stderr, "in reconstrusction, here is right {}", i);
    tuple_content.push_back(tmp);
    
    fmt::println(stderr, "after reconstrusction, here is right {}", i);
  }
  fmt::println(stderr, "in reconstrusction, here is right down");
  for (auto log : undo_logs)
  {
    if (log.is_deleted_) {
      std::optional<Tuple> emp;
      return emp;
    }
    uint32_t idx = 0;
    for (std::size_t i = 0; i < tuple_content.size(); i++) {
      if (log.modified_fields_[i]) {
        tuple_content[i] = log.tuple_.GetValue(schema, idx++);
      }
    }
  }

  return std::make_optional<Tuple>(tuple_content, schema);
  
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
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
