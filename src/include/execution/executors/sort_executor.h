//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
//#include <algorithm>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */

using sortKey = std::pair<std::vector<Value>, std::vector<Value>>;

class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /*
   *start first pos of duplicate value
   *end first pos of new value
   *idx current idx to find duplicate value
   *return false means that there is no more duplicate value in the rest of vector
   */
  auto FindDuplicateRange(const std::vector<sortKey>::iterator &search_begin,
                          const std::vector<sortKey>::iterator &search_end, std::vector<sortKey>::iterator &start,
                          std::vector<sortKey>::iterator &last, uint32_t idx) -> bool {
    const auto &it = std::adjacent_find(search_begin, search_end, [idx](const sortKey &first, const sortKey &second) {
      if (first.second.at(idx).CompareEquals(second.second.at(idx)) == CmpBool::CmpTrue) {
        return true;
      }
      return false;
    });

    // 如果找到了重复的相邻元素，我们需要找到这个重复值区间的结束位置
    if (it != search_end) {
      // 找到重复值的起始位置
      start = it;
      // 跳过重复值，指向下一个可能不重复的值
      last = it + 2;
      while (last != search_end) {
        if (last->second.at(idx).CompareEquals(it->second.at(idx)) == CmpBool::CmpTrue) {
          last++;
        } else {
          break;
        }
      }
      return true;
    }
    return false;
  }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<sortKey> sorted_vec_;

  /*represent if is finished*/
  bool status_{false};

  bool sorted_{false};
};
}  // namespace bustub
