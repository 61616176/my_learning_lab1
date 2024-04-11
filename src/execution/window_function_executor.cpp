#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/*
 *start first pos of duplicate value
 *end first pos of new value
 *idx current idx to find duplicate value
 *return false means that there is no more duplicate value in the rest of vector
 */
auto WindowFindDuplicateRange(const std::vector<sortKey>::iterator &search_begin,
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

void WindowFunctionExecutor::WindowFunctionAggregateAndSort() {
  Tuple child_tuple{};
  RID child_rid{};
  const Schema &schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> evaluate_vals;
    std::vector<Value> tuple_vals;
    uint32_t window_idx{0};
    for (const auto &col : plan_->columns_) {
      // aggregate
      if (col->GetChildren().size() == 0) {
        //在这一步insertcombine？
        // tuple_vals.push_back(GenerateInitialWindowFunctionValue(plan_->window_functions_[idx++].type_))
        auto &window_function = plan_->window_functions_.at(window_idx);
        //生成key、value
        AggregateKey key;
        AggregateValue value;
        for (const auto &partition_by : window_function.partition_by_) {
          key.group_bys_.push_back(partition_by->Evaluate(&child_tuple, schema));
        }
        const auto &function = window_function.function_;
        value.aggregates_.push_back(function->Evaluate(&child_tuple, schema));

        //插入对应的aht_vec_中
        //读取值插入table中, 确定只返回一个Value
        auto res = aht_vec_[window_idx]->InsertCombine(key, value);

        tuple_vals.push_back(res[0]);
      } else {
        tuple_vals.push_back(col->Evaluate(&child_tuple, schema));
      }
    }
    for (const auto &order_by : plan_->window_functions_.at(0).order_by_) {
      evaluate_vals.push_back(order_by.second->Evaluate(&child_tuple, schema));
    }
    sorted_table_.push_back(std::make_pair(std::move(tuple_vals), std::move(evaluate_vals)));
  }

  // begin sort
  int idx{0};
  auto order_by_type = plan_->window_functions_.at(0).order_by_[idx].first;

  auto Sort = [&](const sortKey &first, const sortKey &second) {
    if (order_by_type == OrderByType::DEFAULT or order_by_type == OrderByType::ASC) {
      if (first.second.at(idx).CompareLessThan(second.second.at(idx)) == CmpBool::CmpTrue) {
        return true;
      }
    } else {
      if (first.second.at(idx).CompareGreaterThan(second.second.at(idx)) == CmpBool::CmpTrue) {
        return true;
      }
    }
    return false;
  };

  std::sort(sorted_table_.begin(), sorted_table_.end(), Sort);
  // std::cout << "sorted_table_ size: " << sorted_table_.size() << std::endl;
  for (const auto &i : plan_->window_functions_.at(0).order_by_) {
    if (i == plan_->window_functions_.at(0).order_by_[0]) {
      continue;
    }
    idx++;
    auto begin = sorted_table_.begin();
    auto end = sorted_table_.end();
    auto start = begin;
    auto last = end;
    while (/*遍历完成*/ WindowFindDuplicateRange(begin, end, start, last, idx - 1)) {
      order_by_type = plan_->window_functions_.at(0).order_by_[idx].first;
      std::sort(start, last, Sort);
      begin = last;
    }
  }
}

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();

  // init aggregate table
  for (const auto &window_function : plan_->window_functions_) {
    auto aht = std::make_unique<WindowFunctionHashTable>({window_function.second.function_}, {window_function.second.type_});
    aht_vec_.push_back(std::move(aht));
  }

  // aggregate and sort
  WindowFunctionAggregateAndSort();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!status_) {
    return false;
  }


  auto sort_key = sorted_table_.front();
  auto vals = sort_key.first;
  *tuple = Tuple(vals, &GetOutputSchema());

  sorted_table_.erase(sorted_table_.begin());
  if (sorted_table_.empty()) {
    status_ = false;
  }
  return true;

  // reuse aggregate的next
}
}  // namespace bustub
