#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

auto GenerateInitialWindowFunctionValue(WindowFunctionType type) -> Value {
  switch (type) {
    case WindowFunctionType::CountStarAggregate:
    case WindowFunctionType::Rank:
      // Count start starts at zero.
          return ValueFactory::GetIntegerValue(0));
    case WindowFunctionType::CountAggregate:
    case WindowFunctionType::SumAggregate:
    case WindowFunctionType::MinAggregate:
    case WindowFunctionType::MaxAggregate:
      // Others starts at null.
          return ValueFactory::GetNullValueByType(TypeId::INTEGER));
  }
}

void WindowFunctionExecutor::WindowFunctionSort() {
  Tuple child_tuple{};
  RID child_rid{};
  const Schema &schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> evaluate_vals;
    std::vector<Value> tuple_vals;
    uint32_t window_idx{0};
    for (const auto &col : plan_->columns_) {
      // check it's a placeholder. 如何记录哪个对应哪个aggregate？
      if (col->GetChildren().size() == 0) {
        //
        tuple_vals.push_back(GenerateInitialWindowFunctionValue(plan_->window_functions_[idx++].type_))
      }
      tuple_vals.push_back(col->Evaluate(tuple, schema));
    }
    for (const auto &order_by : plan_->window_functions_[0].order_bys_) {
      evaluate_vals.push_back(order_by->Evaluate(tuple, schema));
    }
    sorted_table.push_back(std::make_pair(std::move(tuple_vals), std::move(evaluate_vals)));
  }

  int idx{0};
  auto order_by_type = plan_->window_functions_[0].order_bys_[idx].first;

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

  std::sort(sorted_table.begin(), sorted_table.end(), Sort);
  // std::cout << "sorted_table size: " << sorted_table.size() << std::endl;
  for (const auto &i : plan_->window_functions_[0].order_bys_) {
    if (i == plan_->window_functions_[0].order_bys_[0]) {
      continue;
    }
    idx++;
    auto begin = sorted_table.begin();
    auto end = sorted_table.end();
    auto start = begin;
    auto last = end;
    while (/*遍历完成*/ FindDuplicateRange(begin, end, start, last, idx - 1)) {
      order_by_type = plan_->window_functions_[0].order_bys_[idx].first;
      std::sort(start, last, Sort);
      begin = last;
    }
  }
}

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor->Init();

  // sort
  WindowFunctionSort(sorted_table_, &child_tuple, schema);

  // partition
  for (const auto &window_function : window_functions_) {
    //使用function的partition_by_划分table

    //每划分一次使用function_,type_建立一个hash table
  }

  // aggregate
  // partitionby相当于groupby， function相当于aggregate。这两个生成key、value

  //调用insertCombine
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!status) {
    return false;
  }

  // reuse aggregate的next
}
}  // namespace bustub
