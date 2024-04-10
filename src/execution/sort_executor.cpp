#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() { child_executor_->Init(); }

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // std::cout << "begin sort\n";
  if (plan_->order_bys_[0].first == OrderByType::INVALID) {
    return false;
  }
  // check if end
  if (status_) {
    return false;
  }

  const Schema &schema = child_executor_->GetOutputSchema();

  // if it's sorted
  if (!sorted_) {
    //循环收集entry
    Tuple child_tuple{};
    RID child_rid{};

    while (child_executor_->Next(&child_tuple, &child_rid)) {
      // Insert table
      std::vector<Value> evaluate_vals;
      std::vector<Value> tuple_vals;
      for (uint32_t idx = 0; idx < schema.GetColumnCount(); idx++) {
        tuple_vals.push_back(child_tuple.GetValue(&schema, idx));
      }
      for (const auto &i : plan_->order_bys_) {
        evaluate_vals.push_back(i.second->Evaluate(&child_tuple, schema));
      }
      sorted_vec_.push_back(std::make_pair(std::move(tuple_vals), std::move(evaluate_vals)));
    }
    // std::cout << "scan over\n";

    int idx{0};
    auto order_by_type = plan_->order_bys_[idx].first;

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

    std::sort(sorted_vec_.begin(), sorted_vec_.end(), Sort);
    // std::cout << "sorted_vec_ size: " << sorted_vec_.size() << std::endl;
    for (const auto &i : plan_->order_bys_) {
      if (i == plan_->order_bys_[0]) {
        continue;
      }
      idx++;
      auto begin = sorted_vec_.begin();
      auto end = sorted_vec_.end();
      auto start = begin;
      auto last = end;
      while (/*遍历完成*/ FindDuplicateRange(begin, end, start, last, idx - 1)) {
        order_by_type = plan_->order_bys_[idx].first;
        std::sort(start, last, Sort);
        begin = last;
      }
    }
    // std::cout << "sort finished\n";
    sorted_ = true;
  }

  auto &res = sorted_vec_.front();

  *tuple = Tuple(res.first, &schema);
  sorted_vec_.erase(sorted_vec_.begin());

  if (sorted_vec_.empty()) {
    status_ = true;
  }
  return true;
}

}  // namespace bustub
