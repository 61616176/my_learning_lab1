#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  topn_deque_ = std::make_unique<TopNDeque>(plan_->n_, plan_->GetOrderBy());
}

void TopNExecutor::Init() {
  /*init child executor*/
  child_executor_->Init();

  topn_deque_->ShowCompTypes();

  Tuple child_tuple{};
  RID child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    /*get the key*/
    const Schema &schema = child_executor_->GetOutputSchema();
    std::vector<Value> evaluate_vals;
    std::vector<Value> tuple_vals;
    for (uint32_t idx = 0; idx < schema.GetColumnCount(); idx++) {
      tuple_vals.push_back(child_tuple.GetValue(&schema, idx));
    }
    for (const auto &i : plan_->order_bys_) {
      evaluate_vals.push_back(i.second->Evaluate(&child_tuple, schema));
    }
    sortKey key = std::make_pair(std::move(tuple_vals), std::move(evaluate_vals));
    topn_deque_->Push(key);
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  sortKey key{};
  bool status = topn_deque_->Pop(key);

  if (!status) {
    return false;
  }

  std::vector<Value> vals = key.first;
  *tuple = Tuple(vals, &child_executor_->GetOutputSchema());

  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return topn_deque_->Size(); };

}  // namespace bustub
