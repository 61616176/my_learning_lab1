//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_executor)),
      right_child_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // check whether it's end
  /// fmt::print("loop join\n");
  while (true) {
    // fmt::print("1\n");
    auto &left_schema = left_child_executor_->GetOutputSchema();
    // fmt::print("left: {}\n", left_schema.ToString());
    auto &right_schema = right_child_executor_->GetOutputSchema();
    // fmt::print("right: {}\n", right_schema.ToString());

    if (!inner_status_) {
      // fmt::print("2\n");
      outer_status_ = left_child_executor_->Next(&outer_tuple_, &outer_tuple_rid_);
      if (!outer_status_) {
        // fmt::print("**************************finish outer table ,end loop*****************\n");
        return false;
      }

      outer_vals_.clear();

      for (uint32_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
        outer_vals_.push_back(outer_tuple_.GetValue(&left_schema, idx));
      }
      for (uint32_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
        Value val = outer_tuple_.GetValue(&left_schema, idx);
        // fmt::print("outer value : {}\n", val.ToString());
      }
      right_child_executor_->Init();
      left_join_status_ = false;
      inner_status_ = true;
    }

    Tuple inner_tuple{};
    RID inner_tuple_rid;

    while (true) {
      inner_status_ = right_child_executor_->Next(&inner_tuple, &inner_tuple_rid);
      if (!inner_status_) {
        break;
      }
      // for (uint32_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
      //  Value val = inner_tuple.GetValue(&right_schema, idx);
      //  fmt::print("inner value: {}\n", val.ToString());
      //}
      Value value = plan_->Predicate()->EvaluateJoin(&outer_tuple_, left_child_executor_->GetOutputSchema(),
                                                     &inner_tuple, right_child_executor_->GetOutputSchema());

      // fmt::print("evaluated value's value: {} \n", value.ToString());

      if (!value.IsNull() && value.GetAs<bool>()) {
        // fmt::print("get equal tuples\n");
        left_join_status_ = true;
        break;
      }
    }
    if (inner_status_) {
      //生成新tuple
      auto vals = outer_vals_;
      for (uint32_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
        vals.push_back(inner_tuple.GetValue(&right_schema, idx));
      }
      Tuple res{vals, &GetOutputSchema()};
      // fmt::print("nest loop return value: ");

      *tuple = res;

      return true;
    }

    if (plan_->GetJoinType() == JoinType::LEFT && !left_join_status_) {
      auto vals = outer_vals_;
      for (uint32_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
        vals.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
      }
      Tuple res{vals, &GetOutputSchema()};
      *tuple = res;

      return true;
    }
  }
  return false;
}

}  // namespace bustub
