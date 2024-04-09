//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  //std::cout << 1 << std::endl;
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_rid;
  // construct hash join table
  if (!is_hmap_built_) {
    const Schema &left_schema = left_child_->GetOutputSchema();
    while (left_child_->Next(&child_tuple, &child_rid)) {
      HashJoinKey key;
      for (const auto &i : plan_->LeftJoinKeyExpressions()) {
        key.group_bys_.push_back(i->Evaluate(&child_tuple, left_schema));
      }

      TupleValue value;
      for (uint32_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
        value.values_.push_back(child_tuple.GetValue(&left_schema, idx));
      }

      if (hmap_.count(key) > 0) {
        hmap_[key].tuple_values_.push_back(value);
      } else {
        // HashJoinValue h_value;
        // h_value.tuple_values_.push_back(value);
        hmap_.insert({key, {value}});
      }
      if (std::find(not_matching_.begin(), not_matching_.end(), key) == not_matching_.end()) {
        not_matching_.push_back(key);
        //std::cout << "first not matching size: " << not_matching_.size() << std::endl;
      }
    }
    is_hmap_built_ = true;
  }

  if (!tuple_keeper_.empty()) {
    *tuple = tuple_keeper_.back();
    tuple_keeper_.pop_back();
    return true;
  }

  // scan right table
  const Schema &right_schema = right_child_->GetOutputSchema();
  while (right_child_->Next(&child_tuple, &child_rid)) {
    HashJoinKey key;
    for (const auto &i : plan_->RightJoinKeyExpressions()) {
      key.group_bys_.push_back(i->Evaluate(&child_tuple, right_schema));
    }

    if (hmap_.count(key) > 0) {
      // construct a join tuple
      TupleValue right_value;
      for (uint32_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
        right_value.values_.push_back(child_tuple.GetValue(&right_schema, idx));
      }

      for (auto i : hmap_[key].tuple_values_) {
        i.values_.insert(i.values_.end(), right_value.values_.begin(), right_value.values_.end());
        tuple_keeper_.push_back(Tuple(i.values_, &GetOutputSchema()));
      }

      *tuple = tuple_keeper_.back();
      tuple_keeper_.pop_back();

      auto pos = std::find(not_matching_.begin(), not_matching_.end(), key);
      if (pos != not_matching_.end()) {
        not_matching_.erase(pos);
        //std::cout << "not matching - 1,now size is: " << not_matching_.size() << std::endl;
      }

      return true;
    }
  }
  //std::cout << "not matching size: " << not_matching_.size() << std::endl;
  while (plan_->GetJoinType() == JoinType::LEFT && not_matching_.size() > 0) {
    //std::cout << "join type==left\n";
    HashJoinKey key = not_matching_.back();

    // construct null value of right schema
    std::vector<Value> right_value;
    for (uint32_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
      right_value.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
    }

    for (auto i : hmap_[key].tuple_values_) {
      i.values_.insert(i.values_.end(), right_value.begin(), right_value.end());
      tuple_keeper_.push_back(Tuple(i.values_, &GetOutputSchema()));
    }

    *tuple = tuple_keeper_.back();
    tuple_keeper_.pop_back();

    not_matching_.pop_back();
    return true;
  }

  hmap_.clear();
  not_matching_.clear();
  return false;
}

}  // namespace bustub
