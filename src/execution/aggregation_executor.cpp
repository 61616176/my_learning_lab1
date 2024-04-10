//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->aggregates_, plan_->agg_types_);
}

void AggregationExecutor::Init() {
  child_executor_->Init();
  status = true;
  is_first = true;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // fmt::print("start aggragate\n");
  if (!status) {
    return false;
  }

  // check if ht_ is empty
  bool is_empty{true};

  if (is_first) {
    // aggregate 操作
    while (child_executor_->Next(tuple, rid)) {
      is_empty = false;
      // std::cout << "1" << std::endl;

      auto key = MakeAggregateKey(tuple);
      auto val = MakeAggregateValue(tuple);

      // std::cout << "key: " << std::endl;
      // for (auto i : key.group_bys_) {
      //  std::cout << i.ToString() << std::endl;
      //}

      // std::cout << "val: " << std::endl;
      // for (auto i : val.aggregates_) {
      //  std::cout << i.ToString() << std::endl;
      //}

      aht_->InsertCombine(key, val);
    }

    aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
    is_first = false;
  }

  // std::cout << "2.5" << std::endl;
  //查找hash table
  std::vector<Value> vals;

  if (*aht_iterator_ != aht_->End()) {
    is_empty = false;
    // std::cout << "3" << std::endl;
    if (!plan_->GetGroupBys().empty()) {
      auto key = aht_iterator_->Key().group_bys_;
      vals.insert(vals.end(), key.begin(), key.end());
    }

    auto val = aht_iterator_->Val().aggregates_;
    vals.insert(vals.end(), val.begin(), val.end());

    ++(*aht_iterator_);
    *tuple = Tuple(vals, &GetOutputSchema());
    return true;
  } else if (*aht_iterator_ == aht_->Begin()) {
    // hash table is empty
    if (plan_->group_bys_.size() > 0) {
      return false;
    } else {
      vals = aht_->GenerateInitialAggregateValue().aggregates_;
      *tuple = Tuple(vals, &GetOutputSchema());

      aht_->Clear();
      status = false;
      return true;
    }
  } else {
    aht_->Clear();
    return false;
  }
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
