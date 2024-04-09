//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() { child_executor_->Init(); }

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //std::cout << "begin limit\n";
  if (cur_num == plan_->GetLimit()) {
    return false;
  }

  Tuple child_tuple{};
  RID child_rid;

  bool status = child_executor_->Next(&child_tuple, &child_rid);
  if (!status) {
    return false;
  }

  cur_num++;
  *tuple = child_tuple;
  *rid = child_rid;

  return true;
}

}  // namespace bustub
