#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto CheckAndExtract(const AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> &left_exprs,
                     std::vector<AbstractExpressionRef> &right_exprs, bool &parent_of_leave) -> bool {
  if (predicate->GetChildren().size() == 0) {
    // extract expressionRef
    // std::cout << "try to cast col_predicate\n";
    auto col_predicate = dynamic_cast<const ColumnValueExpression &>(*predicate);
    // std::cout << "cast to col_predicate succedd\n";
    if (col_predicate.GetTupleIdx() == 0) {
      left_exprs.push_back(predicate);
    } else {
      right_exprs.push_back(predicate);
    }
    parent_of_leave = true;
    return true;
  }
  if (!CheckAndExtract(predicate->GetChildAt(0), left_exprs, right_exprs, parent_of_leave)) {
    return false;
  }
  if (!CheckAndExtract(predicate->GetChildAt(1), left_exprs, right_exprs, parent_of_leave)) {
    return false;
  }
  if (parent_of_leave) {
    parent_of_leave = false;
    // check compare type
    // std::cout << "try to cast comp_predicate\n";
    auto comp_predicate = dynamic_cast<const ComparisonExpression &>(*predicate);
    // std::cout << "cast to comp_predicate succedd\n";
    if (comp_predicate.comp_type_ != ComparisonType::Equal) {
      return false;
    }
    // if not equal
    return true;
  }
  return true;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  // std::cout << "nlj as hash\n";
  // std::cout << plan->ToString() << std::endl;
  // std::cout << plan->GetChildren().size() << std::endl;
  int value = static_cast<int>(plan->GetType());
  // std::cout << "plan type: " << value << std::endl;
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    // std::cout << child->ToString() << std::endl;
    children.emplace_back(OptimizeNLJAsHashJoin(child));
    // std::cout << "herehere\n";
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  value = static_cast<int>(optimized_plan->GetType());

  // std::cout << "plan type: " << value << std::endl;

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    // std::cout << "confirm\n";
    const auto &plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // std::cout << "cast to nlj succeed\n";
    BUSTUB_ASSERT(plan.GetChildren().size() == 2, "must have two children\n");
    // check if predicate has only =
    std::vector<AbstractExpressionRef> left_exprs{};
    std::vector<AbstractExpressionRef> right_exprs{};
    bool parent_of_leave{false};
    auto predicate = plan.Predicate();
    fmt::print("check predicate: {}\n", predicate->ToString());

    if (!CheckAndExtract(predicate, left_exprs, right_exprs, parent_of_leave)) {
      return optimized_plan;
    }

    return std::make_shared<HashJoinPlanNode>(std::make_shared<bustub::Schema>(plan.OutputSchema()), plan.GetChildAt(0),
                                              plan.GetChildAt(1), left_exprs, right_exprs, plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
