#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  //递归优化plan子节点
  std::vector<AbstractPlanNodeRef> children;
  for (auto &i : plan->GetChildren()) {
    children.push_back(OptimizeSortLimitAsTopN(i));
  }
  AbstractPlanNodeRef optimized_plan = plan->CloneWithChildren(children);

  //判断是否为limit plannode
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    std::size_t limit = limit_plan.GetLimit();

    auto child = limit_plan.GetChildPlan();
    if (child->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child);
      auto &order_bys = sort_plan.GetOrderBy();
      auto sort_plan_child = sort_plan.GetChildPlan();
      //std::cout << "cast to topn success\n";
      return std::make_shared<TopNPlanNode>(std::make_shared<bustub::Schema>(plan->OutputSchema()), sort_plan_child,
                                            order_bys, limit);
    }
  }

  return optimized_plan;
}

}  // namespace bustub
