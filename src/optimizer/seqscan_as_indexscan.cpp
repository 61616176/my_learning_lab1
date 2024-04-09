#include "optimizer/optimizer.h"

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule

  // optimize children
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  // auto optimized_plan = plan->CloneWithChildren(std::move(children));
  auto optimized_plan = plan->CloneWithChildren(children);

  // check the type
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const SeqScanPlanNode &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    const TableInfo *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
    const std::vector<IndexInfo *> index_infoes = catalog_.GetTableIndexes(table_info->name_);

    // check the child type
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      auto pred_children = seq_scan_plan.filter_predicate_->GetChildren();

      if (pred_children.empty()) {
        return optimized_plan;
      }

      BUSTUB_ASSERT(pred_children.size() == 2, "filled filter_predicate_ must have 2 kids with at least one layer");

      if (pred_children[0]->GetChildren().empty() || pred_children[1]->GetChildren().empty()) {
        const ComparisonExpression &comp_expr =
            dynamic_cast<const ComparisonExpression &>(*(seq_scan_plan.filter_predicate_));

        // assume left child is column; right child is key
        const ColumnValueExpression &col_expr = dynamic_cast<const ColumnValueExpression &>(*(comp_expr.children_[0]));
        const std::vector<uint32_t> col_idx = {col_expr.GetColIdx()};
        ConstantValueExpression &cons_expr = dynamic_cast<ConstantValueExpression &>(*(comp_expr.children_[1]));

        for (const auto &index_info : index_infoes) {
          if (index_info->index_->GetMetadata()->GetKeyAttrs() == col_idx) {
            return std::make_unique<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                       index_info->index_oid_, seq_scan_plan.filter_predicate_,
                                                       &cons_expr);
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
