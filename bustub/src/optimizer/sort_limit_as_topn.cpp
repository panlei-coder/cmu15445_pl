#include <memory>
#include <vector>
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  
  // 1.后序遍历
  std::vector<AbstractPlanNodeRef> children;
  for(const auto &child : plan->GetChildren()){ // 这里必须是引用，在递归调用的时候会修改子树，如果copy的话，修改后的结果不会产生影响
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  // 2.生成优化后的查询计划
  auto optimized_plan = plan->CloneWithChildren(std::move(children)); // 子树生成优化后的查询计划

  // 3.判断当前节点的plan是否应用TopN优化
  // 这里尤其需要注意的是必须当前节点的PlanType为Limit，子节点的PlanType为Sort才能转化为TopN
  if(plan->GetType() == PlanType::Limit && 
      plan->GetChildren().size() == 1 && plan->GetChildAt(0)->GetType() == PlanType::Sort){
      BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 1,"sort limit no possible !!!");
      const auto &child_plan = optimized_plan->GetChildren()[0];
      const auto &sort_plan = dynamic_cast<const SortPlanNode&>(*child_plan);
      const auto &limit_plan = dynamic_cast<const LimitPlanNode&>(*plan);
      return std::make_shared<TopNPlanNode>(plan->output_schema_,child_plan->GetChildAt(0),
                  sort_plan.GetOrderBy(),limit_plan.GetLimit());
  }

  return optimized_plan;
}

}  // namespace bustub
