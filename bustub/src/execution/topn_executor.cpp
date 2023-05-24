#include "execution/executors/topn_executor.h"
#include <vector>
#include "binder/bound_order_by.h"
#include "common/rid.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "type/type.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {
    // priority_queue_ = std::priority_queue<Tuple,std::vector<Tuple>,decltype(&TopNExecutor::Compare)>(&TopNExecutor::Compare);
}

void TopNExecutor::Init() {
    // 定义比较函数，并初始化一个优先队列
    // auto compare = [this](const Tuple &a,const Tuple &b) -> bool{
    //     for (auto [order_by_type, expression] : plan_->GetOrderBy()) {
    //         auto value_a = expression->Evaluate(&a, child_executor_->GetOutputSchema());
    //         auto value_b = expression->Evaluate(&b, child_executor_->GetOutputSchema());

    //         if(value_a.CompareEquals(value_b) == CmpBool::CmpTrue){
    //             continue;
    //         }

    //         if(order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC){
    //             if(value_a.CompareLessThan(value_b) == CmpBool::CmpTrue){
    //                 return true;
    //             }

    //             if(value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue){
    //                 return false;
    //             }
    //         }else if(order_by_type == OrderByType::DESC){
    //             if(value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue){
    //                 return true;
    //             }

    //             if(value_a.CompareLessThan(value_b) == CmpBool::CmpTrue){
    //                 return false;
    //             }
    //         }
    //     }

    //     UNREACHABLE("doesn't support duplicate key");
    // };
    // std::priority_queue<Tuple,std::vector<Tuple>,decltype(compare)> priority_queue(compare);

    // 1.获取限制的数量和初始化
    limit_count_ = plan_->GetN();
    child_executor_->Init();

    // 2.获取并添加到优先队列中
    Tuple tuple;
    RID rid;
    std::vector<Tuple> tuples;
    while(child_executor_->Next(&tuple, &rid)){
        tuples.push_back(tuple);
    }


    // 3.保存前limit_count_个tuple
    SortTuples(tuples);
    std::reverse(tuples.begin(),tuples.end());
    while(!tuples.empty() && limit_count_ > 0){
        queue_.push(tuples.back());
        tuples.pop_back();
        limit_count_--;
    }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // 1.判断队列是否为空
    if(queue_.empty()){
        return false;
    }

    *tuple = queue_.front();
    *rid = tuple->GetRid();
    queue_.pop();

    return true;
}

// 对 vector 中的元素使用冒泡排序升序排序
void TopNExecutor::SortTuples(std::vector<Tuple> &tuples) {
  int n = tuples.size();
  bool swapped = true;
  while (swapped) {
    swapped = false;
    for (int i = 1; i < n; i++) {
      if (!CompareTuples(tuples[i - 1], tuples[i])) {
        std::swap(tuples[i - 1], tuples[i]);
        swapped = true;
      }
    }
    n--;
  }
}

// 比较两个 tuple 的大小关系
auto TopNExecutor::CompareTuples(Tuple &a, Tuple &b) -> bool{
  for (auto [order_by_type, expression] : plan_->GetOrderBy()) {
    auto value_a = expression->Evaluate(&a, child_executor_->GetOutputSchema());
    auto value_b = expression->Evaluate(&b, child_executor_->GetOutputSchema());

    if(value_a.CompareEquals(value_b) == CmpBool::CmpTrue){
        continue;
    }

    if(order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC){
        if(value_a.CompareLessThan(value_b) == CmpBool::CmpTrue){
            return true;
        }

        if(value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue){
            return false;
        }
    }else if(order_by_type == OrderByType::DESC){
        if(value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue){
            return true;
        }

        if(value_a.CompareLessThan(value_b) == CmpBool::CmpTrue){
            return false;
        }
    }
  }

  UNREACHABLE("doesn't support duplicate key");
}

}  // namespace bustub
