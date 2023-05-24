#include "execution/executors/sort_executor.h"
#include <iterator>
#include "binder/bound_order_by.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include <iostream>
#include <ostream>

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {}

void SortExecutor::Init() {
    // 1.初始化子执行器
    child_executor_->Init();

    // 2.获取所有的tuple
    Tuple tuple;
    RID rid;
    while(child_executor_->Next(&tuple, &rid)){
        tuples_.push_back(tuple);
    }

    // for(const auto& t : tuples_){
    //     for(auto [orderby_type,expression] : plan_->GetOrderBy()){
    //         auto value = expression->Evaluate(&t, child_executor_->GetOutputSchema());
    //         auto a = 1;
    //         a++;
    //     }
    // }

    // auto compare = [this](const Tuple &tuple_first,const Tuple &tuple_second){ // &plan = plan_,&child_executor = child_executor_
    //     for(auto [orderby_type,expression] : plan_->GetOrderBy()){ // 这里的OrderBy可能有多个排序规则，依次获取对应的规则进行比较
    //         auto value_first = expression->Evaluate(&tuple_first, child_executor_->GetOutputSchema());
    //         auto value_second = expression->Evaluate(&tuple_second, child_executor_->GetOutputSchema());

    //         if(value_first.CompareEquals(value_second) == CmpBool::CmpTrue){
    //             continue;
    //         }

    //         if(orderby_type == OrderByType::DEFAULT || orderby_type == OrderByType::ASC){ // 默认或者是升序的排序方式
    //             if(value_first.CompareLessThan(value_second) == CmpBool::CmpTrue){
    //                 return true;
    //             }

    //             if(value_first.CompareGreaterThan(value_second) == CmpBool::CmpTrue){
    //                 return false;
    //             }
    //         }else if(orderby_type == OrderByType::DESC){  // 降序的排序方式
    //             if(value_first.CompareGreaterThan(value_second) == CmpBool::CmpTrue){
    //                 return true;
    //             }

    //             if(value_first.CompareLessThan(value_second) == CmpBool::CmpTrue){
    //                 return false;
    //             }
    //         }
    //     }

    //     UNREACHABLE("doesn't support duplicate key");
    // };

    // 3.对获取的所有tuple进行排序
    // std::stable_sort(tuples_.begin(),tuples_.end(),compare);
    SortTuples(tuples_);

    // 4.将排序好的tuples_倒序过来，方便取出
    std::reverse(tuples_.begin(),tuples_.end());
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // 1.判断是否为空
    if(tuples_.empty()){
        return false;
    }

    // 2.不为空(从尾部取出来)
    *tuple = tuples_.back();
    *rid = tuple->GetRid();
    tuples_.pop_back();

    return true;
}

// 对 vector 中的元素使用冒泡排序升序排序
void SortExecutor::SortTuples(std::vector<Tuple> &tuples) {
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
auto SortExecutor::CompareTuples(Tuple &a, Tuple &b) -> bool{
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
