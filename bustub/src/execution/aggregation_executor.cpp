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

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::forward<std::unique_ptr<AbstractExecutor>>(child)),
      aht_(plan_->GetAggregates(),plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      has_no_tuple_(true){}

void AggregationExecutor::Init() { // 初始化的时候将所有tuple按照聚集函数的要求进行一遍，然后再调用next判断hash表是不是空的
    aht_.Clear();
    child_->Init();

    Tuple tuple;
    RID rid;
    while(child_->Next(&tuple, &rid)){
        aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    }

    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // 1.如果已经遍历完了
    if(aht_iterator_ == aht_.End()){ // 说明hash表是空的
        if(has_no_tuple_ && plan_->GetGroupBys().empty()){ // 没有元组且plan_中的group by也是空的，需要返回对应空的情况 todo ? 
            std::vector<Value> values(aht_.GenerateInitialAggregateValue().aggregates_);
            *tuple = Tuple(values,&plan_->OutputSchema());
            *rid = (*tuple).GetRid();
            has_no_tuple_ = false;
            return true;
        }

        return false;
    }

    // 2.如果还没有遍历完
    auto key = aht_iterator_.Key().group_bys_;    // aggregateKey
    auto value = aht_iterator_.Val().aggregates_; // aggregateValue
    auto result = key;
    for(const auto &item : value){
        result.push_back(item);
    }

    *tuple = Tuple(result,&plan_->OutputSchema());
    *rid = tuple->GetRid();
    has_no_tuple_ = false;
    ++aht_iterator_;
    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
