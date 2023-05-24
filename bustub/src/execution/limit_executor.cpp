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
#include <memory>

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)){}

void LimitExecutor::Init() {
    limit_count_ = plan_->GetLimit();
    child_executor_->Init();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while (child_executor_->Next(tuple, rid)){
        if(limit_count_ > 0){
            limit_count_--;
            return true;
        }

        return false;
    }    

    return false;
}

}  // namespace bustub
