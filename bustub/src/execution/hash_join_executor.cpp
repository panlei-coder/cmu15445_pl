//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/rid.h"
#include "execution/executors/abstract_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(left_child)),
      right_child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(right_child)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // 1.初始化两个子查询
  left_child_executor_->Init();
  right_child_executor_->Init();
  current_index_ = -1;
  is_matched_ = false;

  // 2.获取左表的tuple并hash
  Tuple right_tuple;
  RID right_rid;
  while(right_child_executor_->Next(&right_tuple, &right_rid)){
    // 2.1.获取join_key并hash
    JoinKey right_join_key{plan_->RightJoinKeyExpression().Evaluate(&right_tuple, right_child_executor_->GetOutputSchema())};
    
    // 2.2.先判断对应的hash_key是否已经存在了，如果不存在，则初始化vector,存在的话就直接emplace_back
    if(right_hash_map_.find(right_join_key) == right_hash_map_.end()){
      right_hash_map_.insert({right_join_key,{right_tuple}});
    }else{
      right_hash_map_[right_join_key].emplace_back(right_tuple);
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid;

  while(true){
    // 1.判断是否需要获取下一个tuple（last_index_ == -1：初始化/当前的左表某个tuple完成了连接）
    if(static_cast<int>(current_index_) == -1 || right_hash_map_[left_join_key_].size() <= current_index_){
      // 1.1.获取左表的下一个tuple,如果获取失败，直接返回false
      if(!left_child_executor_->Next(&left_tuple_, &left_rid)){
        return false;
      }

      // 1.2.更新last_index_、is_matched_、left_join_key_
      current_index_ = 0;
      is_matched_ = false;
      left_join_key_ = (JoinKey){plan_->LeftJoinKeyExpression().Evaluate(&left_tuple_,left_child_executor_->GetOutputSchema())};
    }

    // 2.右表中可能存在可以与左表中匹配的tuple
    if(right_hash_map_.find(left_join_key_) != right_hash_map_.end()){
      for(;current_index_ < right_hash_map_[left_join_key_].size();){
        auto right_tuple = right_hash_map_[left_join_key_][current_index_]; 
        auto right_join_key = (JoinKey){plan_->RightJoinKeyExpression().Evaluate(&right_tuple, right_child_executor_->GetOutputSchema())};
        if(left_join_key_ == right_join_key){
          is_matched_ = true; // 更改标记，已经找到了连接匹配的tuple

          std::vector<Value> value;
          auto left_schema = left_child_executor_->GetOutputSchema();
          auto right_schema = right_child_executor_->GetOutputSchema();
          // 2.1.获取左表中对应的数据列
          for(uint32_t i = 0;i < left_schema.GetColumnCount();i++){
            value.push_back(left_tuple_.GetValue(&left_schema, i));
          }
          // 2.2.获取右表中对应的数据列
          for(uint32_t j = 0;j < right_schema.GetColumnCount();j++){
            value.push_back(right_tuple.GetValue(&right_schema, j));
          }
          // 2.3.创建新的tuple，并返回true
          *tuple = Tuple(value,&plan_->OutputSchema());

          // 2.4.更新current_index_
          current_index_++;

          return true;
        }
      }
    }

    // 3.右表中没有可以匹配的tuple且是左连接
    if(plan_->GetJoinType() == JoinType::LEFT && !is_matched_){
      std::vector<Value> value;
      auto left_schema = left_child_executor_->GetOutputSchema();
      auto right_schema = right_child_executor_->GetOutputSchema();
      // 3.1.获取左表中对应的数据列
      for(uint32_t i = 0;i < left_schema.GetColumnCount();i++){
        value.push_back(left_tuple_.GetValue(&left_schema, i));
      }
      // 3.2.获取右表中对应的数据列
      for(uint32_t j = 0;j < right_schema.GetColumnCount();j++){
        value.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(j).GetType()));
      }
      // 3.3.创建新的tuple，并返回true
      *tuple = Tuple(value,&plan_->OutputSchema());
      return true;
    }
  }
}

}  // namespace bustub
