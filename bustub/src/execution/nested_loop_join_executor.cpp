//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(left_executor)),
      right_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(right_executor)),
      has_no_tuple_(false){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();
  is_matched_ = false;
  left_is_initialized_ = false;
  right_is_need_initialized_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;

  if(has_no_tuple_){
    return false;
  }
  // 1.判断left_tuple_是否是初始化的
  // 如果left_tuple_为空，需要从获取一个，如果不为空，则不需要获取，同时如果获取不到说明已经连接结束了
  // if(left_tuple_.GetLength() == 0){ // 说明是未初始化的
  //   if(!left_executor_->Next(&left_tuple_, &left_rid)){
  //     return false; // 获取失败了说明是已经连接结束了
  //   }
  // }
  if(!left_is_initialized_){
    if(!left_executor_->Next(&left_tuple_, &left_rid)){
      return false; // 获取失败了说明是已经连接结束了
    }
    left_is_initialized_ = true;
  }

  // 2.不断获取右表的tuple，判断是否匹配，匹配不成功，获取下一个tuple
  while(true){
    // 2.1.获取右表的tuple
    if(right_is_need_initialized_ || !right_executor_->Next(&right_tuple, &right_rid)){ // 右表遍历完了
      // 更改右表的标记位，表示已经遍历到了右表的尾部
      right_is_need_initialized_ = true;

      // 右表遍历完了之后需要判断是否找到了可以匹配的连接对象，这里针对左连接特殊处理
      if(!is_matched_ && plan_->join_type_ == JoinType::LEFT){ // 没有找到且是左连接
        is_matched_ = true; // 更改标记位

        std::vector<Value> value;
        auto left_schema = left_executor_->GetOutputSchema();
        auto right_schema = right_executor_->GetOutputSchema();
        // 2.1.1.获取左表中对应的数据列
        for(uint32_t i = 0;i < left_schema.GetColumnCount();i++){
          value.push_back(left_tuple_.GetValue(&left_schema, i));
        }
        // 2.1.2.获取右表中对应的数据列
        for(uint32_t j = 0;j < right_schema.GetColumnCount();j++){
          value.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(j).GetType())); //GetNullValueByType(TypeId::INTEGER)
        }
        // 2.1.3.创建新的tuple，并返回true
        *tuple = Tuple(value,&plan_->OutputSchema());
        return true;
      }

      if(left_executor_->Next(&left_tuple_, &left_rid)){ // 左表获取到了下一个
        is_matched_ = false; // 初始化为false
      }else{  // 左表遍历完了
        has_no_tuple_ = true;
        return false; // 连接结束，右表不需要回溯
      }

      // 右表需要回溯，并更新right_tuple
      right_executor_->Init();
      right_executor_->Next(&right_tuple, &right_rid);
      right_is_need_initialized_ = false;
    }

    // 2.2.判断是否匹配，如果匹配成功
    if(right_tuple.GetLength() != 0 && 
          plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), 
          &right_tuple, right_executor_->GetOutputSchema()).CompareEquals(Value(TypeId::BOOLEAN,
          static_cast<int8_t>(1))) == CmpBool::CmpTrue){
      is_matched_ = true; // 标记当前左表的tuple在右表中找到了可以连接的对象
      
      std::vector<Value> value;
      auto left_schema = left_executor_->GetOutputSchema();
      auto right_schema = right_executor_->GetOutputSchema();
      // 2.2.1.获取左表中对应的数据列
      for(uint32_t i = 0;i < left_schema.GetColumnCount();i++){
        value.push_back(left_tuple_.GetValue(&left_schema, i));
      }
      // 2.2.2.获取右表中对应的数据列
      for(uint32_t j = 0;j < right_schema.GetColumnCount();j++){
        value.push_back(right_tuple.GetValue(&right_schema, j));
      }
      // 2.2.3.创建新的tuple，并返回true
      *tuple = Tuple(value,&plan_->OutputSchema());
      return true;
    }
  }
}

}  // namespace bustub
