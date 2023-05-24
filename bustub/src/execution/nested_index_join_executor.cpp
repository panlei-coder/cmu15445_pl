//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  inner_table_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid;
  Tuple right_tuple;

  while(true){
    std::vector<RID> result_rid;
    std::vector<Value> key_value;
    // 1.先获取左表的tuple
    if(!child_executor_->Next(&left_tuple_, &left_rid)){
      return false; // 如果获取失败，则遍历完了
    }

    // 2.获取key
    auto left_schema = child_executor_->GetOutputSchema();
    auto right_schema = inner_table_info_->schema_;
    auto key_schema = inner_table_index_info_->index_->GetKeySchema();
    auto key = plan_->KeyPredicate()->Evaluate(&left_tuple_,right_schema);
    key_value.push_back(key);
    Tuple key_tuple(key_value,key_schema);

    // 3.获取rid
    inner_table_index_info_->index_->ScanKey(key_tuple, &result_rid, exec_ctx_->GetTransaction());

    // 4.获取对应的tuple
    std::vector<Value> value;
    if(!result_rid.empty()){
      inner_table_info_->table_->GetTuple(result_rid[0], &right_tuple, exec_ctx_->GetTransaction());
      // 4.1.获取左表中对应的数据列
      for(uint32_t i = 0;i < left_schema.GetColumnCount();i++){
        value.push_back(left_tuple_.GetValue(&left_schema, i));
      }
      // 4.2.获取右表中对应的数据列
      for(uint32_t j = 0;j < right_schema.GetColumnCount();j++){
        value.push_back(right_tuple.GetValue(&right_schema, j));
      }

      // 4.3.创建新的tuple，并返回true
      *tuple = Tuple(value,&plan_->OutputSchema());
      return true;
    }
    
    // 5.如果未找匹配的连接，且plan_为左连接
    if(plan_->GetJoinType() == JoinType::LEFT){ // 为空，且为左连接
      // 2.1.1.获取左表中对应的数据列
      for(uint32_t i = 0;i < left_schema.GetColumnCount();i++){
        value.push_back(left_tuple_.GetValue(&left_schema, i));
      }
      // 2.1.2.获取右表中对应的数据列
      for(uint32_t j = 0;j < right_schema.GetColumnCount();j++){
        value.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(j).GetType()));
      }
      // 2.1.3.创建新的tuple，并返回true
      *tuple = Tuple(value,&plan_->OutputSchema());
      return true;
    }
  }
}

}  // namespace bustub
