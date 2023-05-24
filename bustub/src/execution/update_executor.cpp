//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/update_executor.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)),
      has_no_tuple_(false){
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid()); // 获取需要被更新的表的信息
    table_indexes_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_); // 获取被更新的表的所有索引的信息
}

void UpdateExecutor::Init() {
    child_executor_->Init();
}

// 这里
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    Tuple dummy_tuple; // 更新后的tuple
    RID old_rid;     // 更新后tuple的rid
    Tuple old_tuple; // 更新之前的tuple
    int update_count = 0;   // 更新的tuple数

    if(has_no_tuple_){ // 一次性将所有的tuple都更新完了，再次调用的时候直接返回false
        return false;
    }

    // 1.获取下一个更新后的tuple
    while(child_executor_->Next(&dummy_tuple, &old_rid)){
        // 1.1.根据update_rid获取更新之前的tuple，如果没有找到，则直接返回false，如果找到了生成新的tuple
        auto is_find = table_info_->table_->GetTuple(old_rid, &old_tuple,exec_ctx_->GetTransaction());
        if(!is_find){
            LOG_INFO("not found the tuple to update");
            return false;
        }
        auto new_tuple = GenerateUpdateTuple(old_tuple);    // 生成更新之后的tuple

        // 1.2.更新tuple
        auto is_updated = table_info_->table_->UpdateTuple(new_tuple, old_rid, exec_ctx_->GetTransaction());
        if(!is_updated){
            LOG_INFO("update fail");
            return false;
        }

        // 1.3.更新所有的索引（先删除在添加）
        for(auto table_index_info : table_indexes_info_){
            // 1.3.1.获取更新之前索引table_index_info对应的before_update_tuple的key
            auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, table_index_info->key_schema_, table_index_info->index_->GetKeyAttrs());
            table_index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
            
            // 1.3.2.获取更新之后索引table_index_info对应的update_tuple的key
            auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, table_index_info->key_schema_, table_index_info->index_->GetKeyAttrs());
            table_index_info->index_->InsertEntry(new_key, old_rid, exec_ctx_->GetTransaction());
        }

        // 1.4.更新update_count
        update_count++;
    }

    // 2.返回更新了多少条tuple的记录
    *tuple = Tuple(std::vector<Value>(1,Value(TypeId::INTEGER,update_count)),&plan_->OutputSchema());
    *rid = tuple->GetRid();
    has_no_tuple_ = true;

    return true;
}

// 生成更新后的tuple
auto UpdateExecutor::GenerateUpdateTuple(const Tuple &old_tuple) -> Tuple{
    std::vector<Value> value;
    for(const auto &expression : plan_->target_expressions_){
        value.emplace_back(expression->Evaluate(&old_tuple, table_info_->schema_));
    }

    return Tuple(value,&table_info_->schema_);
}

}  // namespace bustub
