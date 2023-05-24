//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/logger.h"
#include "common/rid.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)) {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    table_indexes_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() {
    if(child_executor_ != nullptr){
        child_executor_->Init();

        // 1.根据事务的隔离级别加锁（先加表锁）,均加IX锁
        try {
            if(!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_)){
                throw ExecutionException(std::string("executor fail"));
            }     
        } catch (TransactionAbortException e) {
            throw ExecutionException(e.GetInfo() + std::string(" executor fail"));
        }
    }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    Tuple delete_tuple;
    RID delete_rid;
    int delete_count = 0;

    // 1.判断has_no_tuple_
    if(has_no_tuple_){
        return false;
    }

    // 2.从子执行器中获取需要删除的元素
    while(child_executor_->Next(&delete_tuple, &delete_rid)){
        // 2.1.如果有需要删除的元组，掉用table_info_表的删除函数，如果删除成功
        if(table_info_->table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction())){
            // 根据事务的隔离级别加锁,均加X锁
            try {
                if(!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), 
                LockManager::LockMode::EXCLUSIVE, table_info_->oid_,delete_rid)){
                    throw ExecutionException(std::string("executor fail"));
                }     
            } catch (TransactionAbortException e) {
                throw ExecutionException(e.GetInfo() + std::string(" executor fail"));
            }

            // 2.1.1.修改count
            delete_count++;

            // 2.1.2.修改对应的索引
            for(auto table_index_info : table_indexes_info_){
                // 从tuple中提取table_index_info索引所对应的key值，将被删除的元素对应的索引进行修改
                auto key = delete_tuple.KeyFromTuple(table_info_->schema_, table_index_info->key_schema_, table_index_info->index_->GetKeyAttrs());
                table_index_info->index_->DeleteEntry(key, delete_rid, exec_ctx_->GetTransaction());

                // 需要维护IndexWriteSet
                exec_ctx_->GetTransaction()->GetIndexWriteSet()->emplace_back(delete_rid,table_info_->oid_,
                    WType::DELETE,delete_tuple,table_index_info->index_oid_,exec_ctx_->GetCatalog());
            }
        }else{
            LOG_INFO("delete fail");
            break;
        }
    }

    Tuple temp_tuple(std::vector<Value>(1,Value(TypeId::INTEGER,delete_count)),&GetOutputSchema());
    *tuple = temp_tuple;
    has_no_tuple_ = true;

    return true;
}

}  // namespace bustub
