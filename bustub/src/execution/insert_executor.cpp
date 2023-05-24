//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>

#include "common/logger.h"
#include "execution/executors/insert_executor.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),child_executor_(std::forward<std::unique_ptr<AbstractExecutor>>(child_executor)),
      has_no_tuple_(false){
        table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
        table_indexes_info_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
      }

void InsertExecutor::Init() {
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

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    Tuple insert_tuple; // 存放被插入的元组
    RID insert_rid;     // 被插入元组的id
    int insert_count = 0;      // 一共插入了多少条元组

    // 1.先判断是否还有tuple从chiild_executor子执行器中传过来，如果没有，则直接返回false
    if(has_no_tuple_){
        return false;
    }

    // 2.如果子执行器中有元组传过来，依次获取，然后插入到对应的表中
    // （因为被插入的元组只能通过子执行器传递过来，所以当has_no_tuple不为真，必须从子执行器pull）
    while(child_executor_->Next(&insert_tuple, &insert_rid)){
        // 2.1.将获取的元组插入到表中，插入的时候会检查被插入的元组是否符合对应表的schema
        if(table_info_->table_->InsertTuple(insert_tuple, &insert_rid, exec_ctx_->GetTransaction())){ // 插入成功
            // 根据事务的隔离级别加锁,均加X锁
            try {
                if(!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), 
                LockManager::LockMode::EXCLUSIVE, table_info_->oid_,insert_rid)){
                    throw ExecutionException(std::string("executor fail"));
                }     
            } catch (TransactionAbortException e) {
                throw ExecutionException(e.GetInfo() + std::string(" executor fail"));
            }

            // 2.1.1.修改tuple_count
            insert_count++;
            
            // 2.1.2.更新对应的索引
            for(auto table_index_info : table_indexes_info_){
                // 从tuple中提取table_index_info索引所对应的key值，将对应的新增加的索引添加到索引中
                auto key = insert_tuple.KeyFromTuple(table_info_->schema_, table_index_info->key_schema_, table_index_info->index_->GetKeyAttrs());
                table_index_info->index_->InsertEntry(key, insert_rid, exec_ctx_->GetTransaction());

                // 需要维护IndexWriteSet
                exec_ctx_->GetTransaction()->GetIndexWriteSet()->emplace_back(insert_rid,table_info_->oid_,
                    WType::INSERT,insert_tuple,table_index_info->index_oid_,exec_ctx_->GetCatalog());
            }
        }else{ // 插入失败了
            LOG_INFO("insert fail");
            break;
        }
    }

    Tuple temp_tuple(std::vector<Value>(1,Value(TypeId::INTEGER,insert_count)),&GetOutputSchema());
    *tuple = temp_tuple;
    has_no_tuple_ = true; // 插入完了更新为true

    return true; 
}

}  // namespace bustub
