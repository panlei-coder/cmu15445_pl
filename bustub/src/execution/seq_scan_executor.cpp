//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
                AbstractExecutor(exec_ctx),plan_(plan) {
    table_info_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid()); // 通过tableOid来找到对应的table的元信息
}

void SeqScanExecutor::Init() {
    // 1.根据事务的隔离级别加锁（先加表锁）,如果是读未提交，则不需要加锁，其他的均加IS
    try {
        if(exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED){
            if(!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_)){
                throw ExecutionException(std::string("executor fail"));
            }
        }        
    } catch (TransactionAbortException e) {
        throw ExecutionException(e.GetInfo() + std::string(" executor fail"));
    }
    
    // 2.获取表的迭代器
    table_iter_ = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    // 1.判断是否已经迭代完了
    if(*table_iter_ == table_info_->table_->End()){
        // // 1.1.如果事务的隔离级别是读已提交，需要释放之前获取的IS
        // try {
        //     if(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
        //         if(!exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_)){
        //             throw ExecutionException(std::string("executor fail"));
        //         }
        //     }
        // } catch (TransactionAbortException e) {
        //     throw ExecutionException(e.GetInfo() + std::string(" executor fail"));
        // }

        return false;
    }

    // 如果是读已提交或者可重复读，需要提前加S锁
    try {
        if(exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED){
            if(!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), 
                LockManager::LockMode::SHARED,table_info_->oid_,(*table_iter_)->GetRid())){
                throw ExecutionException(std::string("executor fail"));
            }
        }
    } catch (TransactionAbortException e) {
        throw ExecutionException(e.GetInfo() + std::string(" executor fail"));
    }

    // 2.获取下一个tuple
    *tuple = *(*table_iter_);   // 深拷贝
    *rid = tuple->GetRid();
    ++(*table_iter_);

    // 如果是读已提交或者可重复读，需要释放S锁
    try {
        if(exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED){
            if(!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(),table_info_->oid_,(*table_iter_)->GetRid())){
                throw ExecutionException(std::string("executor fail"));
            }
        }
    } catch (TransactionAbortException e) {
        throw ExecutionException(e.GetInfo() + std::string(" executor fail"));
    }

    return true; 
}

}  // namespace bustub
