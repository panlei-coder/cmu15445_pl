//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <climits>
#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1.输出加锁的日志信息
  LOG_INFO("lock table");
  Log(txn,lock_mode,oid);

  // 2.根据事务的状态，判断是否符合加锁的条件
  if(txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED){
    throw std::logic_error("transaction state is impossible");
    return false;
  }

  // 3.获取事务id
  auto txn_id = txn->GetTransactionId();

  // 4.根据事务状态来进行相应的锁的授予
  // 第一种情况：事务增长阶段
  if(txn->GetState() == TransactionState::GROWING){ 
    LOG_INFO("GROWING");

    // 4.1.检查锁授予的合理性
    // 如果事务的隔离级别是READ_UNCOMMITED，不允许加S/SIX/IS
    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
      if(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
        // 4.1.1申请的锁是非法的，事务被中止，修改事务的状态，并抛出异常
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id,AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }

    // 4.2.更新表锁的等待队列
    if(!UpdateLock(txn, lock_mode, oid)){
      LOG_INFO("update lock queue fail");
      return false;
    }

    // 4.3.获取表锁的等待队列，并获取表锁等待队列的锁
    auto table_lock_queue_ptr = GetLRQueuePtr(oid);
    std::unique_lock<std::mutex> latch(table_lock_queue_ptr->latch_); 

    // 4.4.根据等待队列判断锁是否能够被授予，如果不能被授予，则阻塞当前事务的锁请求，直到锁被授予为止
    while(!GrantLock(txn, lock_mode, oid)){
      // 4.4.1.当前事务的锁申请被阻塞了
      table_lock_queue_ptr->cv_.wait(latch);
      Log(txn,lock_mode,oid);

      // 4.4.2.如果当前事务在等待锁的授予过程中被中止了（即阻塞之后被唤醒发现事务被中止了，发生了死锁）
      if(txn->GetState() == TransactionState::ABORTED){
        LOG_INFO("granting abort");
        // 4.4.2.1.如果当前事务被中止之前进行了锁升级，需要重新设置当前等待队列的upgrading_
        if(table_lock_queue_ptr->upgrading_ == txn_id){
          table_lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
        }

        // 4.4.2.2.将当前事务的加锁请求从等待队列中剔除，并通过条件变量通知被阻塞的其他事务（这个锁请求还没有添加到事务的锁集合中）
        for(auto iter = table_lock_queue_ptr->request_queue_.begin();iter != table_lock_queue_ptr->request_queue_.end();iter++){
          if((*iter)->txn_id_ == txn_id){
            table_lock_queue_ptr->request_queue_.erase(iter);
            LOG_INFO("size of table_lock_queue->request_queue_ is %ld",table_lock_queue_ptr->request_queue_.size());
            table_lock_queue_ptr->cv_.notify_all();
            return false;
          }
        }
      }
    }

    // 4.5.锁申请成功之后，需要将申请的锁添加到事务的锁集合中，并通知其状态他被阻塞的事务
    table_lock_queue_ptr->cv_.notify_all();
    BookKeeping(txn,lock_mode,oid);
    LOG_INFO("blocking finish");
    return true;
  }
  
  // 第二种情况：事务缩减阶段
  if(txn->GetState() == TransactionState::SHRINKING){ 
    LOG_INFO("SHRINKING");

    // 4.1.如果事务的隔离级别是可重复读，则缩减阶段不可以加任何锁
    if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id,AbortReason::LOCK_ON_SHRINKING);
      return false;
    }

    // 4.2.如果事务的隔离级别是读未提交，只能够加X/IX，若缩减阶段加X/IX抛出LOCK_ON_SHRINKING，否认则抛出LOCK_SHARED_ON_READ_UNCOMMITTED
    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
      if(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id,AbortReason::LOCK_ON_SHRINKING);
        return false;
      }

      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id,AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }

    // 4.3.如果事务的隔离级别是读已提交,则缩减阶段只可以加S/IS
    if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
      if(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED){
        // 4.3.1.将锁添加到等待队列中
        if(!UpdateLock(txn, lock_mode, oid)){
          LOG_INFO("update lock queue fail");
          return false;
        }

        // 4.3.2.获取等待队列，并获取等待队列上的锁
        auto table_lock_queue_ptr = GetLRQueuePtr(oid);
        std::unique_lock<std::mutex> latch(table_lock_queue_ptr->latch_);

        // 4.3.3.根据等待队列判断锁是否能够被授予，如果不能被授予，则阻塞当前事务的锁请求，直到锁被授予为止
        while(!GrantLock(txn,lock_mode,oid)){
          // 4.3.1.当前事务的锁申请被阻塞了
          table_lock_queue_ptr->cv_.wait(latch);
          Log(txn,lock_mode,oid);

          // 4.3.2.如果当前事务在等待锁的授予过程中被中止了（即阻塞之后被唤醒发现事务被中止了，发生了死锁）
          if(txn->GetState() == TransactionState::ABORTED){
            LOG_INFO("granting abort");
            // 4.3.2.1.如果当前事务被中止之前进行了锁升级，需要重新设置当前等待队列的upgrading_
            if(table_lock_queue_ptr->upgrading_ == txn_id){
              table_lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
            }

            // 4.3.2.2.将当前事务的加锁请求从等待队列中剔除，并通过条件变量通知被阻塞的其他事务（这个锁请求还没有添加到事务的锁集合中）
            for(auto iter = table_lock_queue_ptr->request_queue_.begin();iter != table_lock_queue_ptr->request_queue_.end();iter++){
              if((*iter)->txn_id_ == txn_id){
                table_lock_queue_ptr->request_queue_.erase(iter);
                LOG_INFO("size of table_lock_queue->request_queue_ is %ld",table_lock_queue_ptr->request_queue_.size());
                table_lock_queue_ptr->cv_.notify_all();
                return false;
              }
            }
          }
        }

        // 4.4.锁申请成功之后，需要将申请的锁添加到事务的锁集合中，并通知其他被阻塞的事务
        table_lock_queue_ptr->cv_.notify_all();
        BookKeeping(txn,lock_mode,oid);
        LOG_INFO("blocking finish");
        return true;
      }

      // 如果加锁的类型不是X/IX，需要抛出异常
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id,AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  LOG_INFO("unlock table");

  // 1.做行锁的检查，在释放表锁之前对应页上的行锁必须全部释放掉，否则直接抛出异常
  if(!(*txn->GetExclusiveRowLockSet())[oid].empty() || !(*txn->GetSharedRowLockSet())[oid].empty()){
    LOG_ERROR("the row can't unlock");
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(),AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }

  // 2.获取对应的表锁等待队列，并获取表锁等待队列的锁
  auto table_lock_queue_ptr = GetLRQueuePtr(oid);
  std::unique_lock<std::mutex> latch(table_lock_queue_ptr->latch_);

  // 3.遍历表锁的等待队列，删除对应的锁,同时把事务表锁集合也需要删除掉
  for(auto iter = table_lock_queue_ptr->request_queue_.begin();iter != table_lock_queue_ptr->request_queue_.end();iter++){
    if((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_){ // 锁必须授予了才能有释放的过程
      // 3.1.根据锁的类型修改事务的状态
      UnLockChangeState(txn, (*iter)->lock_mode_);

      // 3.2.从表锁的等待队列和事务的表锁集合中剔除对应的锁，并利用条件变量通知被阻塞的事务
      BookKeepingRemove(txn,(*iter)->lock_mode_,oid);
      table_lock_queue_ptr->request_queue_.erase(iter);
      LOG_INFO("notify all");
      table_lock_queue_ptr->cv_.notify_all();
      return true;
    }
  }

  // 4.如果没有在表锁的等待队列中找到对应的锁，需要抛出异常，并设置事务为中止状态
  LOG_ERROR("abort");
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(),AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false; 
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1.输出日志信息
  LOG_INFO("lock row");
  Log(txn,lock_mode,oid);

  // 2.因为在加行锁之前，先要加表锁，那么需要检查对应的表是否加锁，并且表锁与行锁之间的逻辑是否相符，如果不符合直接抛出异常
  IsTableFit(txn, lock_mode, oid);
  // 行锁只有S/X
  if(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    throw std::logic_error("row lock should not be intention lock");
    return false;
  }

  // 3.根据事务的状态判断，是否满足加锁的条件
  if(txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED){
    throw std::logic_error("transaction state is impossible");
    return false;
  }

  auto txn_id = txn->GetTransactionId();

  // 4.如果事务是GROWING状态
  if(txn->GetState() == TransactionState::GROWING){
    LOG_INFO("growing");

    // 4.1.判断事务的隔离级别，根据不同的隔离级别来判断释放锁是否合理（如果是读未提交，只能申请X）
    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
      if(lock_mode != LockMode::EXCLUSIVE){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id,AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }

    // 4.2.更新锁等待队列
    if(!UpdateLock(txn,lock_mode,oid,rid)){
      LOG_INFO("the same fail");
      return false;
    }

    // 4.3.获取行锁的等待队列，并获取行锁等待队列的锁
    auto row_lock_queue_ptr = GetLRQueuePtr(rid);
    std::unique_lock<std::mutex> latch(row_lock_queue_ptr->latch_);


    // 4.4.授予锁
    while(!GrantLock(txn,lock_mode,rid)){
      // 4.4.1.当前事务的锁申请被阻塞了
      row_lock_queue_ptr->cv_.wait(latch);
      Log(txn,lock_mode,rid);

      // 4.4.2.如果当前事务在等待锁的授予过程中被中止了（即阻塞之后被唤醒发现事务被中止了，发生了死锁）
      if(txn->GetState() == TransactionState::ABORTED){
        LOG_INFO("granting abort");
        // 4.4.2.1.如果当前事务被中止之前进行了锁升级，需要重新设置当前等待队列的upgrading_
        if(row_lock_queue_ptr->upgrading_ == txn_id){
          row_lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
        }

        // 4.4.2.2.将当前事务的加锁请求从等待队列中剔除，并通过条件变量通知被阻塞的其他事务（这个锁请求还没有添加到事务的锁集合中）
        for(auto iter = row_lock_queue_ptr->request_queue_.begin();iter != row_lock_queue_ptr->request_queue_.end();iter++){
          if((*iter)->txn_id_ == txn_id){
            row_lock_queue_ptr->request_queue_.erase(iter);
            LOG_INFO("size of row_lock_queue->request_queue_ is %ld",row_lock_queue_ptr->request_queue_.size());
            row_lock_queue_ptr->cv_.notify_all();
            return false;
          }
        }
      }
    }

    // 4.5.锁申请成功之后，需要将申请的锁添加到事务的锁集合中，并通知其状态他被阻塞的事务
    row_lock_queue_ptr->cv_.notify_all();
    BookKeeping(txn,lock_mode,oid,rid);
    LOG_INFO("blocking finish");
    return true;
  }

  // 5.如果事务是SHRINKING状态
  if(txn->GetState() == TransactionState::SHRINKING){
    LOG_INFO("SHRINKING");
    
    // 5.1.如果事务的隔离级别是可重复读，则缩减阶段不可以加任何锁
    if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id,AbortReason::LOCK_ON_SHRINKING);
      return false;
    }

    // 5.2.如果事务的隔离级别是读未提交，只能够加X/IX，若缩减阶段加X抛出LOCK_ON_SHRINKING，否认则抛出LOCK_SHARED_ON_READ_UNCOMMITTED
    if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
      if(lock_mode == LockMode::EXCLUSIVE){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id,AbortReason::LOCK_ON_SHRINKING);
        return false;
      }

      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id,AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }

    // 5.3.如果事务的隔离级别是读已提交,则缩减阶段只可以加S
    if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
      if(lock_mode == LockMode::SHARED){
        // 5.3.1.将锁添加到等待队列中
        if(!UpdateLock(txn, lock_mode, oid)){
          LOG_INFO("update lock queue fail");
          return false;
        }

        // 5.3.2.获取等待队列，并获取等待队列上的锁
        auto row_lock_queue_ptr = GetLRQueuePtr(oid);
        std::unique_lock<std::mutex> latch(row_lock_queue_ptr->latch_);

        // 5.3.3.根据等待队列判断锁是否能够被授予，如果不能被授予，则阻塞当前事务的锁请求，直到锁被授予为止
        while(!GrantLock(txn,lock_mode,oid)){
          // 5.3.1.当前事务的锁申请被阻塞了
          row_lock_queue_ptr->cv_.wait(latch);
          Log(txn,lock_mode,oid);

          // 5.3.2.如果当前事务在等待锁的授予过程中被中止了（即阻塞之后被唤醒发现事务被中止了，发生了死锁）
          if(txn->GetState() == TransactionState::ABORTED){
            LOG_INFO("granting abort");
            // 5.3.2.1.如果当前事务被中止之前进行了锁升级，需要重新设置当前等待队列的upgrading_
            if(row_lock_queue_ptr->upgrading_ == txn_id){
              row_lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
            }

            // 5.3.2.2.将当前事务的加锁请求从等待队列中剔除，并通过条件变量通知被阻塞的其他事务（这个锁请求还没有添加到事务的锁集合中）
            for(auto iter = row_lock_queue_ptr->request_queue_.begin();iter != row_lock_queue_ptr->request_queue_.end();iter++){
              if((*iter)->txn_id_ == txn_id){
                row_lock_queue_ptr->request_queue_.erase(iter);
                LOG_INFO("size of row_lock_queue->request_queue_ is %ld",row_lock_queue_ptr->request_queue_.size());
                row_lock_queue_ptr->cv_.notify_all();
                return false;
              }
            }
          }
        }

        // 5.4.锁申请成功之后，需要将申请的锁添加到事务的锁集合中，并通知其他被阻塞的事务
        row_lock_queue_ptr->cv_.notify_all();
        BookKeeping(txn,lock_mode,oid,rid);
        LOG_INFO("blocking finish");
        return true;
      }

      // 如果加锁的类型不是S，需要抛出异常
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id,AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  LOG_INFO("unlock row");
  
  // 1.获取行锁的等待队列，并获取等待队列的锁
  auto row_lock_queue_ptr = GetLRQueuePtr(rid);
  std::scoped_lock<std::mutex> latch(row_lock_queue_ptr->latch_);

  auto txn_id = txn->GetTransactionId();

  // 2.遍历行锁的等待队列
  for(auto iter = row_lock_queue_ptr->request_queue_.begin();iter != row_lock_queue_ptr->request_queue_.end();iter++){
    if((*iter)->txn_id_ == txn_id && (*iter)->granted_){
      // 2.1.根据要释放锁的类型来修改事务的状态
      UnLockChangeState(txn, (*iter)->lock_mode_);

      // 2.2.将锁从行锁的等待队列和事务的锁集合中剔除
      BookKeepingRemove(txn,(*iter)->lock_mode_,oid,rid);
      row_lock_queue_ptr->request_queue_.erase(iter);
      row_lock_queue_ptr->cv_.notify_all();
      
      return true;
    }
  }

  // 3.如果没有找到需要释放的锁，中止事务，并抛出异常
  LOG_INFO("unlock fail %d",txn_id);
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn_id,AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  return false; 
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  LOG_INFO("add %d->%d",t1,t2);
  waits_for_[t1].insert(t2);
  txn_id_set_[t1] = 1;
  txn_id_set_[t2] = 1;
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  LOG_INFO("remove %d->%d",t1,t2);
  waits_for_[t1].erase(t2);
}

// 死锁检测的DFS算法，检测等待图中是否存在环
auto LockManager::DFS(txn_id_t txn_id,std::stack<txn_id_t> &cycle_stack,
  std::unordered_map<txn_id_t,int> &traversed_flag,std::set<txn_id_t> &has_traversed_txn_set) -> bool{
  // 1.将当前遍历的事务id添加到栈中，记录下遍历的路径，方便回溯
  cycle_stack.push(txn_id);
  traversed_flag[txn_id] = 1; // 添加标记，用于判断是否存在环
  has_traversed_txn_set.insert(txn_id);

  // 2.寻找事务txn_id的下一层的事务id（始终寻找下一层最小的那个事务id）
  if(waits_for_.find(txn_id) != waits_for_.end()){ // 如果能够找到事务txn_id的等待队列
    for(auto next_txn_id : waits_for_[txn_id]){
      if(traversed_flag[next_txn_id] == 1){ // 说明之前已经遍历过了，等待图中存在环
        return true;
      }

      // 遍历下一层，如果存在环，直接返回true
      if(DFS(next_txn_id,cycle_stack,traversed_flag,has_traversed_txn_set)){
        return true;
      }
    }   
  }

  // 3.找不到说明该条路径已经遍历完了
  cycle_stack.pop();
  traversed_flag[txn_id] = 0;
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // 1.寻找当前最小的事务id，且该事务还没有从等待图中剔除
  auto min_txn_id = INT_MAX;
  for(const auto &[key,val] : txn_id_set_){
    if(val == 1){
      min_txn_id = std::min(min_txn_id,key);
    }
  }

  // 2.如果没有找到合适的事务id
  if(min_txn_id == INT_MAX){
    return false;
  }

  // 3.DFS
  std::stack<txn_id_t> cycle_stack;
  std::unordered_map<txn_id_t,int> traversed_flag;
  std::set<txn_id_t>has_traversed_txn_set;
  while(!DFS(min_txn_id,cycle_stack,traversed_flag,has_traversed_txn_set)){
    // 3.1.寻找当前最小的事务id，且该事务还没有从等待图中剔除
    min_txn_id = INT_MAX;
    for(const auto &[key,val] : txn_id_set_){
      if(val == 1 && has_traversed_txn_set.find(key) == has_traversed_txn_set.end()){
        min_txn_id = std::min(min_txn_id,key);
      }
    }

    // 3.2.如果没有找到合适的事务id
    if(min_txn_id == INT_MAX){
      LOG_INFO("has no cycle");
      return false;
    }
  }

  // 4.如果有环，从cycle_stack寻找事务id最大的那个
  *txn_id = cycle_stack.top();
  cycle_stack.pop();
  while(!cycle_stack.empty()){
    *txn_id = std::max(cycle_stack.top(),*txn_id);
    cycle_stack.pop();
  }
  txn_id_set_[*txn_id] = 0;
  return true;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  
  for(const auto &[key,vec] : waits_for_){
    for(const auto &val : vec){
      edges.emplace_back(std::make_pair(key,val));
    }
  }

  for(const auto &[t1,t2] : edges){
    LOG_INFO("%d->%d",t1,t2);
  }

  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // 1.获取等待图的锁
      std::scoped_lock<std::mutex> latch(waits_for_latch_);

      // 2.构建等待图
      // (1)表锁的等待图
      // 获取表锁集合的锁
      table_lock_map_latch_.lock();
      for(const auto &[table_id,table_lock_queue_ptr] : table_lock_map_){
        // 获取表锁等待队列的锁
        table_lock_queue_ptr->latch_.lock();

        for(auto iter_first = table_lock_queue_ptr->request_queue_.begin();iter_first != table_lock_queue_ptr->request_queue_.end();iter_first++){
          // 拷贝一个迭代器，进行循环遍历
          auto iter_second = iter_first;
          iter_second++;

          // 循环遍历，判断是否为需要添加到等待图中的边（注意iter_first和iter_second指向的锁，必须要有一个是授予的锁，有一个是未授予的锁，且两个锁之间是不兼容的）
          for(;iter_second != table_lock_queue_ptr->request_queue_.end();iter_second++){
            if((((*iter_first)->granted_ && !(*iter_second)->granted_) || (
              !(*iter_first)->granted_ && (*iter_second)->granted_)) && 
              !GrantCompatiable((*iter_first)->lock_mode_, (*iter_second)->lock_mode_)){
              // 将边添加到等待图中（未授予锁->授予锁）
              if((*iter_first)->granted_){
                AddEdge((*iter_second)->txn_id_,(*iter_first)->txn_id_);
              }else{
                AddEdge((*iter_first)->txn_id_,(*iter_second)->txn_id_);
              }
            }
          }
        } 
        
        // 释放表锁等待队列的锁
        table_lock_queue_ptr->latch_.unlock();
      }
      // 释放表锁集合的锁
      table_lock_map_latch_.unlock();
      
      // (2)行锁的等待图
      // 获取行锁集合的锁
      row_lock_map_latch_.lock();
      for(const auto &[row_id,row_lock_queue_ptr] : row_lock_map_){
        // 获取行锁等待队列的锁
        row_lock_queue_ptr->latch_.lock();

        for(auto iter_first = row_lock_queue_ptr->request_queue_.begin();iter_first != row_lock_queue_ptr->request_queue_.end();iter_first++){
          // 拷贝一个迭代器，进行循环遍历
          auto iter_second = iter_first;
          iter_second++;

          // 循环遍历，判断是否为需要添加到等待图中的边（注意iter_first和iter_second指向的锁，必须要有一个是授予的锁，有一个是未授予的锁，且两个锁之间是不兼容的）
          for(;iter_second != row_lock_queue_ptr->request_queue_.end();iter_second++){
            if((((*iter_first)->granted_ && !(*iter_second)->granted_) || (
              !(*iter_first)->granted_ && (*iter_second)->granted_)) && 
              !GrantCompatiable((*iter_first)->lock_mode_, (*iter_second)->lock_mode_)){
              if((*iter_first)->granted_){
                AddEdge((*iter_first)->txn_id_,(*iter_second)->txn_id_);
              }else{
                AddEdge((*iter_second)->txn_id_,(*iter_first)->txn_id_);
              }
            }
          }
        } 
        
        // 释放行锁等待队列的锁
        row_lock_queue_ptr->latch_.unlock();
      }
      // 释放行锁集合的锁
      row_lock_map_latch_.unlock();

      // 3.判断是否存在环，如果有环需要中止环中事务id最大的那个，并再次检测是否有环，直到没有环为止
      txn_id_t txn_id;
      while(HasCycle(&txn_id)){
        // 3.1.根据事务id获取对应的事务，修改事务的状态（该事务一定是处于阻塞状态）
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->LockTxn();
        txn->SetState(TransactionState::ABORTED);
        txn->UnlockTxn();

        // 3.2.将被中止的事务从等待图中剔除
        waits_for_.erase(txn_id);
        for(auto &val : waits_for_){
          RemoveEdge(val.first, txn_id);
        }

        // 3.3.找到事务txn_id所在的等待队列，将队列上的线程唤醒
        // (1)表锁
        // 获取表锁集合的锁
        table_lock_map_latch_.lock();
        for(const auto &[table_id,table_lock_queue_ptr] : table_lock_map_){
          // 获取表锁等待队列的锁
          table_lock_queue_ptr->latch_.lock();

          for(auto iter = table_lock_queue_ptr->request_queue_.begin();iter != table_lock_queue_ptr->request_queue_.end();iter++){
            if((*iter)->txn_id_ == txn_id){
              table_lock_queue_ptr->cv_.notify_all();
              break;
            }
          } 
          
          // 释放表锁等待队列的锁
          table_lock_queue_ptr->latch_.unlock();
        }
        // 释放表锁集合的锁
        table_lock_map_latch_.unlock();

        // (2)行锁
        // 获取表锁集合的锁
        row_lock_map_latch_.lock();
        for(const auto &[row_id,row_lock_queue_ptr] : row_lock_map_){
          // 获取表锁等待队列的锁
          row_lock_queue_ptr->latch_.lock();

          for(auto iter = row_lock_queue_ptr->request_queue_.begin();iter != row_lock_queue_ptr->request_queue_.end();iter++){
            if((*iter)->txn_id_ == txn_id){
              row_lock_queue_ptr->cv_.notify_all();
              break;
            }
          } 
          
          // 释放表锁等待队列的锁
          row_lock_queue_ptr->latch_.unlock();
        }
        // 释放表锁集合的锁
        row_lock_map_latch_.unlock();
      }
    }
  }
}

/************************************************表锁和行锁公共的函数*********************************************/
// 判断两个锁是否满足升级的条件
auto LockManager::IsCompatible(LockMode lock_mode_before,LockMode lock_mode_after) -> bool{
  // S/X/IS/IX/SIX之间进行锁升级时满足的关系：S->[X/SIX],IX->[X/SIX],IS->[S/X/IX/SIX],SIX->[X]
  if(lock_mode_before == LockMode::SHARED && (lock_mode_after == LockMode::EXCLUSIVE || lock_mode_after == LockMode::SHARED_INTENTION_EXCLUSIVE)){
    return true;
  }
  
  if(lock_mode_before == LockMode::INTENTION_EXCLUSIVE && (lock_mode_after == LockMode::EXCLUSIVE || lock_mode_after == LockMode::SHARED_INTENTION_EXCLUSIVE)){
    return true;
  }

  if(lock_mode_before == LockMode::INTENTION_SHARED && (lock_mode_after == LockMode::SHARED || 
    lock_mode_after == LockMode::EXCLUSIVE || lock_mode_after == LockMode::INTENTION_EXCLUSIVE || lock_mode_after == LockMode::SHARED_INTENTION_EXCLUSIVE)){
      return true;
  }

  if(lock_mode_before == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode_after == LockMode::EXCLUSIVE)){
    return true;
  }

  return false;
}

// 锁的授予是否是可以兼容的(相容性矩阵)
auto LockManager::GrantCompatiable(LockMode lock_mode_first,LockMode lock_mode_second) -> bool{
  // 只要有一个是写锁，就不能兼容
  if(lock_mode_first == LockMode::EXCLUSIVE || lock_mode_second == LockMode::EXCLUSIVE){
    return false;
  }

  if((lock_mode_first == LockMode::SHARED && lock_mode_second == LockMode::INTENTION_EXCLUSIVE) || 
    (lock_mode_first == LockMode::INTENTION_EXCLUSIVE && lock_mode_second == LockMode::SHARED)){
    return false;
  }

  if((lock_mode_first == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode_second != LockMode::INTENTION_SHARED) || 
    (lock_mode_first != LockMode::INTENTION_SHARED && lock_mode_second == LockMode::SHARED_INTENTION_EXCLUSIVE)){
      return false;
    }

  return true;
}

// 基于不同的隔离级别下，事务释放不同的锁，事务的状态也相应的需要发生改变
void LockManager::UnLockChangeState(Transaction *txn,LockMode lock_mode){
  // 1.先判断事务的状态是不是GROWING，如果不是，那么释放锁之后，事务的状态不会发生改变（主要是从GROWING->SHRINKING）
  if(txn->GetState() != TransactionState::GROWING){
    return;
  }

  // 2.根据不同的事务隔离级别，实现锁释放之后事务状态的更改
  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)){
    txn->SetState(TransactionState::SHRINKING);
    LOG_INFO("start shrinking");
  }else if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode == LockMode::EXCLUSIVE){
    txn->SetState(TransactionState::SHRINKING);
    LOG_INFO("start shrinking");
  }else if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::EXCLUSIVE){
    txn->SetState(TransactionState::SHRINKING);
    LOG_INFO("start shrinking");
  }
}

/************************************************表锁*********************************************/
// 日志输出加锁的信息
void LockManager::Log(Transaction *txn,LockMode lock_mode,table_oid_t oid){
  // 1.判断锁的类型
  std::string lock_type_string;
  if(lock_mode == LockMode::EXCLUSIVE){
    lock_type_string = "X";
  }else if(lock_mode == LockMode::SHARED){
    lock_type_string = "S";
  }else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    lock_type_string = "SIX";
  }else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
    lock_type_string = "IX";
  }else if(lock_mode == LockMode::INTENTION_SHARED){ 
    lock_type_string = "IS";
  }

  // 2.判断事务的隔离级别
  std::string isolation_level;
  if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
    isolation_level = "RC";
  }else if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    isolation_level = "RU";
  }else if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
    isolation_level = "PR";
  }

  // 3.事务的状态
  std::string txn_state;
  if(txn->GetState() == TransactionState::GROWING){
    txn_state = "GROWING";
  }else if(txn->GetState() == TransactionState::SHRINKING){
    txn_state = "SHRINKING";
  }else if(txn->GetState() == TransactionState::COMMITTED){
    txn_state = "COMMITED";
  }else if(txn->GetState() == TransactionState::ABORTED){
    txn_state = "ABORTED";
  }

  // 4.输出日志
  LOG_INFO("txn_id = %d,lock_type = %s,isolation_level = %s,txn_state = %s,table_id = %d",
            (int)txn->GetTransactionId(),lock_type_string.c_str(),isolation_level.c_str(),txn_state.c_str(),(int)oid);
}

// 获取table的锁等待队列
auto LockManager::GetLRQueuePtr(table_oid_t oid) -> std::shared_ptr<LockRequestQueue>{
  // 1.获取锁
  std::scoped_lock<std::mutex> latch(table_lock_map_latch_);

  /*for(const auto& table_lock_queue : table_lock_map_){
    for(const auto &lock : table_lock_queue.second->request_queue_){
      LOG_INFO("txn_id = %d,table_id = %d,lock_mode = %d,granted = %d",(int)lock->txn_id_,(int)lock->oid_,(int)lock->lock_mode_,(int)lock->granted_);
    }
  }*/

  // 2.获取oid对应的等待队列
  auto table_queue = table_lock_map_[oid];
  if(table_queue == nullptr){
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    return table_lock_map_[oid];
  }

  return table_queue;
}

// 将某个锁添加到事务txn的锁集合中
void LockManager::BookKeeping(Transaction *txn,LockMode lock_mode,table_oid_t oid){
  if(lock_mode == LockMode::EXCLUSIVE){
    txn->GetExclusiveTableLockSet()->insert(oid);
  }else if(lock_mode == LockMode::SHARED){
    txn->GetSharedTableLockSet()->insert(oid);
  }else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  }else if(lock_mode == LockMode::INTENTION_SHARED){
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  }else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
}

// 从事务txn的锁集合中删除某个锁
void LockManager::BookKeepingRemove(Transaction *txn,LockMode lock_mode,table_oid_t oid){
  if(lock_mode == LockMode::EXCLUSIVE){
    txn->GetExclusiveTableLockSet()->erase(oid);
  }else if(lock_mode == LockMode::SHARED){
    txn->GetSharedTableLockSet()->erase(oid);
  }else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  }else if(lock_mode == LockMode::INTENTION_SHARED){
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  }else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
}

// 更新表锁，将新申请的锁添加到等待队列中
auto LockManager::UpdateLock(Transaction *txn,LockMode lock_mode,table_oid_t oid) -> bool{
  // 1.获取表锁的等待队列(指针)
  auto table_lock_queue_ptr = GetLRQueuePtr(oid);

  // 2.获取表锁的等待队列的锁
  std::scoped_lock<std::mutex> latch(table_lock_queue_ptr->latch_);

  // 3.在表锁的等待队列里面寻找该事务是否已经加锁了，是否需要进行锁的升级（如果需要进行锁升级，那么该事务之前申请的锁一定被授予了，因为没有被授予则事务会被阻塞住）
  txn_id_t txn_id = txn->GetTransactionId();
  for(auto iter = table_lock_queue_ptr->request_queue_.begin();iter != table_lock_queue_ptr->request_queue_.end();iter++){
    // 3.1.找到了事务txn加锁的记录
    if((*iter)->txn_id_ == txn_id){
      // 3.1.1.判断事务目前加的锁和之前加的锁是否相同，相同直接返回，不做任何处理
      if((*iter)->lock_mode_ == lock_mode){
        // todo ? 需要考虑true还是false
        return true;
      }

      // 3.1.2.如果当前等待队列中没有事务进行锁的升级，则该事务可以进行锁升级
      if(table_lock_queue_ptr->upgrading_ == INVALID_TXN_ID){
        // 3.1.2.1.判断是否满足锁升级的条件
        if(IsCompatible((*iter)->lock_mode_, lock_mode)){
          // 3.1.2.1.1.更新等待队列锁升级的事务id
          table_lock_queue_ptr->upgrading_ = txn_id;

          // 3.1.2.1.2.将之前的锁从事务的锁集合中删除掉，后续在添加升级之后的锁
          BookKeepingRemove(txn,(*iter)->lock_mode_,oid);

          // 3.1.2.1.3.更新表锁的等待队列（直接添加到队尾）
          table_lock_queue_ptr->request_queue_.erase(iter);
          auto lock_request = std::make_shared<LockRequest>(txn_id,lock_mode,oid);
          table_lock_queue_ptr->request_queue_.push_back(lock_request);
          /*int advance_count = 0;   // 迭代器需要移动次数
          // 寻找到第一个锁请求没有被授予的位置
          for(auto upgraded_iter = table_lock_queue_ptr->request_queue_.begin();upgraded_iter != table_lock_queue_ptr->request_queue_.end();upgraded_iter++){
            if((*upgraded_iter)->granted_){
              advance_count++;
            }
            break;
          }
          // 将迭代器移动到第一个锁请求没有被授予的位置
          auto it = table_lock_queue_ptr->request_queue_.begin();
          std::advance(it,advance_count);
          // 将升级之后的锁请求添加到等待队列中
          table_lock_queue_ptr->request_queue_.insert(it,lock_request);*/
          
          LOG_INFO("lock upgrade success!");
          return true;
        }

        // 不满足锁升级的条件
        LOG_INFO("can't upgrade lock");
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id,AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }

      // 多个事务对同一个表进行锁升级，发生了锁升级冲突
      LOG_INFO("two upgrade abort");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id,AbortReason::UPGRADE_CONFLICT);
      return false;
    }
  }

  // 该事务之前没有对该表加锁
  LOG_INFO("no same lock");
  auto lock_request = std::make_shared<LockRequest>(txn_id,lock_mode,oid);
  table_lock_queue_ptr->request_queue_.push_back(lock_request);
  return true;
}

// 表锁的授予
auto LockManager::GrantLock(Transaction *txn,LockMode lock_mode,table_oid_t oid) -> bool{
  // 1.获取表锁的等待队列(等待队列的锁已经在上层函数调用的时候已经获取了)
  auto table_lock_queue_ptr = GetLRQueuePtr(oid);

  LOG_INFO("table_lock_queue is %ld",table_lock_queue_ptr->request_queue_.size());

  // 2.先找到所有目前已经被授予的锁
  std::vector<std::shared_ptr<LockRequest>> has_granted_lock_request;
  for(auto iter = table_lock_queue_ptr->request_queue_.begin();iter != table_lock_queue_ptr->request_queue_.end();iter++){
    if((*iter)->granted_){
      has_granted_lock_request.push_back(*iter);
    }
  }

  // 3.检查锁的授予是否满足兼容性
  for(const auto& granted_row_lock : has_granted_lock_request){
    if(!GrantCompatiable(granted_row_lock->lock_mode_,lock_mode)){
      LOG_INFO("The requested lock is incompatible with the already granted lock.");
      return false;
    }
  }

  // 4.如果当前事务需要锁升级（如果当前等待队列中没有锁被授予，则优先进行这个升级后的锁授予;如果当前已经有锁被授予，且升级后的锁能够兼容则授予，否则锁授予失败）
  auto txn_id = txn->GetTransactionId();
  if(table_lock_queue_ptr->upgrading_ == txn_id){ // 当前表锁的等待队列中进行锁升级的事务如果是txn
    for(auto iter = table_lock_queue_ptr->request_queue_.begin();iter != table_lock_queue_ptr->request_queue_.end();iter++){
      // 4.1.找到事务txn需要升级的锁请求
      if((*iter)->txn_id_ == txn_id){
        table_lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
        (*iter)->granted_ = true;
        LOG_INFO("upgrade lock grant success");
        return true;
      }
    }
  }

  Log(txn,lock_mode,oid);
  LOG_INFO("%d",(int)table_lock_queue_ptr->request_queue_.size());

  // 3.普通锁的授予，遍历等待队列，查看目前还没有被授予的锁，如果能够兼容则授予锁
  bool flag = false; // 标记txn的锁是否被授予了
  for(auto iter = table_lock_queue_ptr->request_queue_.begin();iter != table_lock_queue_ptr->request_queue_.end();iter++){
    if(!(*iter)->granted_){ // 如果锁没有被授予
      bool is_compatiable = true;
      for(const auto &lock_request : has_granted_lock_request){
        // 判断是否兼容
        if(!GrantCompatiable(lock_request->lock_mode_,(*iter)->lock_mode_)){
          // LOG_INFO("grant return %i %d",flag,txn_id);
          // return false;
          is_compatiable = false;
          break;
        }
      }

      // 如果兼容
      if(is_compatiable){
        (*iter)->granted_ = true;
        if((*iter)->txn_id_ == txn_id){
          flag = true;
        }
        has_granted_lock_request.push_back(*iter);
      }      
    }else{
      // 如果当前事务的锁已经被授予，则返回true，后续唤醒当前事务
      if((*iter)->txn_id_ == txn_id){
        flag = true;
      }
    }
  }

  LOG_INFO("grant return %i %d",flag,txn_id);
  return flag;
}

/************************************************行锁*********************************************/
// 日志输出加锁的信息
void LockManager::Log(Transaction *txn,LockMode lock_mode,RID rid){
  // 1.判断锁的类型
  std::string lock_type_string;
  if(lock_mode == LockMode::EXCLUSIVE){
    lock_type_string = "X";
  }else if(lock_mode == LockMode::SHARED){
    lock_type_string = "S";
  }else if(lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE){
    lock_type_string = "SIX";
  }else if(lock_mode == LockMode::INTENTION_EXCLUSIVE){
    lock_type_string = "IX";
  }else if(lock_mode == LockMode::INTENTION_SHARED){ 
    lock_type_string = "IS";
  }

  // 2.判断事务的隔离级别
  std::string isolation_level;
  if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
    isolation_level = "RC";
  }else if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    isolation_level = "RU";
  }else if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ){
    isolation_level = "PR";
  }

  // 3.事务的状态
  std::string txn_state;
  if(txn->GetState() == TransactionState::GROWING){
    txn_state = "GROWING";
  }else if(txn->GetState() == TransactionState::SHRINKING){
    txn_state = "SHRINKING";
  }else if(txn->GetState() == TransactionState::COMMITTED){
    txn_state = "COMMITED";
  }else if(txn->GetState() == TransactionState::ABORTED){
    txn_state = "ABORTED";
  }

  // 4.输出日志
  LOG_INFO("txn_id = %d,lock_type = %s,isolation_level = %s,txn_state = %s,page_id = %d,slot_num = %d",
            (int)txn->GetTransactionId(),lock_type_string.c_str(),isolation_level.c_str(),txn_state.c_str(),
            (int)rid.GetPageId(),(int)rid.GetSlotNum());
}

// 获取tuple的锁等待队列
auto LockManager::GetLRQueuePtr(RID rid) -> std::shared_ptr<LockRequestQueue>{
  // 1.获取锁
  std::scoped_lock<std::mutex> latch(row_lock_map_latch_);

  // 2.获取rid对应的等待队列
  auto tuple_queue = row_lock_map_[rid];
  if(tuple_queue == nullptr){
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    return row_lock_map_[rid];
  }

  return tuple_queue;
}

// 加行锁之前，需要检查表锁和行锁之间的逻辑是否相符（行锁：[IS/IX/SIX/S/X]）
void LockManager::IsTableFit(Transaction *txn,LockMode lock_mode,table_oid_t oid){
  // 1.如果行锁为X，那么表锁必须是[IX/X/SIX]，否则抛出异常
  if(lock_mode == LockMode::EXCLUSIVE){
    if(!(txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) || 
        txn->IsTableSharedIntentionExclusiveLocked(oid))){
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(),AbortReason::TABLE_LOCK_NOT_PRESENT);
    }

    return;
  }

  // 2.如果行锁为S，那么表锁必须是[IS/S/SIX/X/IX]
  // if(lock_mode == LockMode::SHARED){
  //   if(!(txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) || 
  //       txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableSharedLocked(oid) || 
  //       txn->IsTableIntentionSharedLocked(oid))){
  //     txn->SetState(TransactionState::ABORTED);
  //     throw TransactionAbortException(txn->GetTransactionId(),AbortReason::TABLE_LOCK_NOT_PRESENT);
  //   }

  //   return;
  // }
}

// 将某个锁添加到事务txn的锁集合中
void LockManager::BookKeeping(Transaction *txn,LockMode lock_mode,table_oid_t oid,RID rid){
  if(lock_mode == LockMode::EXCLUSIVE){
    (*(txn->GetExclusiveRowLockSet()))[oid].insert(rid);
  }else if(lock_mode == LockMode::SHARED){
    (*(txn->GetSharedRowLockSet()))[oid].insert(rid);
  }
}

// 从事务txn的锁集合中删除某个锁
void LockManager::BookKeepingRemove(Transaction *txn,LockMode lock_mode,table_oid_t oid,RID rid){
  if(lock_mode == LockMode::EXCLUSIVE){
    (*(txn->GetExclusiveRowLockSet()))[oid].erase(rid);
  }else if(lock_mode == LockMode::SHARED){
    (*(txn->GetSharedRowLockSet()))[oid].erase(rid);
  }
}

// 更新行锁，将新申请的锁添加到等待队列中
auto LockManager::UpdateLock(Transaction *txn,LockMode lock_mode,table_oid_t oid,RID rid) -> bool{
  // 1.获取行锁的等待队列，并获取行锁等待队列的锁
  auto row_lock_queue_ptr = GetLRQueuePtr(rid);
  std::scoped_lock<std::mutex> latch(row_lock_queue_ptr->latch_);

  auto txn_id = txn->GetTransactionId();

  // 2.遍历行锁的等待队列，判断事务txn之前是否已经申请过锁（如果能够发生锁升级，说明之前申请的锁一定被授予了，因为如果没有被授予那么事务会被阻塞住，不会申请新的锁）
  for(auto iter = row_lock_queue_ptr->request_queue_.begin();iter != row_lock_queue_ptr->request_queue_.end();iter++){
    // 事务txn之前申请过该行锁
    if((*iter)->txn_id_ == txn_id){
      // 2.1.如果之前申请过一样的锁，则直接返回
      if((*iter)->lock_mode_ == lock_mode){
        return true;
      } 

      // 2.2.如果当前行锁的等待队列，没有发生锁升级，那么如果事务txn满足锁升级的条件，将升级后的锁添加到等待队列中
      if(row_lock_queue_ptr->upgrading_ == INVALID_TXN_ID){
        if(IsCompatible((*iter)->lock_mode_, lock_mode)){
          // 2.2.1.更新upgrading_
          row_lock_queue_ptr->upgrading_ = txn_id;

          // 2.2.2.将之前的锁从事务的锁集合剔除，后续会将升级之后的锁（授予之后）重新添加回来
          BookKeepingRemove(txn,lock_mode,oid,rid);

          // 2.2.3.更新行锁的等待队列
          row_lock_queue_ptr->request_queue_.erase(iter);
          auto lock_request = std::make_shared<LockRequest>(txn_id,lock_mode,oid,rid);
          row_lock_queue_ptr->request_queue_.push_back(lock_request);
          /*int advance_count = 0;   // 迭代器需要移动次数
          // 寻找到第一个锁请求没有被授予的位置
          for(auto upgraded_iter = row_lock_queue_ptr->request_queue_.begin();upgraded_iter != row_lock_queue_ptr->request_queue_.end();upgraded_iter++){
            if((*upgraded_iter)->granted_){
              advance_count++;
            }
            break;
          }
          // 将迭代器移动到第一个锁请求没有被授予的位置
          auto it = row_lock_queue_ptr->request_queue_.begin();
          std::advance(it,advance_count);
          // 将升级之后的锁请求添加到等待队列中
          row_lock_queue_ptr->request_queue_.insert(it,lock_request);*/

          LOG_INFO("upgrade lock success");
          return true;
        }

        LOG_INFO("imcompatiable");
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id,AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }

      LOG_INFO("two upgrade abort");
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id,AbortReason::UPGRADE_CONFLICT);
      return false;
    }
  }

  LOG_INFO("no same");
  auto lock_request = std::make_shared<LockRequest>(txn_id,lock_mode,oid,rid);
  row_lock_queue_ptr->request_queue_.push_back(lock_request);
  return true;
}

// 行锁的授予
auto LockManager::GrantLock(Transaction *txn,LockMode lock_mode,RID rid) -> bool{
  // 1.获取行锁的等待队列
  auto row_lock_queue_ptr = GetLRQueuePtr(rid);

  auto txn_id = txn->GetTransactionId();

  LOG_INFO("table_lock_queue is %ld",row_lock_queue_ptr->request_queue_.size());

  // 2.先找到所有目前已经被授予的锁
  std::vector<std::shared_ptr<LockRequest>> has_granted_lock_request;
  for(auto iter = row_lock_queue_ptr->request_queue_.begin();iter != row_lock_queue_ptr->request_queue_.end();iter++){
    if((*iter)->granted_){
      has_granted_lock_request.push_back(*iter);
    }
  }

  // 3.检查锁的授予是否满足兼容性
  for(const auto& granted_row_lock : has_granted_lock_request){
    if(!GrantCompatiable(granted_row_lock->lock_mode_,lock_mode)){
      LOG_INFO("The requested lock is incompatible with the already granted lock.");
      return false;
    }
  }

  // 4.如果当前事务需要锁升级（如果当前等待队列中没有锁被授予，则优先进行这个升级后的锁授予;如果当前已经有锁被授予，且升级后的锁能够兼容则授予，否则锁授予失败）
  if(row_lock_queue_ptr->upgrading_ == txn_id){ // 当前表锁的等待队列中进行锁升级的事务如果是txn
    for(auto iter = row_lock_queue_ptr->request_queue_.begin();iter != row_lock_queue_ptr->request_queue_.end();iter++){
      // 4.1.找到事务txn需要升级的锁请求
      if((*iter)->txn_id_ == txn_id){
        row_lock_queue_ptr->upgrading_ = INVALID_TXN_ID;
        (*iter)->granted_ = true;
        LOG_INFO("upgrade lock grant success");
        return true;
      }
    }
  }

  Log(txn,lock_mode,rid);
  LOG_INFO("%d",(int)row_lock_queue_ptr->request_queue_.size());

  // 3.普通锁的授予，遍历等待队列，查看目前还没有被授予的锁，如果能够兼容则全部授予锁
  bool flag = false; // 标记txn的锁是否被授予了
  for(auto iter = row_lock_queue_ptr->request_queue_.begin();iter != row_lock_queue_ptr->request_queue_.end();iter++){
    if(!(*iter)->granted_){ // 如果锁没有被授予
      bool is_compatiable = true;
      for(const auto &lock_request : has_granted_lock_request){
        // 判断是否兼容
        if(!GrantCompatiable(lock_request->lock_mode_,(*iter)->lock_mode_)){
          // LOG_INFO("grant return %i %d",flag,txn_id);
          // return false;
          is_compatiable = false;
          break;
        }
      }

      // 如果兼容
      if(is_compatiable){
        (*iter)->granted_ = true;
        if((*iter)->txn_id_ == txn_id){
          flag = true;
        }
        has_granted_lock_request.push_back(*iter);
      }      
    }else{
      // 如果当前事务的锁已经被授予，则返回true，后续唤醒当前事务
      if((*iter)->txn_id_ == txn_id){
        flag = true;
      }
    }
  }

  LOG_INFO("grant return %i %d",flag,txn_id);
  return flag;

}

}  // namespace bustub
