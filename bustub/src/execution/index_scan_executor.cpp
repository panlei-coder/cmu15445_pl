//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn*>(
        exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_.get())),
      begin_(tree_->GetBeginIterator()),
      end_(tree_->GetEndIterator()){}

void IndexScanExecutor::Init() {
  begin_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // 1.判断是否遍历完了
    if(begin_ == end_){
        return false;
    }

    // 2.没有遍历完
    *rid = (*begin_).second;
    std::string name = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->table_name_;  // 获取索引对应的表名（索引中存放了其对应的表名）
    const TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(name);      // 根据表名获取对应的表信息
    table_info->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());     // 根据rid从表中获取对应的元组

    // 3.迭代器加1(只有前置的++)
    ++begin_;

    return true;
}

}  // namespace bustub
