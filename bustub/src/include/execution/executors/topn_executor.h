//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  // 比较函数
  // (注意这里需要将比较函数定义成静态成员函数才能传递到priority_queue中，但比较函数中会用到实例化之后的成员变量，故这里只能使用lamda匿名函数)
  // auto Compare(const Tuple &tuple_first,const Tuple &tuple_second) -> bool;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }
  void SortTuples(std::vector<Tuple> &tuples);
  auto CompareTuples(Tuple &a, Tuple &b) -> bool;
  
 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_; // 子执行器
  std::queue<Tuple> queue_;
  size_t limit_count_;
};
}  // namespace bustub
