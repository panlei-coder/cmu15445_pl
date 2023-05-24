//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/bustub_instance.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub{
  struct JoinKey{
    Value value_;
    auto operator==(const struct JoinKey &other) const -> bool{return value_.CompareEquals(other.value_) == CmpBool::CmpTrue;}
  };
} // namespace std;

namespace std{  // 针对JoinKey类型来特化hash函数
  template<>
  struct hash<bustub::JoinKey>{
    auto operator()(const bustub::JoinKey &key)const -> size_t{
      size_t cur_hash = 0;
      if(!key.value_.IsNull()){
        cur_hash = bustub::HashUtil::CombineHashes(cur_hash, bustub::HashUtil::HashValue(&key.value_));
      }

      return cur_hash;
    }
  };
}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_executor_;
  std::unique_ptr<AbstractExecutor> right_child_executor_;
  std::unordered_map<JoinKey, std::vector<Tuple>> right_hash_map_; // 用于存放左表hash之后的数据（注意这里使用的是模板特化之后的hash函数）
  size_t current_index_; // 上一次遍历到tuple的位置
  JoinKey left_join_key_; // 记录左表tuple的JoinKey
  Tuple left_tuple_;  // 记录左表的tuple
  bool is_matched_; // 标记左表的tuple是否连接成功
};

}  // namespace bustub

