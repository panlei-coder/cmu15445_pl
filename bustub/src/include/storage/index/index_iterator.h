//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  IndexIterator();
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *buffer_pool_manager,page_id_t page_id,Page *page,int index = 0);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &; // 前置的++

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;  // 需要缓存管理器
  Page *page_ = nullptr;                              // 获取的当前页的指针
  page_id_t page_id_ = INVALID_PAGE_ID;     // 当前遍历的页号
  int index_ = 0;                           // 获取到了当前叶子页的哪个一个位置
};

}  // namespace bustub
