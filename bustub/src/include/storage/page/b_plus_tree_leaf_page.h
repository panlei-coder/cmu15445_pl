//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyIndex(const KeyType &key,const KeyComparator &comparator)const -> int;
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  auto GetItem(int index) -> const MappingType&;

  // insert
  auto LookUp(const KeyType &key,ValueType *value,const KeyComparator &comparator)const ->bool;
  auto Insert(const KeyType &key,const ValueType &value,const KeyComparator &comparator)->int;
  void MoveHalfTo(BPlusTreeLeafPage* sibling_leaf_page);

  // 删除操作
  auto Remove(const KeyType &key,const KeyComparator &comparator)->int;
  void MoveFirstToEnd(BPlusTreeLeafPage* sibling_leaf_page);
  void MoveLastToFront(BPlusTreeLeafPage* sibling_leaf_page);
  void MoveAllTo(BPlusTreeLeafPage* sibling_leaf_page);

 private:
  void CopyNFrom(MappingType* items,int size);
  void CopyLastFrom(const MappingType &item);
  void CopyFirstFrom(const MappingType &item);

  page_id_t next_page_id_; // 在所有叶子页中维护一条链表
  // Flexible array member for page data.
  MappingType array_[0]; //这是一个柔性数组，因为page中固定分配了一个页的大小，在将page中的data数据转化成叶子页时，会自动填充除去header_page之后的内容
};
}  // namespace bustub
