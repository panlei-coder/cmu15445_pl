//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key,const KeyComparator &comparator)const -> int{
  auto key_it = std::lower_bound(array_,array_ + GetSize(),key,
            [&comparator](const auto &pair,auto key){return comparator(pair.first,key) < 0;});
  return std::distance(array_,key_it);
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

// 获取ValueType
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const ->ValueType{
  return array_[index].second;
}

// 返回对应下标的item
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType&{
  return array_[index];
}

// 查找key是否已经在叶子页中存在
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key,ValueType *value,const KeyComparator &comparator)const ->bool{
  // 1.在array_中寻找第一个大于等于key的位置，返回对应位置的迭代器，利用comparator进行大小比较
  auto key_it = std::lower_bound(array_,array_ + GetSize(),key,[&comparator](const auto &pair,auto key){return comparator(pair.first,key) < 0;});
  
  // 2.如果不存在第一个大于等于key的位置，直接返回false
  if(key_it == (array_ + GetSize()) || comparator(key_it->first,key) != 0){
    return false;
  }

  // 3.如果存在第一个大于等于key的位置，则获取对应的value，并返回true
  *value = key_it->second;
  return true;
}

// 直接向叶子页中插入数据
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,const ValueType &value,const KeyComparator &comparator)->int{
  // 1.在array_中寻找第一个大于等于key的位置，返回对应位置的迭代器，利用comparator进行大小比较
  auto key_it = std::lower_bound(array_,array_ + GetSize(),key,[&comparator](const auto &pair,auto key){return comparator(pair.first,key) < 0;});

  // 2.如果不存在第一个大于等于key的位置，则可以进行插入
  if(key_it == (array_ + GetSize()) || comparator(key_it->first,key) != 0){ // key不存在
    if(key_it != (array_ + GetSize())){ // 说明要插入的位置不是最后一个，需要进行挪动
      std::move_backward(key_it,array_ + GetSize(),array_ + GetSize() + 1); // 整体向后挪动一个位置
    }
    key_it->first = key;
    key_it->second = value;
    IncreaseSize(1);
    return GetSize();
  }

  // 3.如果存在第一个大于等于key的位置，则需要返回当前的大小
  return GetSize();  // 返回当前页的大小是用于判断是否是插入相同的key
}

// 将叶子页的一半数据移动到兄弟页中
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage* sibling_leaf_page){
  // 1.获取分裂的开始位置和数量
  MappingType* move_start = array_ + GetMinSize();
  int move_size = GetMaxSize() - GetMinSize();

  // 2.调用复制函数，将分裂的数据复制到兄弟页中
  sibling_leaf_page->CopyNFrom(move_start,move_size);

  // 3.修改自身键值对的数量
  IncreaseSize(-1 * move_size);
}

// 删除操作
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key,const KeyComparator &comparator)->int{
  // 1.在内部页中找到第一个大于等于key的键值对
  auto key_it = std::lower_bound(array_,array_ + GetSize(),key,[&comparator](const auto &pair,auto key){return comparator(pair.first,key) < 0;});
  
  // 2.如果不存在第一个大于等于key的位置，直接返回内部页键值对的大小
  if(key_it == (array_ + GetSize()) || comparator(key_it->first,key) != 0){
    return GetSize();
  }

  // 3.向前移动，将记录覆盖掉，并更改size
  std::move(key_it + 1,array_ + GetSize(),key_it);
  IncreaseSize(-1);

  return GetSize();
}

// 将自己页中的第一个键值对移动到兄弟页中的最后一个（从右兄弟中借）
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEnd(BPlusTreeLeafPage* sibling_leaf_page){
  // 1.获取第一个item
  const MappingType &first_item = GetItem(0);

  // 2.整体向前移动一个，size减一
  std::move(array_ + 1,array_ + GetSize(),array_);
  IncreaseSize(-1);

  // 3.将item复制到sibling_leaf_page的尾部
  sibling_leaf_page->CopyLastFrom(first_item);
}

// 将自己页中的最后一个键值对移动到兄弟页中的第一个（从左兄弟中借）
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFront(BPlusTreeLeafPage* sibling_leaf_page){
  // 1.获取最后一个item
  const MappingType &last_item = GetItem(GetSize() - 1);

  // 2.size减一
  IncreaseSize(-1);

  // 3.将item复制到sibling_leaf_page的头部
  sibling_leaf_page->CopyFirstFrom(last_item);
}

// 将自己页中的所有键值对移动到兄弟页中
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage* sibling_leaf_page){
  // 1.将自身所有的键值对复制到兄弟页中
  sibling_leaf_page->CopyNFrom(array_,GetSize());

  // 2.修改size
  IncreaseSize(-1*GetSize());
}

// 将自己部分数据移动到另一个兄弟页中（分裂与合并中会使用）
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType* items,int size){
  // 1.将对应的内容复制到该页中，但是不影响原本的数据，原本的数据通过控制size的大小来实现逻辑删除
  std::copy(items,items + size,array_ + GetSize()); 

  // 2、增加复制之后自身键值对的数量
  IncreaseSize(size);
}

// 将item复制到自己的最后一个
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item){
  // 1.添加键值对到尾部
  *(array_ + GetSize()) = item;

  // 2.修改size
  IncreaseSize(1);
}

// 将item复制到自己的第一个
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item){
  // 1.将键值对整体向后挪动一个
  std::move_backward(array_,array_ + GetSize(),array_ + GetSize() + 1);

  // 2.将item添加到头部
  *array_ = item;
  
  // 3.修改size
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
