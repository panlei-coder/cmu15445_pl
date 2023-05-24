//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> page_id_t {
  return array_[index].second;
}

// 通过value查找value在内部页中的index
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value)const->int{
  auto it = std::find_if(array_,array_ + GetSize(),[&value](const auto& pair){return pair.second == value;});
  return std::distance(array_,it); // 返回下标
}

// 当根页刚刚被建立的时候，进行键值对的插入（注意需要多插入一个key为空的键值对）
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &first_value,const KeyType &second_key,const ValueType &second_value){
  array_[0].second = first_value;
  array_[1].first = second_key;  // 使用了柔性数组，这里原则上是不会发生下标越界的情况
  array_[1].second = second_value;
  IncreaseSize(2);
}

// 内部页插入数据，然后返回插入之后的键值对的数量(利用old_value来插入new_key:new_value的插入位置)
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const ValueType &old_value,const KeyType &new_key,const ValueType &new_value) -> int{
  // 1.找到old_value对应的下标，new_value对应的为其下标+1
  auto index = ValueIndex(old_value) + 1;

  // 2.将对应的内容向后移动
  std::move_backward(array_ + index,array_ + GetSize(),array_ + GetSize() + 1);

  // 3.将数据插入到index中
  array_[index].first = new_key;
  array_[index].second = new_value;

  // 4.size+1,然后返回插入之后的size
  IncreaseSize(1);
  return GetSize();
}

// 页分裂，进行键值对复制
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage* sibling_internal_page,BufferPoolManager* buffer_pool_manager){
  // 1.获取分裂的开始位置和数量
  MappingType* move_start = array_ + GetMinSize();
  int move_size = GetMaxSize() - GetMinSize() + 1;

  // 2.调用复制函数，并修改分裂之后的size
  sibling_internal_page->CopyNFrom(move_start, move_size, buffer_pool_manager);
  IncreaseSize(-1*move_size);
}

// 根据index，从内部页中删除对应的key:value
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index){
  // 1.整体向前移动
  std::move(array_ + index + 1,array_ + GetSize(),array_ + index);

  // 2.修改size
  IncreaseSize(-1);
}

// 将自身的第一个键值对移动到兄弟页的尾部
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEnd(BPlusTreeInternalPage* sibling_internal_page,const KeyType &middle_key,BufferPoolManager *buffer_pool_manager){
  // 1.将middle_key设置成自身第一个的key（因为自己的第一个key是空的,后续再将自己更新后的第一个key添加到父亲页中）
  SetKeyAt(0,middle_key);

  // 2.获取第一个item，添加到兄弟页的尾部
  auto first_item = array_[0];
  sibling_internal_page->CopyLastFrom(first_item,buffer_pool_manager);

  // 3.整体向前移动一个，并修改size
  std::move(array_ + 1,array_ + GetSize(),array_);
  IncreaseSize(-1);
}

// 将自身的最后一个键值对移动到兄弟页的头部
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFront(BPlusTreeInternalPage* sibling_internal_page,const KeyType &middle_key,BufferPoolManager *buffer_pool_manager){
  // 1.将middle_key设置成兄弟页第一个的key
  sibling_internal_page->SetKeyAt(0,middle_key);

  // 2.获取当前页的最后一个item，并添加到兄弟页中
  auto last_item = array_[GetSize() - 1];
  sibling_internal_page->CopyFirstFrom(last_item,buffer_pool_manager);

  // 3.修改size
  IncreaseSize(-1);
}

// 两个页合并（在调用之前做处理，保证这里的兄弟页是左兄弟）
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage* sibling_internal_page,const KeyType &middle_key,BufferPoolManager *buffer_pool_manager){
  // 1.将middle_key添加到当前页中
  SetKeyAt(0,middle_key);
  
  // 2.将当前页合并到左兄弟中
  sibling_internal_page->CopyNFrom(array_,GetSize(),buffer_pool_manager);

  // 3.修改size
  IncreaseSize(-1 * GetSize());
}

// 具体实现复制操作的函数
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType* items,int size,BufferPoolManager* buffer_pool_manager){
  // 1.先将要拆分的内容复制过来
  int before_copy_size = GetSize();
  std::copy(items,items + size,array_ + before_copy_size);
  
  // 2.重新设置孩子页的父亲页ID
  int page_id = GetPageId();
  for(int index = before_copy_size;index < before_copy_size + size;index++){
    // 2.1.获取对应的孩子页
    Page* page = buffer_pool_manager->FetchPage(ValueAt(index));
    if(page == nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
    }
    auto bplus_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

    // 2.2.设置父亲页ID
    bplus_page->SetParentPageId(page_id);

    // 2.3.取消盯住
    buffer_pool_manager->UnpinPage(bplus_page->GetPageId(), true);
  }

  // 3.修改页的键值对数量
  IncreaseSize(size);
}

// 将item复制到自己的最后一个
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &item,BufferPoolManager* buffer_pool_manager){
  // 1.将item添加到尾部
  *(array_ + GetSize()) = item;

  // 2.重新设置孩子页的父亲页ID
  int page_id = GetPageId();
  // 2.1.获取对应的孩子页
  Page* page = buffer_pool_manager->FetchPage(item.second);
  if(page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
  }
  auto bplus_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
  // 2.2.设置父亲页ID
  bplus_page->SetParentPageId(page_id);
  // 2.3.取消盯住
  buffer_pool_manager->UnpinPage(bplus_page->GetPageId(), true);

  // 3.修改size
  IncreaseSize(1);
}

// 将item复制到自己的第一个
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &item,BufferPoolManager* buffer_pool_manager){
  // 1.整体向后移动一个，并将item添加到头部
  std::move_backward(array_,array_ + GetSize(),array_ + GetSize() + 1);
  *array_ = item;

  // 2.重新设置孩子页的父亲页ID
  int page_id = GetPageId();
  // 2.1.获取对应的孩子页
  Page* page = buffer_pool_manager->FetchPage(item.second);
  if(page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
  }
  auto bplus_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
  // 2.2.设置父亲页ID
  bplus_page->SetParentPageId(page_id);
  // 2.3.取消盯住
  buffer_pool_manager->UnpinPage(bplus_page->GetPageId(), true);

  // 3.修改size
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub