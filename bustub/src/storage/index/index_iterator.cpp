/**
 * index_iterator.cpp
 */
#include <cassert>
#include <exception>

#include "common/config.h"
#include "common/exception.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator():page_id_(INVALID_PAGE_ID){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager,page_id_t page_id,
    Page *page,int index):buffer_pool_manager_(buffer_pool_manager),page_(page),page_id_(page_id),index_(index){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
    // 将页的读锁释放，取消盯住(如果页不为空的话)
    if(page_ != nullptr){
        page_->RUnlatch();
        buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    // 如果leaf_page_的nextPageId不存在且当前已经遍历到叶子页的最后一个键值对的后面一个时，说明已经遍历完了
    auto leaf_page = reinterpret_cast<LeafPage*>(page_->GetData());
    return leaf_page->GetSize() == index_ && leaf_page->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    auto leaf_page = reinterpret_cast<LeafPage*>(page_->GetData());
    return leaf_page->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    // 1.第一种情况：如果遍历到了叶子页的最后一个元素，且叶子页的下一个是存在的，需要更新当前的迭代器的信息
    auto leaf_page = reinterpret_cast<LeafPage*>(page_->GetData());
    auto next_page_id = leaf_page->GetNextPageId();
    if(index_ == leaf_page->GetSize() - 1 && next_page_id != INVALID_PAGE_ID){
        // 1.1.获取next_page
        Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
        if(next_page == nullptr){
            throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
        }
        auto next_leaf_page = reinterpret_cast<LeafPage*>(next_page->GetData());

        // 1.2.获取next_page的读锁
        next_page->RLatch();    // todo ? 这里可能会发生死锁

        // 1.3.释放page_的读锁，取消对leaf_page_的盯住
        page_->RUnlatch();
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

        // 1.4.修改迭代器的信息
        page_ = next_page;
        leaf_page = next_leaf_page;
        index_ = 0;
    }else{ // 1.第二种情况：当前叶子页还没有遍历完，继续遍历，直接index_++
        index_++;
    }

    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
    // 判断是否遍历的同一个叶子页且是否是同一个叶子页的相同位置，那么两个迭代器是相等的
    return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
    // 如果遍历的不是一个叶子页或者同一个叶子页的不同位置，那么两个迭代器都是不相等的
    return page_id_ != itr.page_id_ || index_ != itr.index_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
