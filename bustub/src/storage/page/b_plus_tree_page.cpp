//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"
#include "common/config.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
// 判断是否是叶子页
auto BPlusTreePage::IsLeafPage() const -> bool {
    return page_type_ == IndexPageType::LEAF_PAGE;
}
// 判断是否是根页
auto BPlusTreePage::IsRootPage() const -> bool { 
    return parent_page_id_ == INVALID_PAGE_ID; 
}
// 设置页的类型
void BPlusTreePage::SetPageType(IndexPageType page_type) {
    page_type_ = page_type;
}
// 获取页的类型
auto BPlusTreePage::GetPageType()const->IndexPageType{
    return page_type_;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
 // 获取当前页面已经存放键值对的数量
auto BPlusTreePage::GetSize() const -> int { 
    return size_;
}
// 设置当前页面已经存放键值对的数量
void BPlusTreePage::SetSize(int size) {
    size_ = size;
}
// 增加当前页面已经存放键值对的数量
void BPlusTreePage::IncreaseSize(int amount) {
    size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
 // 获取当前页面所能够存放键值对的最大数量
auto BPlusTreePage::GetMaxSize() const -> int {
    return max_size_;
}
// 设置当前页面所能够存放键值对的最大数量
void BPlusTreePage::SetMaxSize(int size) {
    max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
 // 获取当前页面所能够存放的键值对的最小数量（max_size_的一半）
auto BPlusTreePage::GetMinSize() const -> int {
    // 如果是内部页，且max_size为奇数，必须先加1在除以2
    if(GetPageType() == IndexPageType::INTERNAL_PAGE){ 
        auto min_size = (max_size_ % 2 == 0) ? max_size_ / 2 : (max_size_ + 1) / 2;
        return min_size;
    }

    return max_size_ / 2;
}

/*
 * Helper methods to get/set parent page id
 */
 // 获取当前页面的父亲页ID
auto BPlusTreePage::GetParentPageId() const -> page_id_t {
    return parent_page_id_;
}
// 设置当前页面的父亲页ID
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
    parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
 // 获取当前页ID
auto BPlusTreePage::GetPageId() const -> page_id_t {
    return page_id_;
}
// 设置当前页ID
void BPlusTreePage::SetPageId(page_id_t page_id) {
    page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
 // 设置当前页面的日志序列号
void BPlusTreePage::SetLSN(lsn_t lsn) { 
    lsn_ = lsn; 
}

}  // namespace bustub
