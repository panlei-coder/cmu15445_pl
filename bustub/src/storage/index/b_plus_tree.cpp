#include <string>

#include "binder/bound_expression.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { // 判断根节点是否还存在
  return root_page_id_ == INVALID_PAGE_ID;
}

// 判断页是否是安全的
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage *btree_page,Operation op) -> bool{
  if(op == Operation::SEARCH){ // 1.如果是一个查找操作，那么直接返回true
    return true;
  }
  
  if(op == Operation::INSERT){ // 2.如果是插入操作，需要对叶子页和内部页分别判断
    if(btree_page->IsLeafPage()){ // 如果是叶子页
      return btree_page->GetSize() < btree_page->GetMaxSize() - 1;  // 注意叶子页不能存满的
    }

    return btree_page->GetSize() < btree_page->GetMaxSize();
  }
  
  if(op == Operation::DELETE){ // 3.如果是删除操作，需要对叶子页和内部页分别判断
    if(btree_page->IsRootPage()){   // 如果是根节点的话需要单独处理
      if(btree_page->IsLeafPage()){
        return btree_page->GetSize() > 1;  // 叶子页作为根节点的时候，必须size大于1的时候才是安全，树不会变成空
      } 

      return btree_page->GetSize() > 2;    // 内部页作为根节点的时候，必须size大于2的时候才是安全的，不会发生树结构的改变
    }

    return btree_page->GetSize() > btree_page->GetMinSize();
  }

  return false;
}

// 释放掉祖先的锁（这里释放的都是写锁，锁住的都是会被修改的页，所以在取消盯住的时候需要设为脏页,）
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseWLatches(Transaction *transaction,bool is_dirty){ // todo ？ 这里应该要设置一个is_dirty的标志位
  // 1.判断事务是否为空，如果为空，则直接返回（因为读的时候事务是空的）
  if(transaction == nullptr){ 
    return;
  }

  // 2.如果事务不为空，则依次获取page的集合（是一个双端队列，被一个共享的智能指针指向），释放锁
  auto page_set = transaction->GetPageSet();

  // 3.如果page_set不为空，依次获取页，然后释放锁
  //（这里注意使用的是双端队列，在从树的根节点从上往下遍历的时候是依次添加的，在释放锁的时候也要依次从上往下）
  while(!page_set->empty()){
    auto page = page_set->front();  // 从队头开始获取
    page_set->pop_front();          // 从队头删除掉
    if(page == nullptr){ // 对于根页使用nullptr存放，根页不取消盯住
      root_latch_.WUnlock();
    }else{
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
    }
  }
}

// 查找最左侧的叶子页
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeftMostLeafPage() -> Page*{
  page_id_t page_id = root_page_id_;
  Page *pre_page = nullptr;
  while(true){
    // 1.从根节点开始获取页
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if(page == nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
    }
    auto btree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

    // 2.获取page的读锁
    page->RLatch();

    // 3.如果pre_page为空，则释放root_latch_的读锁，否则释放pre_page的读锁
    if(pre_page != nullptr){
      pre_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), false);
    }else{
      root_latch_.RUnlock();
    }

    // 4.并判断是否是叶子页，如果是叶子页，说明已经找到了对应的页
    if(btree_page->IsLeafPage()){
      return page;
    }      

    // 5.如果是内部页，获取内部页的第一个page_id
    auto internal_page = static_cast<InternalPage*>(btree_page);
    page_id = internal_page->ValueAt(0);

    // 6.页使用完之后，将页赋值给pre_page
    pre_page = page;
  }
}

// 查找最右侧的叶子页
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRightMostLeafPage() -> Page*{
  page_id_t page_id = root_page_id_;
  Page *pre_page = nullptr;
  while(true){
    // 1.从根节点开始获取页，并判断是否是叶子页，如果是叶子页，说明已经找到了对应的页
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if(page == nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
    }
    auto btree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

    // 2.获取page的读锁
    page->RLatch();

    // 3.判断pre_page是否是空，如果是空，则释放root_latch_的读锁，否则释放pre_page的读锁
    if(pre_page != nullptr){
      pre_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), false);
    }else{
      root_latch_.RUnlock();
    }

    // 4.如果是叶子页，则直接返回这个页
    if(btree_page->IsLeafPage()){
      return page;
    }

    // 5.如果是内部页，获取内部页的最后一个page_id
    auto last_index = btree_page->GetSize() - 1;
    auto internal_page = static_cast<InternalPage*>(btree_page);
    page_id = internal_page->ValueAt(last_index);

    // 6.将pre_page指向page
    pre_page = page;
  }
}

// 查找满足条件的叶子页，返回对应页的指针
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key,Transaction *transaction,Operation op,bool first_pass)->Page*{
  // 1.判断transaction是否为空，如果不是查找操作，那么事务不能为空
  if(transaction == nullptr && op != Operation::SEARCH){
    throw std::logic_error("Insert or remove operation must be given a not-null transaction");
  }

  // 2.判断这是不是第一次调用，第一次使用乐观锁的模式，第二次调用的时候使用悲观锁的模式(如果是第二次，则根节点直接被加上写锁)
  if(!first_pass){
    root_latch_.WLock();
    transaction->AddIntoPageSet(nullptr);
  }

  // 3.依次从根节点开始向下遍历
  page_id_t page_id = root_page_id_;
  Page* pre_page = nullptr;
  while(true){
    // 3.1.从根节点开始获取页
    Page* page = buffer_pool_manager_->FetchPage(page_id);    // 获取页的时候会被盯住
    if(page == nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
    }
    auto btree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

    // 3.2.判断是否是叶子页，如果是叶子页，说明已经找到了对应的页(分两种情况：乐观锁和悲观锁)
    if(first_pass){ // 第一种情况：如果是第一次调用，使用乐观锁的模式
      // 3.2.1.处理btree_page,乐观锁的模式只有是叶子页且不是查找操作的时候才会获取写锁
      if(btree_page->IsLeafPage() && op != Operation::SEARCH){ 
        page->WLatch();
        transaction->AddIntoPageSet(page);
      }else{  // 直接获取读锁
        page->RLatch();
      }

      // 3.2.2.处理pre_page，乐观锁的模式会释放祖先的锁
      if(pre_page != nullptr){ // 如果pre_page不为空，那么可以直接释放读锁，并取消盯住
        pre_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), false);
      }else{ // 如果pre_page为空，意味着根节点的信息不会被修改，root_latch_的读锁可以释放掉
        root_latch_.RUnlock();
      }
    }else{  // 3.2.第二种情况：如果是第二种次调用，使用悲观锁的模式
      // 3.2.1.只有插入/删除操作会进入悲观锁的模式，第二次获取锁
      assert(op != Operation::SEARCH);

      // 3.2.2.直接从根节点开始重新遍历，但是第二次都是加写锁
      page->WLatch();

      // 3.2.3.判断当前页是否是安全页，如果是安全页，则直接释放所有的祖先页（这里只要某个节点是安全的，那么就不用管祖先是不是安全的）
      if(IsPageSafe(btree_page, op)){
        ReleaseWLatches(transaction,false);  // todo ？ 这里应该要设置一个is_dirty的标志位
      }

      // 3.2.4.释放完所有的祖先锁之后，需要将当前页添加到事务中
      transaction->AddIntoPageSet(page);
    }
    
    // 4.判断是不是叶子页，如果是叶子页，需要判断是不是安全的，如果是安全的可以直接返回，如果是不安全的，需要再一次获取
    if(btree_page->IsLeafPage()){
      if(first_pass && !IsPageSafe(btree_page, op)){ // 第一次获取，且不是安全的，需要重新获取
        ReleaseWLatches(transaction,false); // 将这个不安全的叶子页释放掉，不过这里需要设置一个标志位：is_dirty
        return GetLeafPage(key, transaction, op, false);
      }

      return page;
    }

    // 5.如果是内部页，需要比较key，判断是在哪个孩子页中，并获取出来
    auto internal_page = static_cast<InternalPage*>(btree_page);
    page_id = internal_page->ValueAt(internal_page->GetSize() - 1); // 将内部页中最右侧的value设置成初始值,如果key比内部页中所有的都大，那么就意味着要获取最右边的value
    for(int i = 1;i < internal_page->GetSize();i++){   // 这里需要注意的是内部页的第一个键值对只有value（孩子页页号），没有key
      if(comparator_(internal_page->KeyAt(i),key) > 0){
        page_id = internal_page->ValueAt(i - 1);
        break;
      }
    }

    pre_page = page;
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 1.获取根页的读锁
  root_latch_.RLock();

  // 2.判断树是否为空，如果为空则直接返回false
  if(IsEmpty()){
    root_latch_.RUnlock();
    return false;
  }

  // 3.找到对应的叶子页
  bool found = false;
  Page *page = GetLeafPage(key,nullptr,Operation::SEARCH,true); // 对于查找操作都是一边获取孩子页的读锁，一边释放父亲页的读锁，不需要事务，所以这里传的是nullptr
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  
  // 4.先去查找是否存在，如果存在则将value添加到result中
  ValueType value;
  bool existed = leaf_page->LookUp(key,&value,comparator_);
  if(existed){
    result->emplace_back(value);
    found = true;
  }

  // 5.释放page的读锁，并取消盯住
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false); // 用完了之后就释放盯住（计数的方式）
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 1.首先获取root_latch_的读锁
  root_latch_.RLock();

  // 2.如果根节点为空，需要重新创建一个根节点，并插入数据
  if(IsEmpty()){
    // 2.1.这里需要将读锁释放掉，重新获取写锁，因为会对根节点信息发生修改
    root_latch_.RUnlock();
    root_latch_.WLock();

    // 2.2.需要再一次判断树是不是为空
    if(IsEmpty()){
      StartNewTree(key,value);
      root_latch_.WUnlock();
      return true;
    }
    
    // 2.3.树不为空了，释放写锁，重新获取读锁
    root_latch_.WUnlock();
    root_latch_.RLock();
  }

  // 3.根节点不为空的情况，则直接插入数据到叶子节点中
  return InsertIntoLeaf(key,value,transaction);
}

// 根节点为空，需要创建一个根节点，并将key:value插入到根节点中
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key,const ValueType &value){
  // 1.创建新的页
  Page* page = buffer_pool_manager_->NewPage(&root_page_id_);
  if(page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't allocate new page"));
  }

  // 2.新的页初始化之后，直接向根节点中插入数据
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  leaf_page->Init(root_page_id_,INVALID_PAGE_ID,leaf_max_size_); // 新创建的页，需要初始化
  leaf_page->Insert(key,value,comparator_);   // 插入数据
  
  // 3.使用完这个页之后释放盯住，并更新根节点信息
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(1);
}

// 根节点不为空，将key:value，插入到叶子页中
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key,const ValueType &value,Transaction *transaction) -> bool{
  // 1.获取key应该被添加到哪个叶子页中
  Page* page = GetLeafPage(key,transaction,Operation::INSERT,true);
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());

  // 2.判断插入的是否是重复值
  auto before_insert_size = leaf_page->GetSize();
  auto after_insert_size = leaf_page->Insert(key,value,comparator_);
  if(before_insert_size == after_insert_size){ // 说明是重复值
    ReleaseWLatches(transaction,false);
    // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  // 3.判断插入之后，叶子页是否满了，如果没有满
  if(after_insert_size < leaf_max_size_){
    ReleaseWLatches(transaction,true);
    // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return true;
  }

  // 4.如果满了，需要分裂
  auto sibling_leaf_page = Split(leaf_page); // 分裂出一个兄弟页(右兄弟)
  sibling_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(sibling_leaf_page->GetPageId());

  // 5.向父亲页中插入
  auto parent_key = sibling_leaf_page->KeyAt(0);
  InsertIntoParent(leaf_page,parent_key,sibling_leaf_page,transaction);

  // 6.leaf_page/sibling_leaf_page都需要释放盯住
  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_leaf_page->GetPageId(), true); // 兄弟页是新创建的页，不会被添加到transaction中，这里需要单独取消盯住
  return true;
}

// 页的分裂
INDEX_TEMPLATE_ARGUMENTS
template<typename Node> // 这里似乎没必要使用函数模板，可以直接使用BPlusTreePage*，实现父类到子类的动态转换
auto BPLUSTREE_TYPE::Split(Node* node)->Node*{
  // 1.创建一个新的页，用于存放被分裂出来的数据
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  if(page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't allocate new page"));
  }

  // 2.进行页类型的转化
  Node* sibling_node = reinterpret_cast<Node*>(page->GetData());

  // 3.页的分裂
  if(node->IsLeafPage()){ // 如果是叶子页
    auto leaf_page = reinterpret_cast<LeafPage*>(node);
    auto sibling_leaf_page = reinterpret_cast<LeafPage*>(sibling_node);
    sibling_leaf_page->Init(page->GetPageId(),leaf_page->GetParentPageId(),leaf_max_size_);
    leaf_page->MoveHalfTo(sibling_leaf_page);
  }else{ // 如果是内部页
    auto internal_page = reinterpret_cast<InternalPage*>(node);
    auto sibling_internal_page = reinterpret_cast<InternalPage*>(sibling_node);
    sibling_internal_page->Init(page->GetPageId(),internal_page->GetParentPageId(),internal_max_size_);
    internal_page->MoveHalfTo(sibling_internal_page,buffer_pool_manager_); // 需要重新设置孩子页的父亲页ID
  }

  return sibling_node;
}

// 向父亲页中插入数据
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage* left_page,const KeyType &key,BPlusTreePage* right_page,Transaction* transaction){
  // 1.先判断left_page是否是根页，如果是根页的话，需要重新创建一个根页出来
  if(left_page->IsRootPage()){
    // 1.1.需要创建一个新的页作为根页
    Page* page = buffer_pool_manager_->NewPage(&root_page_id_);
    if(page == nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't allocate new page"));
    }

    // 1.2.完成对根页的转化和初始化
    auto root_page = reinterpret_cast<InternalPage*>(page->GetData());
    root_page->Init(root_page_id_,INVALID_PAGE_ID,internal_max_size_);

    // 1.3.将key插入到根页中，但是需要注意的是，因为一开始的时候根页是空的，所以还需要多插入一个key为空的键值对
    root_page->PopulateNewRoot(left_page->GetPageId(),key,right_page->GetPageId());

    // 1.4.重新设置left_page和right_page的父亲页ID
    left_page->SetParentPageId(root_page->GetPageId());
    right_page->SetParentPageId(root_page->GetPageId());

    // 1.5.取消盯住，并更新根节点信息
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
    UpdateRootPageId(0); // 根节点已经存在了，“0”表示更新根节点信息

    // 1.6.释放掉锁
    ReleaseWLatches(transaction,true);

    return;
  }
  
  
  // 2.获取父亲页
  Page* parent_page = buffer_pool_manager_->FetchPage(left_page->GetParentPageId());
  if(parent_page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
  }

  // 3.将key插入到父亲页中
  auto parent_internal_page = reinterpret_cast<InternalPage*>(parent_page->GetData());
  int after_insert_size = parent_internal_page->Insert(left_page->GetPageId(),key,right_page->GetPageId());

  // 4.如果父亲页没有满的话，直接返回
  if(after_insert_size <= internal_max_size_){
    ReleaseWLatches(transaction,true);
    buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
    return;
  }

  // 5.如果父亲页满了的话，需要进行分裂
  auto sibling_parent_internal_page = Split(parent_internal_page);
  auto parent_key = sibling_parent_internal_page->KeyAt(0); // 这里设计的比较巧妙，sibling_parent_internal_page的第一个键值对的是没有key的，直接拿出来就好
  InsertIntoParent(parent_internal_page,parent_key,sibling_parent_internal_page,transaction);

  // 6.取消盯住
  buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_parent_internal_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 1.获取root_latch_的读锁
  root_latch_.RLock();

  // 2.判断根节点是否为空,如果为空则不做任何操作
  if(IsEmpty()){
    root_latch_.RUnlock(); 
    return;
  }

  // 2.获取要删除的元素在哪个叶子页上
  Page* page = GetLeafPage(key,transaction,Operation::DELETE,true);
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());

  // 3.在叶子页上删除对应的key，需要判断删除之前和删除之后的size是否相等，如果相等，说明没有对应的key，可以直接返回
  auto before_remove_size = leaf_page->GetSize();
  auto after_remove_size = leaf_page->Remove(key,comparator_);
  if(before_remove_size == after_remove_size){
    ReleaseWLatches(transaction,false);
    // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return;   
  }

  // 4.判断当前页是否需要借键值对或者合并
  auto should_delete_leaf_page = CoalesceOrRedistribute(leaf_page,transaction);

  // 5.判断是否需要被删除（需要被删除，加到事务中，然后等所有删除操作完成后统一处理）
  if(should_delete_leaf_page){
    transaction->AddIntoDeletedPageSet(leaf_page->GetPageId());
  }

  // 6.释放所有页的锁和取消盯住
  ReleaseWLatches(transaction,true);
  
  // 7.将需要被删除的页全部删除掉，并将deletePageSet清空
  std::for_each(transaction->GetDeletedPageSet()->begin(),transaction->GetDeletedPageSet()->end(),
                [&bmp = buffer_pool_manager_](const page_id_t page_id){bmp->DeletePage(page_id);});
  transaction->GetDeletedPageSet()->clear();
}

// 判断删除之后是否需要进行调整（返回值用于判断是否需要对根节点进行删除）
INDEX_TEMPLATE_ARGUMENTS
template<typename Node>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(Node* node,Transaction *transaction) -> bool{
  // 1.判断是否是根节点，如果是根节点需要进行调整
  if(node->IsRootPage()){
    auto should_delete_root = AdjustRoot(node); // 这里对应着的是Adjust中的第二种情况
    return should_delete_root;
  }

  // 2.如果不是根节点，根据size判断是否需要调整
  if(node->GetSize() >= node->GetMinSize()){
    return false;
  }

  // 3.size太小，需要调整，首先获取父亲页
  auto parent_page_id = node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  if(parent_page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
  }
  auto parent_internal_page = reinterpret_cast<InternalPage*>(parent_page->GetData());

  // 4.获取node在父亲页中的下标
  auto index = parent_internal_page->ValueIndex(node->GetPageId());

  // 5.获取node的兄弟页（如果index==0,则获取右兄弟页，否则获取左兄弟页，这里只需要获取一个兄弟页，不是重新分配就是合并，不需要考虑太过复杂）
  auto sibling_page_id = parent_internal_page->ValueAt(index == 0 ? 1 : index - 1);
  Page* sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if(sibling_page == nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
  }
  sibling_page->WLatch();   // 这里需要注意，因为会涉及到与兄弟页的重新分配或者是合并，所以需要给兄弟页加上写锁
  auto sibling_node = reinterpret_cast<Node*>(sibling_page->GetData());

  // 6.判断是否需要重新分配，如果不能重新分配，则直接合并
  if(node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()){
    // 6.1.调用重新分配的函数
    Redistribute(sibling_node,node,parent_internal_page,index);

    // 6.2.释放sibling_page的写锁，并取消sibling_page和parent_internal_page的盯住
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);

    // 6.3.返回false，意味着不需要删除这个节点
    return false;
  }
  
  // 7.直接合并
  auto should_delete_parent_page = Coalesce(&sibling_node, &node, &parent_internal_page, index, transaction);
  
  // 8.合并了之后需要判断是否要删除父亲页（原则上应该添加到事务中，完成删除操作统一删除）,如果should_delete_parent_page是真的话，需要把parent_internal_page删除掉
  if(should_delete_parent_page){
    // buffer_pool_manager_->DeletePage(parent_page->GetPageId());
    transaction->AddIntoDeletedPageSet(parent_page->GetPageId());
  }

  // 9.取消盯住
  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

  return true;
}

// 对根节点进行调整（返回值用于判断是否删除根节点）
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage * root_node) -> bool{
  // 1.判断根节点是不是内部页，且只剩下一个孩子页，需要更新根节点（第一种情况）
  if(!root_node->IsLeafPage() && root_node->GetSize() == 1){
    // 1.1.获取根页中的唯一的孩子页
    auto root_internal_page = reinterpret_cast<InternalPage*>(root_node);
    auto child_page_id = root_internal_page->ValueAt(0);
    Page* page = buffer_pool_manager_->FetchPage(child_page_id);
    if(page == nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY,std::string("can't fetch the page"));
    }
    auto child_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

    // 1.2.调整根页信息,并设置根页的父亲页ID
    root_page_id_ = child_page_id;
    child_page->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(0);

    // 1.3.取消盯住
    buffer_pool_manager_->UnpinPage(child_page_id, true);

    return true;
  }

  // 2.判断根节点是叶子页，并且所有的内容都被删除了（第二种情况）
  if(root_node->IsLeafPage() && root_node->GetSize() == 0){
    // 应该要将root_page_id设置为INVALID_PAGE_ID，重新更新header_page中记录的树信息
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }

  return false;
}

// 重新分配
INDEX_TEMPLATE_ARGUMENTS
template<typename Node>
void BPLUSTREE_TYPE::Redistribute(Node* sibling_node,Node* node,InternalPage* parent_page,int index){ // index为node在parent_page中的下标
  // 1.第一种情况：对叶子页重新分配
  if(node->IsLeafPage()){
    // 1.1.对node和sibling_node强制转换
    auto leaf_page = reinterpret_cast<LeafPage*>(node);
    auto sibling_leaf_page = reinterpret_cast<LeafPage*>(node);

    // 1.2.根据index的值判断，sibling_leaf_page是左兄弟还是右兄弟
    if(index == 0){ // sibling_leaf_page为leaf_page右兄弟
      sibling_leaf_page->MoveFirstToEnd(leaf_page);
      parent_page->SetKeyAt(1,sibling_leaf_page->KeyAt(0));
    }else{ // sibling_leaf_page为leaf_page左兄弟
      sibling_leaf_page->MoveLastToFront(leaf_page);
      parent_page->SetKeyAt(index,leaf_page->KeyAt(0));
    }
  }else{ // 2.第二种情况：对内部页重新分配
    // 2.1.对node和sibling_node强制转换
    auto internal_page = reinterpret_cast<InternalPage*>(node);
    auto sibling_internal_page = reinterpret_cast<InternalPage*>(sibling_node);

    // 2.2.根据index的值判断，sibling_internal_page是左兄弟还是右兄弟
    if(index == 0){ // sibling_internal_page为internal_page右兄弟
      sibling_internal_page->MoveFirstToEnd(internal_page,parent_page->KeyAt(1),buffer_pool_manager_);
      parent_page->SetKeyAt(1,sibling_internal_page->KeyAt(0));
    }else{ // sibling_internal_page为internal_page左兄弟
      sibling_internal_page->MoveLastToFront(internal_page,parent_page->KeyAt(index),buffer_pool_manager_);
      parent_page->SetKeyAt(index,internal_page->KeyAt(0));
    }
  }
}

// 合并节点（这个函数的返回值是判断parent_page是否需要删除）
INDEX_TEMPLATE_ARGUMENTS
template<typename Node>
auto BPLUSTREE_TYPE::Coalesce(Node** sibling_node,Node** node,InternalPage** parent_page,int index,Transaction *transaction) -> bool{ // index为node在parent_page中的下标
  // 1.实现两个页的交换，确保sibling_node为node的左兄弟，这样合并的时候直接将node全部向前移动到sibling_node中
  auto parent_key_index = index;
  if(index == 0){
    parent_key_index = 1;
    std::swap(node,sibling_node); // 此时sibling_node为node的右兄弟，需要交换，这里传的是二级指针，交换的是一级指针变量本身的值（todo ? 个人认为这里一级指针也是可以的）
  }

  // 2.第一种情况：叶子页
  if((*node)->IsLeafPage()){
    // 2.1.强制类型转换为叶子页
    auto leaf_page = reinterpret_cast<LeafPage*>(*node);
    auto left_sibling_leaf_page = reinterpret_cast<LeafPage*>(*sibling_node);

    // 2.2.合并
    leaf_page->MoveAllTo(left_sibling_leaf_page);

    // 2.3.重新设置left_sibling_leaf_page的next_Page_id
    left_sibling_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  }else{ // 2.第二种情况：内部页
    // 2.1.强制类型转换为内部页
    auto internal_page = reinterpret_cast<InternalPage*>(*node);
    auto left_sibling_internal_page = reinterpret_cast<InternalPage*>(*sibling_node);

    // 2.2.合并
    internal_page->MoveAllTo(left_sibling_internal_page,(*parent_page)->KeyAt(parent_key_index),buffer_pool_manager_);
  }

  // 3.删除父亲页中的parent_key_index位置的key:value
  (*parent_page)->Remove(parent_key_index);

  return CoalesceOrRedistribute(*parent_page, transaction); // 再一次判断是否需要合并或者重新分配
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // 1.root_latch_加读锁
  root_latch_.RLock();

  // 2.如果树为空，直接返回End()
  if(IsEmpty()){
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE();
  }

  // 3.按照查找的方式（乐观锁的方式）获取最左侧的叶子页
  auto first_leaf_page =  GetLeftMostLeafPage();
  return INDEXITERATOR_TYPE(buffer_pool_manager_,first_leaf_page->GetPageId(),first_leaf_page,0); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // 1.root_latch_加读锁
  root_latch_.RLock();

  // 2.如果树为空，直接返回End()
  if(IsEmpty()){
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE();
  }

  // 3.获取对应的叶子页和key在叶子页中的位置
  Page* page = GetLeafPage(key,nullptr,Operation::SEARCH,true);
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  auto index = leaf_page->KeyIndex(key,comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_,page->GetPageId(),page,index); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // 1.root_latch_加读锁
  root_latch_.RLock();

  // 2.如果树为空，直接返回End()
  if(IsEmpty()){
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE();
  }

  // 3.获取对应的叶子页和key在叶子页中的位置
  Page* page = GetRightMostLeafPage();
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  auto index = leaf_page->GetSize();
  return INDEXITERATOR_TYPE(buffer_pool_manager_,page->GetPageId(),page,index); 
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  return root_page_id_; 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
