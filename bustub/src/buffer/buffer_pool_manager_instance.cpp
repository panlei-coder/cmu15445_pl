//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

// 缓存池的构造函数
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) { // 初始化的时候pages_中没有存放任何页
    free_list_.push_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

// 缓存池的析构函数
BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

// 从缓存池中获取存放一个新创建的页或者是从磁盘中读取的页的位置
auto BufferPoolManagerInstance::GetAvailableFrame(frame_id_t *out_frame_id)->bool{
  // 1.先判断free_list_是否为空，如果不为空，则从中获取一个frame_id
  if(!free_list_.empty()){ 
    *out_frame_id = free_list_.front();  // 从队头取出一个
    free_list_.pop_front();
    return true;
  }

  // 2.如果free_list_已经为空了，那么只能先从缓存池中剔除一个，然后再返回out_frame_id，
  // 但是如果所有页都不能剔除的话，那么只能返回false
  if(replacer_->Evict(out_frame_id)){
    // 2.1.如果剔除成功了，需要判断是否是脏页，脏页需要刷到磁盘中去
    if(pages_[*out_frame_id].is_dirty_){
      disk_manager_->WritePage(pages_[*out_frame_id].page_id_, pages_[*out_frame_id].GetData());
    }

    // 2.2.需要将page_id_：frame_id_键值对从page_table_中删除
    page_table_->Remove(pages_[*out_frame_id].page_id_); 
    return true;
  }


  // 3.如果没有可以剔除的页，则直接返回false
  return false;
}

// 创建一个新的页，但是如果缓存池中没有存放新页的位置，会创建失败
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 1.先加锁，注意直接加的锁是互斥锁，在调用GetAvailableFrame时不能够在这里函数里面继续加锁，不然会导致死锁
  std::scoped_lock<std::mutex> lock(latch_);

  // 2.先判断缓存池中是否有存放新的页的空间,如果有的话
  frame_id_t frame_id;
  if(GetAvailableFrame(&frame_id)){
    // 2.1.获取下一个要分配的页的页号
    *page_id = AllocatePage();

    // 2.2.因为页的空间在一开始创建缓存池的时候就分配好了，因此只需要修改相关的信息就可以了，但是一定要将data进行清空
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].ResetMemory();

    // 2.3.需要将页的信息添加到映射表中，并添加访问记录和设置不可以被剔除掉
    page_table_->Insert(*page_id, frame_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  // 3.如果没有的话，直接返回nullptr
  return nullptr; 
}

// 根据页号从缓存池中获取页
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.先加锁，注意直接加的锁是互斥锁，在调用GetAvailableFrame时不能够在这里函数里面继续加锁，不然会导致死锁
  std::scoped_lock<std::mutex> lock(latch_);

  // 2.在缓存池中找到需要获取的页，如果则直接获取
  frame_id_t frame_id;
  if(page_table_->Find(page_id, frame_id)){
    // 2.1.将盯住的计数加1
    pages_[frame_id].pin_count_++;
    
    // 2.2.添加访问记录并设置不可以被剔除
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  // 3.缓存池中没有，需要从磁盘中读，先要判断是否有可以替换的页，如果有可以替换的页
  if(GetAvailableFrame(&frame_id)){
    // 3.1.重新设置页的信息，然后将data清空并从磁盘中读取页的信息
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

    // 3.2.添加到映射表中、添加访问记录、设置成不可以被剔除
    page_table_->Insert(page_id, frame_id); 
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  // 4.如果没有，则直接返回false
  return nullptr; 
}

// 根据页号将页unpin
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // 1.先加锁，注意直接加的锁是互斥锁，在调用GetAvailableFrame时不能够在这里函数里面继续加锁，不然会导致死锁
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;

  // 2.查找有没有这个页，没有直接不做任何处理
  if(!page_table_->Find(page_id, frame_id)){
    return false;
  }

  // 3.如果有这个页的话，判断is_dirty是true的话，意味着需要将该页设置成脏页，等到被剔除的时候需要刷到磁盘中去
  if(is_dirty){
    pages_[frame_id].is_dirty_ = true;
  }

  // 4.如果页的pin_count_不为0的话，说明需要进行unpin的操作，否则直接返回false
  if(pages_[frame_id].pin_count_ != 0){
    // 4.1.先将pin_count_减1
    pages_[frame_id].pin_count_--;

    // 4.2.判断pin_count_是否已经等于0了，只有在做减1之后pin_count为0的时候才能够被释放掉，设置其可以被剔除的属性
    if(pages_[frame_id].pin_count_ == 0){
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }

  // 5.如果页的pin_count_不为0,直接返回false
  return false; 
}

// 根据页号将对应的页刷到磁盘中
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // 1.先加锁，注意直接加的锁是互斥锁，在调用GetAvailableFrame时不能够在这里函数里面继续加锁，不然会导致死锁
  std::scoped_lock<std::mutex> lock(latch_);

  // 2.找到这个页，如果这个页存在的话，不管其是不是脏页，都直接刷到磁盘去(刷到磁盘之后，pages_对应的is_dirty_需要设置成false)
  frame_id_t frame_id;
  if(page_table_->Find(page_id, frame_id)){ 
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  
  // 3.找不到这个页，直接返回false
  return false; 
}

// 刷缓存池中所有的页
void BufferPoolManagerInstance::FlushAllPgsImp() { 
  // 注意这里调用的是FlushPgImp函数，里面已经加锁了，这里不能够再加锁了，否则会直接死锁，但是这里会存在对pool_size_访问不一致的情况出现
  // for(size_t i = 0;i < pool_size_;i++){
  //   FlushPgImp(pages_[i].page_id_);
  // }

  // 1.先加锁，注意直接加的锁是互斥锁，在调用GetAvailableFrame时不能够在这里函数里面继续加锁，不然会导致死锁
  std::scoped_lock<std::mutex> lock(latch_);

  // 2.循环遍历缓存池中所有的页
  frame_id_t frame_id;
  page_id_t page_id;
  for(size_t i = 0;i < pool_size_;i++){
    page_id = pages_[i].page_id_;
    if(page_table_->Find(page_id, frame_id)){
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
  }

}

// 将某个页从缓存池中删除，因为缓存池中存放页的存储空间是固定的，只需要将这个页的相关信息清除掉即可
// （但要判断当前的页是否被盯住了，如果被盯住了，那么是不可以被删除掉的）
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 1.先加锁，注意直接加的锁是互斥锁，在调用GetAvailableFrame时不能够在这里函数里面继续加锁，不然会导致死锁
  std::scoped_lock<std::mutex> lock(latch_);

  // 2.找到这个页，如果没有这个页，则直接返回
  frame_id_t frame_id;
  if(!page_table_->Find(page_id, frame_id)){ // 如果没有找到，则直接返回
    return true;
  }

  // 3.如果找到了这个页，需要判断pin_count_是否大于0,如果是那么不能被删
  if(pages_[frame_id].pin_count_ > 0){ // 被盯住了，不能够被删除
    return false;
  }

  // 4.如果能够被删除，
  // a.需要将page_table_/replacer_/free_list中的相关信息都修改掉
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  // b.重新设置pages_的相关信息
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  DeallocatePage(page_id);

  return true; 
}

// 返回下一个即将被分配的页号，页号是持续不断增长的
auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  // 这里不能够加锁，因为这个函数一般都是被那些已经加锁了的函数调用，如果这里加锁，就直接导致死锁了
  return next_page_id_++; 
}

}  // namespace bustub
