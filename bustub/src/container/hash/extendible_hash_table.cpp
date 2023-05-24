//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "type/value.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> bucket = std::make_shared<ExtendibleHashTable<K, V>::Bucket>(bucket_size_,global_depth_);
      dir_.emplace_back(bucket);
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;  // 需要注意的是这里比较序列的时候是从后往前进行比较的
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  size_t index = IndexOf(key);
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[index]->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t index = IndexOf(key);
  std::scoped_lock<std::mutex> lock(latch_);
  std::shared_ptr<Bucket>& bucket = dir_[index];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  // 如果key存在那么直接进行更新
  std::shared_ptr<Bucket>& bucket = dir_[index];
  V val;
  if(bucket->Find(key,val)){ // 如果插入成功说明要么key已经存在了，要么桶不是满的
    bucket->Insert(key,value);
    return;
  }

  // 如果失败了，说明桶已经满了，需要进行分裂（这里需要注意的是，多次分裂是可能发生的，因为就算分裂了之后也有可能会存在向满的那个桶进行插入操作）
  // 第一种情况：localDepth == globalDepth，需要增加globalDepth和localDepth，然后桶的数量也需要加1
  // 第二种情况：localDepth < globalDepth，只需要增加localDepth和桶的数量即可
  while(dir_[index].get()->IsFull()){
    int local_depth = GetLocalDepthInternal(index);
    if(local_depth == global_depth_){ // 如果localDepth与globalDepth相同时，意味着需要增加指针数
      size_t size = dir_.size();
      dir_.reserve(size*2);  // 设置dir_的空闲大小，将dir_扩展两倍
      // 这里要注意的是比较序列是从后往前比较的，所以将原有的内容复制一份到扩展的空间位置之后就已经实现了指针的调整（画图理解可能比较直观）
      std::copy_n(dir_.begin(),size,std::back_inserter(dir_)); // 从dir_起始位置开始复制，复制size大小的内容，复制到dir_的第一个空闲位置，依次添加
      global_depth_++;
    }

    // 增加桶的个数
    std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> new_bucket_0 = std::make_shared<ExtendibleHashTable<K, V>::Bucket>(bucket_size_,local_depth + 1);
    std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> new_bucket_1 = std::make_shared<ExtendibleHashTable<K, V>::Bucket>(bucket_size_,local_depth + 1);
    int local_mask = 1 << local_depth; // 在桶没有拆分之前比较的是后local_depth个比特位，现在后local_depth个比特位都是相同的，只需要比较倒数第local_depth+1的比特位就可以将桶拆分开来
    for(auto &[k,v] : dir_[index].get()->GetItems()){
      if(static_cast<bool>(std::hash<K>()(k) & local_mask)){ // 如果倒数第local_depth个比特位是1，就添加到桶1中
        new_bucket_1->Insert(k,v);
      }else{                              // 如果倒数第local_depth个比特位是0，就添加到桶0中
        new_bucket_0->Insert(k,v);
      }
    }

    for(size_t i = std::hash<K>()(key) & (local_mask - 1);i < dir_.size();i += local_mask){ // i每次增加的偏移量可以通过比较两个对应序列的差值算出来的，正好是local_mask大小
      if(static_cast<bool>(i & local_mask)){ // 如果i的倒数第local_depth+1的比特位是1,就指向桶1
        dir_[i] = new_bucket_1;
      }else{              // 如果i的倒数第local_depth+1的比特位是0,就指向桶0
        dir_[i] = new_bucket_0;
      }
    }

    index = IndexOf(key); // 调整之后需要重新获取index
  }

  std::shared_ptr<Bucket>& bucket_least = dir_[index];
  bucket_least->Insert(key,value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(const std::pair<K, V>& pair_bucket : list_){
    auto pair_key = pair_bucket.first;
    if(pair_key == key){
      value = pair_bucket.second;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for(auto it = list_.begin(); it != list_.end();it++){
    if(it->first == key){
      list_.erase(it);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for(auto it = list_.begin();it != list_.end();it++){
    if(it->first == key){
      it->second = value;
      return true;
    }
  }

  if(!IsFull()){
    list_.emplace_back(key,value);
    return true;
  }

  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
