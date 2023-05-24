//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include "common/config.h"
#include "common/logger.h"
#include <iostream>
#include <fstream>

namespace bustub {

void OutPutFile(const frame_id_t &frame_id,const std::list<frame_id_t> &cache_list,
                    const std::unordered_map<frame_id_t, LRUKReplacer::FrameEntry> &entries){
    std::ofstream output_file;
    output_file.open("../../output.txt",std::ios::app);

    if(!output_file){
        std::cerr << "Failed to open file!" << std::endl;
        return; 
    }

    // 将数据写入到文件中
    output_file << "frame_id = " << frame_id << std::endl;
    for(const auto &cache : cache_list){
        output_file << "cache = " << cache << std::endl;
    }
    for(const auto &entry : entries){
        output_file << "frame_id = " << entry.first << "pos = " << *entry.second.pos_ << std::endl; 
    }

    output_file.close();
}

void OutPutFile(){
    std::ofstream output_file;
    output_file.open("../../output.txt",std::ios::app);

    if(!output_file){
        std::cerr << "Failed to open file!" << std::endl;
        return; 
    }

    output_file << "1" << std::endl;

    output_file.close();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::scoped_lock<std::mutex> lock(latch_);
    bool is_find = false;

    // 先从hist_list_中查找，如果可以剔除的，需要再从cache_list_中寻找
    if(!hist_list_.empty() && !is_find){
        for(auto rit = hist_list_.rbegin();rit != hist_list_.rend();rit++){ // 添加的时候是添加到队头，删除从队尾开始删除
            if(entries_[*rit].evictable_){
                *frame_id = *rit;
                hist_list_.erase(std::next(rit).base());
                is_find = true;
                break;
            }
        }
    }
    
    if(!cache_list_.empty() && !is_find){
        for(auto rit = cache_list_.rbegin();rit != cache_list_.rend();rit++){ // 添加的时候是添加到队头，删除从队尾开始删除
            if(entries_[*rit].evictable_){
                *frame_id = *rit;
                cache_list_.erase(std::next(rit).base());
                is_find = true;
                break;
            }
        }
    }

    if(is_find){
        entries_.erase(*frame_id);
        curr_size_--;
        return true;
    }

    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if(frame_id > static_cast<int32_t>(replacer_size_)){  // 如果frame_id要比replace_size_的要大的话，说明frame_id是不合理的
        throw std::invalid_argument(std::string("Invalild frame_id ") + std::to_string(frame_id));
    }

    size_t count = ++entries_[frame_id].hit_count_;
    if(count == 1){ // 如果没有找到说明是新添加的frame
        curr_size_++; 
        hist_list_.push_front(frame_id);
        entries_[frame_id].pos_ = hist_list_.begin();
    }else{ // 如果找到了说明已经存在了，需要判断它是在哪个list里面
        if(count == k_){ // 说明frame_id在hist_list_中
            hist_list_.erase(entries_[frame_id].pos_);
            cache_list_.push_front(frame_id);
            entries_[frame_id].pos_ = cache_list_.begin();
        }else if (count > k_) { // 说明frame_id在cache_list_中
            // OutPutFile(frame_id,cache_list_,entries_);
            // OutPutFile();
            cache_list_.erase(entries_[frame_id].pos_);
            cache_list_.push_front(frame_id);
            entries_[frame_id].pos_ = cache_list_.begin();
        }
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    if(frame_id > static_cast<int32_t>(replacer_size_)){ // 判断frame_id是否合理
        throw std::invalid_argument(std::string("Invalid frame_id ") + std::to_string(frame_id));
    }

    if(entries_.find(frame_id) == entries_.end()){
         return;
    }

    if(!entries_[frame_id].evictable_ && set_evictable){ // 如果从false到true，需要加一
        curr_size_++;
    }else if(entries_[frame_id].evictable_ && !set_evictable){ // 如果从true到false，需要减一
        curr_size_--;
    }

    entries_[frame_id].evictable_ = set_evictable;    
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if(frame_id > static_cast<int32_t>(replacer_size_)){ // 判断frame_id是否合理
        throw std::invalid_argument(std::string("Invalid frame_id ") + std::to_string(frame_id));
    }

    if(entries_.find(frame_id) != entries_.end()){
        if(!entries_[frame_id].evictable_){
            throw std::logic_error(std::string("Can't remove an inevcitable frame ") + std::to_string(frame_id));
        }

        if(entries_[frame_id].hit_count_ < k_){
            hist_list_.erase(entries_[frame_id].pos_);
        }else{
            cache_list_.erase(entries_[frame_id].pos_);
        }
        curr_size_--;
        entries_.erase(frame_id);
    }
}

auto LRUKReplacer::Size() -> size_t {
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_; 
}

}  // namespace bustub
