//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <stack>

#include "common/exception.h"
#include "common/rwlatch.h"
#include "type/type_id.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode { // 非终结点
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) { // explicit修饰类的构造函数时，在进行使用的时候必须显示转换，不能隐式转换
    this->key_char_ = key_char;
    this->is_end_ = false;
    this->children_.clear();
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
   // 这里需要注意构造函数的参数是引用类型，因此不能使用的拷贝构造函数
   // noexcept用于设定该函数不能够抛出异常，由于该构造函数使用的是引用参数，
   // 意味着是移动构造函数，需要使用noexcept进行修饰，目的在于进行参数移动时是将某个数据对象的所有权进行转移，
   // 如果抛出了异常，表明源对象与目标对象的一致性被破坏，从而终止程序，而不是恢复运行
  TrieNode(TrieNode &&other_trie_node) noexcept { // 主要用于终结点调用父类构造函数时进行初始化使用
    this->key_char_ = other_trie_node.key_char_;
    this->is_end_ = other_trie_node.is_end_;
    this->children_.swap(other_trie_node.children_);
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  auto HasChild(char key_char) const->bool {  // 判断有没有key_char对应的子节点
    return this->children_.find(key_char) != this->children_.end();  // 只要通过key_char找到的值不是尾元素的后面一个，就认为不是空
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  auto HasChildren() const->bool { // 判断是否还有孩子节点
    return !this->children_.empty(); 
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  auto IsEndNode() const -> bool { // 判断是否是终结点
    return this->is_end_; 
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  auto GetKeyChar() const -> char { // 获取节点的key_char
    return this->key_char_; 
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  auto InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) -> std::unique_ptr<TrieNode> * { // 将key_char对应的子节点插入到当前节点中
    // 1、需要判断这个key_char是否在this中已经存在了，如果存在了，则说明已经添加过了
    // 2、需要判断key_char是否和child中存放的key_char一样
    if(key_char != child->GetKeyChar() || this->HasChild(key_char)){ 
      return nullptr;
    }
      
    this->children_[key_char] = std::move(child);
    return &this->children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  auto GetChildNode(char key_char)->std::unique_ptr<TrieNode> * { // 根据key_char从当前节点中获取对应的子节点，返回unique_ptr类型的指针
    if(!this->HasChild(key_char)){
      return nullptr;
    }
    
    return &this->children_[key_char];
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) { // 根据key_char从当前节点中删除对应的子节点
    if(this->HasChild(key_char)){
      this->children_.erase(key_char);
    }       
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { // 设置当前节点的类型：非终结点/终结点
    this->is_end_ = is_end;
  }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that mark the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode { // 继承非终结点，表示终结点
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value):TrieNode(std::move(trieNode)) {// 这里不能使用拷贝构造函数，因为在父类中使用的是引用
    this->value_ = value;
    this->SetEndNode(true); // 要注意将TrieNode设置成终结点
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) {
    this->TrieNode(key_char);
    this->value_ = value;
    this->SetEndNode(true);
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  auto GetValue() const->T { 
    return this->value_; 
  }
};

/**
 * Trie is a concurrent key-value store. Each key is string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie(){
    //auto root = new TrieNode('\0');
    //this->root_.reset(root);
    this->root_ = std::make_unique<TrieNode>('\0'); // 构造一个key_char为'\0'的TrieNode智能指针
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If key is empty string, return false immediately.
   *
   * If key alreadys exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and return false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if key already exists
   */
  template <typename T>
  auto Insert(const std::string &key, T value)->bool { // 插入节点
    if(key.empty()){  // 如果字符串为空，那么直接返回false
      return false;
    }

    this->latch_.WLock(); // 对这段内容加上写锁
    auto key_char = key.begin();
    std::unique_ptr<TrieNode> *node = &this->root_;
    while(key_char != key.end()){  // 只处理非终结点
      if((key_char + 1) == key.end()){ // 判断key_char是否到了是最后一个字符，如果是则直接退出循环
        break;
      }

      if(node->get()->HasChild(*key_char)){ // 如果存在这个key_char，则直接获取对应的节点，不做任何处理
        node = node->get()->GetChildNode(*key_char);
      }else{  // 如果不存在则，则需要添加对应的非终结点
        node = node->get()->InsertChildNode(*key_char, std::make_unique<TrieNode>(*key_char));
      }

      key_char++;
    }

    if(node->get()->HasChild(*key_char)){ // 判断key中的最后一个字符是否已经存在
      node = node->get()->GetChildNode(*key_char);
      if(node->get()->IsEndNode()){ // 如果存在需要判断该节点是否是终结点，如果是，直接return false，如果不是需要重新设置成终结点
        this->latch_.WUnlock();
        return false;
      }      
    }else{  // 如果key_char不存在需要添加一个非终结点，然后后续做转换
      node = node->get()->InsertChildNode(*key_char, std::make_unique<TrieNode>(*key_char));
    }

    auto node_with_value = new TrieNodeWithValue<T>(std::move(**node),value);  // **node是引用的引用，*node是指针所指对象的裸指针，*node.get()是指针所指对象的值
    node->reset(node_with_value); 
    this->latch_.WUnlock();
    
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and is not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find correct node
   * @return True if key exists and is removed, false otherwise
   */
  auto Remove(const std::string &key)->bool { // 删除节点
    if(key.empty()){
      return false;
    }

    this->latch_.WLock();
    auto key_char = key.begin();
    std::unique_ptr<TrieNode>* node = &this->root_;
    std::stack<std::unique_ptr<TrieNode>*> stack_node;  // 用于存放在从根部节点遍历到终结点过程中所有访问到的节点，用于后续的删除操作，不会存放终结点
    while(key_char != key.end()){
      if(node->get()->HasChild(*key_char)){ // 如果当前节点有这个key_char的子节点，那么就将该节点添加到stack_node中，并获取子节点
        stack_node.push(node);
        node = node->get()->GetChildNode(*key_char); 
      }else{  // 如果不满足则直接return false
        this->latch_.WUnlock();
        return false;
      }
      
      key_char++;
    }

    key_char--; // 回退到key中的最后一个字符
    while(!stack_node.empty()){ // 如果stack_node不为空
      std::unique_ptr<TrieNode>* node = stack_node.top(); // 获取栈顶元素
      stack_node.pop();
      auto child_node = node->get()->GetChildNode(*key_char); // 根据key_char获取node节点的孩子节点
      key_char--;
      if(child_node != nullptr && child_node->get()->HasChildren()){ // 如果孩子节点不为空，且孩子节点的也有孩子节点，那么不做删除操作
        continue;
      }

      node->get()->RemoveChildNode(*key_char); // 将孩子节点child_node从node节点中删除
    }

    this->latch_.WUnlock();

    return true; 
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  auto GetValue(const std::string &key, bool *success)->T { // 根据key获取对应的value
    if(key.empty()){
      *success = false;
      return {};
    }

    this->latch_.RLock();
    auto key_char = key.begin();
    std::unique_ptr<TrieNode>* node = &this->root_;
    while(key_char != key.end()){
      if(node->get()->HasChild(*key_char)){
        node = node->get()->GetChildNode(*key_char);
      }else{
        this->latch_.RUnlock();
        *success = false;
        return {};
      }

      key_char++;
    }

    auto node_with_value = dynamic_cast<TrieNodeWithValue<T> *>(node->get());
    if(node_with_value != nullptr){
      this->latch_.RUnlock();
      *success = true;
      return node_with_value->GetValue();
    }      
    
    this->latch_.RUnlock();
    *success = false;
    return {};
  }
};
}  // namespace bustub
