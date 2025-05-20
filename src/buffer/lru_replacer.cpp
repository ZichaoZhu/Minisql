#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : capacity(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.empty()) {
    return false;
  } else {
    *frame_id = lru_list_[0];
    lru_list_.erase(lru_list_.begin());
    return true;
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  for (size_t i = 0; i < lru_list_.size(); i++) {
    if (lru_list_[i] == frame_id) {
      lru_list_.erase(lru_list_.begin() + i);
    }
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_list_.size() < capacity) {
    for (size_t i = 0; i < lru_list_.size(); i++) {
      if (lru_list_[i] == frame_id) {
        return;
      }
    }
    lru_list_.push_back(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() { return lru_list_.size(); }
