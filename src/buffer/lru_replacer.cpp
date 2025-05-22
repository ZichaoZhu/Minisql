#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : capacity(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (map_.empty()) {
    return false;
  } else {
    *frame_id = list_.back();
    list_.pop_back();
    map_.erase(*frame_id);
    return true;
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if (map_.find(frame_id) == map_.end()) {
    return;
  }
  auto iter = map_[frame_id];
  map_.erase(frame_id);
  list_.erase(iter);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (map_.find(frame_id) != map_.end()) {
    return;
  }
  if (map_.size() == capacity) {
    frame_id_t delete_frame_id = list_.back();
    list_.pop_back();
    map_.erase(delete_frame_id);
  }
  list_.push_front(frame_id);
  map_[frame_id] = list_.begin();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() { return map_.size(); }
