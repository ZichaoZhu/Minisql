#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ == GetMaxSupportedSize()) {
    // is full
    return false;
  } else {
    // update bitmap
    uint32_t byte_index = next_free_page_ / 8;
    uint8_t bit_index = next_free_page_ % 8;
    uint8_t bit = 1 << bit_index;
    page_offset = next_free_page_;
    // update byte, update the bit to 1
    bytes[byte_index] = bytes[byte_index] | bit;

    // page_allocated
    page_allocated_++;
    // update the next_free_page
    if (page_allocated_ == GetMaxSupportedSize()) {
      next_free_page_ = GetMaxSupportedSize();
      return true;
    } else {
      for (uint32_t i = next_free_page_ + 1; i < GetMaxSupportedSize(); i++) {
        if (IsPageFree(i)) {
          next_free_page_ = i;
          return true;
        }
      }
      next_free_page_ = GetMaxSupportedSize();
      return true;
    }
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  } else if (IsPageFree(page_offset)) {
    return false;
  } else {
    // update page_allocated
    page_allocated_--;
    // update next_free_page
    if (page_offset < next_free_page_) {
      next_free_page_ = page_offset;
    }
    // update bitmap
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    uint8_t bit = 1 << bit_index;
    // update the bit to 0
    bytes[byte_index] = bytes[byte_index] & (~bit);
    return true;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset < GetMaxSupportedSize()) {
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    return IsPageFreeLow(byte_index, bit_index);
  } else {
    return false;
  }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (byte_index < MAX_CHARS) {
    uint8_t byte = bytes[byte_index];
    uint8_t bit = 1 << bit_index;
    if ((byte & bit) == 0) {
      // the bit = 0, is free
      return true;
    } else {
      // not free
      return false;
    }
  } else {
    return false;
  }
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
