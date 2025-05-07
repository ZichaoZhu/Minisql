#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  table_heap_ = table_heap;
  rid_ = RowId(rid);
  txn_ = txn;
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  rid_ = other.rid_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  table_heap_ = nullptr;
  rid_ = RowId();
  txn_ = nullptr;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return rid_ == itr.rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  // step 1: 获得当前的page
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  if (page == nullptr) {
    return Row();
  }
  // step 2: 获得当前的row
  Row *row = new Row(rid_);
  page->RLatch();
  bool get_tuple = page->GetTuple(row, table_heap_->schema_, txn_, table_heap_->lock_manager_);
  page->RUnlatch();
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  if (!get_tuple) {
    delete row;
    return Row();
  }
  return *row;
}

Row *TableIterator::operator->() {
  // step 1: 获取当前page
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  if (page == nullptr) {
    return nullptr;
  }
  // step 2: 获取当前row
  Row *row = new Row(rid_);
  page->RLatch();
  bool get_tuple = page->GetTuple(row, table_heap_->schema_, txn_, table_heap_->lock_manager_);
  page->RUnlatch();
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  if (!get_tuple) {
    delete row;
    return nullptr;
  }
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_ = itr.table_heap_;
  rid_ = itr.rid_;
  txn_ = new Txn();
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // step 1: 获取现在的page
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  if (page == nullptr) {
    table_heap_ = nullptr;
    rid_ = RowId();
    txn_ = nullptr;
    return *this;
  }
  RowId next_rid;
  bool get_tuple = page->GetNextTupleRid(rid_, &next_rid);
  // 检测能不能找到next，找不到则去下一张page找
  if (get_tuple) {
    rid_ = next_rid;
  } else {  // step 2: 找next_page
    // 检测next_page是不是空的
    if (page->GetNextPageId() >= 0) {
      auto next_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      if (page->GetFirstTupleRid(&next_rid)) {
        rid_ = next_rid;
      } else {
        table_heap_ = nullptr;
        rid_ = RowId();
        txn_ = nullptr;
      }
      table_heap_->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
    } else {
      table_heap_ = nullptr;
      rid_ = RowId();
      txn_ = nullptr;
    }
  }
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator tmp(*this);
  ++*this;
  // 显式调用，返回临时变量，解决生命周期的问题
  return TableIterator(tmp);
}
