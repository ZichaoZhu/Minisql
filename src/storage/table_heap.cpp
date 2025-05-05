#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // 首先判断当前的row是否过大
  if (row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW) {
    return false;
  }
  // 获得第一个page
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  if (page == nullptr) {
    return false;
  }
  page->WLatch();
  while (!page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {  // 若是无法插入
    page->WUnlatch(); // 解锁
    auto next_page_id = page->GetNextPageId();  // 获取下一个page_id
    // 若next_page_id恰好为INVALID_PAGE_ID，则说明当前的page已经是最后一个page了，而且无法插入，此时我们需要新建一个page
    if (next_page_id == INVALID_PAGE_ID) {
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if (new_page == nullptr) {
        return false;
      }
      // 初始化新页
      new_page->Init(next_page_id, page->GetTablePageId(), log_manager_, txn);
      new_page->SetNextPageId(INVALID_PAGE_ID);
      // 更新前面的页
      page->SetNextPageId(next_page_id);
      // unpin，同时由于修改了page，所以需要将其标记为dirty
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      page = new_page;
    }
    // 若不相等，则不为最后一个page
    else {
      auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      page = next_page;
    }
    // 这里需要加锁
    page->WLatch();
  }
  // 插入成功，解锁
  page->WUnlatch();
  // 这里需要unpin
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  // 找到要替换的page
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  // 获得旧的row
  Row old_row(rid);
  bool get_old_row = page->GetTuple(&old_row, schema_, txn, lock_manager_);
  if (!get_old_row) {
    return false;
  }
  // 先获取锁
  page->WLatch();
  // 更新tuple
  int update_res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  // 根据返回结果
  switch (update_res) {
    // 更新成功
    case 1: {
      row.SetRowId(rid);
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }
    // the slot number 或者是 tuple deleted
    case -1:
    case -2: {
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return false;
    }
    // not enough space
    case -3: {
      page->WLatch();
      // 先删除
      ApplyDelete(rid, txn);
      if (!InsertTuple(row, txn)) {
        return false;
      }
      else
        return true;
    }
  }

}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { return false; }

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { return TableIterator(nullptr, RowId(), nullptr); }

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(nullptr, RowId(), nullptr); }
