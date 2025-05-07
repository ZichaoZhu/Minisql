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
    page->WUnlatch();                                                           // 解锁
    auto next_page_id = page->GetNextPageId();                                  // 获取下一个page_id
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
  // 先获取锁
  page->WLatch();
  // 更新tuple
  int update_res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  // 根据返回结果
  if (update_res == 1) {
    row.SetRowId(rid);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }
  else {
    int res = -update_res;
    string statement[] = {"slot number is invalid", "tuple is deleted", "not enough space to update"};
    cout << "UpdateTuple error " << res << " : " << statement[res - 1] << endl;
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr, "Can not find the page, invalid rid.");
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  // Step3: Unpin the page.
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
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
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  // step 1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    return false;
  }
  // step 2: Get the tuple from the page.
  page->RLatch();
  bool get_tuple = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  if (!get_tuple) {
    return false;
  } else
    return true;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

void TableHeap::FreeHeap() {
  // 获取当前页的 ID
  page_id_t current_page_id = first_page_id_;

  // 遍历所有页并释放
  while (current_page_id != INVALID_PAGE_ID) {
    // 获取当前页
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (page == nullptr) {
      break;
    }

    // 获取下一页的 ID
    page_id_t next_page_id = page->GetNextPageId();

    // 释放当前页
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    buffer_pool_manager_->DeletePage(current_page_id);

    // 移动到下一页
    current_page_id = next_page_id;
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  // step 1: 获取第一页
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  if (page == nullptr) {
    buffer_pool_manager_->UnpinPage(GetFirstPageId(), false);
    return TableIterator(nullptr, RowId(), nullptr);
  }
  // step 2: 获取第一个tuple
  RowId rid;
  page->RLatch();
  bool get_first_tuple = page->GetFirstTupleRid(&rid);
  page->RUnlatch();
  if (!get_first_tuple) {
    buffer_pool_manager_->UnpinPage(GetFirstPageId(), false);
    return TableIterator(nullptr, RowId(), nullptr);
  }
  // step 3: 返回迭代器
  buffer_pool_manager_->UnpinPage(GetFirstPageId(), false);
  return TableIterator(this, rid, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  // end返回的是指向容器最后一个元素的下一个位置的迭代器
  return TableIterator(this, RowId(INVALID_ROWID), nullptr);
}