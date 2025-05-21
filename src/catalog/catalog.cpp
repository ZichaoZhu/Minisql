#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return 4 + 4 + 4 + table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  Page *page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  // init
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    FlushCatalogMetaPage();
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, true);
  } else {
    // load
    catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData());
    for (auto &iter : catalog_meta_->table_meta_pages_) {
      LoadTable(iter.first, iter.second);
    }
    for (auto &iter : catalog_meta_->index_meta_pages_) {
      LoadIndex(iter.first, iter.second);
    }
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  table_id_t table_id = catalog_meta_->GetNextTableId();
  page_id_t page_id;

  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }
  // table meta
  Schema *table_schema = Schema::DeepCopySchema(schema);
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, INVALID_PAGE_ID, table_schema);
  table_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);

  // table heap info
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  // catalog
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  table_info = nullptr;
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_.at(table_name);
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_.at(table_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto &iter : tables_) {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  index_id_t index_id = catalog_meta_->GetNextIndexId();
  table_id_t table_id = table_names_[table_name];
  std::vector<uint32_t> key_map;
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  // key map
  TableInfo *table_info = tables_[table_id];
  Schema *table_schema = table_info->GetSchema();
  for (auto &iter : index_keys) {
    uint32_t key_id;
    dberr_t dberr = table_schema->GetColumnIndex(iter, key_id);
    if (dberr != DB_SUCCESS) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(key_id);
  }

  // index meta, info
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page_id == INVALID_PAGE_ID) {
    return DB_FAILED;
  }
  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map);
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  index_meta->SerializeTo(page->GetData());
  if (buffer_pool_manager_->UnpinPage(page_id, true) == false) {
    delete index_info;
    return DB_FAILED;
  }

  // catalog
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  index_info = nullptr;
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = index_names_.at(table_name).at(index_name);
  if (indexes_.find(index_id) == indexes_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_info = indexes_.at(index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  std::unordered_map<std::string, index_id_t> table_indexes = index_names_.at(table_name);
  for (auto &iter : table_indexes) {
    index_id_t index_id = iter.second;
    indexes.push_back(indexes_.at(index_id));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  return DropTable(table_id);
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  if (index_names_[table_name].find(index_name) == index_names_[table_name].end()) {
    return DB_INDEX_NOT_FOUND;
  }

  index_id_t index_id = index_names_[table_name][index_name];
  // catalog meta
  catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id);
  // catalog management
  IndexInfo *index_info = indexes_[index_id];
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  delete index_info;

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(table_id_t table_id) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  TableInfo *table_info = tables_[table_id];
  string table_name = table_info->GetTableName();
  // delete index
  std::unordered_map<std::string, index_id_t> table_indexes = index_names_[table_name];
  for (auto &iter : table_indexes) {
    DropIndex(table_name, iter.first);
  }
  index_names_.erase(table_name);
  // delete table
  // free table heap
  TableHeap *table_heap = table_info->GetTableHeap();
  table_heap->FreeTableHeap();
  // catalog meta
  page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
  buffer_pool_manager_->DeletePage(page_id);
  catalog_meta_->table_meta_pages_.erase(table_id);
  table_names_.erase(table_name);
  tables_.erase(table_id);
  // table info
  delete table_info;

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // table meta
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_meta = nullptr;
  TableMetadata::DeserializeFrom(page->GetData(), table_meta);
  // table heap
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                            log_manager_, lock_manager_);
  // table info
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  // catalog management
  table_names_[table_meta->GetTableName()] = table_id;
  tables_[table_id] = table_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // index meta
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_meta = nullptr;
  IndexMetadata::DeserializeFrom(page->GetData(), index_meta);
  table_id_t table_id = index_meta->GetTableId();
  // table not exist
  if (tables_.find(table_id) == tables_.end()) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_TABLE_NOT_EXIST;
  }
  // index info
  TableInfo *table_info = tables_[table_id];
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  // catalog management
  index_names_[table_info->GetTableName()][index_meta->GetIndexName()] = index_id;
  indexes_[index_id] = index_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.find(table_id) == tables_.end()) {
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  } else {
    table_info = tables_.at(table_id);
    return DB_SUCCESS;
  }
}
