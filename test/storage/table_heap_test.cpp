#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

// Insert and Get
TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);
}

// Delete
TEST(TableHeapTest, ApplyDeleteTest) {
  // 初始化测试环境
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);

  // 插入数据
  int32_t id = 1;
  char name[] = "test_name";
  float account = 100.0f;
  Fields fields = {Field(TypeId::kTypeInt, id), Field(TypeId::kTypeChar, name, strlen(name), true),
                   Field(TypeId::kTypeFloat, account)};
  Row row(fields);
  ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));

  // 获取插入的元组的 RowId
  RowId rid = row.GetRowId();

  // 调用 ApplyDelete() 删除元组
  table_heap->ApplyDelete(rid, nullptr);

  // 验证元组是否被删除
  Row deleted_row(rid);
  ASSERT_FALSE(table_heap->GetTuple(&deleted_row, nullptr));

  // 清理资源
  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}


// Update
TEST(TableHeapTest, UpdateFunctionsTest) {
  // 初始化测试环境
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);

  // 插入数据
  std::unordered_map<int64_t, Fields *> row_values;
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    row_values.emplace(row.GetRowId().Get(), fields);
    delete[] characters;
  }

  // 验证 UpdateTuple()
  for (auto &row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);

    // 修改字段值
    Fields *updated_fields = new Fields{Field(TypeId::kTypeInt, 9999), Field(*row.GetField(1)), Field(*row.GetField(2))};
    Row updated_row(*updated_fields);
    ASSERT_TRUE(table_heap->UpdateTuple(updated_row, row.GetRowId(), nullptr));

    // 验证更新后的值
    Row verify_row(RowId(row_kv.first));
    table_heap->GetTuple(&verify_row, nullptr);

    ASSERT_EQ(kTrue, verify_row.GetField(0)->CompareEquals(updated_fields->at(0)));
    delete updated_fields;
  }
  // 我们验证特殊情况，需要注意的是，因为“tuple 已被删除”这种情况需要删除元组，因此我们调换第三种与第二种特殊情况的位置。
  // 验证特殊情况 -1: slot number 无效
  // 构造一个RowId无效的row
  Fields *invalid_fields = new Fields{Field(TypeId::kTypeInt, 9999),
                               Field(TypeId::kTypeChar, "invalid", 7, true),
                               Field(TypeId::kTypeFloat, 9999.0f)};
  Row invalid_row(*invalid_fields);
  // 设置无效的RowId
  RowId invalid_rid(8, 9999);
  invalid_row.SetRowId(invalid_rid);
  ASSERT_FALSE(table_heap->UpdateTuple(invalid_row, invalid_row.GetRowId(), nullptr));  // 应返回 false

  // 验证特殊情况 -3: 空间不足
  for (auto &row_kv : row_values) {
    cout<<"num"<<endl;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);

    // 构造一个超大字段，导致页面空间不足
    int32_t large_len = 4096 / 2 - 1;  // 假设超出页面大小
    char *large_data = new char[large_len];
    memset(large_data, 'a', large_len);
    Fields *large_fields = new Fields{Field(TypeId::kTypeInt, 9999), Field(TypeId::kTypeChar, large_data, large_len, true),
                                      Field(*row.GetField(2))};
    Row large_row(*large_fields);
    ASSERT_FALSE(table_heap->UpdateTuple(large_row, row.GetRowId(), nullptr));  // 应返回 false
    delete[] large_data;
    delete large_fields;
    break;  // 测试一个即可
  }

  // 验证特殊情况 -2: tuple 已被删除
  for (auto &row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);

    // 删除元组
    table_heap->ApplyDelete(row.GetRowId(), nullptr);

    // 尝试更新已删除的元组
    Fields *updated_fields = new Fields{Field(TypeId::kTypeInt, 9999), Field(*row.GetField(1)), Field(*row.GetField(2))};
    Row updated_row(*updated_fields);
    ASSERT_FALSE(table_heap->UpdateTuple(updated_row, row.GetRowId(), nullptr));  // 应返回 false
    delete updated_fields;
    break;  // 测试一个即可
  }
}

// FreeHeap 测试
TEST(TableHeapTest, FreeHeapTest) {
  // 初始化测试环境
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);

  // 插入数据
  int32_t id = 1;
  char name[] = "test_name";
  float account = 100.0f;
  Fields fields = {Field(TypeId::kTypeInt, id), Field(TypeId::kTypeChar, name, strlen(name), true),
                   Field(TypeId::kTypeFloat, account)};
  Row row(fields);
  ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));

  // 调用 FreeHeap() 释放堆内存
  table_heap->FreeHeap();

  // 验证堆内存是否已释放
  RowId rid = row.GetRowId();
  Row deleted_row(rid);
  ASSERT_FALSE(table_heap->GetTuple(&deleted_row, nullptr));  // 应返回 false，表示内存已释放

  // 清理资源
  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}