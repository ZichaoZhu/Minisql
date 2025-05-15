#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

// 测试 operator== 以及构造函数
TEST(TableIteratorTest, EqualityOperatorTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  Schema *schema = nullptr;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema, nullptr, nullptr, nullptr);
  RowId rid1(1, 1), rid2(2, 2);

  TableIterator iter1(table_heap, rid1, nullptr);
  TableIterator iter2(table_heap, rid1, nullptr);
  TableIterator iter3(table_heap, rid2, nullptr);

  ASSERT_TRUE(iter1 == iter2);
  ASSERT_FALSE(iter1 == iter3);

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}

// 测试 TableIterator 拷贝构造函数
TEST(TableIteratorTest, CopyConstructorTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  Schema *schema = nullptr;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema, nullptr, nullptr, nullptr);

  RowId rid(1, 2);

  TableIterator iter1(table_heap, rid, nullptr);
  TableIterator iter2(iter1);

  ASSERT_TRUE(iter2 == iter1);

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}

// 测试 TableIterator 析构函数
TEST(TableIteratorTest, DestructorTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  Schema *schema = nullptr;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema, nullptr, nullptr, nullptr);

  RowId rid(1, 1);
  auto txn = new Txn();
  {
    TableIterator iter(table_heap, rid, txn);
    // 析构函数会在作用域结束时调用
  }
  ASSERT_NO_THROW(delete txn);

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}

// 测试 operator!=
TEST(TableIteratorTest, InequalityOperatorTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  Schema *schema = nullptr;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema, nullptr, nullptr, nullptr);

  RowId rid1(1, 1), rid2(2, 2);
  TableIterator iter1(table_heap, rid1, nullptr);
  TableIterator iter2(table_heap, rid1, nullptr);
  TableIterator iter3(table_heap, rid2, nullptr);

  ASSERT_FALSE(iter1 != iter2);
  ASSERT_TRUE(iter1 != iter3);

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
}

// 测试 operator*
TEST(TableIteratorTest, DereferenceOperatorTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false)};
  auto schema = new Schema(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema, nullptr, nullptr, nullptr);

  RowId rid(0, 0);
  Fields fields = {Field(TypeId::kTypeInt, 123)};
  Row row(fields);
  table_heap->InsertTuple(row, nullptr);

  TableIterator iter(table_heap, rid, nullptr);
  const Row &result_row = *iter;

  ASSERT_TRUE(result_row.GetField(0)->CompareEquals(Field(TypeId::kTypeInt, 123)));

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
  delete schema;
}

// 测试 operator->
TEST(TableIteratorTest, ArrowOperatorTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false)};
  auto schema = new Schema(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema, nullptr, nullptr, nullptr);

  RowId rid(0, 0);
  Fields fields = {Field(TypeId::kTypeInt, 456)};
  Row row(fields);
  table_heap->InsertTuple(row, nullptr);

  TableIterator iter(table_heap, rid, nullptr);
  Row *result_row = iter.operator->();

  ASSERT_NE(result_row, nullptr);
  ASSERT_TRUE(result_row->GetField(0)->CompareEquals(Field(TypeId::kTypeInt, 456)));

  delete result_row;
  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
  delete schema;
}

// 测试 ++iter
TEST(TableIteratorTest, PreIncrementOperatorTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false)};
  auto schema = new Schema(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema, nullptr, nullptr, nullptr);

  RowId rid(0, 0);
  Fields fields1 = {Field(TypeId::kTypeInt, 123)};
  Row row1(fields1);
  Fields fields2 = {Field(TypeId::kTypeInt, 789)};
  Row row2(fields2);
  table_heap->InsertTuple(row1, nullptr);
  table_heap->InsertTuple(row2, nullptr);

  TableIterator iter(table_heap, rid, nullptr);

  ASSERT_TRUE(iter->GetRowId() == rid);
  ASSERT_FALSE((++iter)->GetRowId() == rid);

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
  delete schema;
}

// 测试 iter++
TEST(TableIteratorTest, PostIncrementOperatorTest) {
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false)};
  auto schema = new Schema(columns);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema, nullptr, nullptr, nullptr);

  RowId rid(0, 0);
  Fields fields1 = {Field(TypeId::kTypeInt, 123)};
  Row row1(fields1);
  Fields fields2 = {Field(TypeId::kTypeInt, 789)};
  Row row2(fields2);
  table_heap->InsertTuple(row1, nullptr);
  table_heap->InsertTuple(row2, nullptr);

  TableIterator iter(table_heap, rid, nullptr);

  ASSERT_TRUE((iter++)->GetRowId() == rid);
  ASSERT_FALSE(iter->GetRowId() == rid);

  delete table_heap;
  delete bpm_;
  delete disk_mgr_;
  delete schema;
}