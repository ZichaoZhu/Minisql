#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(ColumnTest, ConstructorAndAccessors) {
  // 测试构造函数和基本属性访问
  Column column("id", TypeId::kTypeInt, 0, false, false);
  EXPECT_EQ("id", column.GetName());
  EXPECT_EQ(TypeId::kTypeInt, column.GetType());
  EXPECT_EQ(0, column.GetTableInd());
  EXPECT_FALSE(column.IsNullable());
  EXPECT_FALSE(column.IsUnique());

  Column column2("name", TypeId::kTypeChar, 64, 1, true, false);
  EXPECT_EQ("name", column2.GetName());
  EXPECT_EQ(TypeId::kTypeChar, column2.GetType());
  EXPECT_EQ(64, column2.GetLength());
  EXPECT_TRUE(column2.IsNullable());
  EXPECT_FALSE(column2.IsUnique());
}

TEST(ColumnTest, SerializeDeserialize) {
  // 测试序列化和反序列化
  Column column1("account", TypeId::kTypeFloat, 2, true, true);
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));

  // 序列化
  uint32_t size = column1.SerializeTo(buffer);
  EXPECT_GT(size, 0);

  // 反序列化
  Column *deserialized_column = nullptr;
  uint32_t deserialized_size = Column::DeserializeFrom(buffer, deserialized_column);
  EXPECT_EQ(size, deserialized_size);

  // 验证反序列化后的数据
  EXPECT_EQ(column1.GetName(), deserialized_column->GetName());
  EXPECT_EQ(column1.GetType(), deserialized_column->GetType());
  EXPECT_EQ(column1.GetTableInd(), deserialized_column->GetTableInd());
  EXPECT_TRUE(deserialized_column->IsNullable());
  EXPECT_TRUE(deserialized_column->IsUnique());

  delete deserialized_column;

  // Int
  Column column2("department", TypeId::kTypeInt, 7, false, true);
  memset(buffer, 0, sizeof(buffer));

  // 序列化
  size = column2.SerializeTo(buffer);
  EXPECT_GT(size, 0);

  // 反序列化
  deserialized_column = nullptr;
  deserialized_size = Column::DeserializeFrom(buffer, deserialized_column);
  EXPECT_EQ(size, deserialized_size);

  // 验证反序列化后的数据
  EXPECT_EQ(column2.GetName(), deserialized_column->GetName());
  EXPECT_EQ(column2.GetType(), deserialized_column->GetType());
  EXPECT_EQ(column2.GetTableInd(), deserialized_column->GetTableInd());
  EXPECT_FALSE(deserialized_column->IsNullable());
  EXPECT_TRUE(deserialized_column->IsUnique());

  delete deserialized_column;

  // char
  Column column3("studentname", TypeId::kTypeChar, 10, 9, false, false);
  memset(buffer, 0, sizeof(buffer));

  // 序列化
  size = column3.SerializeTo(buffer);
  EXPECT_GT(size, 0);

  // 反序列化
  deserialized_column = nullptr;
  deserialized_size = Column::DeserializeFrom(buffer, deserialized_column);
  EXPECT_EQ(size, deserialized_size);

  // 验证反序列化后的数据
  EXPECT_EQ(column3.GetName(), deserialized_column->GetName());
  EXPECT_EQ(column3.GetType(), deserialized_column->GetType());
  EXPECT_EQ(column3.GetLength(), deserialized_column->GetLength());
  EXPECT_EQ(column3.GetTableInd(), deserialized_column->GetTableInd());
  EXPECT_FALSE(deserialized_column->IsNullable());
  EXPECT_FALSE(deserialized_column->IsUnique());

  delete deserialized_column;
}