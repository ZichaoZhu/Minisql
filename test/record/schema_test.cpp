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

TEST(SchemaTest, SchemaValidation) {
  // 创建列
  std::vector<Column *> columns = {
      new Column("id", TypeId::kTypeInt, 0, false, false),
      new Column("name", TypeId::kTypeChar, 64, 1, true, false),
      new Column("account", TypeId::kTypeFloat, 2, true, false)};

  // 创建Schema
  Schema schema(columns);

  // 验证列数
  ASSERT_EQ(schema.GetColumnCount(), 3);

  // 验证列名和属性
  const Column *col1 = schema.GetColumn(0);
  ASSERT_EQ(col1->GetName(), "id");
  ASSERT_EQ(col1->GetType(), TypeId::kTypeInt);
  ASSERT_FALSE(col1->IsNullable());
  ASSERT_FALSE(col1->IsUnique());

  const Column *col2 = schema.GetColumn(1);
  ASSERT_EQ(col2->GetName(), "name");
  ASSERT_EQ(col2->GetType(), TypeId::kTypeChar);
  ASSERT_TRUE(col2->IsNullable());
  ASSERT_FALSE(col2->IsUnique());

  const Column *col3 = schema.GetColumn(2);
  ASSERT_EQ(col3->GetName(), "account");
  ASSERT_EQ(col3->GetType(), TypeId::kTypeFloat);
  ASSERT_TRUE(col3->IsNullable());
  ASSERT_FALSE(col3->IsUnique());

}

TEST(SchemaTest, SerializeDeserialize) {
  // 创建列
  std::vector<Column *> columns = {
      new Column("id", TypeId::kTypeInt, 0, false, false),
      new Column("name", TypeId::kTypeChar, 64, 1, true, false),
      new Column("account", TypeId::kTypeFloat, 2, true, false)};

  // 创建Schema
  Schema schema(columns);

  // 序列化
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  uint32_t size = schema.SerializeTo(buffer);
  ASSERT_GT(size, 0);

  // 反序列化
  Schema *deserialized_schema = nullptr;
  uint32_t deserialized_size = Schema::DeserializeFrom(buffer, deserialized_schema);
  ASSERT_EQ(size, deserialized_size);

  // 验证反序列化后的数据
  ASSERT_EQ(schema.GetColumnCount(), deserialized_schema->GetColumnCount());
  for (size_t i = 0; i < schema.GetColumnCount(); i++) {
    const Column *original_col = schema.GetColumn(i);
    const Column *deserialized_col = deserialized_schema->GetColumn(i);
    ASSERT_EQ(original_col->GetName(), deserialized_col->GetName());
    ASSERT_EQ(original_col->GetType(), deserialized_col->GetType());
    ASSERT_EQ(original_col->GetTableInd(), deserialized_col->GetTableInd());
    ASSERT_EQ(original_col->IsNullable(), deserialized_col->IsNullable());
    ASSERT_EQ(original_col->IsUnique(), deserialized_col->IsUnique());
  }
}