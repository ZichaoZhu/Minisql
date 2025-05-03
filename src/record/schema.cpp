#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  /* 我们采用以下存储格式：
   * | SCHEMA_MAGIC_NUM | columns_num | columns_ | is_manage_ |
   */
  uint32_t offset = 0;  // 初始化偏移量
  // 存储SCHEMA_MAGIC_NUM
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  // 存储columns_num
  size_t columns_num = columns_.size();
  MACH_WRITE_TO(size_t, buf + offset, columns_num);
  offset += sizeof(size_t);
  // 存储columns_
  for (const auto &column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }
  // 存储is_manage_
  MACH_WRITE_TO(bool, buf + offset, is_manage_);
  offset += sizeof(bool);
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  /* 就是按照上面的格式就好了
   * | SCHEMA_MAGIC_NUM | columns_num | columns_ | is_manage_ |
   */
  uint32_t len = 0;
  len += sizeof(SCHEMA_MAGIC_NUM) + sizeof(uint32_t) + sizeof(uint32_t);
  return len;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  // 相同地，根据上面的格式
  // | SCHEMA_MAGIC_NUM | columns_num | columns_ | is_manage_ |
  // offset初始化
  uint32_t offset = 0;
  // 读取SCHEMA_MAGIC_NUM
  uint32_t magic_num = MACH_READ_UINT32(buf);
  offset += sizeof(uint32_t);
  // 检查magic_num
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid magic number.");
  // 读取columns_num
  size_t columns_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(size_t);
  // 读取columns_
  std::vector<Column *> columns;
  for (size_t i = 0; i < columns_num; i++) {
    Column *column;
    offset += Column::DeserializeFrom(buf + offset, column);
    columns.push_back(column);
  }
  // 读取is_manage_
  bool is_manage = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);
  // 创建schema
  schema = new Schema(columns, is_manage);
  // 释放columns
  if (!is_manage) {
    for (auto &column : columns) {
      delete column;
    }
  }
  // 返回偏移量
  return offset;
}