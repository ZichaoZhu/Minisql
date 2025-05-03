#include "record/row.h"

/**
 * TODO: Student Implement
 */

/**
   * Note: Make sure that bytes write to buf is equal to GetSerializedSize()
   */
  /**
   * 我们采用上面讲述的方式：| Field Nums | Null bitmap | Field-1 | ... | Field-N |
   * 其中，| Header | = | Field Nums | + | Null bitmap |
   */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;  // 定义偏移量
  size_t field_nums = fields_.size(); // 得到字段个数
  MACH_WRITE_TO(size_t, buf, field_nums); // 写入字段个数
  offset = sizeof(field_nums);
  /* 对于Null bitmap，我们考虑采用多个char类型变量进行序列化
   * 简单来说，就是每个char类型变量的每一位代表一个字段是否为空
   * 例如：| 0 | 1 | 0 | 1 | 0 | 1 | 0 | 1 |
   * 然后，每一个char类型都可以表示8个字段是否为空
   * 对于最后多出不满8个的字段，我们在后面补0
   * 也就是说，我们使用了单页的bitmap，组成了多页的bitmaps
   */
  uint32_t bitmap_num = (field_nums + 7) / 8; // 计算需要多少个bitmap
  for (uint32_t i = 0; i < bitmap_num; i++) {
    /* 此时，是第i个null_bitmap表被序列化
     * 我们首先计算null_bitmap表
     * 然后再将其写入到buf中
     */
    // 计算null_bitmap
    char null_bitmap = 0;
    for (uint32_t j = 0; j<8; j++) {
      // 后续补0的情况
      null_bitmap <<= 1; // 左移一位
      if (i * 8 + j >= field_nums) {
        null_bitmap += 0;
      }
      // 正常的情况
      else {
        if (fields_[i * 8 + j]->IsNull()) {
          null_bitmap += 0;
        } else {
          null_bitmap += 1;
        }
      }
    }
    // 存储到buf中
    MACH_WRITE_TO(char, buf + offset, null_bitmap);
    offset += sizeof(char);
  }

  /* 最后，我们序列化每一个字段
   * 考虑到字段的实现已经在field.h中实现了
   * 我们只需要调用其SerializeTo函数即可
   */
  for (auto &field : fields_) {
    if (!field->IsNull()) {
      offset += field->Field::SerializeTo(buf + offset);
      // 在此处的SerializeTo()和GetSerializedSize()函数的返回值应该是一样的
      // 这里方便起见采用SerializeTo()进行返回
    }
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;  // 定义偏移量
  size_t field_nums = MACH_READ_FROM(size_t, buf);  // 需要注意这里的类型
  offset = sizeof(size_t);
  uint32_t bitmap_num = (field_nums + 7) / 8;
  std::vector<char> null_bitmaps;
  // 读取所有的null_bitmap
  for (uint32_t i = 0; i < bitmap_num; i++) {
    char null_bitmap = MACH_READ_FROM(char, buf + offset);
    offset += sizeof(char);
    null_bitmaps.push_back(null_bitmap);
  }
  // 将field内容插入field_后面
  for (uint32_t i = 0; i < bitmap_num; i++) {
    std::bitset<8> bit(null_bitmaps[i]);
    for (uint32_t j = 0; j < 8 && i * 8 + j < field_nums; j++) {
      // 首先，我们先要知道这一个要解决的field的类型
      TypeId field_type = schema->GetColumn(i * 8 + j)->GetType();
      // 我们使用二进制来解决
      // 例如：| 0 | 1 | 0 | 1 | 0 | 1 | 0 | 1 |
      Field *field = nullptr;
      offset += Field::DeserializeFrom(buf + offset, field_type, &field, !bit[8 - 1 - j]);
      fields_.push_back(field);
    }
  }
  return offset;
}

/* 序列化的长度由三部分决定
     * 1. Field Nums
     * 2. Null bitmaps
     * 3. Field-1 + ... + Field-N
     */
uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  // Field Nums
  uint32_t field_nums_len = sizeof(size_t);
  // Null bitmaps
  uint32_t bitmap_num = (fields_.size() + 7) / 8;
  uint32_t null_bitmaps_len = sizeof(char) * bitmap_num;
  // Field 1-n
  uint32_t fields_len = 0;
  for (auto &field : fields_) {
    if (!field->IsNull()) {
      fields_len += field->GetSerializedSize();
    }
  }
  return field_nums_len + null_bitmaps_len + fields_len;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
