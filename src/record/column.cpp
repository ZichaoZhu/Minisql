#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  /*我们采用以下格式进行序列化：
   * | COLUMN_MAGIC_NUM | name_len | name | type | len | table_ind | nullable | unique |
   * 因为name是string类型，因此我们需要记录其长度
   * 幸运的是，string.length()是size_t类型的，是定长的
   */
  uint32_t offset = 0;  // 初始化偏移量
  // 序列化COLUMN_MAGIC_NUM
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  offset += sizeof(uint32_t);
  // 序列化name_len以及name
  size_t name_len = name_.length();
  MACH_WRITE_UINT32(buf + offset, name_len);
  offset += sizeof(uint32_t);
  MACH_WRITE_STRING(buf + offset, name_);
  offset += name_len;
  // 序列化type
  MACH_WRITE_TO(TypeId, buf + offset, type_);
  offset += sizeof(type_);
  // 序列化len
  MACH_WRITE_UINT32(buf + offset, len_);
  offset += sizeof(uint32_t);
  // 序列化table_ind
  MACH_WRITE_UINT32(buf + offset, table_ind_);
  offset += sizeof(uint32_t);
  // 序列化nullable
  MACH_WRITE_TO(bool, buf + offset, nullable_);
  offset += sizeof(bool);
  // 序列化unique
  MACH_WRITE_TO(bool, buf + offset, unique_);
  offset += sizeof(bool);
  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return 0;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  return 0;
}
