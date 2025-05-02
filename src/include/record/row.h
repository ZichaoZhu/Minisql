#ifndef MINISQL_ROW_H
#define MINISQL_ROW_H

#include <memory>
#include <vector>

#include "common/macros.h"
#include "common/rowid.h"
#include "record/field.h"
#include "record/schema.h"

/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *
 *
 */
class Row {
 public:
  /**
   * Row used for insert
   * Field integrity should check by upper level
   */
  Row(std::vector<Field> &fields) {
    // deep copy
    for (auto &field : fields) {
      fields_.push_back(new Field(field));
    }
  }

  void destroy() {
    if (!fields_.empty()) {
      for (auto field : fields_) {
        delete field;
      }
      fields_.clear();
    }
  }

  ~Row() { destroy(); };

  /**
   * Row used for deserialize
   */
  Row() = default;

  /**
   * Row used for deserialize and update
   */
  Row(RowId rid) : rid_(rid) {}

  /**
   * Row copy function, deep copy
   */
  Row(const Row &other) {
    destroy();
    rid_ = other.rid_;
    for (auto &field : other.fields_) {
      fields_.push_back(new Field(*field));
    }
  }

  /**
   * Assign operator, deep copy
   */
  Row &operator=(const Row &other) {
    destroy();
    rid_ = other.rid_;
    for (auto &field : other.fields_) {
      fields_.push_back(new Field(*field));
    }
    return *this;
  }

  /**
   * Note: Make sure that bytes write to buf is equal to GetSerializedSize()
   */
  /**
   * 我们采用上面讲述的方式：| Field Nums | Null bitmap | Field-1 | ... | Field-N |
   * 其中，| Header | = | Field Nums | + | Null bitmap |
   */
  uint32_t SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid Schema");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Invalid field");
    unsigned offset = 0;  // 定义偏移量
    size_t field_nums = fields_.size(); // 得到字段个数
    MACH_WRITE_UINT32(buf, field_nums); // 写入字段个数
    offset = sizeof(field_nums);
    /* 对于Null bitmap，我们考虑采用多个char类型变量进行序列化
     * 简单来说，就是每个char类型变量的每一位代表一个字段是否为空
     * 例如：| 0 | 1 | 0 | 1 | 0 | 1 | 0 | 1 |
     * 然后，每一个char类型都可以表示8个字段是否为空
     * 对于最后多出不满8个的字段，我们在后面补0
     * 也就是说，我们使用了单页的bitmap，组成了多页的bitmaps
     */
    unsigned char_num = (field_nums + 7) / 8; // 计算需要多少个char类型变量
    for (unsigned i = 0; i < char_num; i++) {
      /* 此时，是第i个null_bitmap表被序列化
       * 我们首先计算null_bitmap表
       * 然后再将其写入到buf中
       */
      // 计算null_bitmap
      char null_bitmap = 0;
      for (unsigned j = 0; j<8; j++) {
        // 后续补0的情况
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
        null_bitmap <<= 1; // 左移一位
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
        offset += field->SerializeTo(buf + offset);
        // 在此处的SerializeTo()和GetSerializedSize()函数的返回值应该是一样的
        // 这里方便起见采用SerializeTo()进行返回
      }
    }
    return offset;
  }

  uint32_t DeserializeFrom(char *buf, Schema *schema);

  /**
   * For empty row, return 0
   * For non-empty row with null fields, eg: |null|null|null|, return header size only
   * @return
   */
  uint32_t GetSerializedSize(Schema *schema) const;

  void GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row);

  inline const RowId GetRowId() const { return rid_; }

  inline void SetRowId(RowId rid) { rid_ = rid; }

  inline std::vector<Field *> &GetFields() { return fields_; }

  inline Field *GetField(uint32_t idx) const {
    ASSERT(idx < fields_.size(), "Failed to access field");
    return fields_[idx];
  }

  inline size_t GetFieldCount() const { return fields_.size(); }

 private:
  RowId rid_{};
  std::vector<Field *> fields_; /** Make sure that all field ptr are destructed*/
};

#endif  // MINISQL_ROW_H
