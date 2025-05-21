#if 0
#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    return nullptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    return nullptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    return nullptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    return nullptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    return nullptr;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    return nullptr;
}

#endif  // MINISQL_LOG_REC_H

#endif


#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include "recovery_manager.h"
#include <unordered_map>
#include <utility>
#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,  // 无效
    kInsert,   // 插入
    kDelete,   // 删除
    kUpdate,   // 更新
    kBegin,    // 开始
    kCommit,   // 提交
    kAbort,    // 中止
};

// 仅用于测试
using KeyType = std::string;
using ValType = int32_t;

/**
 * 日志记录结构体
 */
struct LogRec
{
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};  // 日志记录类型
    lsn_t lsn_{INVALID_LSN};                // 日志序列号
    lsn_t prev_lsn_{INVALID_LSN};           // 上一条日志记录的日志序列号

    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;  // 事务ID到最新LSN的映射
    static lsn_t next_lsn_;                                   // 下一个可用的LSN

    txn_id_t txn_id;  // 事务ID
    
    // 插入操作相关字段
    KeyType ins_key;
    ValType ins_val;
    // 更新操作相关字段
    KeyType old_key;
    ValType old_val;
    KeyType new_key;
    ValType new_val;  
    // 删除操作相关字段
    KeyType del_key;
    ValType del_val;
};

// 静态成员初始化
std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;  // 日志记录智能指针

/**
 * 创建插入日志
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val)
{
    // 生成并更新LSN
    lsn_t now_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;
    
    auto i = LogRec::prev_lsn_map_.find(txn_id);
    if (i != LogRec::prev_lsn_map_.end())
    {
        prev_lsn = i->second;
        i->second = now_lsn;
    }
    else
        LogRec::prev_lsn_map_.emplace(txn_id, now_lsn);

    auto log_ptr = std::make_shared<LogRec>();
    log_ptr->type_ = LogRecType::kInsert;
    log_ptr->lsn_ = now_lsn;
    log_ptr->prev_lsn_ = prev_lsn;
    log_ptr->txn_id = txn_id;
    log_ptr->ins_key = ins_key;
    log_ptr->ins_val = ins_val;

    return log_ptr;
}

/**
 * 创建删除日志
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val)
{
    // 生成并更新LSN
    lsn_t now_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;
    
    auto i = LogRec::prev_lsn_map_.find(txn_id);
    if (i != LogRec::prev_lsn_map_.end())
    {
        prev_lsn = i->second;
        i->second = now_lsn;
    }
    else
        LogRec::prev_lsn_map_.emplace(txn_id, now_lsn);

    auto log_ptr = std::make_shared<LogRec>();
    log_ptr->type_ = LogRecType::kDelete;
    log_ptr->lsn_ = now_lsn;
    log_ptr->prev_lsn_ = prev_lsn;
    log_ptr->txn_id = txn_id;
    log_ptr->del_key = del_key;
    log_ptr->del_val = del_val;

    return log_ptr;
}

/**
 * 创建更新日志
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val)
{
    // 生成并更新LSN
    lsn_t now_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;
    
    auto i = LogRec::prev_lsn_map_.find(txn_id);
    if (i != LogRec::prev_lsn_map_.end())
    {
        prev_lsn = i->second;
        i->second = now_lsn;
    }
    else
        LogRec::prev_lsn_map_.emplace(txn_id, now_lsn);

    auto log_ptr = std::make_shared<LogRec>();
    log_ptr->type_ = LogRecType::kUpdate;
    log_ptr->lsn_ = now_lsn;
    log_ptr->prev_lsn_ = prev_lsn;
    log_ptr->txn_id = txn_id;
    log_ptr->old_key = old_key;
    log_ptr->old_val = old_val;
    log_ptr->new_key = new_key;
    log_ptr->new_val = new_val;

    return log_ptr;
}

/**
 * 创建事务开始日志
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id)
{
    // 生成并更新LSN
    lsn_t now_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;
    
    auto i = LogRec::prev_lsn_map_.find(txn_id);
    if (i != LogRec::prev_lsn_map_.end())
    {
        prev_lsn = i->second;
        i->second = now_lsn;
    }
    else
        LogRec::prev_lsn_map_.emplace(txn_id, now_lsn);

    auto log_ptr = std::make_shared<LogRec>();
    log_ptr->type_ = LogRecType::kBegin;
    log_ptr->lsn_ = now_lsn;
    log_ptr->prev_lsn_ = prev_lsn;
    log_ptr->txn_id = txn_id;

    return log_ptr;
}

/**
 * 创建事务提交日志
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id)
{
    // 生成并更新LSN
    lsn_t now_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;
    
    auto i = LogRec::prev_lsn_map_.find(txn_id);
    if (i != LogRec::prev_lsn_map_.end())
    {
        prev_lsn = i->second;
        i->second = now_lsn;
    }
    else
        LogRec::prev_lsn_map_.emplace(txn_id, now_lsn);

    auto log_ptr = std::make_shared<LogRec>();
    log_ptr->type_ = LogRecType::kCommit;
    log_ptr->lsn_ = now_lsn;
    log_ptr->prev_lsn_ = prev_lsn;
    log_ptr->txn_id = txn_id;

    return log_ptr;
}

/**
 * 创建事务中止日志
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id)
{
    // 生成并更新LSN
    lsn_t now_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;
    
    auto i = LogRec::prev_lsn_map_.find(txn_id);
    if (i != LogRec::prev_lsn_map_.end())
    {
        prev_lsn = i->second;
        i->second = now_lsn;
    }
    else
        LogRec::prev_lsn_map_.emplace(txn_id, now_lsn);

    auto log_ptr = std::make_shared<LogRec>();
    log_ptr->type_ = LogRecType::kAbort;
    log_ptr->lsn_ = now_lsn;
    log_ptr->prev_lsn_ = prev_lsn;
    log_ptr->txn_id = txn_id;

    return log_ptr;
}

#endif  // MINISQL_LOG_REC_H