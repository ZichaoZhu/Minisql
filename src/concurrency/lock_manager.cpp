#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
    // 检查事务状态和隔离级别
    if (txn->GetState() == TxnState::kShrinking) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
    }

    std::unique_lock<std::mutex> lock(latch_);
    auto &req_queue = lock_table_[rid];
    
    // 如果事务已经持有共享锁，直接返回true
    if (txn->GetSharedLockSet().count(rid) > 0) {
        return true;
    }
    
    // 如果事务已经持有排他锁，直接返回true
    if (txn->GetExclusiveLockSet().count(rid) > 0) {
        return true;
    }
    
    // 创建新的锁请求
    req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
    
    // 等待直到获得锁或被中止
    while (true) {
        // 检查是否可以授予锁
        bool can_grant = !req_queue.is_writing_ && !req_queue.is_upgrading_;
        
        if (can_grant) {
            // 授予锁
            auto req_iter = req_queue.GetLockRequestIter(txn->GetTxnId());
            req_iter->granted_ = LockMode::kShared;
            txn->GetSharedLockSet().emplace(rid);
            req_queue.sharing_cnt_++;
            return true;
        }
        
        // 检查是否应该中止
        CheckAbort(txn, req_queue);
        
        // 等待条件变量
        req_queue.cv_.wait(lock);
    }
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    std::unique_lock<std::mutex> lock(latch_);
    auto &req_queue = lock_table_[rid];
    
    // 如果事务已经持有排他锁，直接返回true
    if (txn->GetExclusiveLockSet().count(rid) > 0) {
        return true;
    }
    
    // 创建新的锁请求
    req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);
    
    // 等待直到获得锁或被中止
    while (true) {
        // 检查是否可以授予锁
        bool can_grant = !req_queue.is_writing_ && !req_queue.is_upgrading_ && req_queue.sharing_cnt_ == 0;
        
        if (can_grant) {
            // 授予锁
            auto req_iter = req_queue.GetLockRequestIter(txn->GetTxnId());
            req_iter->granted_ = LockMode::kExclusive;
            txn->GetExclusiveLockSet().emplace(rid);
            req_queue.is_writing_ = true;
            return true;
        }
        
        // 检查是否应该中止
        CheckAbort(txn, req_queue);
        
        // 等待条件变量
        req_queue.cv_.wait(lock);
    }
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    // 异常：事务状态错误
    if (txn->GetState() != TxnState::kGrowing) {
        // 更新事务状态
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }
    auto &lock_request_queue = lock_table_[rid];
    // 异常：冲突升级
    if (lock_request_queue.is_upgrading_) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
    }
    // 获取原有的锁
    auto old_lock = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
    if (old_lock == lock_request_queue.req_list_.end()) {
        throw "No lock to upgrade";
    }
    // 告知锁升级
    std::unique_lock<std::mutex> lock(latch_);
    lock_request_queue.is_upgrading_ = true;
    old_lock->lock_mode_ = LockMode::kExclusive;
    // 阻塞情况：
    // 1. 有排他锁
    // 因为有 UpgradeConflict，所以必然不会有其他锁等待
    // 3. 有共享锁
    // 到 Upgrade 时，应当保证共享锁是自己的
    lock_request_queue.cv_.wait(lock, [&] {
        // //DLOG(INFO) << "notified";
        if (lock_request_queue.is_writing_ || lock_request_queue.sharing_cnt_ > 1) {
            return false;
        }
        return true;
    });
    // 异常：事务状态错误
    if (txn->GetState() != TxnState::kGrowing) {
        // 放弃升级
        // //DLOG(INFO) << "txn not found";
        lock_request_queue.is_upgrading_ = false;
        lock_request_queue.cv_.notify_all();
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
    }
    // 更新锁的类型
    old_lock->granted_ = LockMode::kExclusive;
    // 更新请求队列状态
    lock_request_queue.sharing_cnt_--;
    lock_request_queue.is_writing_ = true;
    lock_request_queue.is_upgrading_ = false;
    // 更新事务锁计数
    txn->GetSharedLockSet().erase(rid);
    txn->GetExclusiveLockSet().insert(rid);
    // 通知等待
    lock_request_queue.cv_.notify_all();
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
    auto &lock_request_queue = lock_table_[rid];
    std::unique_lock<std::mutex> lock(latch_);
    // 获取原有的锁请求
    auto lock_request = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
    // 清除锁请求，最多只有一个
    if (lock_request == lock_request_queue.req_list_.end()) {
        return false;
    }
    // 共享锁
    if (lock_request->granted_ == LockMode::kShared) {
        lock_request_queue.sharing_cnt_--;
    }  // 排他锁
    else if (lock_request->granted_ == LockMode::kExclusive) {
        lock_request_queue.is_writing_ = false;
    }
    // 清理请求
    auto res = lock_request_queue.EraseLockRequest(txn->GetTxnId());
    if (!res) {
        return false;
    }
    // 通知等待
    lock_request_queue.cv_.notify_all();
    // 事务状态变更
    if (txn->GetState() == TxnState::kGrowing) txn->SetState(TxnState::kShrinking);
    txn->GetExclusiveLockSet().erase(rid);
    txn->GetSharedLockSet().erase(rid);
    return res;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
    // 检查事务状态，Shrinking 阶段不能加锁
    if (txn->GetState() == TxnState::kShrinking) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }
    // Read Uncommitted 隔离级别不允许加共享锁
    if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
        // 这里只做演示，实际应在LockShared中调用
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockRequestQueue &req_queue) {
    // 检查当前事务是否被标记为Aborted
    if (txn->GetState() == TxnState::kAborted) {
        // 唤醒所有等待该资源的事务
        req_queue.cv_.notify_all();
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    if (waits_for_.find(t1) == waits_for_.end()) {
        waits_for_[t1] = std::set<txn_id_t>();
    }
    if (waits_for_.find(t2) == waits_for_.end()) {
        waits_for_[t2] = std::set<txn_id_t>();
    }
    waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    if (waits_for_.count(t1)) {
        waits_for_[t1].erase(t2);
        if (waits_for_[t1].empty()) {
            waits_for_.erase(t1);
        }
    }
}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
    std::unordered_set<txn_id_t> explored_set_{};
    revisited_node_ = INVALID_TXN_ID;
    for (const auto &kv : waits_for_) {
        if (explored_set_.find(kv.first) != explored_set_.end()) {
            continue;
        }
        if (kv.second.empty()) {
            explored_set_.insert(kv.first);
            continue;
        }
        std::vector<txn_id_t> path;
        std::unordered_set<txn_id_t> visited;
        path.push_back(kv.first);
        visited.insert(kv.first);
        
        std::vector<std::pair<txn_id_t, std::set<txn_id_t>::iterator>> stack;
        stack.emplace_back(kv.first, waits_for_[kv.first].begin());
        
        while (!stack.empty()) {
            auto &top = stack.back();
            txn_id_t curr = top.first;
            auto &it = top.second;
            
            if (it == waits_for_[curr].end()) {
                stack.pop_back();
                path.pop_back();
                continue;
            }
            
            txn_id_t next = *it;
            ++it;
            
            auto cycle_start = std::find(path.begin(), path.end(), next);
            if (cycle_start != path.end()) {
                // 找到环，计算环中最大的txn_id
                txn_id_t max_tid = next;
                for (auto iter = cycle_start; iter != path.end(); ++iter) {
                    max_tid = std::max(max_tid, *iter);
                }
                // 确保max_tid不为0
                if (max_tid == 0) {
                    max_tid = 1;
                }
                newest_tid_in_cycle = max_tid;
                revisited_node_ = next;
                return true;
            }
            
            if (visited.find(next) == visited.end()) {
                path.push_back(next);
                visited.insert(next);
                stack.emplace_back(next, waits_for_[next].begin());
            }
        }
    }
    return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id: txn->GetSharedLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }

    for (const auto &row_id: txn->GetExclusiveLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::RunCycleDetection() {
    while (enable_cycle_detection_) {
        // 从lock_table_创建等待图
        waits_for_.clear();
        for (const auto &kv : lock_table_) {
            auto &req_list = kv.second.req_list_;
            for (auto it = req_list.rbegin(); it != req_list.rend(); it++) {
                if (it->granted_ == LockMode::kNone) {
                    break;
                }
                // 如果 it 未被授予，说明此后的都未被授予
                // lock_mode | granted_ | 说明
                // *         | none     | 未授予
                // shared    | shared   | 共享锁，将影响后续未授予的排他锁和升级锁
                // exclusive | exclusive| 排他锁，将影响后续未授予的任何锁
                // exclusive | shared   | 升级锁，将影响后续未授予的任何锁
                for (auto it2 = it; it2 != req_list.rend(); it2++) {
                    if (it2->granted_ == LockMode::kNone) {
                        if ((it->lock_mode_ == LockMode::kShared && it2->lock_mode_ == LockMode::kExclusive) ||
                            (it->lock_mode_ == LockMode::kExclusive)) {
                            AddEdge(it2->txn_id_, it->txn_id_);
                        }
                    }
                }
            }
        }
        txn_id_t newest_tid_in_cycle;
        if (HasCycle(newest_tid_in_cycle)) {
            txn_mgr_->Abort(txn_mgr_->GetTransaction(newest_tid_in_cycle));
        }
        if (!enable_cycle_detection_) {
            break;
        }
        std::this_thread::sleep_for(cycle_detection_interval_);
    }
}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;
    for (const auto &kv : waits_for_) {
        for (const auto &neighbor : kv.second) {
            result.emplace_back(kv.first, neighbor);
        }
    }
    return result;
}