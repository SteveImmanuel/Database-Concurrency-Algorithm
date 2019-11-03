// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <deque>

#include "txn/lock_manager.h"

using std::deque;

LockManager::~LockManager() {
  for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
    delete it->second;
  }
}

deque<LockManager::LockRequest>* LockManager::_getLockQueue(const Key& key) {
  deque<LockRequest> *dq = lock_table_[key];
  if (!dq) {
    dq = new deque<LockRequest>();
    lock_table_[key] = dq;
  }
  return dq;
}

SimpleLocking::SimpleLocking(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool SimpleLocking::WriteLock(Txn* txn, const Key& key) {
  LockRequest request(EXCLUSIVE, txn);
  deque<LockRequest> *requests = _getLockQueue(key);
  bool empty = requests->empty();
  if (!empty) { // Add to wait list, doesn't own lock.
    txn_waits_[txn]++;
  }
  requests->push_back(request);
  return empty;
}

bool SimpleLocking::ReadLock(Txn* txn, const Key& key) {
  return WriteLock(txn, key);
}

void SimpleLocking::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *requests = _getLockQueue(key);
  bool removed = true;

  // Delete the txn's exclusive lock.
  for (auto it = requests->begin(); it < requests->end(); it++) {
    if (it->txn_ == txn) { 
        requests->erase(it);
        break;
    }
    removed = false;
  }

  if (!requests->empty() && removed) {
    LockRequest next = requests->front();

    if (--txn_waits_[next.txn_] == 0) {
        ready_txns_->push_back(next.txn_);
        txn_waits_.erase(next.txn_);
    }
  }
}

LockMode SimpleLocking::Status(const Key& key, vector<Txn*>* owners) {

  deque<LockRequest> *requests = _getLockQueue(key);
  if (!requests->empty()) {

    vector<Txn*> _owners;
    Txn* txn;

    txn = requests->front().txn_;
    _owners.push_back(txn);
    *owners = _owners;

    return EXCLUSIVE;
  }

  return UNLOCKED;
}