// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode) : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);

  // Create the storage

  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else {
    storage_ = new Storage();
  }

  storage_->InitStorage();
  // std::cout<<"complete initialization"<<std::endl;

  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  CPU_SET(1, &cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(3, &cpuset);
  CPU_SET(4, &cpuset);
  CPU_SET(5, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void *>(this));
}

void *TxnProcessor::StartScheduler(void *arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING) delete lm_;

  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn *txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn *TxnProcessor::GetTxnResult() {
  Txn *txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:
      RunSerialScheduler();
      break;
    case LOCKING:
      RunLockingScheduler();
      break;
    case LOCKING_EXCLUSIVE_ONLY:
      RunLockingScheduler();
      break;
    case OCC:
      RunOCCScheduler();
      break;
    case P_OCC:
      RunOCCParallelScheduler();
      break;
    case MVCC:
      RunMVCCScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn *txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn *txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }

      if (blocked == false) {
        // Request write locks.
        for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin();
                   it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)) {
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(this, &TxnProcessor::ExecuteTxn, txn));
    }
  }
}

void TxnProcessor::ExecuteTxn(Txn *txn) {
  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn *txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin(); it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

void TxnProcessor::RunOCCScheduler() {
  while (tp_.Active()) {
    // Get the next new transaction request.
    Txn *next_txn;
    if (txn_requests_.Pop(&next_txn)) {
      // If one is pending, pass it to an execution thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(this, &TxnProcessor::ExecuteTxn, next_txn));
    }

    // Deal with all transactions that have finished running.
    Txn *completed_txn;
    while (completed_txns_.Pop(&completed_txn)) {
      // Validation Phase
      bool valid = true;

      // Check for completed_txn's read sets.
      for (set<Key>::iterator it = completed_txn->readset_.begin();
           it != completed_txn->readset_.end(); ++it) {
        if (!valid) break;
        valid = valid && storage_->Timestamp(*it) <= completed_txn->occ_start_time_;
      }

      // Check for completed_txn's write sets.
      for (set<Key>::iterator it = completed_txn->writeset_.begin();
           it != completed_txn->writeset_.end(); ++it) {
        if (!valid) break;
        valid = valid && storage_->Timestamp(*it) <= completed_txn->occ_start_time_;
      }

      if (!valid) {
        // Cleanup completed_txn.
        completed_txn->reads_.clear();
        completed_txn->writes_.clear();
        completed_txn->status_ = INCOMPLETE;

        // Restart transaction.
        mutex_.Lock();
        completed_txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(completed_txn);
        mutex_.Unlock();
      } else {
        // Apply all writes, mark transaction as committed.
        ApplyWrites(completed_txn);
        completed_txn->status_ = COMMITTED;
      }

      txn_results_.Push(completed_txn);
    }
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::MVCCExecuteTxn(Txn *txn) {
  Value result;
  for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it) {
    if (storage_->Read(*it, &result, txn->unique_id_)) {
      txn->reads_[*it] = result;
    }
  }

  for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it) {
    if (storage_->Read(*it, &result, txn->unique_id_)) {
      txn->reads_[*it] = result;
    }
  }

  txn->Run();

  if (txn->Status() == COMPLETED_A) {
    txn->status_ = ABORTED;
  } else if (txn->Status() == COMPLETED_C) {
    MVCCLockWriteKeys(txn);

    bool valid = true;
    set<Key>::iterator it = txn->writeset_.begin();
    while (valid && it != txn->writeset_.end()) {
      valid = storage_->CheckWrite(*it, txn->unique_id_);
      it++;
    }

    if (valid) {
      ApplyWrites(txn);
      txn->status_ = COMMITTED;
      MVCCUnlockWriteKeys(txn);
      txn_results_.Push(txn);
    } else {
      MVCCUnlockWriteKeys(txn);
      txn->reads_.empty();
      txn->writes_.empty();
      txn->status_ = INCOMPLETE;
      mutex_.Lock();
      txn->unique_id_ = next_unique_id_;
      next_unique_id_++;
      txn_requests_.Push(txn);
      mutex_.Unlock();
    }
  } else {
    DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
  }
}

void TxnProcessor::MVCCLockWriteKeys(Txn *txn) {
  for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it) {
    storage_->Lock(*it);
  }
}

void TxnProcessor::MVCCUnlockWriteKeys(Txn *txn) {
  for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it) {
    storage_->Unlock(*it);
  }
}

void TxnProcessor::RunMVCCScheduler() {
  while (tp_.Active()) {
    Txn *txn;
    if (txn_requests_.Pop(&txn)) {
      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(this, &TxnProcessor::MVCCExecuteTxn, txn));
    }
  }
}
