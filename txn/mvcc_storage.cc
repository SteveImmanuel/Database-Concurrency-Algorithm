// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000; i++) {
    mvcc_data_[i] = new deque<Version*>();
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;
  }

  mvcc_data_.clear();

  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin(); it != mutexs_.end(); ++it) {
    delete it->second;
  }

  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the
// version_list
void MVCCStorage::Lock(Key key) { mutexs_[key]->Lock(); }

// Unlock the key.
void MVCCStorage::Unlock(Key key) { mutexs_[key]->Unlock(); }

Version* MVCCStorage::GetLargestValidVersion(Key key, int unique_id) {
  deque<Version*>* item_data = mvcc_data_[key];
  size_t i = 0;
  Version* selected_version = (*item_data)[0];

  while (i < item_data->size()) {
    if ((*item_data)[i]->version_id_ <= unique_id) {
      if ((*item_data)[i]->version_id_ > selected_version->version_id_) {
        selected_version = (*item_data)[i];
      }
    }
    i++;
  }

  return selected_version;
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  if (mvcc_data_.count(key)) {  // if key exist
    Lock(key);
    Version* selected_version = GetLargestValidVersion(key, txn_unique_id);
    // set the result to value_ and max_read_id_ of the related data to txn_unique_id
    *result = selected_version->value_;
    selected_version->max_read_id_ = txn_unique_id;
    Unlock(key);
    return true;
  } else {  // if key not exist
    return false;
  }
}

// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  if (mvcc_data_.count(key)) {  // if key exist
    Version* selected_version = GetLargestValidVersion(key, txn_unique_id);

    if (txn_unique_id < selected_version->max_read_id_) {  // TS(txn) < RTS(selected)
      return false;
    } else {
      return true;
    }

  } else {  // if key not exist
    return false;
  }
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  Version* selected_version = GetLargestValidVersion(key, txn_unique_id);
  if (mvcc_data_[key]->size() > 0 && selected_version->version_id_ == txn_unique_id &&
      txn_unique_id != 0) {
    selected_version->value_ = value;
  } else {
    Version* version = new Version();
    if (version != NULL) {
      version->max_read_id_ = txn_unique_id;
      version->value_ = value;
      version->version_id_ = txn_unique_id;
      mvcc_data_[key]->push_back(version);
    }
  }
}
