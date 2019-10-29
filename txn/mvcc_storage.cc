// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0, INSERT);

    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
  // std::cout<<"complete init mvcc storage"<<std::endl;
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    size_t temp = it->second.size();
    for(size_t i = 0; i < temp; i++){
      delete it->second[i];
    }
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  if(mvcc_data_.count(key)){ //if key exist
    Lock(key);
    Version* selected_version;
    size_t i = 0;
    
    //iterate to get largest version that is less than or equal to txn_unique_id
    while(i< mvcc_data_[key].size()){ 
      if( mvcc_data_[key][i]->version_id_ <= txn_unique_id){
        selected_version = mvcc_data_[key][i];
      }
      i++;
    }

    //set the result to value_ and max_read_id_ of the related data to txn_unique_id
    *result = selected_version->value_;
    selected_version->max_read_id_ = txn_unique_id;
    Unlock(key);
    return true;
  }else{ //if key not exist
    return false;
  }
}


// Check whether apply or abort the write
// return :
// INVALID when key doesnt exist
// ROLLEDBACK when TS(txn) < RTS(selected)
// OVERWRITE when TS(txn) = RTS(selected)
// INSERT when TS(txn) > RTS(selected)
// Note that you don't have to call Lock(key) in this method, just
// call Lock(key) before you call this method and call Unlock(key) afterward.
WriteMode MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  if(mvcc_data_.count(key)){ //if key exist
    deque<Version*> item_data = mvcc_data_[key];
    Version* selected_version;
    size_t i = 0;
    
    //iterate to get largest version that is less than or equal to txn_unique_id
    while(i<item_data.size()){ 
      if( item_data[i]->version_id_ <= txn_unique_id){
        selected_version = item_data[i];
      }
      i++;
    }

    if(txn_unique_id < selected_version->max_read_id_){ //TS(txn) < RTS(selected)
      return ROLLEDBACK;
    }else{
      if(txn_unique_id == selected_version->version_id_){ //TS(txn) = WTS(selected)
        return OVERWRITE;
      }else{ //TS(txn) <> WTS(selected)
        return INSERT;
      }
    }

  }else{ //if key not exist
    return INVALID;
  }
}

// MVCC Write, call this method only if CheckWrite return OVERWRITE or INSERT.
// Note that you don't have to call Lock(key) in this method, just
// call Lock(key) before you call this method and call Unlock(key) afterward.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id, WriteMode mode) {
  
  if(mode == INSERT){
    //allocate new version
    Version* new_data = (Version*) malloc(sizeof(Version));

    if (new_data != NULL){
      new_data->value_ = value;
      new_data->max_read_id_ = 0;
      new_data->version_id_ = txn_unique_id;
    }

    mvcc_data_[key].push_back(new_data);
  }else{ //mode = OVERWRITE
    size_t i = 0;
    bool found = false;
    while(i<mvcc_data_[key].size() && !found){
      if( mvcc_data_[key][i]->version_id_ == txn_unique_id){
        mvcc_data_[key][i]->value_ = value;
        found = true;
      }
      i++;
    }
  }

}


