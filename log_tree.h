#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#include <libpmemobj.h>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <chrono>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>
#include<queue>
#include<bitset>
#include <atomic>
// #include <gperftools/profiler.h>
#include <stdexcept>
#include <algorithm>
#include <random>

#include"trie.h"
#include"bucket.h"

#define MAX_KEY         ((uint64_t)(0xffffffffffffffffULL))
#define PAGESIZE 512
#define CACHE_LINE_SIZE 64 
#define IS_FORWARD(c) (c % 2 == 0)
#define TWO_MB   2*1024*1024  //2MB
#define KB_512   1024*512   //512kb
#define MB_1   1024*1024  //1MB
#define MB_4   4*1024*1024
#define MB_8   8*1024*1024
#define interval 2359296  //PMDK allocate  2MB size

// #define DDMS

#define MAX_INDEX  (TWO_MB/64)
#define WB_MAX_INDEX 3
#define NO_DELETE 1
#define DELETE 2

typedef trie::trie_map<char, struct Bucket*> TMap;
TMap radix_trie;
void cal_space2();
void cal_temp();
std::vector<Bucket*> buckets;
uint64_t find_min(struct log* lg);
uint64_t find_max(struct log* lg);
uint64_t baseaddr = 0;

struct migrate_log{
  uint64_t log;
  uint64_t slot_index;
  uint64_t kv_index;
};

void initBuckets() {
  for (int i = 0; i < 11; i++) {
      Bucket* head = new Bucket();
      head->prev = head;
      head->next = head;
      
      buckets.push_back(head);
  }
}

#define USE_PMDK
#define PMEM
uint64_t no_delete = 0;
using type_entry_key = uint64_t;


std::mutex log_mux;
uint64_t head_index=0;  
uint64_t wb_index=0;  
bool isPtr=false;
uint64_t alloc1_count =0;
std::vector<uint64_t>log_man;
using namespace std;
uint64_t not_found = 0;

inline void mfence()
{
  asm volatile("mfence":::"memory");
}

inline void clflush(char *data, int len)
{
  volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
  mfence();
  for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)ptr));
  }
  mfence();
}

string numTostring(uint64_t num);

struct MinMax {
    uint64_t min;
    uint64_t max;
    uint64_t ptr;
    MinMax() : min(0), max(0),ptr(0) {}
    MinMax(uint64_t min, uint64_t max) : min(min), max(max),ptr(ptr) {}
};
class entry{ 
  public :
    type_entry_key key; // 8 bytes
    char* ptr; // 8 bytes
    entry(){
      key = LONG_MAX;
      ptr = NULL;
    }
    friend class page;
    friend class btree;
};

#pragma pack(1)
struct PM_entry {
  entry e;                   // 16 bytes
  char isdelete;             // 1 byte
  std::atomic<uint32_t> version;     // lock_version 4bytes
  PM_entry() : isdelete(0), version(0) {}
    uint32_t get_version() const noexcept {
      return version.load(std::memory_order_acquire);
  }
  bool cas_increment(uint32_t expected_ver) noexcept {
    return version.compare_exchange_strong(
        expected_ver,
        expected_ver + 1,
        std::memory_order_acq_rel,
        std::memory_order_acquire
    );
  }
};
#pragma pack()



struct WB{
  char flag;
  PM_entry kv[WB_MAX_INDEX];
};

struct log{
  struct WB slots[TWO_MB/sizeof(WB)];   
};




void *pm_free(void *obj){
  // PMEMoid e = pmemobj_oid(obj);
  // pmemobj_free(&e);
  
  //printf("pm_free %p\n", obj);
  int index = ((uint64_t)obj-baseaddr)/interval;

  if(index<=0){
    printf("pm_free error, index is %d\n",index);
    return NULL;
  }
  if(radix_trie[numTostring(index)]==NULL){
    printf("radix_trie[%s] is NULL\n",numTostring(index));
    return NULL;
  }
  radix_trie[numTostring(index)]->delete_count = 0;
  radix_trie[numTostring(index)]->temp_count = 0;
  radix_trie[numTostring(index)]->ratio = 0.0;
  radix_trie[numTostring(index)]->is_free = true;
  PMEMoid oid = pmemobj_oid(obj);
  pmemobj_free(&oid);
}



struct Tif {
  uint64_t index;
  struct log *t_log;
  std::thread td;
  uint64_t  wb_index;
  std::vector<PM_entry*>arr;
  uint64_t max_key;
  uint64_t min_key;
  std::mutex mutex;
};



class page;

class btree{
  public:
  int height;
    char* root;
    btree();
    ~btree();
    void setNewRoot(char *);
    void btree_insert_child(type_entry_key, char*, char **pred, bool*);
    void btree_insert_internal(char *, type_entry_key, char *, uint32_t);
    void btree_delete(type_entry_key);
    char *btree_search(type_entry_key, bool *f, char**,page *&,char  **, bool);
    void printAll();
    void insert(type_entry_key, char*,struct Tif *); // Insert
    void recovery(type_entry_key key,char *right);
    void remove(type_entry_key);        // Remove
    char* search(type_entry_key,char **);       // Search
    int scan(type_entry_key, int scan_size, std::vector<uint64_t>&);
    void btree_insert_page(page* , uint64_t , char* ) ;
    char *left();

    friend class page;
};


class meta{
  public:
    page* left_ptr;         // 8 bytes
    page* sib_ptr;          // 8 bytes
    page* pred_ptr;             // 8 bytes
    uint32_t level;             // 4 bytes
    uint8_t switch_counter;     // 1 bytes
    int16_t final_index;         // 2 bytes
    std::mutex *mtx;            // 8 bytes]
    uint64_t reverse;
    friend class page;
    friend class btree;
    meta() {
      mtx = new std::mutex();
      left_ptr = NULL;  
      sib_ptr = NULL;
      pred_ptr = NULL;
      switch_counter = 0;
      final_index = -1;
    }

    ~meta() {
      delete mtx;
    }
};

#ifdef USE_PMDK
POBJ_LAYOUT_BEGIN( log);
POBJ_LAYOUT_TOID( log, struct log);
POBJ_LAYOUT_TOID( log, struct MinMax);
POBJ_LAYOUT_TOID( log, struct migrate_log);
POBJ_LAYOUT_END( log);
PMEMobjpool *pop;
#endif
#define get_mlog() ((struct migrate_log*)pmemobj_direct(pmemobj_root(pop, sizeof(struct migrate_log) * 4)))
#define get_minmax() ((struct MinMax*)pmemobj_direct(pmemobj_root(pop, sizeof(struct MinMax) * 2048)))
struct migrate_log *mlog;
struct MinMax *mm;

void initmm(){
  mm = new MinMax[8192]; 
    for (int i = 0; i < 8192; i++) {
        mm[i].min = 0;
        mm[i].max = 0;
    }
  printf("initmm ok\n");
}

void *alloc(size_t size) {
#ifdef USE_PMDK
  TOID(struct log) p;
  POBJ_ZALLOC(pop, &p, struct log, size);
  alloc1_count++;
  void *ret = pmemobj_direct(p.oid);
  if(ret!=NULL){
    if(baseaddr==0) {
      baseaddr = (uint64_t)ret;
    }
    struct Bucket *b = new struct Bucket();
    b->ptr = (uint64_t)ret; //
    uint64_t no = ((uint64_t)ret-baseaddr)/interval;  //add 1 avoid 0
    string index = numTostring(no);
    log_man.push_back(no);
    radix_trie.insert(index, b);
    headInsert(buckets[9],b);
  }
  return ret;
#else

#endif
}



const int cardinality = (PAGESIZE-sizeof(meta))/sizeof(entry);


class page{
  public:
  meta hdr;  
  entry block[cardinality]; 
    friend class btree;

    page(uint32_t level = 0) {
      hdr.level = level;
      block[0].ptr = NULL;
    }

    page(page* left, type_entry_key key, page* right, uint32_t level = 0) {
      hdr.left_ptr = left;  
      hdr.level = level;
      block[0].key = key;
      block[0].ptr = (char*) right;
      block[1].ptr = NULL;

      hdr.final_index = 0;
    }

    void *operator new(size_t size) {
      void *ret;
      posix_memalign(&ret,64,size);
      return ret;
    }

    inline int count() {
      uint8_t temp_switch_counter;
      int count = 0;
      do {
        temp_switch_counter = hdr.switch_counter;
        count = hdr.final_index + 1;

        while(count >= 0 && block[count].ptr != NULL) {
          if(IS_FORWARD(temp_switch_counter))
            ++count;
          else
            --count;
        } 

        if(count < 0) {
          count = 0;
          while(block[count].ptr != NULL) {
            ++count;
          }
        }

      } while(temp_switch_counter != hdr.switch_counter);

      return count;
    }

    inline bool remove_leaf_key(type_entry_key key) {
      if(IS_FORWARD(hdr.switch_counter)) 
        ++hdr.switch_counter;

      bool shift = false;
      int i;
      int first = 0;
      for(i = 0; block[i].ptr != NULL; ++i) {
       if(!shift && block[i].key == key) {
          block[i].ptr = (i == 0) ? 
            (char *)hdr.left_ptr : block[i - 1].ptr; 
          shift = true;
       }
        if(shift) {
          block[i].key = block[i + 1].key;
          block[i].ptr = block[i + 1].ptr;
        }
      }

      if(shift) {
        --hdr.final_index;
      }
      return shift;
    }

    bool remove(btree* bt, type_entry_key key, bool only_rebalance = false, bool with_lock = true) {
      hdr.mtx->lock();

      bool ret = remove_leaf_key(key);

      hdr.mtx->unlock();

      return ret;
    }


    inline void insert_leaf_key(type_entry_key key, char* ptr, int *count_entries, bool flush = true,
        bool update_final_index = true) {
          if(!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

          if(*count_entries == 0) {  // this page is empty
            entry* new_entry = (entry*) &block[0];
            entry* array_end = (entry*) &block[1];
            new_entry->key = (type_entry_key) key;
            new_entry->ptr = (char*) ptr;

            array_end->ptr = (char*)NULL;

          }
          else {
            int i = *count_entries - 1, inserted = 0;
            block[*count_entries+1].ptr = block[*count_entries].ptr; 
          
          for(i = *count_entries - 1; i >= 0; i--) {
            if(key < block[i].key ) {
              block[i+1].ptr = block[i].ptr;
              block[i+1].key = block[i].key;
            }
            else{
              block[i+1].ptr = block[i].ptr;
              block[i+1].key = key;
              block[i+1].ptr = ptr;
              inserted = 1;
              break;
            }
          }
          if(inserted==0){
            block[0].ptr =(char*) hdr.left_ptr;
            block[0].key = key;
            block[0].ptr = ptr;
          }
        }

        if(update_final_index) {
          hdr.final_index = *count_entries;
        }
        ++(*count_entries);
      }


    page *put(btree* bt, char* left, type_entry_key key, char* right,
       bool flush, bool with_lock, page *invalid_sib = NULL) {
        if(with_lock) {
          hdr.mtx->lock(); // Lock the write lock
        }

        register int count_entries = count();

        for (int i = 0; i < count_entries; i++)
          if (key == block[i].key) {
            block[i].ptr = right;
            if (with_lock)
              hdr.mtx->unlock();
            return this;
          }


        if(hdr.sib_ptr && (hdr.sib_ptr != invalid_sib)) {
          // Compare this key with the first key of the sibling
          if(key > hdr.sib_ptr->block[0].key) {
            if(with_lock) { 
              hdr.mtx->unlock(); // Unlock the write lock
            }
            return hdr.sib_ptr->put(bt, NULL, key, right, 
                true, with_lock, invalid_sib);
          }
        }
        if(count_entries < cardinality - 1) {
          insert_leaf_key(key, right, &count_entries, flush);

          if(with_lock) {
            hdr.mtx->unlock(); // Unlock the write lock
          }

          return this;
        }
        else {

          page* sibling = new page(hdr.level); 
          register int m = (int) ceil(count_entries/2);
          type_entry_key split_key = block[m].key;

          int sibling_cnt = 0;
          if(hdr.left_ptr == NULL){ 
            for(int i=m; i<count_entries; ++i){ 
              sibling->insert_leaf_key(block[i].key, block[i].ptr, &sibling_cnt, false);
            }
          }
          else{ 
            for(int i=m+1;i<count_entries;++i){ 
              sibling->insert_leaf_key(block[i].key, block[i].ptr, &sibling_cnt, false);
            }
            sibling->hdr.left_ptr = (page*) block[m].ptr;
          }

          sibling->hdr.sib_ptr = hdr.sib_ptr;
          sibling->hdr.pred_ptr = this;
          if (sibling->hdr.sib_ptr != NULL)
            sibling->hdr.sib_ptr->hdr.pred_ptr = sibling;
          hdr.sib_ptr = sibling;

          // set to NULL
          if(IS_FORWARD(hdr.switch_counter))
            hdr.switch_counter += 2;
          else
            ++hdr.switch_counter;
          block[m].ptr = NULL;
          hdr.final_index = m - 1;
          count_entries = hdr.final_index + 1;

          page *ret;

          // insert the key
          if(key < split_key) {
            insert_leaf_key(key, right, &count_entries);
            ret = this;
          }
          else {
            sibling->insert_leaf_key(key, right, &sibling_cnt);
            ret = sibling;
          }


          if(bt->root == (char *)this) { // only one node can update the root ptr
            page* new_root = new page((page*)this, split_key, sibling, 
                hdr.level + 1);
            bt->setNewRoot((char *)new_root);

            if(with_lock) {
              hdr.mtx->unlock(); // Unlock the write lock
            }
          }
          else {
            if(with_lock) {
              hdr.mtx->unlock(); // Unlock the write lock
            }
            bt->btree_insert_internal(NULL, split_key, (char *)sibling, 
                hdr.level + 1);
          }

          return ret;
        }

      }
    inline void insert_leaf_key(type_entry_key key, char* ptr, int *count_entries, char **pred, bool flush = true,
          bool update_final_index = true) {
        // update switch_counter
        if(!IS_FORWARD(hdr.switch_counter))
          ++hdr.switch_counter;

        if(*count_entries == 0) {  // this page is empty
          entry* new_entry = (entry*) &block[0];
          entry* array_end = (entry*) &block[1];
          new_entry->key = (type_entry_key) key;
          new_entry->ptr = (char*) ptr;

          array_end->ptr = (char*)NULL;

          if (hdr.pred_ptr != NULL)
            *pred = hdr.pred_ptr->block[hdr.pred_ptr->count() - 1].ptr;
        }
        else {
          int i = *count_entries - 1, inserted = 0;
          block[*count_entries+1].ptr = block[*count_entries].ptr; 
          
          // FAST
          for(i = *count_entries - 1; i >= 0; i--) {
            if(key < block[i].key ) {
              block[i+1].ptr = block[i].ptr;
              block[i+1].key = block[i].key;
            }
            else{
              block[i+1].ptr = block[i].ptr;
              block[i+1].key = key;
              block[i+1].ptr = ptr;
              *pred = block[i].ptr;
              inserted = 1;
              break;
            }
          }
          if(inserted==0){
            block[0].ptr =(char*) hdr.left_ptr;
            block[0].key = key;
            block[0].ptr = ptr;
            if (hdr.pred_ptr != NULL)
              *pred = hdr.pred_ptr->block[hdr.pred_ptr->count() - 1].ptr;
          }
        }

        if(update_final_index) {
          hdr.final_index = *count_entries;
        }
        ++(*count_entries);
      }


    char *linear_search(type_entry_key key) {
      int i = 1;
      uint8_t temp_switch_counter;
      char *ret = NULL;
      char *t; 
      type_entry_key k;

      if(hdr.left_ptr == NULL) { // Search a leaf node
        do {
          temp_switch_counter = hdr.switch_counter;
          ret = NULL;

          // search from left ro right
          if(IS_FORWARD(temp_switch_counter)) { 
            if((k = block[0].key) == key) { 
              if((t = block[0].ptr) != NULL) {
                if(k == block[0].key) {
                  ret = t;
                  continue;
                }
              }
            }

            for(i=1; block[i].ptr != NULL; ++i) { 
              if((k = block[i].key) == key) {
                if(block[i-1].ptr != (t = block[i].ptr)) {
                  if(k == block[i].key) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
          else { // search from right to left
            for(i = count() - 1; i > 0; --i) {
              if((k = block[i].key) == key) {
                if(block[i - 1].ptr != (t = block[i].ptr) && t) {
                  if(k == block[i].key) {
                    ret = t;
                    break;
                  }
                }
              }
            }

            if(!ret) {
              if((k = block[0].key) == key) {
                if(NULL != (t = block[0].ptr) && t) {
                  if(k == block[0].key) {
                    ret = t;
                    continue;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != temp_switch_counter);

        if(ret) {
          return ret;
        }

        if((t = (char *)hdr.sib_ptr) && key >= ((page *)t)->block[0].key)
          return t;

        return NULL;
      }
      else { // internal node
        do {
          temp_switch_counter = hdr.switch_counter;
          ret = NULL;

          if(IS_FORWARD(temp_switch_counter)) {
            if(key < (k = block[0].key)) {
              if((t = (char *)hdr.left_ptr) != block[0].ptr) { 
                ret = t;
                continue;
              }
            }

            for(i = 1; block[i].ptr != NULL; ++i) { 
              if(key < (k = block[i].key)) { 
                if((t = block[i-1].ptr) != block[i].ptr) {
                  ret = t;
                  break;
                }
              }
            }

            if(!ret) {
              ret = block[i - 1].ptr; 
              continue;
            }
          }
          else { // search from right to left
            for(i = count() - 1; i >= 0; --i) {
              if(key >= (k = block[i].key)) {
                if(i == 0) {
                  if((char *)hdr.left_ptr != (t = block[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
                else {
                  if(block[i - 1].ptr != (t = block[i].ptr)) {
                    ret = t;
                    break;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != temp_switch_counter);

        if((t = (char *)hdr.sib_ptr) != NULL) {
          if(t!=NULL &&key >= ((page *)t)->block[0].key)
            return t;
        }

        if(ret) {
          return ret;
        }
        else
          return (char *)hdr.left_ptr;
      }

      return NULL;
    }

    char *linear_search_child(type_entry_key key, char **pred, page **tp,char **en ,bool debug=false) {
      int i = 1;
      uint8_t temp_switch_counter;
      char *ret = NULL;
      char *t; 
      type_entry_key k, k1;

      if(hdr.left_ptr == NULL) { // Search a leaf node
        do {
          temp_switch_counter = hdr.switch_counter;
          ret = NULL;

          // search from left to right
          if(IS_FORWARD(temp_switch_counter)) {
            if (debug) {
              printf("search from left to right\n");
              printf("page:\n");
              printAll();
            }
            k = block[0].key;
            if (key < k) {
              if (hdr.pred_ptr != NULL){
                *pred = hdr.pred_ptr->block[hdr.pred_ptr->count() - 1].ptr;
                if (debug)
                  printf("line 752, *pred=%p\n", *pred);
              }
            }
            if (key > k){
              *pred = block[0].ptr;
              if (debug)
                printf("line 757, *pred=%p\n", *pred);
            }
              

            if(k == key) {
              if (hdr.pred_ptr != NULL) {
                *pred = hdr.pred_ptr->block[hdr.pred_ptr->count() - 1].ptr;
                if (debug)
                  printf("line 772, *pred=%p\n", *pred);
              }
              if((t = block[0].ptr) != NULL) {
                if(k == block[0].key) {
                  *en=(char *)&(block[0]);
                  ret = t;
                  continue;
                }
              }
            }

            for(i=1; block[i].ptr != NULL; ++i) { 
              k = block[i].key;
              k1 = block[i - 1].key;
              if (k < key){
                *pred = block[i].ptr;
                if (debug)
                  printf("line 775, *pred=%p\n", *pred);
              }
              if(k == key) {
                if(block[i-1].ptr != (t = block[i].ptr)) {
                  if(k == block[i].key) {
                    *en=(char *)&(block[i]);
                    ret = t;
                    break;
                  }
                }
              }
            }
          }else { // search from right to left
            if (debug){
              printf("search from right to left\n");
              printf("page:\n");
              printAll();
            }
            bool once = true;
            
            for (i = count() - 1; i > 0; --i) {
              if (debug)
                printf("line 793, i=%d, block[i].key=%d\n", i,block[i].key);
              k = block[i].key;
              k1 = block[i - 1].key;
              if (k1 < key && once) {
                *pred = block[i - 1].ptr;
                if (debug)
                  printf("line 794, *pred=%p\n", *pred);
                once = false;
              }
              if(k == key) {
                if(block[i - 1].ptr != (t = block[i].ptr) && t) {
                  if(k == block[i].key) {
                    ret = t;
                    *en=(char *)&(block[i]);
                    break;
                  }
                }
              }
            }

            if(!ret) {
              k = block[0].key;
              if (key < k){
                if (hdr.pred_ptr != NULL){
                  *pred = hdr.pred_ptr->block[hdr.pred_ptr->count() - 1].ptr;
                  if (debug)
                    printf("line 811, *pred=%p\n", *pred);
                }
              }
              if (key > k)
                *pred = block[0].ptr;
              if(k == key) {
                if (hdr.pred_ptr != NULL) {
                  *pred = hdr.pred_ptr->block[hdr.pred_ptr->count() - 1].ptr;
                  if (debug)
                    printf("line 844, *pred=%p\n", *pred);
                }
                if(NULL != (t = block[0].ptr) && t) {
                  if(k == block[0].key) {
                    ret = t;
                    *en=(char *)&(block[0]);
                    continue;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != temp_switch_counter);
        if(ret) {
          *tp = this;
          return ret;
        }

        if((t = (char *)hdr.sib_ptr) && key >= ((page *)t)->block[0].key){
          *tp = this;
          *en=(char *)&(block[i-1]);
          return t;
        }
          
        return NULL;
      }
      else { // internal node
        do {
          temp_switch_counter = hdr.switch_counter;
          ret = NULL;

          if(IS_FORWARD(temp_switch_counter)) {
            if(key < (k = block[0].key)) {
              if((t = (char *)hdr.left_ptr) != block[0].ptr) { 
                ret = t;
                *en=(char *)&(block[i]);
                continue;
              }
            }

            for(i = 1; block[i].ptr != NULL; ++i) { 
              if(key < (k = block[i].key)) { 
                if((t = block[i-1].ptr) != block[i].ptr) {
                  ret = t;
                  *en=(char *)&(block[i]);
                  break;
                }
              }
            }

            if(!ret) {
              ret = block[i - 1].ptr; 
              *en=(char *)&(block[i-1]);
              continue;
            }
          }
          else { // search from right to left
            for(i = count() - 1; i >= 0; --i) {
              if(key >= (k = block[i].key)) {
                if(i == 0) {
                  if((char *)hdr.left_ptr != (t = block[i].ptr)) {
                    ret = t;
                    *en=(char *)&(block[i]);
                    break;
                  }
                }
                else {
                  if(block[i - 1].ptr != (t = block[i].ptr)) {
                    ret = t;
                    *en=(char *)&(block[i]);
                    break;
                  }
                }
              }
            }
          }
        } while(hdr.switch_counter != temp_switch_counter);

        if((t = (char *)hdr.sib_ptr) != NULL) {
          if(key >= ((page *)t)->block[0].key){
            *tp = this;
            *en=(char *)&(((page *)t)->block[0]);
            return t;
          }
            
        }

        if(ret) {
          *tp = this;
          return ret;
        }
        else{
          *tp = this;
          return (char *)hdr.left_ptr;
        }
      }
      return NULL;
    }
    // print a node 
    void print() {
      if(hdr.left_ptr == NULL) {
        //printf("hdr->level is %lu\n",hdr.level);
        for(int i=0;block[i].ptr != NULL;++i)
          {
            printf("%lu,%lu ", block[i].key, block[i].ptr);
          }
      }

    }



    void printAll() {
      if(hdr.left_ptr == NULL) {
        printf("printing leaf node: ");
        print();
      }
      else {
        print();
        ((page*) hdr.left_ptr)->printAll();
        for(int i=0;block[i].ptr != NULL;++i){
          ((page*) block[i].ptr)->printAll();
        }
      }
    }
};

#ifdef USE_PMDK
int file_exists(const char *filename) {
  struct stat buffer;
  return stat(filename, &buffer);
}

void openPmemobjPool() {
  printf("use pmdk!\n");
  char pathname[100] = "/pmem0/Log-Btree";
  int sds_write_value = 0;
  pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
  if (file_exists(pathname) != 0) {
    printf("create new one.\n");
    if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(btree),
                              (uint64_t)170 * 1024 * 1024 * 1024, 0666)) ==
        NULL) {
      perror("failed to create pool.\n");
      return;
    }
  } else {
    printf("open existing one.\n");
    if ((pop = pmemobj_open(pathname, POBJ_LAYOUT_NAME(btree))) == NULL) {
      perror("failed to open pool.\n");
      return;
    }
  }
}
#endif



/*
 * class btree
 */
btree::btree(){
  root = (char*)new page();
  height = 1;
}

btree::~btree() { 
#ifdef USE_PMDK
  pmemobj_close(pop); 
#endif
}

void btree::setNewRoot(char *new_root) {
  this->root = (char*)new_root;
  ++height;
}

char *btree::btree_search(type_entry_key key, bool *f, char **other, page*& resultPage,char **en,bool debug=false){
  page* p = (page*)root;

  while(p->hdr.left_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }
  
  page *t;
 // page* resultPage;
  page** tp = &resultPage;
  while((t = (page *)p->linear_search_child(key, other,tp, en,debug)) == p->hdr.sib_ptr) {
    p = t;
    if(!p) {
      break;
    }
  }
  if (tp==NULL){
    printf("this page is NULL\n");
  }

  if(!t) {
    //printf("NOT FOUND %lu, t = %p\n", key, t);
    *f = false;
    return NULL;
  }

  *f = true;
  return (char *)t;
}


char *btree::search(type_entry_key key,char **en) {
  bool f = false;
  char *other;
  page *res;
  char *ptr = btree_search(key, &f, &other,res,en);
  if (f) {
    retry:
    struct PM_entry *n = (struct PM_entry *)ptr;
    char *ans = NULL;
    uint32_t current_ver = n->get_version();
    if (n->e.ptr !=NULL&&n->isdelete==NO_DELETE){
      ans = n->e.ptr;
      if(n->get_version() != current_ver) goto retry;
      return ans; 
    }else{
      //++;
      //printf("search ERROR\n");
    }
  } else {
    not_found++;
  }
  return NULL;
}


void btree::btree_insert_child(type_entry_key key, char* right, char **pred, bool *update){ //need to be string
  page* p = (page*)root;

  while(p && p->hdr.left_ptr != NULL) { 
    p = (page*)p->linear_search(key);
  }
  if(!p->put(this, NULL, key, right, true, true)) { 

    *update = true;
  } else {
    *update = false;
  }
}
int btree::scan(type_entry_key key, int scan_size, std::vector<uint64_t>&buf) {
  bool f = false;
  char *other;
  page *res;
  char *in=NULL;
  char **en = &in;
  char *ptr = btree_search(key, &f, &other,res,en);
  int scanned = 0;
  page* p = (page*)ptr;
  if (!f){
    return  0;
  }
  int index = 0;
  for (;index<res->hdr.final_index;index++){
    if (res->block[index].key==key) break;
  }
  while(scanned<scan_size&&res !=NULL){
    for(;scanned<scan_size&&index<res->hdr.final_index;index++){
      struct PM_entry *p = (struct PM_entry *)res->block[index].ptr;
      if(p!=NULL&&p->isdelete==NO_DELETE){
        retry:
        uint32_t current_ver = p->get_version();
        uint64_t pptr = (uint64_t)p->e.ptr;
        if(current_ver != p->get_version())goto retry;
        buf[scanned] = (uint64_t)p->e.ptr;
        scanned ++;
      }
    }
    res = res->hdr.sib_ptr;
    index = 0;
  }
  //for (int i =0;i<scan_size;i++)printf("buffer[%d] is %lu\n",i,buf[i]);
  return scanned;
}

void btree::recovery(type_entry_key key,char *right){
  entry *p = NULL;
  bool update;
  btree_insert_child(key,right, (char **)&p, &update); 
}


void btree::insert(type_entry_key key, char *right ,struct Tif *t){
  bool f = false;
  char *other=NULL,*in=NULL;
  page *res;
  char **en = &in;
  char *ptr = btree_search(key, &f, &other,res,en);
  if(f){  
    struct PM_entry *p = (struct PM_entry *)ptr;
    uint32_t current_ver = p->get_version();
    do{
      uint32_t new_ver = current_ver+1;
      if(p->cas_increment(current_ver)){
        std::atomic_thread_fence(std::memory_order_release);
        p->e.ptr = right;
        clflush((char *)p,sizeof(struct PM_entry));
        std::atomic_thread_fence(std::memory_order_release);
        break;
      }
      current_ver = p->get_version();
    }while(true);
    return ; 
  }else{  //not find,insert 
    t->t_log->slots[t->index].kv[t->wb_index].e.key = key;
    t->t_log->slots[t->index].kv[t->wb_index].e.ptr = right;
    t->t_log->slots[t->index].kv[t->wb_index].isdelete = NO_DELETE;
    if(key>t->max_key) t->max_key = key;
    if(key<t->min_key) t->min_key = key;
    t->t_log->slots[t->index].flag = t->wb_index;
    clflush((char *)&t->t_log->slots[t->index], CACHE_LINE_SIZE);
    char *addr = (char *)&t->t_log->slots[t->index].kv[t->wb_index];
    t->wb_index ++;
    if(t->wb_index>=WB_MAX_INDEX){
      t->wb_index = 0;
      t->index ++;
    }
    entry *p = NULL;
    bool update;
    btree_insert_child(key,addr, (char **)&p, &update); 
    if(t->index>=MAX_INDEX){ //full
      log_mux.lock();
      int i = ((uint64_t)t->t_log-baseaddr)/interval;
      mm[i].min = t->min_key;
      mm[i].max = t->max_key;
      mm[i].ptr = (uint64_t)t->t_log;
      t->min_key = MAX_KEY;
      t->max_key = 0;
      struct log *new_log = (struct log *)alloc(sizeof(struct log));
      t->index = 0;
      t->t_log = new_log;
      t->wb_index = 0;
      log_mux.unlock();
    }
  }
}


void btree::remove(type_entry_key key) {
  bool f=false;
  bool debug=false;
  char *cur = NULL, *other = NULL;
  page *res;
  char *in=NULL;
  char **en = &in;
  char *ptr = btree_search(key, &f, &other,res,en);
  if (f) {  
    struct PM_entry *p = (struct PM_entry *)ptr;
    uint32_t current_ver = p->get_version();
    if (p->isdelete==NO_DELETE){
      if(p->cas_increment(current_ver)) {
        std::atomic_thread_fence(std::memory_order_release);
        p->isdelete = DELETE;
        clflush((char *)p,sizeof(struct PM_entry));
        std::atomic_thread_fence(std::memory_order_release);
      }
      #ifdef DDMS
        string index = numTostring(((uint64_t)p-baseaddr)/interval);
        int no = ((uint64_t)p-baseaddr)/interval;
        if(key==mm[no].max)mm[no].max = find_max((struct log*)radix_trie[index]->ptr);
        if(key==mm[no].min)mm[no].min = find_min((struct log*)radix_trie[index]->ptr);
        radix_trie[index]->delete_count ++ ;
        radix_trie[index]->temp_count ++ ;
        if(radix_trie[index]->temp_count>=1000){
          double new_ratio = ((double)radix_trie[index]->delete_count )/double(WB_MAX_INDEX*MAX_INDEX);
          radix_trie[index]->ratio = new_ratio;
          radix_trie[index]->temp_count = 0;
        }
      #endif 
    }
    btree_delete(key);
    return;
  }else{
    not_found ++;
    return ;
  }
}






void btree::btree_insert_internal(char *left, type_entry_key key, char *right, uint32_t level) {
  if(level > ((page *)root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while(p->hdr.level > level) 
    p = (page *)p->linear_search(key);

  if(!p->put(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btree_delete(type_entry_key key) {
  page* p = (page*)root;

  while(p->hdr.left_ptr != NULL){
    p = (page*) p->linear_search(key);
  }

  page *t;
  while((t = (page *)p->linear_search(key)) == p->hdr.sib_ptr) {
    p = t;
    if(!p)
      break;
  }

  if(p) {
    if(!p->remove(this, key)) {
      no_delete ++;
      //btree_delete(key);
    }
  }
  else {
    //printf("not found the key to delete %lu\n", key);
  }
}

void btree::printAll(){
  int total_keys = 0;
  page *leftmost = (page *)root;
  printf("root: %x\n", root);
  do {
    page *sibling = leftmost;
    while(sibling) {
      if(sibling->hdr.level == 0) {
        total_keys += sibling->hdr.final_index + 1;
      }
      //sibling->print();
      sibling = sibling->hdr.sib_ptr;
    }
    printf("-----------------------------------------\n");
    leftmost = leftmost->hdr.left_ptr;
  } while(leftmost);

  printf("total number of keys: %d\n", total_keys);
}


void add_log_head(struct log *lg,uint64_t ptr){
   if (head_index <MAX_INDEX){
        if(wb_index==WB_MAX_INDEX){
            wb_index = 0;
            isPtr = false;
            head_index ++;
        }
        if (!isPtr){  //
            lg->slots[head_index].kv[wb_index].e.key = ptr;
            clflush((char *)&lg->slots[head_index].kv[wb_index].e.key,8);
            isPtr = true;
        }else{
            lg->slots[head_index].kv[wb_index].e.ptr = (char *)ptr;
            lg->slots[head_index].kv[wb_index].isdelete = NO_DELETE;
            clflush((char *)&lg->slots[head_index],CACHE_LINE_SIZE);
            isPtr = false;
            wb_index ++;
        }
    }else{ //reallocate new log  //TODO
    }
}




void cal_space2(){
  uint64_t all_kv  =0;
  uint64_t all_log = 0;
  uint64_t delete_count = 0;
  for (int i =0;i<log_man.size();i++){
    //if(!radix_trie[numTostring(i)]->is_free){ //no free
      struct log *lg = (struct log*)(baseaddr+log_man[i]*interval);
      all_log ++;
      delete_count = 0;
      int ktmp = 0 ;
      for (uint64_t k =0;k<MAX_INDEX;k++){
          for (int v = 0;v<WB_MAX_INDEX;v++){
              if(lg->slots[k].kv[v].isdelete== NO_DELETE) {
                  all_kv ++;
                  ktmp ++;
              }else delete_count ++;
          }
      }
      //printf("blocks is %p,delete_count is %lu\n",log_man[i],delete_count);
    }
 // }
  printf("cal_space2  all_kv is %lu  all_log is %lu\n",all_kv,all_log);
}


int tnok = 1;
int migrate_count = 0;


std::atomic<bool> stop_merge{false};
void TN_merge(btree* bt,double T,int s){
  re:
  while (!stop_merge.load()) {
        sleep(s);
        int mc = 0;
        while (!stop_merge.load()) {
            mc++;
  reclassifyBuckets(buckets);  //
  uint64_t src_index = 0;
  uint64_t src_inner_index = 0;
  std::vector<Bucket *>temp_bucket = getsumone(buckets,T);  //
  if(temp_bucket.size() == 0) {
    continue;
  }
  std::vector<struct log*> temp_log;
  double sumratio =0;
  for (int i = 0; i < temp_bucket.size(); i++) {
    if(temp_bucket[i] !=0 && temp_bucket[i]->ptr != 0 && temp_bucket[i]->delete_count > 0) {
      temp_log.push_back((struct log*)temp_bucket[i]->ptr);
    } 
    sumratio += temp_bucket[i]->ratio;
  }
  
  if(sumratio<1) goto re;
  if(temp_log.size()==0) break;
  int maxIndex = getmax(temp_bucket);
  struct log * src_log = temp_log[maxIndex];
  int tar_index = 0,tar_kv_index = 0,tar_slot_index = 0;
  uint64_t  src_no_delete_count = 0;
  struct migrate_log tlog[4];
  //choose
  //printf("start migrate\n");
  for(uint64_t i =0 ;i <MAX_INDEX;i++){
    int mcount = 0;
    for (int j = 0;j < WB_MAX_INDEX;j++){
      if (src_log->slots[i].kv[j].isdelete == NO_DELETE) {
        src_no_delete_count++;
        tlog[0].log = (uint64_t)src_log;
        tlog[0].slot_index = i;
        mcount ++;
      }
    }
    if (mcount ==0) continue;
    int t_index =1,temp_index = mcount;
    while(tar_index < temp_log.size()&&temp_index>0){  
      if (tar_index == maxIndex){
        tar_index++;
        tar_slot_index = 0;
        tar_kv_index = 0;
        continue;
      }
      if(tar_kv_index >= WB_MAX_INDEX){
        tar_slot_index ++;
        tar_kv_index = 0;
      }
      if (tar_slot_index >= MAX_INDEX) {
        tar_index++;
        tar_slot_index = 0;
        tar_kv_index = 0;
        continue;
      }
      if (temp_log[tar_index]->slots[tar_slot_index].kv[tar_kv_index].isdelete == DELETE) {
        tlog[t_index].log = (uint64_t)temp_log[tar_index]; 
        tlog[t_index].slot_index = tar_slot_index;
        tlog[t_index].kv_index = tar_kv_index;
        t_index ++;
        temp_index --;
      }
      tar_kv_index++;
    }
    memcpy(mlog,tlog,sizeof(tlog));
    clflush((char *)(&mlog), CACHE_LINE_SIZE);
    int kindex = 1;
    //migrate
    for(int i =0;i<WB_MAX_INDEX&&kindex<t_index;i++){
      struct log * src_log = (struct log*)tlog[0].log;
      uint64_t src_slot_index = tlog[0].slot_index;
      uint64_t src_kv_index = i;
      if(src_log->slots[src_slot_index].kv[src_kv_index].isdelete == DELETE||src_log->slots[src_slot_index].kv[src_kv_index].isdelete==0){
        continue;
      }
      struct log * dest_log = (struct log*)tlog[kindex].log;
      uint64_t dest_slot_index = tlog[kindex].slot_index;
      uint64_t dest_kv_index = tlog[kindex].kv_index;
      
      if(dest_log!=NULL) {  
        PM_entry *src_entry = &src_log->slots[src_slot_index].kv[src_kv_index]; 
        uint64_t key = src_entry->e.key;
        char *ptr = src_entry->e.ptr;
        if (key == 0 || ptr == nullptr)  printf("this error\n");
        dest_log->slots[dest_slot_index].kv[dest_kv_index].e.key = key;
        dest_log->slots[dest_slot_index].kv[dest_kv_index].e.ptr = ptr;
        dest_log->slots[dest_slot_index].kv[dest_kv_index].isdelete = NO_DELETE;
        clflush((char*)&dest_log->slots[dest_slot_index].kv[dest_kv_index], sizeof(PM_entry));
        //update min and max
        int no = ((uint64_t)dest_log-baseaddr)/interval;
        if(key>mm[no].max) mm[no].max = key;
        if(key<mm[no].min) mm[no].min = key;
        bool key_found = false;
        char* other = nullptr;
        page* res = nullptr;
        char* en_ptr = nullptr;
        char** en = &en_ptr;
        char* pptr = bt->btree_search(key, &key_found, &other, res, en);
        if (en!=NULL){
          entry *pm_e = (entry *)(*en);
          pm_e->key = key;
          pm_e->ptr = (char *)(&dest_log->slots[dest_slot_index].kv[dest_kv_index]);
          clflush((char *)(&dest_log->slots[dest_slot_index]), CACHE_LINE_SIZE);
        }else{ 
          printf("Error: Key %lu not found in B-tree or ptr is NULL\n", key);
        }
      }
      string index = numTostring(((uint64_t)temp_log[tar_index]-baseaddr)/interval);
      radix_trie[index]->delete_count --; //delete
      kindex++;
      src_log->slots[src_slot_index].kv[src_kv_index].isdelete = DELETE;
      clflush((char*)&src_log->slots[src_slot_index].kv[src_kv_index], sizeof(PM_entry));//flush
    }
  }

 // printf("log is %p ,src_no_delete_count is %lu\n",src_log,src_no_delete_count);
  pm_free((void*)src_log);

  migrate_count ++;
  removeFromList(buckets[temp_bucket[maxIndex]->ratio*10],temp_bucket[maxIndex]); //
  for (int i =0;i<temp_bucket.size();i++){
    if(i != maxIndex){
      string index = numTostring(((uint64_t)temp_log[i]-baseaddr)/interval);
      radix_trie[index]->ratio = radix_trie[index]->delete_count/double(WB_MAX_INDEX*MAX_INDEX);
    }
  }
  }
}
txtx:
printf("ending\n");
return;
}



uint64_t find_max(struct log* lg){
  uint64_t res =0;
  for(int i =0;i<MAX_INDEX;i++){
    for(int j =0;j<WB_MAX_INDEX;j++){
      if (lg->slots[i].kv[j].isdelete == NO_DELETE&&lg->slots[i].kv[j].e.key > res) {
        res = lg->slots[i].kv[j].e.key;
      }
    }
  }
  return res;
}

uint64_t find_min(struct log* lg){
  uint64_t res =MAX_KEY;
  for(int i =0;i<MAX_INDEX;i++){
    for(int j =0;j<WB_MAX_INDEX;j++){
      if (lg->slots[i].kv[j].isdelete == NO_DELETE&&lg->slots[i].kv[j].e.key < res) {
        res = lg->slots[i].kv[j].e.key;
      }
    }
  }
  return res;
}

void printfmm(){
  for (int i =0;i<504;i++){
    printf("i is %d, min is %lu, max is %lu\n",i,mm[i].min,mm[i].max);
  }
}

string numTostring(uint64_t num){
  char temp[64];
  sprintf(temp, "%lu", num);
  string s(temp);
  return s;
}
