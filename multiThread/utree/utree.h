#pragma once

#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#ifdef USE_PMDK
#include <libpmemobj.h>
#endif
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <vector>
// #include <gperftools/profiler.h>

#define CACHE_LINE_SIZE 64
#define IS_FORWARD(c) (c % 2 == 0)

#ifndef KEYSIZE
#define KEYSIZE 1
#endif
constexpr size_t key_size = KEYSIZE;
using entry_key_t = std::array<uint64_t, key_size>;


pthread_mutex_t print_mtx;

const uint64_t SPACE_PER_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
const uint64_t SPACE_OF_MAIN_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
extern __thread char *start_addr;
extern __thread char *curr_addr;

using namespace std;

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

template <typename T = int64_t>
struct list_node_t {
    T value;
    entry_key_t key;
    bool isUpdate;
    bool isDelete;
    struct list_node_t *next;
    void printAll();
};

template <typename T>
void list_node_t<T>::printAll() {
    printf("addr=%p, key=%d, ptr=%u, isUpdate=%d, isDelete=%d, next=%p\n",
                    this, this->key, this->value, this->isUpdate, this->isDelete, this->next);
}
#ifdef USE_PMDK
POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_TOID(btree, list_node_t);
POBJ_LAYOUT_END(btree);
PMEMobjpool *pop;
#endif
template <typename T>
T *alloc() {
        auto size = sizeof(T);
#ifdef USE_PMDK
    TOID(list_node_t) p;
    POBJ_ZALLOC(pop, &p, list_node_t, size);
    return pmemobj_direct(p.oid);
#else
    auto ret = reinterpret_cast<T*>(curr_addr);
    if (reinterpret_cast<size_t>(curr_addr) % 4 != 0)
    {
        std::cerr << "Unaligned allocation at " << reinterpret_cast<size_t>(curr_addr) << " with size " << size << std::endl;
    }
    memset(ret, 0, size);
    curr_addr += size;
    if (curr_addr >= start_addr + SPACE_PER_THREAD) {
        printf("start_addr is %p, curr_addr is %p, SPACE_PER_THREAD is %lu, no "
                     "free space to alloc\n",
                     start_addr, curr_addr, SPACE_PER_THREAD);
        exit(0);
    }
    return ret;
#endif
}

template <typename T>
class page;

template <typename T>
class btree{
private:
    int height;
    page<T>* root;

public:
    list_node_t<T> *list_head = nullptr;
    btree();
    ~btree();
    size_t getMemoryUsed();
    size_t getPersistentMemoryUsed();
    void setNewRoot(page<T> *);
    void getNumberOfNodes();
    void btree_insert_pred(entry_key_t, char*, char **pred, bool*);
    void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
    void btree_delete(entry_key_t);
    char *btree_search(entry_key_t);
    char *btree_search_pred(entry_key_t, bool *f, char**, bool debug = false);
    void printAll();
    T* insert(entry_key_t, T);       // Insert
    void remove(entry_key_t);        // Remove
    T* search(entry_key_t);          // Search

    void print()
    {
        int i = 0;
        list_node_t<T> *tmp = list_head;
        while (tmp->next != nullptr) {
            //printf("%d-%d\t", tmp->next->key, tmp->next->ptr);
            tmp = tmp->next;
            printf("node=%d, ", i);
            tmp->printAll();
            i++;
        }
        printf("\n");
    }
    friend class page<T>;
};


template <typename T>
class header{
private:
    page<T>* leftmost_ptr;      // 8 bytes
    page<T>* sibling_ptr;       // 8 bytes
    page<T>* pred_ptr;          // 8 bytes
    uint32_t level;             // 4 bytes
    uint8_t switch_counter;     // 1 bytes
    uint8_t is_deleted;         // 1 bytes
    int16_t last_index;         // 2 bytes
    std::mutex *mtx;            // 8 bytes

    friend class page<T>;
    friend class btree<T>;

public:
    header() {
        mtx = new std::mutex();

        leftmost_ptr = nullptr;
        sibling_ptr = nullptr;
        pred_ptr = nullptr;
        switch_counter = 0;
        last_index = -1;
        is_deleted = false;
    }

    ~header() {
        delete mtx;
    }
};

template <typename T>
class entry{
private:
    entry_key_t key;
    char* ptr; // 8 bytes

public :
    entry(){
        key = {ULONG_MAX};
        ptr = nullptr;
    }

    friend class page<T>;
    friend class btree<T>;
};


constexpr size_t nextPowerOf2(size_t n)
{
    size_t ret = 1;
    while( n > ret) {
        ret <<= 1;
    }
    return ret;
}

template <typename T>
class page{

    constexpr static size_t PAGESIZE = nextPowerOf2(sizeof(header<T>) + 20 * sizeof(entry<T>));
    constexpr static size_t cardinality = (PAGESIZE-sizeof(header<T>))/sizeof(entry<T>);
    constexpr static size_t count_in_line = CACHE_LINE_SIZE / sizeof(entry<T>);
private:
    header<T> hdr;  // header in persistent memory, 16 bytes
    std::array<entry<T>, cardinality> records; // slots in persistent memory, 16 bytes * n

public:
    friend class btree<T>;

    page(uint32_t level = 0) {
        // std::cout << "Header size: " << sizeof(header<T>) << ", entrysize: " << sizeof(entry<T>)
        //  << ", entries: " << cardinality << std::endl;
        hdr.level = level;
        records[0].ptr = nullptr;
    }

    // this is called when tree grows
    page(page* left, entry_key_t key, page* right, uint32_t level = 0) {
        hdr.leftmost_ptr = left;
        hdr.level = level;
        records[0].key = key;
        records[0].ptr = (char*) right;
        records[1].ptr = nullptr;

        hdr.last_index = 0;
    }

    void *operator new(size_t size) {
        void *ret;
        posix_memalign(&ret, 64, size);
        return ret;
    }

    void operator delete(void* ptr) {
        free(ptr);
    }

    inline int count() {
        uint8_t previous_switch_counter;
        int count = 0;
        do {
            previous_switch_counter = hdr.switch_counter;
            count = hdr.last_index + 1;

            while(count >= 0 && records[count].ptr != nullptr) {
                if(IS_FORWARD(previous_switch_counter))
                    ++count;
                else
                    --count;
            }

            if(count < 0) {
                count = 0;
                while(records[count].ptr != nullptr) {
                    ++count;
                }
            }

        } while(previous_switch_counter != hdr.switch_counter);

        return count;
    }

    inline bool remove_key(entry_key_t key) {
        // Set the switch_counter
        if(IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        bool shift = false;
        int i;
        for(i = 0; records[i].ptr != nullptr; ++i) {
            if(!shift && records[i].key == key) {
                records[i].ptr = (i == 0) ?
                    (char *)hdr.leftmost_ptr : records[i - 1].ptr;
                shift = true;
            }

            if(shift) {
                records[i].key = records[i + 1].key;
                records[i].ptr = records[i + 1].ptr;
            }
        }

        if(shift) {
            --hdr.last_index;
        }
        return shift;
    }

    bool remove(btree<T>* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
        hdr.mtx->lock();

        bool ret = remove_key(key);

        hdr.mtx->unlock();

        return ret;
    }


    inline void insert_key(entry_key_t key, char* ptr, int *num_entries, bool flush = true, bool update_last_index = true) {
        // update switch_counter
        if(!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        // FAST
        if(*num_entries == 0) {  // this page is empty
            entry<T>* new_entry = (entry<T>*) &records[0];
            entry<T>* array_end = (entry<T>*) &records[1];
            new_entry->key = (entry_key_t) key;
            new_entry->ptr = (char*) ptr;

            array_end->ptr = (char*)nullptr;

        }
        else {
            int i = *num_entries - 1, inserted = 0;
            records[*num_entries+1].ptr = records[*num_entries].ptr;


            // FAST
            for(i = *num_entries - 1; i >= 0; i--) {
                if(key < records[i].key ) {
                    records[i+1].ptr = records[i].ptr;
                    records[i+1].key = records[i].key;
                }
                else{
                    records[i+1].ptr = records[i].ptr;
                    records[i+1].key = key;
                    records[i+1].ptr = ptr;
                    inserted = 1;
                    break;
                }
            }
            if(inserted==0){
                records[0].ptr =(char*) hdr.leftmost_ptr;
                records[0].key = key;
                records[0].ptr = ptr;
            }
        }

        if(update_last_index) {
            hdr.last_index = *num_entries;
        }
        ++(*num_entries);
    }

    // Insert a new key - FAST and FAIR
    page *store(btree<T>* bt, char* left, entry_key_t key, char* right,
         bool flush, bool with_lock, page *invalid_sibling = nullptr) {
        if(with_lock) {
            hdr.mtx->lock(); // Lock the write lock
        }
        if(hdr.is_deleted) {
            if(with_lock) {
                hdr.mtx->unlock();
            }

            return nullptr;
        }

        int num_entries = count();

        for (int i = 0; i < num_entries; i++)
            if (key == records[i].key) {
                records[i].ptr = right;
                if (with_lock)
                    hdr.mtx->unlock();
                return this;
            }

        // If this node has a sibling node,
        if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
            // Compare this key with the first key of the sibling
            if(key > hdr.sibling_ptr->records[0].key) {
                if(with_lock) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }
                return hdr.sibling_ptr->store(bt, nullptr, key, right,
                        true, with_lock, invalid_sibling);
            }
        }


        // FAST
        if(num_entries < cardinality - 1) {
            insert_key(key, right, &num_entries, flush);

            if(with_lock) {
                hdr.mtx->unlock(); // Unlock the write lock
            }

            return this;
        }
        else {// FAIR
            // overflow
            // create a new node
            page* sibling = new page<T>(hdr.level);
            int m = (int) ceil(num_entries/2);
            entry_key_t split_key = records[m].key;

            // migrate half of keys into the sibling
            int sibling_cnt = 0;
            if(hdr.leftmost_ptr == nullptr){ // leaf node
                for(int i=m; i<num_entries; ++i){
                    sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
                }
            }
            else{ // internal node
                for(int i=m+1;i<num_entries;++i){
                    sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
                }
                sibling->hdr.leftmost_ptr = (page*) records[m].ptr;
            }

            sibling->hdr.sibling_ptr = hdr.sibling_ptr;
            sibling->hdr.pred_ptr = this;
            if (sibling->hdr.sibling_ptr != nullptr)
                sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
            hdr.sibling_ptr = sibling;

            // set to nullptr
            if(IS_FORWARD(hdr.switch_counter))
                hdr.switch_counter += 2;
            else
                ++hdr.switch_counter;
            records[m].ptr = nullptr;
            hdr.last_index = m - 1;
            num_entries = hdr.last_index + 1;

            page *ret;

            // insert the key
            if(key < split_key) {
                insert_key(key, right, &num_entries);
                ret = this;
            }
            else {
                sibling->insert_key(key, right, &sibling_cnt);
                ret = sibling;
            }

            // Set a new root or insert the split key to the parent
            if(bt->root == this) { // only one node can update the root ptr
                auto new_root = new page<T>(this, split_key, sibling, hdr.level + 1);
                bt->setNewRoot(new_root);

                if(with_lock) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }
            }
            else {
                if(with_lock) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }
                bt->btree_insert_internal(nullptr, split_key, (char *)sibling,
                        hdr.level + 1);
            }

            return ret;
        }

    }
    // revised
    inline void insert_key(entry_key_t key, char* ptr, int *num_entries, char **pred, bool flush = true,
                           bool update_last_index = true) {
        // update switch_counter
        if(!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;

        // FAST
        if(*num_entries == 0) {  // this page is empty
            entry<T>* new_entry = (entry<T>*) &records[0];
            entry<T>* array_end = (entry<T>*) &records[1];
            new_entry->key = (entry_key_t) key;
            new_entry->ptr = (char*) ptr;

            array_end->ptr = (char*)nullptr;

            if (hdr.pred_ptr != nullptr)
                *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
        }
        else {
            int i = *num_entries - 1, inserted = 0;
            records.at(*num_entries+1).ptr = records.at(*num_entries).ptr;

            // FAST
            for(i = *num_entries - 1; i >= 0; i--) {
                if(key < records[i].key ) {
                    records[i+1].ptr = records[i].ptr;
                    records[i+1].key = records[i].key;
                }
                else{
                    records[i+1].ptr = records[i].ptr;
                    records[i+1].key = key;
                    records[i+1].ptr = ptr;
                    *pred = records[i].ptr;
                    inserted = 1;
                    break;
                }
            }
            if(inserted==0){
                records[0].ptr =(char*) hdr.leftmost_ptr;
                records[0].key = key;
                records[0].ptr = ptr;
                if (hdr.pred_ptr != nullptr)
                    *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
            }
        }

        if(update_last_index) {
            hdr.last_index = *num_entries;
        }
        ++(*num_entries);
    }

        // revised
        // Insert a new key - FAST and FAIR
        /********
         * if key exists, return nullptr
         */
    page *store(btree<T>* bt, char* left, entry_key_t key, char* right,
                bool flush, bool with_lock, char **pred, page *invalid_sibling = nullptr) {
        if(with_lock) {
            hdr.mtx->lock(); // Lock the write lock
        }
        if(hdr.is_deleted) {
            if(with_lock) {
                hdr.mtx->unlock();
            }
            return nullptr;
        }

        int num_entries = count();

        for (int i = 0; i < num_entries; i++)
            if (key == records[i].key) {
                // Already exists, we don't need to do anything, just return.
                *pred = records[i].ptr;
                if (with_lock)
                    hdr.mtx->unlock();
                return nullptr;
            }

        // If this node has a sibling node,
        if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
            // Compare this key with the first key of the sibling
            if(key > hdr.sibling_ptr->records[0].key) {
                if(with_lock) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }
                return hdr.sibling_ptr->store(bt, nullptr, key, right,
                        true, with_lock, pred, invalid_sibling);
            }
        }


        // FAST
        if(num_entries < cardinality - 1) {
            insert_key(key, right, &num_entries, pred);

            if(with_lock) {
                hdr.mtx->unlock(); // Unlock the write lock
            }

            return this;
        } else {// FAIR
            // overflow
            // create a new node
            page* sibling = new page<T>(hdr.level);
            int m = (int) ceil(num_entries/2);
            entry_key_t split_key = records[m].key;

            // migrate half of keys into the sibling
            int sibling_cnt = 0;
            if(hdr.leftmost_ptr == nullptr){ // leaf node
                for(int i=m; i<num_entries; ++i){
                    sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
                }
            }
            else{ // internal node
                for(int i=m+1;i<num_entries;++i){
                    sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
                }
                sibling->hdr.leftmost_ptr = (page*) records[m].ptr; //记录左侧叶子节点中最大key的ptr
            }

            // b+tree
            sibling->hdr.sibling_ptr = hdr.sibling_ptr;
            sibling->hdr.pred_ptr = this;
            if (sibling->hdr.sibling_ptr != nullptr)
                sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
            hdr.sibling_ptr = sibling;

            // set to nullptr
            if(IS_FORWARD(hdr.switch_counter))
                hdr.switch_counter += 2;
            else
                ++hdr.switch_counter;
            records[m].ptr = nullptr;
            hdr.last_index = m - 1;
            num_entries = hdr.last_index + 1;

            page *ret;

            // insert the key
            if(key < split_key) {
                insert_key(key, right, &num_entries, pred);
                ret = this;
            }
            else {
                sibling->insert_key(key, right, &sibling_cnt, pred);
                ret = sibling;
            }

            // Set a new root or insert the split key to the parent
            if(bt->root == this) { // only one node can update the root ptr
                page* new_root = new page<T>(this, split_key, sibling, hdr.level + 1);
                bt->setNewRoot(new_root);

                if(with_lock) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }
            }
            else {
                if(with_lock) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }
                bt->btree_insert_internal(nullptr, split_key, (char *)sibling,
                        hdr.level + 1);
            }

            return ret;
        }

    }

    char *linear_search(entry_key_t key) {
        uint8_t previous_switch_counter;
        char *ret = nullptr;
        char *t;
        entry_key_t k;

        if(hdr.leftmost_ptr == nullptr) { // Search a leaf node
            do {
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                // search from left ro right
                if(IS_FORWARD(previous_switch_counter)) {
                    if((k = records[0].key) == key) {
                        if((t = records[0].ptr) != nullptr) {
                            if(k == records[0].key) {
                                ret = t;
                                continue;
                            }
                        }
                    }

                    for(int i=1; records[i].ptr != nullptr; ++i) {
                        if((k = records[i].key) == key) {
                            if(records[i-1].ptr != (t = records[i].ptr)) {
                                if(k == records[i].key) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                }
                else { // search from right to left
                    for(int i = count() - 1; i > 0; --i) {
                        if((k = records[i].key) == key) {
                            if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                                if(k == records[i].key) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }

                    if(!ret) {
                        if((k = records[0].key) == key) {
                            if(nullptr != (t = records[0].ptr) && t) {
                                if(k == records[0].key) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }
                    }
                }
            } while(hdr.switch_counter != previous_switch_counter);

            if(ret) {
                return ret;
            }

            if((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->records[0].key)
                return t;

            return nullptr;
        }
        else { // internal node
            do {
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                if(IS_FORWARD(previous_switch_counter)) {
                    if(key < (k = records[0].key)) {
                        if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                            ret = t;
                            continue;
                        }
                    }
                    int i = 1;
                    for(i = 1; records[i].ptr != nullptr; ++i) {
                        if(key < (k = records[i].key)) {
                            if((t = records[i-1].ptr) != records[i].ptr) {
                                ret = t;
                                break;
                            }
                        }
                    }

                    if(!ret) {
                        ret = records[i - 1].ptr;
                        continue;
                    }
                }
                else { // search from right to left
                    for(int i = count() - 1; i >= 0; --i) {
                        if(key >= (k = records[i].key)) {
                            if(i == 0) {
                                if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            }
                            else {
                                if(records[i - 1].ptr != (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                }
            } while(hdr.switch_counter != previous_switch_counter);

            if((t = (char *)hdr.sibling_ptr) != nullptr) {
                if(key >= ((page *)t)->records[0].key)
                    return t;
            }

            if(ret) {
                return ret;
            }
            else
                return (char *)hdr.leftmost_ptr;
        }

        return nullptr;
    }

    char *linear_search_pred(entry_key_t key, char **pred, bool debug=false) {
        uint8_t previous_switch_counter;
        char *ret = nullptr;
        char *t;

        if(hdr.leftmost_ptr == nullptr) { // Search a leaf node
            do {
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                // search from left to right
                if(IS_FORWARD(previous_switch_counter)) {
                    if (debug) {
                        printf("search from left to right\n");
                        printf("page:\n");
                        printAll();
                    }
                    entry_key_t k = records[0].key;
                    if (key < k) {
                        if (hdr.pred_ptr != nullptr){
                            *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                            if (debug)
                                printf("line 752, *pred=%p\n", *pred);
                        }
                    }
                    if (key > k){
                        *pred = records[0].ptr;
                        if (debug)
                            printf("line 757, *pred=%p\n", *pred);
                    }


                    if(k == key) {
                        if (hdr.pred_ptr != nullptr) {
                            *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                            if (debug)
                                printf("line 772, *pred=%p\n", *pred);
                        }
                        if((t = records[0].ptr) != nullptr) {
                            if(k == records[0].key) {
                                ret = t;
                                continue;
                            }
                        }
                    }

                    for(int i=1; records[i].ptr != nullptr; ++i) {
                        entry_key_t k = records[i].key;
                        entry_key_t k1 = records[i - 1].key;
                        if (k < key){
                            *pred = records[i].ptr;
                            if (debug)
                                printf("line 775, *pred=%p\n", *pred);
                        }
                        if(k == key) {
                            if(records[i-1].ptr != (t = records[i].ptr)) {
                                if(k == records[i].key) {
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

                    for (int i = count() - 1; i > 0; --i) {
                        if (debug)
                            printf("line 793, i=%d, records[i].key=%d\n", i,
                                         records[i].key);
                        entry_key_t k = records[i].key;
                        entry_key_t k1 = records[i - 1].key;
                        if (k1 < key && once) {
                            *pred = records[i - 1].ptr;
                            if (debug)
                                printf("line 794, *pred=%p\n", *pred);
                            once = false;
                        }
                        if(k == key) {
                            if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                                if(k == records[i].key) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }

                    if(!ret) {
                        entry_key_t k = records[0].key;
                        if (key < k){
                            if (hdr.pred_ptr != nullptr){
                                *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                                if (debug)
                                    printf("line 811, *pred=%p\n", *pred);
                            }
                        }
                        if (key > k)
                            *pred = records[0].ptr;
                        if(k == key) {
                            if (hdr.pred_ptr != nullptr) {
                                *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                                if (debug)
                                    printf("line 844, *pred=%p\n", *pred);
                            }
                            if(nullptr != (t = records[0].ptr) && t) {
                                if(k == records[0].key) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }
                    }
                }
            } while(hdr.switch_counter != previous_switch_counter);

            if(ret) {
                return ret;
            }

            if((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->records[0].key)
                return t;

            return nullptr;
        }
        else { // internal node
            do {
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;
                entry_key_t k;

                if(IS_FORWARD(previous_switch_counter)) {
                    if(key < (k = records[0].key)) {
                        if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                            ret = t;
                            continue;
                        }
                    }
                    int i = 1;
                    for(i = 1; records[i].ptr != nullptr; ++i) {
                        if(key < (k = records[i].key)) {
                            if((t = records[i-1].ptr) != records[i].ptr) {
                                ret = t;
                                break;
                            }
                        }
                    }

                    if(!ret) {
                        ret = records[i - 1].ptr;
                        continue;
                    }
                }
                else { // search from right to left
                    for(int i = count() - 1; i >= 0; --i) {
                        if(key >= (k = records[i].key)) {
                            if(i == 0) {
                                if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            }
                            else {
                                if(records[i - 1].ptr != (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                }
            } while(hdr.switch_counter != previous_switch_counter);

            if((t = (char *)hdr.sibling_ptr) != nullptr) {
                if(key >= ((page *)t)->records[0].key)
                    return t;
            }

            if(ret) {
                return ret;
            }
            else
                return (char *)hdr.leftmost_ptr;
        }

        return nullptr;
    }
    // print a node
    void print() {
        if(hdr.leftmost_ptr == nullptr)
            printf("[%d] leaf %x \n", this->hdr.level, this);
        else
            printf("[%d] internal %x \n", this->hdr.level, this);
        printf("last_index: %d\n", hdr.last_index);
        printf("switch_counter: %d\n", hdr.switch_counter);
        printf("search direction: ");
        if(IS_FORWARD(hdr.switch_counter))
            printf("->\n");
        else
            printf("<-\n");

        if(hdr.leftmost_ptr != nullptr)
            printf("%x ",hdr.leftmost_ptr);

        for(int i=0;records[i].ptr != nullptr;++i)
            printf("%ld,%x ", records[i].key, records[i].ptr);

        printf("\n%x ", hdr.sibling_ptr);

        printf("\n");
    }

    void printAll() {
        if(hdr.leftmost_ptr == nullptr) {
            printf("printing leaf node: ");
            print();
        }
        else {
            printf("printing internal node: ");
            print();
            ((page*) hdr.leftmost_ptr)->printAll();
            for(int i=0;records[i].ptr != nullptr;++i){
                ((page*) records[i].ptr)->printAll();
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
    char pathname[100] = "mount/pmem0/pool";
    int sds_write_value = 0;
    pmemobj_ctl_set(nullptr, "sds.at_create", &sds_write_value);
    if (file_exists(pathname) != 0) {
        printf("create new one.\n");
        if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(btree),
                                                            (uint64_t)400ULL * 1024ULL * 1024ULL * 1024ULL, 0666)) == nullptr) {
            perror("failed to create pool.\n");
            return;
        }
    } else {
        printf("open existing one.\n");
        if ((pop = pmemobj_open(pathname, POBJ_LAYOUT_NAME(btree))) == nullptr) {
            perror("failed to open pool.\n");
            return;
        }
    }
}
#endif
/*
 * class btree
 */
template<typename T>
btree<T>::btree(){
#ifdef USE_PMDK
    openPmemobjPool();
#else
    printf("without pmdk!\n");
#endif
    root = new page<T>();
    list_head = alloc<list_node_t<T>>();
    printf("list_head=%p\n", list_head);
    list_head->next = nullptr;
    height = 1;
}

template<typename T>
btree<T>::~btree() {
#ifdef USE_PMDK
    pmemobj_close(pop);
#endif
}

template<typename T>
size_t btree<T>::getMemoryUsed()
{
    auto num_nodes = 0;
    auto leftmost = root;
    do {
        page<T> *sibling = leftmost;
        num_nodes += 1;
        while(sibling) {
            num_nodes += 1;
            sibling = sibling->hdr.sibling_ptr;
        }
        leftmost = leftmost->hdr.leftmost_ptr;
    } while(leftmost);
    return num_nodes * sizeof(page<T>);
}

template<typename T>
size_t btree<T>::getPersistentMemoryUsed()
{
    auto num_nodes = 0;
    auto current = list_head;
    while (current != nullptr)
    {
        num_nodes += 1;
        current = current->next;
    }
    return num_nodes * sizeof(list_node_t<T>);
}

template<typename T>
void btree<T>::setNewRoot(page<T> *new_root) {
    this->root = new_root;
    ++height;
}

template<typename T>
char *btree<T>::btree_search_pred(entry_key_t key, bool *f, char **prev, bool debug){
    auto p = root;

    while(p->hdr.leftmost_ptr != nullptr) {
        p = (page<T> *)p->linear_search(key);
    }

    page<T>*t;
    while((t = (page<T> *)p->linear_search_pred(key, prev, debug)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p) {
            break;
        }
    }

    if(!t) {
        //printf("NOT FOUND %lu, t = %p\n", key, t);
        *f = false;
        return nullptr;
    }

    *f = true;
    return (char *)t;
}


template<typename T>
T *btree<T>::search(entry_key_t key) {
    bool f = false;
    char *prev;
    char *ptr = btree_search_pred(key, &f, &prev);
    if (f) {
        list_node_t<T> *n = (list_node_t<T> *)ptr;
        if (&(n->value) != nullptr) {
            return &(n->value);
        }
    } else {
        ;//printf("not found.\n");
    }
    return nullptr;
}

// insert the key in the leaf node
template<typename T>
void btree<T>::btree_insert_pred(entry_key_t key, char* right, char **pred, bool *update){ //need to be string
    auto p = root;

    while(p->hdr.leftmost_ptr != nullptr) {
        p = (page<T>*)p->linear_search(key);
    }
    *pred = nullptr;
    if(!p->store(this, nullptr, key, right, true, true, pred)) { // store
        // The key already exist.
        *update = true;
    } else {
        // Insert a new key.
        *update = false;
    }
}

template<typename T>
T* btree<T>::insert(entry_key_t key, T value) {
    auto n = alloc<list_node_t<T>>();
    //printf("n=%p\n", n);
    n->next = nullptr;
    n->key = key;
    n->value = value;
    n->isUpdate = false;
    n->isDelete = false;
    list_node_t<T> *prev = nullptr;
    bool update;
    bool rt = false;
    btree_insert_pred(key, (char *)n, (char **)&prev, &update);
    if (update && prev != nullptr) {
        // Overwrite.
        prev->value = value;
        //flush.
        clflush((char *)prev, sizeof(list_node_t<T>));
    }
    else {
        int retry_number = 0, w=0;
retry:
    retry_number += 1;
    if (retry_number > 10 && w == 3) {
        return nullptr;
        }
        if (rt) {
            // we need to re-search the key!
            bool f;
            btree_search_pred(key, &f, (char **)&prev);
            if (!f) {
                return nullptr;
                printf("error!!!!\n");
                exit(0);
            }
        }
        rt = true;
        // Insert a new key.
        if (list_head->next != nullptr) {

            if (prev == nullptr) {
                // Insert a smallest one.
                prev = list_head;
            }
            if (prev->isUpdate){
                w = 1;
                goto retry;
            }

            // check the order and CAS.
            list_node_t<T> *next = prev->next;
            n->next = next;
            clflush((char *)n, sizeof(list_node_t<T>));
            if (prev->key < key && (next == nullptr || next->key > key)) {
                if (!__sync_bool_compare_and_swap(&(prev->next), next, n)){
                    w = 2;
                    goto retry;
                }

                clflush((char *)prev, sizeof(list_node_t<T>));
            } else {
                // View changed, retry.
                w = 3;
                goto retry;
            }
        } else {
            // This is the first insert!
            if (!__sync_bool_compare_and_swap(&(list_head->next), nullptr, n))
                goto retry;
        }
    }
    return &(n->value);
}


template<typename T>
void btree<T>::remove(entry_key_t key) {
    bool f, debug=false;
    list_node_t<T> *cur = nullptr, *prev = nullptr;
retry:
    cur = (list_node_t<T> *)btree_search_pred(key, &f, (char **)&prev, debug);
    if (!f) {
        printf("not found.\n");
        return;
    }
    if (prev == nullptr) {
        prev = list_head;
    }
    if (prev->next != cur) {
        if (debug){
            printf("prev list node:\n");
            prev->printAll();
            printf("current list node:\n");
            cur->printAll();
        }
        exit(1);
        goto retry;
    } else {
        // Delete it.
        if (!__sync_bool_compare_and_swap(&(prev->next), cur, cur->next))
            goto retry;
        clflush((char *)prev, sizeof(list_node_t<T>));
        btree_delete(key);
    }

}

// store the key into the node at the given level
template<typename T>
void btree<T>::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level) {
    if(level > root->hdr.level)
        return;

    auto p = root;

    while(p->hdr.level > level)
        p = (page<T> *)p->linear_search(key);

    if(!p->store(this, nullptr, key, right, true, true)) {
        btree_insert_internal(left, key, right, level);
    }
}

template<typename T>
void btree<T>::btree_delete(entry_key_t key) {
    auto p = root;

    while(p->hdr.leftmost_ptr != nullptr){
        p = (page<T>*) p->linear_search(key);
    }

    page<T> *t;
    while((t = (page<T> *)p->linear_search(key)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p)
            break;
    }

    if(p) {
        if(!p->remove(this, key)) {
            btree_delete(key);
        }
    }
    else {
        printf("not found the key to delete %lu\n", key);
    }
}

template<typename T>
void btree<T>::printAll(){
    pthread_mutex_lock(&print_mtx);
    int total_keys = 0;
    auto leftmost = root;
    printf("root: %x\n", root);
    do {
        page<T> *sibling = leftmost;
        while(sibling) {
            if(sibling->hdr.level == 0) {
                total_keys += sibling->hdr.last_index + 1;
            }
            sibling->print();
            sibling = sibling->hdr.sibling_ptr;
        }
        printf("-----------------------------------------\n");
        leftmost = leftmost->hdr.leftmost_ptr;
    } while(leftmost);

    printf("total number of keys: %d\n", total_keys);
    pthread_mutex_unlock(&print_mtx);
}
