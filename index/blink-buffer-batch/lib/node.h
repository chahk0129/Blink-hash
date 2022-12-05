#ifndef BLINK_BUFFER_BATCH_NODE_H__ 
#define BLINK_BUFFER_BATCH_NODE_H__ 

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <immintrin.h>
#include <atomic>
#include <iostream>
#include <cmath>
#include <limits.h>
#include <utility>

#include "entry.h"

#ifdef URL_KEYS
#define PAGE_SIZE (4096)
#elif defined STRING_KEY
#define PAGE_SIZE (1024)
#else
#define PAGE_SIZE (512)
#endif

#define CACHELINE_SIZE 64

#define LINEAR_SEARCH
// UPDATE_LOCK is enabled when sizeof(value) <= 8 for concurrent updates
//#define UPDATE_LOCK
#define CAS(_p, _u, _v) (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

#define FILL_FACTOR (0.8)

namespace BLINK_BUFFER_BATCH{

class node_t{
    public:
	std::atomic<uint64_t> lock;
	node_t* sibling_ptr;
	int cnt;
	uint32_t level;


	inline uint64_t _rdtsc(){
	    uint32_t lo, hi;
	    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
	    return (((uint64_t)hi << 32) | lo);
	}

	node_t(): lock(0), sibling_ptr(nullptr), cnt(0), level(0){ }
	node_t(int _level): lock(0), sibling_ptr(nullptr), cnt(0), level(_level){ }
	node_t(node_t* sibling, uint32_t count, uint32_t _level): lock(0), sibling_ptr(sibling), cnt(count), level(_level) { }

	bool is_locked(uint64_t version){
	    return ((version & 0b10) == 0b10);
	}

	bool is_obsolete(uint64_t version){
	    return (version & 1) == 1;
	}

	uint64_t get_version(bool& need_restart){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		need_restart = true;
	    }
	    return version;
	}


	uint64_t try_readlock(bool& need_restart){
	    uint64_t version = (uint64_t)lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		need_restart = true;
	    }
	    return version;
	}

	#ifdef UPDATE_LOCK
	bool try_writelock(){
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version) || ((version >> 32) > 0)){
		_mm_pause();
		return false;
	    }

	    if(lock.compare_exchange_strong(version, version + 0b10)){
		version += 0b10;
		return true;
	    }
	    else{
		_mm_pause();
		return false;
	    }
	}

	void try_upgrade_writelock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if(((_version & (~0u)) != (version & (~0u))) || ((_version >> 32) > 0)){
		_mm_pause();
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(_version, _version + 0b10)){
		_mm_pause();
		need_restart = true;
	    }
	}

	void try_upgrade_updatelock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if((_version & (~0u)) != (version & (~0u))){
		_mm_pause();
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(_version, _version + ((uint64_t)1 << 32))){
		_mm_pause();
		need_restart = true;
	    }
	}

	void write_unlock(){
	    uint64_t version = lock.load();
	    if((version & (~0u)) == UINT32_MAX-1)
		lock.store(version & 0u, std::memory_order_relaxed);
	    else
		lock.fetch_add(0b10);
	}

	void update_unlock(){
	    lock.fetch_sub((uint64_t)1 << 32);
	}

	void write_unlock_obsolete(){
	    uint64_t version = lock.load();
	    if((version & (~0u)) == UINT32_MAX-1)
		lock.store((version & 0u) + 0b1, std::memory_order_relaxed);
	    else
		lock.fetch_add(0b11);
	}

	#else
	bool try_writelock(){ // exclusive lock
	    uint64_t version = lock.load();
	    if(is_locked(version) || is_obsolete(version)){
		_mm_pause();
		return false;
	    }

	    if(lock.compare_exchange_strong(version, version + 0b10)){
		return true;
	    }
	    else{
		_mm_pause();
		return false;
	    }
	}

	void try_upgrade_writelock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
	    if(version != _version){
		_mm_pause();
		need_restart = true;
		return;
	    }

	    if(!lock.compare_exchange_strong(_version, _version + 0b10)){
		_mm_pause();
		need_restart =true;
	    }
	}

	void write_unlock(){
	    lock.fetch_add(0b10);
	}

	void write_unlock_obsolete(){
	    lock.fetch_add(0b11);
	}

	#endif

	int get_cnt(){
	    return cnt;
	}

};

template <typename Key_t>
class inode_t: public node_t{
    public:
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t) - sizeof(Key_t)) / sizeof(entry_t<Key_t, node_t*>);
	Key_t high_key;
    private:
        entry_t<Key_t, node_t*> entry[cardinality];
    
    public:


    	inode_t() { } 
	inode_t(int _level): node_t(_level){ }

	// constructor when inode needs to split
	inode_t(node_t* sibling, int _cnt, node_t* left, uint32_t _level, Key_t _high_key): node_t(sibling, _cnt, _level), high_key(_high_key){
	    entry[0].value = left;
	}

	// constructor when tree height grows
	inode_t(Key_t split_key, node_t* left, node_t* right, node_t* sibling, uint32_t _level, Key_t _high_key): node_t(sibling, 1, _level){
	    high_key = _high_key;
	    entry[0].key = split_key;
	    entry[0].value = left;
	    entry[1].value = right;
	}

	void* operator new(size_t size) {
	    void* ret;
	    posix_memalign(&ret, 64, size);
	    return ret;
	}

	bool is_full(){
	    return (cnt == cardinality-1);
	}

	int find_lowerbound(Key_t& key){
	    return lowerbound_linear(key);
	}

	node_t* scan_node(Key_t key){
	    if(sibling_ptr && (high_key < key)){
		return sibling_ptr;
	    }
	    else{
		return entry[find_lowerbound(key)].value;
	    } 
	}

	node_t* leftmost_ptr(){
	    return entry[0].value;
	}

	void insert(Key_t key, node_t* value){
	    int pos = find_lowerbound(key);
	    memmove(entry+pos+1, entry+pos, sizeof(entry_t<Key_t, node_t*>)*(cnt-pos+1));
	    entry[pos].key = key;
	    entry[pos].value = value;
	    std::swap(entry[pos].value, entry[pos+1].value);

	    cnt++;

	}

	inode_t<Key_t>* split(Key_t& split_key){
	    int half = cnt - cnt/2;
	    split_key = entry[half].key;

	    int new_cnt = cnt - half - 1;
	    auto new_node = new inode_t<Key_t>(sibling_ptr, new_cnt, entry[half].value, level, high_key);
	    memcpy(new_node->entry, entry+half+1, sizeof(entry_t<Key_t, node_t*>)*(new_cnt+1));

	    sibling_ptr = static_cast<node_t*>(new_node);
	    high_key = entry[half].key;
	    cnt = half;
	    return new_node;
	}

	void batch_insert_root(Key_t* key, node_t** value, int num, node_t* leftmost_ptr){
//	    std::cout << __func__ << ", " << this << " : num " << num << ", level = " << level << std::endl;
	    if(leftmost_ptr){ // leftmost node
		entry[cnt].value = leftmost_ptr;
//		std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = left = " << leftmost_ptr << std::endl;
		for(int i=0; i<num; i++){
		    entry[cnt++].key = key[i];
		    entry[cnt].value = value[i];
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = key[" << i << "] = " << key[i]<< std::endl;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[" << i << "] = " << value[i]<< std::endl;
		}
	    }
	    else{
		for(int i=0; i<num; i++, cnt++){
		    entry[cnt].key = key[i];
		    entry[cnt].value = value[i];
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].key = key[" << i << "] = " << key[i]<< std::endl;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[" << i << "] = " << value[i]<< std::endl;
		}
		cnt--;
	    }
	}
    
	void batch_insert_root(Key_t* key, node_t** value, int batch_size, int& from, int to, node_t* leftmost_ptr=nullptr){
//	    std::cout << __func__ << ", " << this << " : batchsize " << batch_size << ", from " << from << ", to " << to << ", level = " << level << std::endl;
	    if(leftmost_ptr){ // leftmost node
		entry[cnt].value = leftmost_ptr;
//		std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = left = " << leftmost_ptr << std::endl;
		for(int i=from; i<batch_size; i++){
		    entry[cnt++].key = key[i];
		    entry[cnt].value = value[i];
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = key[" << i << "] = " << key[i]<< std::endl;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << ".value = value[" << i << "] = " << value[i]<< std::endl;
		}
		from += batch_size;
		high_key = key[from];
//		std::cout << "    " << __LINE__ << ": high_key = key[" << from << "] = " << high_key << std::endl;
	    }
	    else{
		if(batch_size < (to - from)){ // intermediate nodes
		    entry[cnt].value = value[from++];
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << ".value = value[" << from-1 << "] = " << value[from-1]<< std::endl;
		    for(int i=from; i<from+batch_size; i++){
			entry[cnt++].key = key[i];
			entry[cnt].value = value[i];
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = key[" << i << "] = " << key[i]<< std::endl;
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[" << i << "] = " << value[i]<< std::endl;
		    }
		    from += batch_size;
		    high_key = key[from];
//		    std::cout << "    " << __LINE__ << ": high_key = key[" << from << "] = " << high_key << std::endl;
		}
		else{ // rightmost node
		    entry[cnt].value = value[from++];
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[" << from-1 << "] = " << value[from-1]<< std::endl;
		    for(int i=from; i<to; i++){
			entry[cnt++].key = key[i];
			entry[cnt].value = value[i];
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = key[" << i << "] = " << key[i]<< std::endl;
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[" << i << "] = " << value[i]<< std::endl;
		    }
		    from = to;
		}
	    }
	}

	void batch_insert_last_level(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key,         node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
	    bool from_start = true;
	    if(migrate_idx < migrate_num){
		from_start = false;
		entry[cnt].value = migrate[migrate_idx++].value;
		int copy_num = migrate_num - migrate_idx;
		for(; cnt<copy_num; migrate_idx++){
		    entry[cnt++].key = migrate[migrate_idx].key;
		    entry[cnt].value = migrate[migrate_idx].value;
		}
		migrate_idx += copy_num;
	    }

	    if(idx < num && cnt < batch_size){
		if(from_start){
		    entry[cnt].value = value[idx++];
		}
		from_start = false;
		if(idx < num){
		    for(; cnt<batch_size && idx<num-1; idx++){
			entry[cnt++].key = key[idx];
			entry[cnt].value = value[idx];
		    }

		    if(cnt == batch_size){ // insert in next node
			high_key = key[idx];
			return;
		    }
		    else{
			entry[cnt++].key = key[idx];
			entry[cnt].value = value[idx];
			idx++;
			if(idx == num && cnt == batch_size && buf_num != 0){
			    high_key = buf[buf_idx].key;
			    return;
			}
		    }
		}
	    }

	    if(buf_idx < buf_num && cnt < batch_size){
		if(from_start){
		    entry[cnt].value = buf[buf_idx++].value;
		}
		for(; cnt<batch_size && buf_idx<buf_num-1; buf_idx++){
		    entry[cnt++].key = buf[buf_idx].key;
		    entry[cnt].value = buf[buf_idx].value;
		}

		if(cnt == batch_size){ // insert in next node
		    high_key = buf[buf_idx].key;
		}
		else{
		    entry[cnt++].key = buf[buf_idx].key;
		    entry[cnt].value = buf[buf_idx].value;
		    buf_idx++;
		}
	    }
	}

	void batch_insert_last_level(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>*  buf, int& buf_idx, int buf_num){
	    bool from_start = true;
	    if(idx < num){
		entry[cnt].value = value[idx++];
		from_start = false;
		if(idx < num){
		    for(; cnt<batch_size && idx<num-1; idx++){
			entry[cnt++].key = key[idx];
			entry[cnt].value = value[idx];
		    }

		    if(cnt == batch_size){ // insert in next node
			high_key = key[idx];
			return;
		    }
		    else{
			entry[cnt++].key = key[idx];
			entry[cnt].value = value[idx];
			idx++;
			if(idx == num && cnt == batch_size && buf_num != 0){
			    high_key = buf[buf_idx].key;
			    return;
			}
		    }
		}
	    }

	    if(buf_idx < buf_num && cnt < batch_size){
		if(from_start){
		    entry[cnt].value = buf[buf_idx++].value;
		}

		for(; cnt<batch_size && buf_idx<buf_num-1; buf_idx++){
		    entry[cnt++].key = buf[buf_idx].key;
		    entry[cnt].value = buf[buf_idx].value;
		}
		if(cnt == batch_size){ // insert in next node
		    high_key = buf[buf_idx].key;
		}
		else{
		    entry[cnt++].key = buf[buf_idx].key;
		    entry[cnt].value = buf[buf_idx].value;
		    buf_idx++;
		}
	    }
	}


	inode_t<Key_t>** batch_insert_last_level(Key_t* key, node_t** value, int num, int& new_num){
	    int pos = find_lowerbound(key[0]);
	    int batch_size = cardinality * FILL_FACTOR;
	    bool inplace = (cnt + num) < cardinality ? 1 : 0;
	    int move_num = 0;
	    int idx = 0;
	    int from = pos;
	    move_num = cnt-pos;

	    if(inplace){ // normal insertion
		move_normal_insertion(pos, num, move_num);
		entry[from].value = value[idx++];
		for(; from<pos+num; idx++){
		    entry[from++].key = key[idx];
		    entry[from].value = value[idx];
		}

		cnt += num-1;
		return nullptr;
	    }
	    else{
		auto prev_high_key = high_key;
		entry[from].value = value[idx++];
		
		if(batch_size < pos){ // need insert in the middle (migrated + new kvs + moved)
		    int migrate_num = pos - batch_size;
		    entry_t<Key_t, node_t*> migrate[migrate_num];
		    memcpy(migrate, &entry[batch_size], sizeof(entry_t<Key_t, node_t*>) * migrate_num);

		    entry_t<Key_t, node_t*> buf[move_num];
		    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);
		    cnt = batch_size;

		    int total_num = num + move_num + migrate_num;
		    int last_chunk = 0;
		    int numerator = total_num / (batch_size+1);
		    int remains = total_num % (batch_size+1);
		    if(numerator == 0){ // need only one new node
			new_num = 1;
			last_chunk = remains;
		    }
		    else{ // multiple new nodes
			if(remains == 0){ // exact match
			    new_num = numerator;
			    last_chunk = batch_size;
			}
			else{
			    if(remains < cardinality - batch_size){ // can be squeezed into the last new node
				new_num = numerator;
				last_chunk = batch_size + remains;
			    }
			    else{ // need extra new node
				new_num = numerator + 1;
				last_chunk = remains;
			    }
			}
		    }

		    auto new_nodes = new inode_t<Key_t>*[new_num];
		    for(int i=0; i<new_num; i++)
			new_nodes[i] = new inode_t<Key_t>(level);

		    auto old_sibling = sibling_ptr;
		    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

		    int migrate_idx = 0;
		    int move_idx = 0;

		    high_key = migrate[migrate_idx].key;
		    for(int i=0; i<new_num-1; i++){
			new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
			new_nodes[i]->batch_insert_last_level(migrate, migrate_idx, migrate_num, key, value, idx, num, batch_size, buf,       move_idx, move_num);
		    }
		    new_nodes[new_num-1]->sibling_ptr = old_sibling;
		    new_nodes[new_num-1]->batch_insert_last_level(migrate, migrate_idx, migrate_num, key, value, idx, num, last_chunk, buf,   move_idx, move_num);
		    new_nodes[new_num-1]->high_key = prev_high_key;

		    return new_nodes;
		}
		else{ // need insert in the middle (new_kvs + moved)
		    int move_idx = 0;
		    entry_t<Key_t, node_t*> buf[move_num];
		    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);

		    for(; from<batch_size && idx<num; idx++){
			entry[from++].key = key[idx];
			entry[from].value = value[idx];
		    }

		    cnt += (idx - move_num - 1);

		    entry[cnt++].key = buf[move_idx].key;
		    for(; cnt<batch_size; move_idx++){
			entry[cnt++].key = buf[move_idx].key;
			entry[cnt].value = buf[move_idx].value;
		    }

		    if(idx < num){
			high_key = key[idx];
		    }
		    else{
			high_key = buf[move_idx].key;
		    }

		    int total_num = num - idx + move_num - move_idx;
		    int last_chunk = 0;
		    int numerator = total_num / (batch_size+1);
		    int remains = total_num % (batch_size+1);
		    if(numerator == 0){ // need only one new node
			new_num = 1;
			last_chunk = remains;
		    }
		    else{ // multiple new nodes
			if(remains == 0){ // exact match
			    new_num = numerator;
			    last_chunk = batch_size;
			}
			else{
			    if(remains < cardinality - batch_size){ // can be squeezed into the last new node
				new_num = numerator;
				last_chunk = batch_size + remains;
			    }
			    else{ // need extra new node
				new_num = numerator + 1;
				last_chunk = remains;
			    }
			}
		    }
		    auto new_nodes = new inode_t<Key_t>*[new_num];
		    for(int i=0; i<new_num; i++)
			new_nodes[i] = new inode_t<Key_t>(level);

		    auto old_sibling = sibling_ptr;
		    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

		    for(int i=0; i<new_num-1; i++){
			new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
			new_nodes[i]->batch_insert_last_level(key, value, idx, num, batch_size, buf, move_idx, move_num);
		    }
		    new_nodes[new_num-1]->sibling_ptr = old_sibling;
		    new_nodes[new_num-1]->batch_insert_last_level(key, value, idx, num, last_chunk, buf, move_idx, move_num);
		    new_nodes[new_num-1]->high_key = prev_high_key;

		    return new_nodes;
		}
	    }
	}

	void batch_insert(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key, node_t** value,    int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
	    bool from_start = true;
//	    std::cout << __func__ << ": " << this << " with migrate " << std::endl;
	    if(migrate_idx < migrate_num){
		from_start = false;
		entry[cnt].value = migrate[migrate_idx++].value;
//		std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = migrate[ " << migrate_idx-1 << "].value = " << migrate[migrate_idx-1].value << std::endl;
		int copy_num = migrate_num - migrate_idx;
		for(; cnt<copy_num; migrate_idx++){
		    entry[cnt++].key = migrate[migrate_idx].key;
		    entry[cnt].value = migrate[migrate_idx].value;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = migrate[ " << migrate_idx << "].key = " << migrate[migrate_idx].key << std::endl;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = migrate[ " << migrate_idx << "].value = " << migrate[migrate_idx].value << std::endl;
		}
		migrate_idx += copy_num;
	    }

	    if(idx < num && cnt < batch_size){
		if(from_start){
		    entry[cnt].value = value[idx++];
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[ " << idx-1 << "] = " << value[idx-1] << std::endl;
		}
		from_start = false;
		if(idx < num){
		    for(; cnt<batch_size && idx<num-1; idx++){
			entry[cnt++].key = key[idx];
			entry[cnt].value = value[idx];
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = key[ " << idx << "] = " << key[idx] << std::endl;
//                        std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[ " << idx << "] = " << value[idx] << std::endl;
		    }

		    if(cnt == batch_size){ // insert in next node
			high_key = key[idx];
//			std::cout << "    " << __LINE__ << ": high_key = key[" << idx << "] = " << key[idx] << std::endl;
			return;
		    }
		    else{
			entry[cnt++].key = key[idx];
			entry[cnt].value = value[idx];
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = key[ " << idx << "] = " << key[idx] << std::endl;
  //                      std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[ " << idx << "] = " << value[idx] << std::endl;
			idx++; 
			if(idx == num && cnt == batch_size && buf_num != 0){
			    high_key = buf[buf_idx].key;
//			    std::cout << "    " << __LINE__ << ": high_key = buf[" << buf_idx << "].key = " << buf[buf_idx].key << std::endl;
			}
		    }
		}
	    }

	    if(buf_idx < buf_num && cnt < batch_size){
		if(from_start){
		    entry[cnt].value = buf[buf_idx++].value;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = buf[ " << buf_idx-1 << "].value = " << buf[buf_idx-1].value << std::endl;
		}
		for(; cnt<batch_size && buf_idx<buf_num-1; buf_idx++){
		    entry[cnt++].key = buf[buf_idx].key;
		    entry[cnt].value = buf[buf_idx].value;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = buf[ " << buf_idx << "].key = " << buf[buf_idx].key << std::endl;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = buf[ " << buf_idx << "].value = " << buf[buf_idx].key << std::endl;
		}

		if(cnt == batch_size){ // insert in next node
		    high_key = buf[buf_idx].key;
//		    std::cout << "    " << __LINE__ << ": high_key = buf[ " << buf_idx << "].key = " << buf[buf_idx].key << std::endl;
		}
		else{
		    entry[cnt++].key = buf[buf_idx].key;
		    entry[cnt].value = buf[buf_idx].value;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = buf[ " << buf_idx << "].key = " << buf[buf_idx].key << std::endl;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = buf[ " << buf_idx << "].value = " << buf[buf_idx].key << std::endl;
		    buf_idx++;
		}
	    }
	}

	void batch_insert(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
//	    std::cout << __func__ << ": " << this << std::endl;
	    bool from_start = true;
	    if(idx < num){
		entry[cnt].value = value[idx++];
//		std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[ " << idx-1 << "] = " << value[idx-1] << std::endl;
		from_start = false;
		if(idx < num){
		    for(; cnt<batch_size && idx<num-1; idx++){
			entry[cnt++].key = key[idx];
			entry[cnt].value = value[idx];
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = key[ " << idx << "] = " << key[idx] << std::endl;
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[ " << idx << "] = " << value[idx] << std::endl;
		    }

		    if(cnt == batch_size){ // insert in next node
			high_key = key[idx];
//			std::cout << "    " << __LINE__ << ": high_key = key[" << idx << "] = " << key[idx] << std::endl;
			return;
		    }
		    else{
			entry[cnt++].key = key[idx];
			entry[cnt].value = value[idx];
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = key[ " << idx << "] = " << key[idx] << std::endl;
//			std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = value[ " << idx << "] = " << value[idx] << std::endl;
			idx++; 
			if(idx == num && cnt == batch_size && buf_num != 0){
			    high_key = buf[buf_idx].key;
//			    std::cout << "    " << __LINE__ << ": high_key = buf[" << buf_idx << "] = " << buf[buf_idx].key << std::endl;
			}
		    }
		}
	    }

	    if(buf_idx < buf_num && cnt < batch_size){
		if(from_start){
		    entry[cnt].value = buf[buf_idx++].value;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = buf[ " << buf_idx-1 << "].value = " << entry[cnt].value << std::endl;
		}

		for(; cnt<batch_size && buf_idx<buf_num-1; buf_idx++){
		    entry[cnt++].key = buf[buf_idx].key;
		    entry[cnt].value = buf[buf_idx].value;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = buf[ " << buf_idx << "].key = " << buf[buf_idx].key << std::endl;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = buf[ " << buf_idx << "].value = " << buf[buf_idx].value << std::endl;
		}
		if(cnt == batch_size){ // insert in next node
		    high_key = buf[buf_idx].key;
//		    std::cout << "    " << __LINE__ << ": high_key = buf[" << buf_idx << "] = " << buf[buf_idx].key << std::endl;
		    return;
		}
		else{
		    entry[cnt++].key = buf[buf_idx].key;
		    entry[cnt].value = buf[buf_idx].value;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt-1 << "].key = buf[ " << buf_idx << "].key = " << buf[buf_idx].key << std::endl;
//		    std::cout << "    " << __LINE__ << ": insert entry[" << cnt << "].value = buf[ " << buf_idx << "].value = " << buf[buf_idx].value << std::endl;
		    buf_idx++;
		}
	    }
	}

	inode_t<Key_t>** batch_insert(Key_t* key, node_t** value, int num, int& new_num){
	    int pos = find_lowerbound(key[0]);
	    int batch_size = cardinality * FILL_FACTOR;
	    bool inplace = (cnt + num) < cardinality ? 1 : 0;
	    int move_num = cnt-pos;
	    int idx = 0;
	    int from = pos;

	    if(inplace){
//		std::cout << __func__ << ": " << this << ": inplace, num = " << num << std::endl;
//		std::cout << "   " << " key: " << key[0] << " at pos " << pos << std::endl;
		move_normal_insertion(pos, num, move_num);
		//entry[from].value = value[idx++];
		//std::cout << "   " << __LINE__ << ": insert entry[" << from << "].value = value[" << idx-1 << "] = " << value[idx-1] << std::endl;
		for(; from<pos+num; idx++){
		    entry[from++].key = key[idx];
		    entry[from].value = value[idx];
//		    std::cout << "   " << __LINE__ << ": insert entry[" << from-1 << "].key = key[" << idx << "].key = " << key[idx] << std::endl;
//		    std::cout << "   " << __LINE__ << ": insert entry[" << from << "].value = value[" << idx << "].value = " << value[idx] << std::endl;
		}
		cnt += num-1;
//		std::cout << "cnt: " << cnt << std::endl;
		return nullptr;
	    }
	    else{
		auto prev_high_key = high_key;
//		entry[from].value = value[idx++];

		if(batch_size < pos){ // need insert in the middle (migrated + new kvs + moved)
		    int migrate_num = pos - batch_size;
		    entry_t<Key_t, node_t*> migrate[migrate_num];
		    memcpy(migrate, &entry[batch_size], sizeof(entry_t<Key_t, node_t*>) * migrate_num);

//		    std::cout << __func__ << ": " << this << ": insert in the middle + migrate, num = " << num << std::endl;
//		    std::cout << "   " << __LINE__ << ": insert entry[" << from << "].value = value[" << idx-1 << "] = " << value[idx-1] << std::endl;

		    entry_t<Key_t, node_t*> buf[move_num];
		    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);
		    cnt = batch_size;

		    int total_num = num + move_num + migrate_num;
		    int last_chunk = 0;
		    int numerator = total_num / (batch_size+1);
		    int remains = total_num % (batch_size+1);
		    if(numerator == 0){ // need only one new node
			new_num = 1;
			last_chunk = remains;
		    }
		    else{ // multiple new nodes
			if(remains == 0){ // exact match
			    new_num = numerator;
			    last_chunk = batch_size;
			}
			else{
			    if(remains < cardinality - batch_size){ // can be squeezed into the last new node
				new_num = numerator;
				last_chunk = batch_size + remains;
			    }
			    else{ // need extra new node
				new_num = numerator + 1;
				last_chunk = remains;
			    }
			}
		    }

		    auto new_nodes = new inode_t<Key_t>*[new_num];
		    for(int i=0; i<new_num; i++)
			new_nodes[i] = new inode_t<Key_t>(level);

		    auto old_sibling = sibling_ptr;
		    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

		    int migrate_idx = 0;
		    int move_idx = 0;

		    high_key = migrate[migrate_idx].key;
		    for(int i=0; i<new_num-1; i++){
			new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
			new_nodes[i]->batch_insert(migrate, migrate_idx, migrate_num, key, value, idx, num, batch_size, buf, move_idx,        move_num);
		    }
		    new_nodes[new_num-1]->sibling_ptr = old_sibling;
		    new_nodes[new_num-1]->high_key = prev_high_key;
		    new_nodes[new_num-1]->batch_insert(migrate, migrate_idx, migrate_num, key, value, idx, num, last_chunk, buf, move_idx,    move_num);

		    return new_nodes;
		}
		else{ // need insert in the middle (new_kvs + moved)
		    int move_idx = 0;
		    entry_t<Key_t, node_t*> buf[move_num];
		    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*move_num);

//		    std::cout << __func__ << ": " << this << ": insert in the middle, num = " << num << std::endl;
//		    std::cout << "   " << __LINE__ << ": insert entry[" << from << "].value = value[" << idx-1 << "] = " << value[idx-1] << std::endl;

		    for(; from<batch_size && idx<num; idx++){
			entry[from++].key = key[idx];
			entry[from].value = value[idx];
//			std::cout << "   " << __LINE__ << ": insert entry[" << from-1 << "].key = key[" << idx << "] = " << key[idx] << std::endl;
//			std::cout << "   " << __LINE__ << ": insert entry[" << from << "].value = value[" << idx << "] = " << value[idx] << std::endl;
		    }

		    cnt += (idx - move_num);

//		    entry[cnt++].key = buf[move_idx].key;
//		    std::cout << "   " << __LINE__ << ": insert entry[" << cnt-1 << "].key = buf[" << move_idx << "].key = " << buf[move_idx].key << std::endl;

		    for(; cnt<batch_size; move_idx++){
			entry[cnt++].key = buf[move_idx].key;
			entry[cnt].value = buf[move_idx].value;
//			std::cout << "   " << __LINE__ << ": insert entry[" << cnt-1 << "].key = buf[" << move_idx << "].key = " << buf[move_idx].key << std::endl;
//			std::cout << "   " << __LINE__ << ": insert entry[" << cnt << "].value = buf[" << move_idx << "].value = " << buf[move_idx].value << std::endl;
		    }

		    if(idx < num){
			high_key = key[idx];
//			std::cout << "    " << __LINE__ <<": high_key = " << high_key << " (key[" << idx << "])" << std::endl;
		    }
		    else{
			high_key = buf[move_idx].key;
//			std::cout << "    " << __LINE__ << ": high_key = " << high_key << " (move[" << move_idx << "])" << std::endl;
		    }

		    int total_num = num - idx + move_num - move_idx;
		    int last_chunk = 0;
		    int numerator = total_num / (batch_size+1);
		    int remains = total_num % (batch_size+1);
		    if(numerator == 0){ // need only one new node
			new_num = 1;
			last_chunk = remains;
		    }
		    else{ // multiple new nodes
			if(remains == 0){ // exact match
			    new_num = numerator;
			    last_chunk = batch_size;
			}
			else{
			    if(remains < cardinality - batch_size){ // can be squeezed into the last new node
				new_num = numerator;
				last_chunk = batch_size + remains;
			    }
			    else{ // need extra new node
				new_num = numerator + 1;
				last_chunk = remains;
			    }
			}
		    }

		    auto new_nodes = new inode_t<Key_t>*[new_num];
		    for(int i=0; i<new_num; i++)
			new_nodes[i] = new inode_t<Key_t>(level);

		    auto old_sibling = sibling_ptr;
		    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

		    for(int i=0; i<new_num-1; i++){
			new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
			new_nodes[i]->batch_insert(key, value, idx, num, batch_size, buf, move_idx, move_num);
		    }
		    new_nodes[new_num-1]->sibling_ptr = old_sibling;
		    new_nodes[new_num-1]->high_key = prev_high_key;
		    new_nodes[new_num-1]->batch_insert(key, value, idx, num, last_chunk, buf, move_idx, move_num);

		    return new_nodes;
		}
	    }
	}

	void move_normal_insertion(int pos, int num, int move_num){

//	    std::cout << __func__ << " num=" << num << ", move_num=" << move_num << std::endl;
//	    std::cout << "  before move" << std::endl;
//	    int j;
//	    for(j=0; j<cnt; j++){
//		std::cout << "    entry[" << j << "].key = " << entry[j].key << std::endl;
//		std::cout << "    entry[" << j << "].value = " << entry[j].value << std::endl;
//	    }
//	    std::cout << "    entry[" << j << "].value = " << entry[j].value << std::endl;

	    if(!move_num)
		return;

	    memmove(&entry[pos+num], &entry[pos], sizeof(entry_t<Key_t, node_t*>)*move_num);
//	    std::cout << "move normal insertion : moved " << pos << " th entry to " << pos+num << " th entry" << std::endl;
//	    std::cout << "  after move" << std::endl;
//	    for(int i=0; i<cnt+move_num; i++){
//		std::cout << "    entry[" << i << "].key = " << entry[i].key << std::endl;
//		std::cout << "    entry[" << i << "].value = " << entry[i].value << std::endl;
//	    }
	    std::swap(entry[pos].value, entry[pos+num].value);
//	    std::cout << "    " << __LINE__<< ": value after swap: " << "entry[" << pos <<"].value: " << entry[pos].value << ", entry[" << pos+num << "].value: " << entry[pos+num].value << std::endl;
//	    std::cout << "  after swap" << std::endl;
//	    for(int i=0; i<cnt+move_num; i++){
//		std::cout << "    entry[" << i << "].key = " << entry[i].key << std::endl;
//		std::cout << "    entry[" << i << "].value = " << entry[i].value << std::endl;
//	    }

	}

	void insert_for_root(Key_t* key, node_t** value, node_t* left, int num){
//	    std::cout << __func__ << std::endl;
	    entry[cnt].value = left;
//	    std::cout << "    " << __LINE__ << ": insert left " << left << std::endl;
	    for(int i=0; i<num; i++){
		entry[cnt++].key = key[i];
		entry[cnt].value = value[i];
//		std::cout << "    " << __LINE__ << ": insert key[" << i << "] = " << key[i]<< std::endl;
//		std::cout << "    " << __LINE__ << ": insert value[" << i << "] = " << value[i]<< std::endl;
	    }
	}

	void print(){
	    bool need_restart = false;
	    auto version = get_version(need_restart);
	    if(need_restart)
		std::cout << "this node is locked!" << std::endl;

	    for(int i=0; i<cnt; i++){
		std::cout << " [" << i << "]" << entry[i].key << " " << entry[i].value << " ";
	    }
	    std::cout << "[" << cnt << "]" << entry[cnt].key << " " << entry[cnt].value << " ";
	    std::cout << "  high_key: " << high_key << ",  sibling: " << sibling_ptr << "\n\n";
	}

	void sanity_check(Key_t _high_key, bool first){
	    for(int i=0; i<cnt-1; i++){
		for(int j=i+1; j<cnt; j++){
		    if(entry[i].key > entry[j].key){
			std::cerr << "inode_t::key order is not perserved!!" << std::endl;
			std::cout << "[" << i << "].key: " << entry[i].key << "\t[" << j << "].key: " << entry[j].key << std::endl;
		    }
		}
	    }
	    for(int i=0; i<cnt; i++){
		if(sibling_ptr && (entry[i].key > high_key)){
		    std::cout << "inode_t:: " << i << "(" << entry[i].key << ") is higher than high key " << high_key << std::endl;
		}
		if(!first){
		    if(sibling_ptr && (entry[i].key <= _high_key)){
			std::cout << "inode_t:: " << i << "(" << entry[i].key << ") is smaller than previous high key " << _high_key << std::endl;
			std::cout << "--------- node_address " << this << " , current high_key " << high_key << std::endl;
		    }
		}
	    }
	    if(sibling_ptr != nullptr)
		(static_cast<inode_t<Key_t>*>(sibling_ptr))->sanity_check(high_key, false);
	}

    private:
	int lowerbound_linear(Key_t key){
	    for(int i=0; i<cnt; i++){
		if(key <= entry[i].key){
		    return i;
		}
	    }
	    return cnt;
	}
};

template <typename Key_t, typename Value_t>
class lnode_t: public node_t{
    public: 
	static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t) - sizeof(Key_t)) / sizeof(entry_t<Key_t, Value_t>);

	Key_t high_key;
    private:
	entry_t<Key_t, Value_t> entry[cardinality];

    public:

	// initial constructor
        lnode_t(): node_t() { } 

	// constructor when leaf splits
        lnode_t(node_t* sibling, int _cnt, uint32_t _level): node_t(sibling, _cnt, _level) { }

	void* operator new(size_t size) {
	    void* ret;
	    posix_memalign(&ret, 64, size);
	    return ret;
	}

	bool is_full(){
	    return (cnt == cardinality);
	}

	int find_lowerbound(Key_t key){
	    return lowerbound_linear(key);
	}

	Value_t find(Key_t key){
	    return find_linear(key);
	}

	void insert(Key_t key, Value_t value){
	    if(cnt){
		int pos = find_lowerbound(key);
		memmove(&entry[pos+1], &entry[pos], sizeof(entry_t<Key_t, Value_t>)*(cnt-pos));
		entry[pos].key = key;
		entry[pos].value = value;
	    }
	    else{
		entry[0].key = key;
		entry[0].value= value;
	    }

	    cnt++;
	}

	lnode_t<Key_t, Value_t>* split(Key_t& split_key){
	    int half = cnt/2;
	    int new_cnt = cnt - half;
	    split_key = entry[half-1].key;

	    auto new_leaf = new lnode_t<Key_t, Value_t>(sibling_ptr, new_cnt, level);
	    new_leaf->high_key = high_key;
	    memcpy(new_leaf->entry, entry+half, sizeof(entry_t<Key_t, Value_t>)*new_cnt);

	    sibling_ptr = static_cast<node_t*>(new_leaf);
	    high_key = entry[half-1].key;
	    cnt = half;
	    return new_leaf;
	}

	bool remove(Key_t key){
	    if(cnt){
		int pos = find_pos_linear(key);
		// no matching key found
		if(pos == -1) return false;
		memmove(&entry[pos], &entry[pos+1], sizeof(entry_t<Key_t, Value_t>)*(cnt - pos - 1));
		cnt--;
		return true;
	    }
	    return false;
	}

	bool update(Key_t key, Value_t value){
	    return update_linear(key, value);
	}

	int range_lookup(int idx, Value_t* buf, int count, int range){
	    auto _count = count;
	    for(int i=idx; i<cnt; i++){
		buf[_count++] = entry[i].value;
		if(_count == range) return _count;
	    }
	    return _count;
	}

	Key_t get_high_key(){
	    return entry[cnt-1].key;
	}

	Key_t leftmost_key(){
	    return entry[0].key;
	}

	void set_high_key(){
	    if(cnt)
		high_key = entry[cnt-1].key;
	    else
		memset(&high_key, 0, sizeof(Key_t));
	}

	void batch_insert(entry_t<Key_t, Value_t>* buf, int batch_size, int& from, int to){
	    if(from + batch_size < to){
		memcpy(entry, &buf[from], sizeof(entry_t<Key_t, Value_t>)*batch_size);
		from += batch_size;
		cnt += batch_size;
		if(sibling_ptr)
		    high_key = entry[cnt-1].key;
	    }
	    else{
		memcpy(entry, &buf[from], sizeof(entry_t<Key_t, Value_t>)*(to - from));
		cnt += (to - from);
		from = to;
		if(sibling_ptr)
		    high_key = entry[cnt-1].key;
	    }
	}
	/*
	void batch_insert(entry_t<Key_t, Value_t>* buf, int batch_size, int& from, int to){
	    if(cnt){
		int pos = lowerbound_linear(buf[0].key);
		//debug
		if(pos != cnt)
		    std::cout << "lnode_t::" << __func__ << ": not rightmost entry" << std::endl;
		memcpy(&entry[pos], &buf[from], sizeof(entry_t<Key_t, Value_t>)*(batch_size - cnt));
		high_key = entry[cnt-1].key;
	    }
	    else{
		if(from + batch_size < to){
		    memcpy(entry, &buf[from], sizeof(entry_t<Key_t, Value_t>)* batch_size);
		    from += batch_size;
		    cnt += batch_size;
		    high_key = entry[cnt-1].key;
		}
		else{
		    memcpy(entry, &buf[from], sizeof(entry_t<Key_t, Value_t>)*(to - from));
		    cnt += (to - from);
		    from = to;
		}
	    }
	}*/

	void print(){
	    bool need_restart = false;
	    auto version = get_version(need_restart);
	    if(need_restart)
		std::cout << "this node is locked!" << std::endl;

	    for(int i=0; i<cnt; i++){
		std::cout << "[" << i << "]" << entry[i].key << " ";
	    }
	    std::cout << "  high_key: " << high_key << ", cnt: " << cnt << "\n\n";
	}

	void sanity_check(Key_t _high_key, bool first){
	    for(int i=0; i<cnt-1; i++){
		for(int j=i+1; j<cnt; j++){
		    if(entry[i].key > entry[j].key){
			std::cerr << "lnode_t::key order is not perserved!!" << std::endl;
			std::cout << "[" << i << "].key: " << entry[i].key << "\t[" << j << "].key: " << entry[j].key << std::endl;
		    }
		}
	    }
	    for(int i=0; i<cnt; i++){
		if(sibling_ptr && (entry[i].key > high_key)){
		    std::cout << i << "lnode_t:: " << "(" << entry[i].key << ") is higher than high key " << high_key << std::endl;
		}
		if(!first){
		    if(sibling_ptr && (entry[i].key < _high_key)){
			std::cout << "lnode_t:: " << i << "(" << entry[i].key << ") is smaller than previous high key " << _high_key << std::endl;
			std::cout << "--------- node_address " << this << " , current high_key " << high_key << std::endl;
		    }
		}
	    }
	    if(sibling_ptr != nullptr)
		(static_cast<lnode_t<Key_t, Value_t>*>(sibling_ptr))->sanity_check(high_key, false);
	}

	int get_cnt(){
	    return cnt;
	}

	double utilization(){
	    return (double)cnt / cardinality;
	}
    private:

	int lowerbound_linear(Key_t key){
	    for(int i=0; i<cnt; i++){
		if(key <= entry[i].key)
		    return i;
	    }
	    return cnt;
	}

	bool update_linear(Key_t key, Value_t value){
	    for(int i=0; i<cnt; i++){
		if(key == entry[i].key){
		    #ifdef UPDATE_LOCK
		    auto _value = entry[i].value;
		    while(!CAS(&entry[i].value, &_value, value)){
			_mm_pause();
			_value = entry[i].value;
		    }
		    #else
		    entry[i].value = value;
		    #endif
		    return true;
		}
	    }
	    return false;
	}

	Value_t find_linear(Key_t key){
	    for(int i=0; i<cnt; i++){
		if(key == entry[i].key){
		    auto ret = entry[i].value;
		    return ret;
		}
	    }
	    return 0;
	}

	int find_pos_linear(Key_t key){
	    for(int i=0; i<cnt; i++){
		if(key == entry[i].key){
		    return i;
		}
	    }
	    return -1;
	}
};
}
#endif
