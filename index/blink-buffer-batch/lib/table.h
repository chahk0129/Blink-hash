#ifndef BLINK_BUFFER_BATCH_TABLE_H__
#define BLINK_BUFFER_BATCH_TABLE_H__

#include <algorithm>
#include "bucket.h"
#include "node.h"
#include "hash.h"

namespace BLINK_BUFFER_BATCH{

#define HASH_FUNCS_NUM (2)
#define NUM_SLOT (4)
#define BATCH_TABLE_SIZE (512*1024)

template <typename Key_t, typename Value_t>
class table_t{
    public:
	static constexpr size_t cardinality = (BATCH_TABLE_SIZE - sizeof(uint64_t) - sizeof(Key_t)) / sizeof(bucket_t<Key_t, Value_t>);
	std::atomic<uint64_t> lock;
	#ifdef FLUSH
	std::atomic<uint64_t> cnt;
	#endif
	std::atomic<Key_t> high_key;

	table_t(): lock(0){ 
	    high_key.store(0);
	    #ifdef FLUSH
	    cnt.store(0);
	    #endif
	}

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
            uint64_t version = lock.load();
            if(is_locked(version) || is_obsolete(version)){
                _mm_pause();
                need_restart = true;
            }
            return version;
        }

	void try_upgrade_flushlock(uint64_t version, bool& need_restart){
	    uint64_t _version = lock.load();
            if(version != _version){
                need_restart = true;
                return;
            }

            if(!lock.compare_exchange_strong(version, version + 0b10)){
                _mm_pause();
                need_restart = true;
            }
	}
    
	void lock_buckets(){
	    for(int i=0; i<cardinality; i++){
		while(!bucket[i].try_lock()){ 
		    _mm_pause();
		}
	    }
	}

	void try_upgrade_writelock(uint64_t version, bool& need_restart){
            uint64_t _version = lock.load();
            if(version != _version){
                need_restart = true;
                return;
            }

            if(!lock.compare_exchange_strong(version, version + 0b10)){
                _mm_pause();
                need_restart = true;
		return;
            }

	    for(int i=0; i<cardinality; i++){
		while(!bucket[i].try_lock()){ 
		    _mm_pause();
		}
	    }
        }

        void write_unlock(){
            lock.fetch_add(0b10);
        }
	    

	int insert(Key_t key, Value_t value, uint64_t version){
	    bool need_restart = false;
	    __m128i empty = _mm_setzero_si128();

	    for(int k=0; k<HASH_FUNCS_NUM; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		uint8_t fingerprint = _hash(hash_key) | 1;

		for(int j=0; j<NUM_SLOT; j++){
		    auto loc = (hash_key + j) % cardinality;
		    if(!bucket[loc].try_lock())
			return -1;

		    auto _version = get_version(need_restart);
		    if(need_restart || (version != _version)){
			bucket[loc].unlock();
			return -1;
		    }

		    if(bucket[loc].insert(key, value, fingerprint, empty)){
			#ifdef FLUSH
			cnt.fetch_add(1);
		 	#endif
			bucket[loc].unlock();
			return 0;
		    }
		    bucket[loc].unlock();
		}
	    }

	    return 1; // full, need to flush
	}

	int update(Key_t key, Value_t value, uint64_t version){
	    bool need_restart = false;

	    for(int k=0; k<HASH_FUNCS_NUM; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
		for(int j=0; j<NUM_SLOT; j++){
		    auto loc = (hash_key + j) % cardinality;
		    if(!bucket[loc].try_lock())
			return -1;

		    auto _version = get_version(need_restart);
		    if(need_restart || (version != _version)){
			bucket[loc].unlock();
			return -1;
		    }

		    if(bucket[loc].update(key, value, fingerprint)){
			bucket[loc].unlock();
			return 0;
		    }

		    bucket[loc].unlock();
		}
	    }

	    return 1; // key not found
	}

	int remove(Key_t key, uint64_t version){
	    bool need_restart = false;

	    for(int k=0; k<HASH_FUNCS_NUM; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
		for(int j=0; j<NUM_SLOT; j++){
		    auto loc = (hash_key + j) % cardinality;
		    if(!bucket[loc].try_lock())
			return -1;

		    auto _version = get_version(need_restart);
		    if(need_restart || (version != _version)){
			bucket[loc].unlock();
			return -1;
		    }

		    if(bucket[loc].remove(key, fingerprint)){
			#ifdef FLUSH
			cnt.fetch_sub(1);
			#endif
			bucket[loc].unlock();
			return 0;
		    }

		    bucket[loc].unlock();
		}
	    }

	    return 1; // key not found
	}

	bool find(Key_t key, Value_t& value, bool& need_restart){
	    for(int k=0; k<HASH_FUNCS_NUM; k++){
		auto hash_key = h(&key, sizeof(Key_t), k);
		__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
		for(int j=0; j<NUM_SLOT; j++){
		    auto loc = (hash_key + j) % cardinality;
		    auto bucket_vstart = bucket[loc].get_version(need_restart);
		    if(need_restart)
			return false;

		    if(bucket[loc].find(key, value, fingerprint)){
			auto bucket_vend = bucket[loc].get_version(need_restart);
			if(need_restart || (bucket_vstart != bucket_vend)){
			    need_restart = true;
			    return false;
			}
			return true;
		    }

		    auto bucket_vend = bucket[loc].get_version(need_restart);
		    if(need_restart || (bucket_vstart != bucket_vend)){
			need_restart = true;
			return false;
		    }
		}
	    }
	    return false;
	}

	lnode_t<Key_t, Value_t>** convert(int& num){
	    bool need_restart = false;
	    int idx = 0;
	    int from = 0;
	    entry_t<Key_t, Value_t> buf[cardinality * entry_num];

	    __m128i empty = _mm_setzero_si128();
	    for(int i=0; i<cardinality; i++)
		bucket[i].collect(buf, idx, empty);

	    std::sort(buf, buf+idx, [](entry_t<Key_t, Value_t>& a, entry_t<Key_t, Value_t>& b){
		    return a.key < b.key;
		    });

	    size_t batch_size = FILL_FACTOR * lnode_t<Key_t, Value_t>::cardinality;
	    if(idx % batch_size == 0)
		num = idx / batch_size;
	    else
		num = idx / batch_size + 1;

	    auto leaf = new lnode_t<Key_t, Value_t>*[num];
	    for(int i=0; i<num; i++){
		leaf[i] = new lnode_t<Key_t, Value_t>();
	    }

	    for(int i=0; i<num; i++){
		if(i < num-1)
		    leaf[i]->sibling_ptr = static_cast<node_t*>(leaf[i+1]);
		leaf[i]->batch_insert(buf, batch_size, from, idx);
	    }

	    return leaf;
	}

	Key_t get_high_key(bool& need_restart, uint64_t _version){
	    auto key = high_key.load();
	    if(key == 0){
		need_restart = true;
		return 0;
	    }

	    return key;
	}

	void clear(){
	    #ifdef FLUSH
	    cnt.store(0);
	    #endif
	    for(int i=0; i<cardinality; i++){
		bucket[i].clear();
		bucket[i].unlock();
	    }
	    write_unlock();
	}

	uint8_t _hash(size_t key){
	    return (uint8_t)(key % 256);
	}

	void print(){
	    for(int i=0; i<cardinality; i++){
		bucket[i].print();
	    }
	}

    private:
	bucket_t<Key_t, Value_t> bucket[cardinality];
};
}	
#endif
