#include "lnode.h"
#include "hash.h"

namespace BLINK_HASH{

template <typename Key_t, typename Value_t>
inline bool lnode_hash_t<Key_t, Value_t>::_try_splitlock(uint64_t version){
    bool need_restart = false;
    this->try_upgrade_writelock(version, need_restart);
    if(need_restart) return false;
    for(int i=0; i<cardinality; i++){
	while(!bucket[i].try_lock())
	    _mm_pause();
    }
    return true;
}

template <typename Key_t, typename Value_t>
inline void lnode_hash_t<Key_t, Value_t>::_split_unlock(){
    (static_cast<node_t*>(this))->write_unlock();
    for(int i=0; i<cardinality; i++){
	bucket[i].unlock();
    }
}

template <typename Key_t, typename Value_t>
inline void lnode_hash_t<Key_t, Value_t>::bucket_acquire(){
    for(int i=0; i<cardinality; i++){
	while(!bucket[i].try_lock())
	    _mm_pause();
    }
}

template <typename Key_t, typename Value_t>
inline void lnode_hash_t<Key_t, Value_t>::bucket_release(){
    for(int i=0; i<cardinality; i++){
	bucket[i].unlock();
    }
}

template <typename Key_t, typename Value_t>
inline uint8_t lnode_hash_t<Key_t, Value_t>::_hash(size_t key){
    return (uint8_t)(key % 256);
}

template <typename Key_t, typename Value_t>
int lnode_hash_t<Key_t, Value_t>::insert(Key_t key, Value_t value, uint64_t version){
    bool need_restart = false;
    #ifdef AVX512
    __m256i empty = _mm256_setzero_si256();
    #elif defined AVX2
    __m128i empty = _mm_setzero_si128();
    #else
    uint8_t empty = 0;
    #endif

    for(int k=0; k<HASH_FUNCS_NUM; k++){
	auto hash_key = h(&key, sizeof(Key_t), k);
	uint8_t fingerprint = _hash(hash_key) | 1;
	for(int j=0; j<NUM_SLOT; j++){
	    auto loc = (hash_key + j) % cardinality;
	    if(!bucket[loc].try_lock())
		return -1;

	    auto _version = lnode_t<Key_t, Value_t>::get_version(need_restart);
	    if(need_restart || (version != _version)){
		bucket[loc].unlock();
		return -1;
	    }

	    if(bucket[loc].state != bucket_t<Key_t, Value_t>::STABLE){
		if(!stabilize_bucket(loc)){
		    bucket[loc].unlock();
		    return -1;
		}
	    }

	    if(bucket[loc].insert(key, value, fingerprint, empty)){
		bucket[loc].unlock();
		return 0;
	    }
	    bucket[loc].unlock();

	}
    }

    return 1; // return split flag
}

template <typename Key_t, typename Value_t>
lnode_hash_t<Key_t, Value_t>* lnode_hash_t<Key_t, Value_t>::split(Key_t& split_key, Key_t key, Value_t value, uint64_t version){
    auto new_right = new lnode_hash_t<Key_t, Value_t>(lnode_t<Key_t, Value_t>::sibling_ptr, 0, lnode_t<Key_t, Value_t>::level);
    new_right->high_key = lnode_t<Key_t, Value_t>::high_key;
    new_right->left_sibling_ptr = this;

    struct target_t{
	uint64_t loc;
	uint64_t fingerprint;
    };

    target_t target[HASH_FUNCS_NUM];
    for(int k=0; k<HASH_FUNCS_NUM; k++){
	auto hash_key = h(&key, (size_t)sizeof(Key_t), k);
	target[k].loc = hash_key % cardinality;
	target[k].fingerprint = (_hash(hash_key) | 1);
    }

    if(!stabilize_all(version)){
	delete new_right;
	return nullptr;
    }

    if(!_try_splitlock(version)){
	delete new_right;
	return nullptr;
    }

    /*
       auto util = utilization() * 100;
       std::cout << util << std::endl;
     */

    #ifdef AVX512
    __m256i empty = _mm256_setzero_si256();
    #elif defined AVX2
    __m128i empty = _mm_setzero_si128();
    #else
    uint8_t empty = 0;
    #endif

    #ifdef SAMPLING // entry-based sampling
    Key_t temp[cardinality];
    int valid_num = 0;
    for(int j=0; j<cardinality; j++){
	if(bucket[j].collect_keys(temp, valid_num, cardinality, empty))
	    break;
    }
    #else // non-sampling
    Key_t temp[cardinality*entry_num];
    int valid_num = 0;
    for(int j=0; j<cardinality; j++){
	bucket[j].collect_all_keys(temp, valid_num, empty);
    }
    #endif

    int half = find_median(temp, valid_num);
    split_key = temp[half];
    lnode_t<Key_t, Value_t>::high_key = temp[half];

    #ifdef DEBUG // to find out the error rate of finding median key for sampling-based approaches
    Key_t temp_[cardinality*entry_num];
    int valid_num_ = 0;
    for(int j=0; j<cardinality; j++){
	for(int i=0; i<entry_num; i++){
	    if(bucket[j].fingerprints[i] != 0){
		temp_[valid_num_++] = bucket[j].entry[i].key;
	    }
	}
    }
    std::sort(temp_, temp_+valid_num_);
    int sampled_median_loc;
    for(int i=0; i<valid_num_; i++){
	if(temp_[i] == split_key){
	    sampled_median_loc = i;
	    break;
	}
    }

    std::cout << (double)(sampled_median_loc - valid_num_/2)/valid_num_ * 100 << std::endl;
    #endif

    // set every bucket state in current node to LINKED_RIGHT
    for(int i=0; i<cardinality; i++){
	bucket[i].state = bucket_t<Key_t, Value_t>::LINKED_RIGHT;
    }

    // insert after split
    bool need_insert = true;
    for(int m=0; m<HASH_FUNCS_NUM; m++){
	for(int j=0; j<NUM_SLOT; j++){
	    auto loc = (target[m].loc + j) % cardinality;
	    #ifdef AVX512
	    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[loc].fingerprints));
	    __m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	    uint32_t bitfield = _mm256_movemask_epi8(cmp);
	    for(int i=0; i<32; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){ // eager stabilization
		    if(split_key < bucket[loc].entry[i].key){ // migrate key-value to new node
			memcpy(&new_right->bucket[loc].entry[i], &bucket[loc].entry[i], sizeof(entry_t<Key_t, Value_t>));
			new_right->bucket[loc].fingerprints[i] = bucket[loc].fingerprints[i];
			if(need_insert){
			    if(key <= split_key){ // insert in current node
				bucket[loc].fingerprints[i] = target[m].fingerprint;
				bucket[loc].entry[i].key = key;
				bucket[loc].entry[i].value = value;
				need_insert = false;
			    }
			    else // reset migrated key-value fingerprint
				bucket[loc].fingerprints[i] = 0;
			}
			else // reset migrated key-value fingerprint
			    bucket[loc].fingerprints[i] = 0;
		    }
		    else{ // key-value stays in current node
			if(need_insert){
			    if(split_key < key){ // insert in new node
				new_right->bucket[loc].fingerprints[i] = target[m].fingerprint;
				new_right->bucket[loc].entry[i].key = key;
				new_right->bucket[loc].entry[i].value = value;
				need_insert = false;
			    }
			    // else wait until find an empty entry
			}
		    }
		}
		else{ // empty
		    if(need_insert){
			if(split_key < key){ // insert in new node
			    new_right->bucket[loc].fingerprints[i] = target[m].fingerprint;
			    new_right->bucket[loc].entry[i].key = key;
			    new_right->bucket[loc].entry[i].value = value;
			}
			else{ // insert in current node
			    bucket[loc].fingerprints[i] = target[m].fingerprint;
			    bucket[loc].entry[i].key = key;
			    bucket[loc].entry[i].value = value;
			}
			need_insert = false;
		    }
		}
	    }
	    #elif defined AVX2
	    for(int k=0; k<2; k++){
		__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[loc].fingerprints + k*16));
		__m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		uint16_t bitfield = _mm_movemask_epi8(cmp);
		for(int i=0; i<16; i++){
		    auto bit = (bitfield >> i);
		    auto idx = k*16 + i;
		    if((bit & 0x1) == 0){ // eager stabilization
			if(split_key < bucket[loc].entry[idx].key){  // migrate key-value to new node
			    memcpy(&new_right->bucket[loc].entry[idx], &bucket[loc].entry[idx], sizeof(entry_t<Key_t, Value_t>));
			    new_right->bucket[loc].fingerprints[idx] = bucket[loc].fingerprints[idx];
			    if(need_insert){
				if(key <= split_key){ // insert in current node
				    bucket[loc].fingerprints[idx] = target[m].fingerprint;
				    bucket[loc].entry[idx].key = key;
				    bucket[loc].entry[idx].value = value;
				    need_insert = false;
				}
				else // reset migrated key-value fingerprint
				    bucket[loc].fingerprints[idx] = 0;
			    }
			    else // reset migrated key-value fingerprint
				bucket[loc].fingerprints[idx] = 0;
			}
			else{ // key-value stays in current node
			    if(need_insert){
				if(split_key < key){ // insert in new node
				    new_right->bucket[loc].fingerprints[idx] = target[m].fingerprint;
				    new_right->bucket[loc].entry[idx].key = key;
				    new_right->bucket[loc].entry[idx].value = value;
				    need_insert = false;
				}
				// else wait until find an empty entry
			    }
			}
		    }
		    else{ // empty
			if(need_insert){
			    if(split_key < key){ // insert in new node
				new_right->bucket[loc].fingerprints[idx] = target[m].fingerprint;
				new_right->bucket[loc].entry[idx].key = key;
				new_right->bucket[loc].entry[idx].value = value;
			    }
			    else{ // insert in current node
				bucket[loc].fingerprints[idx] = target[m].fingerprint;
				bucket[loc].entry[idx].key = key;
				bucket[loc].entry[idx].value = value;
			    }
			    need_insert = false;
			}
		    }
		}
	    }
	    #else
	    for(int i=0; i<entry_num; i++){
		if(bucket[loc].fingerprints[i] != 0){ // eager stabilization
		    if(split_key < bucket[loc].entry[i].key){ // migrate key-value to new node
			memcpy(&new_right->bucket[loc].entry[i], &bucket[loc].entry[i], sizeof(entry_t<Key_t, Value_t>));
			new_right->bucket[loc].fingerprints[i] = bucket[loc].fingerprints[i];
			if(need_insert){
			    if(key <= split_key){  // insert in current node
				bucket[loc].fingerprints[i] = target[m].fingerprint;
				bucket[loc].entry[i].key = key;
				bucket[loc].entry[i].value = value;
				need_insert = false;
			    }
			    else // reset migrated key-value fingerprint
				bucket[loc].fingerprints[i] = 0;
			    // else wait until find an empty entry
			}
			else // reset migrated key-value fingerprint
			    bucket[loc].fingerprints[i] = 0;
		    }
		    else{ // key-value stays in current node
			if(need_insert){
			    if(split_key < key){ // insert in new node
				new_right->bucket[loc].fingerprints[i] = target[m].fingerprint;
				new_right->bucket[loc].entry[i].key = key;
				new_right->bucket[loc].entry[i].value = value;
				need_insert = false;
			    }
			    // else wait until find an empty entry
			}
		    }
		}
		else{ // empty
		    if(need_insert){ // split after insert
			if(split_key < key){ // insert in new node
			    new_right->bucket[loc].fingerprints[i] = target[m].fingerprint;
			    new_right->bucket[loc].entry[i].key = key;
			    new_right->bucket[loc].entry[i].value = value;
			}
			else{ // insert in new node
			    bucket[loc].fingerprints[i] = target[m].fingerprint;
			    bucket[loc].entry[i].key = key;
			    bucket[loc].entry[i].value = value;
			}
			need_insert = false;
		    }
		}
	    }
	    #endif
	    // set migrated/inserted buckets STABLE
	    bucket[loc].state = bucket_t<Key_t, Value_t>::STABLE;
	    new_right->bucket[loc].state = bucket_t<Key_t, Value_t>::STABLE;
	    if(!need_insert) // if new key/value is inserted, proceed to next steps
		goto PROCEED;
	}
    }


    PROCEED:
    if(lnode_t<Key_t, Value_t>::sibling_ptr != nullptr){
	// update right sibling node's left sibling pointer
	auto right_sibling = static_cast<lnode_t<Key_t, Value_t>*>(lnode_t<Key_t, Value_t>::sibling_ptr);
	if(right_sibling->type == lnode_t<Key_t, Value_t>::HASH_NODE){ // need to update right sibling's left sibling pointer
	    auto right = static_cast<lnode_hash_t<Key_t, Value_t>*>(right_sibling);
	    auto old = this;
	    while(!CAS(&right->left_sibling_ptr, &old, new_right)){
		old = this;
	    }
	}
    }
    // update current node's right sibling pointer
    lnode_t<Key_t, Value_t>::sibling_ptr = new_right;

    if(need_insert){
	blink_printf("%s %s %llu\n", __func__, ": insert after split failed key- ", key);
    }
    /*
       util = utilization() * 100;
       std::cout << util << std::endl;
     */
    return new_right;
}

template <typename Key_t, typename Value_t>
int lnode_hash_t<Key_t, Value_t>::update(Key_t key, Value_t value, uint64_t vstart){
    bool need_restart = false;
    for(int k=0; k<HASH_FUNCS_NUM; k++){
	auto hash_key = h(&key, sizeof(Key_t), k);
	#ifdef AVX512
	__m256i fingerprint = _mm256_set1_epi8(_hash(hash_key) | 1);
	#elif defined AVX2
	__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
	#else
	uint8_t fingerprint = _hash(hash_key) | 1;
	#endif

	for(int j=0; j<NUM_SLOT; j++){
	    auto loc = (hash_key + j) % cardinality;
	    if(!bucket[loc].try_lock())
		return -1;

	    auto vend = lnode_t<Key_t, Value_t>::get_version(need_restart);
	    if(need_restart || (vstart != vend)){
		bucket[loc].unlock();
		return -1;
	    }

	    if(bucket[loc].state != bucket_t<Key_t, Value_t>::STABLE){
		auto ret = stabilize_bucket(loc);
		if(!ret){
		    bucket[loc].unlock();
		    return -1;
		}
	    }

	    if(bucket[loc].update(key, value, fingerprint)){ // updated
		bucket[loc].unlock();
		return 0;
	    }

	    bucket[loc].unlock();
	}
    }
    return 1; // key not found
}

template <typename Key_t, typename Value_t>
Value_t lnode_hash_t<Key_t, Value_t>::find(Key_t key, bool& need_restart){
    for(int k=0; k<HASH_FUNCS_NUM; k++){
	auto hash_key = h(&key, sizeof(Key_t), k);
	#ifdef AVX512
	__m256i fingerprint = _mm256_set1_epi8(_hash(hash_key) | 1);
	#elif defined AVX2
	__m128i fingerprint = _mm_set1_epi8(_hash(hash_key) | 1);
	#else
	uint8_t fingerprint = _hash(hash_key) | 1;
	#endif

	for(int j=0; j<NUM_SLOT; j++){
	    auto loc = (hash_key + j) % cardinality;

	    auto bucket_vstart = bucket[loc].get_version(need_restart);
	    if(need_restart)
		return 0;

	    if(bucket[loc].state != bucket_t<Key_t, Value_t>::STABLE){
		if(!stabilize_bucket(loc, bucket_vstart)){
		    need_restart = true;
		    return 0;
		}

		bucket_vstart += 0b100;
	    }

	    Value_t ret;
	    if(bucket[loc].find(key, ret, fingerprint)){ // found
		auto bucket_vend = bucket[loc].get_version(need_restart);
		if(need_restart || (bucket_vstart != bucket_vend)){
		    need_restart = true;
		    return 0;
		}
		return ret;
	    }

	    auto bucket_vend = bucket[loc].get_version(need_restart);
	    if(need_restart || (bucket_vstart != bucket_vend)){
		need_restart = true;
		return 0;
	    }
	}
    }
    return 0;
}


template <typename Key_t, typename Value_t>
int lnode_hash_t<Key_t, Value_t>::range_lookup(Key_t key, Value_t* buf, int count, int range){
    bool need_restart = false;

    entry_t<Key_t, Value_t> _buf[cardinality * entry_num];
    int _count = count;
    int idx = 0;

    #ifdef AVX512
    __m256i empty = _mm256_setzero_si256();
    #elif defined AVX2
    __m128i empty = _mm_setzero_si128();
    #else
    uint8_t empty = 0;
    #endif

    for(int j=0; j<cardinality; j++){
	auto bucket_vstart = bucket[j].get_version(need_restart);
	if(need_restart) return -1;

	if(bucket[j].state != bucket_t<Key_t, Value_t>::STABLE){
	    if(!stabilize_bucket(j, bucket_vstart)){
		return -1;
	    }

	    bucket_vstart += 0b100;
	}

	bucket[j].collect(key, _buf, idx, empty);

	auto bucket_vend = bucket[j].get_version(need_restart);
	if(need_restart || (bucket_vstart != bucket_vend))
	    return -1;
    }

    std::sort(_buf, _buf+idx, [](entry_t<Key_t, Value_t>& a, entry_t<Key_t, Value_t>& b){
	    return a.key < b.key;
	    });

    bool lower_bound = true;
    for(int i=0; i<idx; i++){
	buf[_count++] = _buf[i].value;
	if(_count == range){
	    return _count;
	}
    }
    return _count;
}

// need to use structure to return output
template <typename Key_t, typename Value_t>
lnode_btree_t<Key_t, Value_t>** lnode_hash_t<Key_t, Value_t>::convert(int& num, uint64_t version){
    _mm_pause();
    bool need_restart = false;
    int idx = 0;
    entry_t<Key_t, Value_t> buf[cardinality * entry_num];

    if(!stabilize_all(version)){
	return nullptr;
    }

    if(!_try_splitlock(version)){
	return nullptr;
    }

    #ifdef AVX512
    __m256i empty = _mm256_setzero_si256();
    #elif defined AVX2
    __m128i empty = _mm_setzero_si128();
    #else
    uint8_t empty = 0;
    #endif
    for(int i=0; i<cardinality; i++){
	bucket[i].collect(buf, idx, empty);
    }

    std::sort(buf, buf+idx, [](entry_t<Key_t, Value_t>& a, entry_t<Key_t, Value_t>& b){
	    return a.key < b.key;
	    });

    size_t batch_size = FILL_SIZE<Key_t, Value_t>;
    if(idx % batch_size == 0)
	num = idx / batch_size;
    else
	num = idx / batch_size + 1;

    //lnode_btree_t<Key_t, Value_t>* leaf[num];
    auto leaf = new lnode_btree_t<Key_t, Value_t>*[num];
    for(int i=0; i<num; i++)
	leaf[i] = new lnode_btree_t<Key_t, Value_t>();

    int from = 0;
    for(int i=0; i<num; i++){
	if(i < num-1)
	    leaf[i]->sibling_ptr = static_cast<node_t*>(leaf[i+1]);
	else
	    leaf[i]->sibling_ptr = this->sibling_ptr;
	leaf[i]->batch_insert(buf, batch_size, from, idx);
    }
    leaf[num-1]->high_key = this->high_key;

    if(left_sibling_ptr)
	left_sibling_ptr->sibling_ptr = static_cast<node_t*>(leaf[0]);
    if(this->sibling_ptr){
	auto right_sibling = static_cast<lnode_t<Key_t, Value_t>*>(this->sibling_ptr);
	if(right_sibling->type == lnode_t<Key_t, Value_t>::HASH_NODE)
	    (static_cast<lnode_hash_t<Key_t, Value_t>*>(right_sibling))->left_sibling_ptr = reinterpret_cast<lnode_hash_t<Key_t, Value_t>*>(leaf[num-1]);
    }

    return leaf;
}

template <typename Key_t, typename Value_t>
void lnode_hash_t<Key_t, Value_t>::print(){
    std::cout << "left_sibling: " << left_sibling_ptr << std::endl;
    std::cout << "right_sibling: " << this->sibling_ptr << std::endl;
    std::cout << "node_high_key: " << this->high_key << std::endl;
    for(int i=0; i<cardinality; i++){
	std::cout << "Bucket " << i << std::endl;
	bucket[i].print();
    }
    std::cout << "\n\n";
}

template <typename Key_t, typename Value_t>
void lnode_hash_t<Key_t, Value_t>::sanity_check(Key_t _high_key, bool first){
    if(this->sibling_ptr != nullptr)
	(static_cast<lnode_t<Key_t, Value_t>*>(this->sibling_ptr))->sanity_check(this->high_key, false);
}

template <typename Key_t, typename Value_t>
double lnode_hash_t<Key_t, Value_t>::utilization(){
    int cnt = 0;
    for(int j=0; j<cardinality; j++){
	for(int i=0; i<entry_num; i++){
	    if(bucket[j].fingerprints[i] != 0)
		cnt++;
	}
    }
    return (double)cnt/(cardinality*entry_num);
}

template <typename Key_t, typename Value_t>
bool lnode_hash_t<Key_t, Value_t>::stabilize_all(uint64_t version){
    bool need_restart = false;
    #ifdef AVX512
    __m256i empty = _mm256_setzero_si256();
    #elif defined AVX2
    __m128i empty = _mm_setzero_si128();
    #else
    uint8_t empty = 0;
    #endif

    for(int j=0; j<cardinality; j++){
	if(bucket[j].state != bucket_t<Key_t, Value_t>::STABLE){
	    if(!bucket[j].try_lock())
		return false;
	}
	else continue;

	auto cur_version = this->get_version(need_restart);
	if(need_restart || (version != cur_version)){
	    bucket[j].unlock();
	    return false;
	}

	if(bucket[j].state == bucket_t<Key_t, Value_t>::LINKED_LEFT){
	    auto left = left_sibling_ptr;
	    auto left_bucket = &left->bucket[j];
	    if(!left_bucket->try_lock()){
		bucket[j].unlock();
		return false;
	    }
	    #ifdef AVX512
	    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(left_bucket->fingerprints));
	    __m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	    uint32_t bitfield = _mm256_movemask_epi8(cmp);
	    for(int i=0; i<32; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){
		    if(left->high_key < left_bucket->entry[i].key){ // migrate key-value in left node's bucket to current node's bucket
			bucket[j].fingerprints[i] = left_bucket->fingerprints[i];
			memcpy(&bucket[j].entry[i], &left_bucket->entry[i], sizeof(entry_t<Key_t, Value_t>));
			left_bucket->fingerprints[i] = 0;
		    }
		    // else key-value stays in left node's bucket
		}
	    }
	    #elif defined AVX2
	    for(int m=0; m<2; m++){
		__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(left_bucket->fingerprints + m*16));
		__m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		uint16_t bitfield = _mm_movemask_epi8(cmp);
		for(int i=0; i<16; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			auto idx = m*16 + i;
			if(left->high_key < left_bucket->entry[idx].key){ // migrate key-value in left node's bucket to current node's bucket
			    bucket[j].fingerprints[idx] = left_bucket->fingerprints[idx];
			    memcpy(&bucket[j].entry[idx], &left_bucket->entry[idx], sizeof(entry_t<Key_t, Value_t>));
			    left_bucket->fingerprints[idx] = 0;
			}
			// else key-value stays in left nodes' bucket
		    }
		}
	    }
	    #else
	    for(int i=0; i<entry_num; i++){
		if(left_bucket->fingerprints[i] != empty){
		    if(left->high_key < left_bucket->entry[i].key){ // migrate key-value in left node's bucket to current node's bucket
			bucket[j].fingerprints[i] = left_bucket->fingerprints[i];
			memcpy(&bucket[j].entry[i], &left_bucket->entry[i], sizeof(entry_t<Key_t, Value_t>));
			left_bucket->fingerprints[i] = 0;
		    }
		    // else key-value stays in left node's bucket
		}
	    }
	    #endif
	    bucket[j].state = bucket_t<Key_t, Value_t>::STABLE;
	    left_bucket->state = bucket_t<Key_t, Value_t>::STABLE;
	    left_bucket->unlock();
	    bucket[j].unlock();
	}
	else if(bucket[j].state == bucket_t<Key_t, Value_t>::LINKED_RIGHT){
	    auto right = static_cast<lnode_hash_t<Key_t, Value_t>*>(this->sibling_ptr);
	    auto right_bucket = &right->bucket[j];
	    if(!right_bucket->try_lock()){
		bucket[j].unlock();
		return false;
	    }
	    #ifdef AVX512
	    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[j].fingerprints));
	    __m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	    uint32_t bitfield = _mm256_movemask_epi8(cmp);
	    for(int i=0; i<32; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){
		    if(high_key < bucket[j].entry[i].key){ // migrate key-value in current node's bucket to right node's bucket
			right_bucket->fingerprints[i] = bucket[j].fingerprints[i];
			memcpy(&right_bucket->entry[i], &bucket[j].entry[i], sizeof(entry_t<Key_t, Value_t>));
			bucket[j].fingerprints[i] = 0;
		    }
		    // else key-value stays in current node's bucket
		}
	    }
	    #elif defined AVX2
	    for(int m=0; m<2; m++){
		__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[j].fingerprints + m*16));
		__m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		uint16_t bitfield = _mm_movemask_epi8(cmp);
		for(int i=0; i<16; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			auto idx = m*16 + i;
			if(lnode_t<Key_t, Value_t>::high_key < bucket[j].entry[idx].key){ // migrate key-value in current node's bucket to right node's bucket
			    right_bucket->fingerprints[idx] = bucket[j].fingerprints[idx];
			    memcpy(&right_bucket->entry[idx], &bucket[j].entry[idx], sizeof(entry_t<Key_t, Value_t>));
			    bucket[j].fingerprints[idx] = 0;
			}
			// else key-value stays in current nodes' bucket
		    }
		}
	    }
	    #else
	    for(int i=0; i<entry_num; i++){
		if(bucket[j].fingerprints[i] != empty){
		    if(high_key < bucket[j].entry[i].key){ // migrate key-value in current node's bucket to right node's bucket
			right_bucket->fingerprints[i] = bucket[j].fingerprints[i];
			memcpy(&right_bucket->entry[i], &bucket[j].entry[i], sizeof(entry_t<Key_t, Value_t>));
			bucket[j].fingerprints[i] = 0;
		    }
		    // else key-value stays in current node's bucket
		}
	    }
	    #endif
	    bucket[j].state = bucket_t<Key_t, Value_t>::STABLE;
	    right_bucket->state = bucket_t<Key_t, Value_t>::STABLE;
	    right_bucket->unlock();
	    bucket[j].unlock();
	}
	// else STABLE
    }
    return true;
}

template <typename Key_t, typename Value_t>
//bool lnode_hash_t<Key_t, Value_t>::stabilize_bucket(int loc , uint64_t bucket_version=0){
bool lnode_hash_t<Key_t, Value_t>::stabilize_bucket(int loc , uint64_t bucket_version){
    RETRY:
    bool need_restart = false;
    if(bucket_version){ // acquire bucket latch for read operations
	if(!bucket[loc].upgrade_lock(bucket_version)){
	    return false;
	}
    }

    if(bucket[loc].state == bucket_t<Key_t, Value_t>::LINKED_LEFT){
	auto left = left_sibling_ptr;
	auto left_bucket = &left->bucket[loc];
	auto left_vstart = left->get_version(need_restart);
	if(need_restart){
	    if(bucket_version)
		bucket[loc].unlock();
	    return false;
	}

	if(!left_bucket->try_lock()){
	    if(bucket_version)
		bucket[loc].unlock();
	    return false;
	}

	auto left_vend = left->get_version(need_restart);
	if(need_restart || (left_vstart != left_vend)){
	    left_bucket->unlock();
	    if(bucket_version)
		bucket[loc].unlock();
	    return false;
	}

	if(left_bucket->state == bucket_t<Key_t, Value_t>::LINKED_RIGHT){
	    #ifdef AVX512
	    __m256i empty = _mm256_setzero_si256();
	    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(left_bucket->fingerprints));
	    __m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	    uint32_t bitfield = _mm256_movemask_epi8(cmp);
	    for(int i=0; i<32; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){
		    if(left->high_key < left_bucket->entry[i].key){ // migrate key-value from left node's bucket to current node's bucket
			bucket[loc].fingerprints[i] = left_bucket->fingerprints[i];
			memcpy(&bucket[loc].entry[i], &left_bucket->entry[i], sizeof(entry_t<Key_t, Value_t>));
			left_bucket->fingerprints[i] = 0;
		    }
		    // else stays in left node's bucket
		}
	    }
	    #elif defined AVX2
	    __m128i empty = _mm_setzero_si128();
	    for(int m=0; m<2; m++){
		__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(left_bucket->fingerprints + m*16));
		__m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		uint16_t bitfield = _mm_movemask_epi8(cmp);
		for(int i=0; i<16; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			auto idx = m*16 + i;
			if(left->high_key < left_bucket->entry[idx].key){ // migrate key-value from left node's bucket to current node's bucket
			    bucket[loc].fingerprints[idx] = left_bucket->fingerprints[idx];
			    memcpy(&bucket[loc].entry[idx], &left_bucket->entry[idx], sizeof(entry_t<Key_t, Value_t>));
			    left_bucket->fingerprints[idx] = 0;
			}
			// else stays in left node's bucket
		    }
		}
	    }
	    #else
	    for(int i=0; i<entry_num; i++){
		if(left_bucket->fingerprints[i] != 0){
		    if(left->high_key < left_bucket->entry[i].key){ // migrate key-value from left node's bucket to current node's bucket
			bucket[loc].fingerprints[i] = left_bucket->fingerprints[i];
			memcpy(&bucket[loc].entry[i], &left_bucket->entry[i], sizeof(entry_t<Key_t, Value_t>));
			left_bucket->fingerprints[i] = 0;
		    }
		    // else stays in left node's bucket
		}
	    }
	    #endif
	    bucket[loc].state = bucket_t<Key_t, Value_t>::STABLE;
	    left_bucket->state = bucket_t<Key_t, Value_t>::STABLE;
	    left_bucket->unlock();
	}
	else{
	    std::cout << "[" << __func__ << "]: something wrong!" << std::endl;
	    std::cout << "\t current bucket state: " << bucket[loc].state << ", \t left bucket state: " << left_bucket->state << std::endl;
	    return false;
	}
    }
    else if(bucket[loc].state == bucket_t<Key_t, Value_t>::LINKED_RIGHT){
	auto right = static_cast<lnode_hash_t<Key_t, Value_t>*>(this->sibling_ptr);
	auto right_bucket = &right->bucket[loc];
	auto right_vstart = right->get_version(need_restart);
	if(need_restart){
	    if(bucket_version)
		bucket[loc].unlock();
	    return false;
	}

	if(!right_bucket->try_lock()){
	    if(bucket_version)
		bucket[loc].unlock();
	    return false;
	}

	auto right_vend = right->get_version(need_restart);
	if(need_restart || (right_vstart != right_vend)){
	    right_bucket->unlock();
	    if(bucket_version)
		bucket[loc].unlock();
	    return false;
	}

	if(right_bucket->state == bucket_t<Key_t, Value_t>::LINKED_LEFT){
	    #ifdef AVX512
	    __m256i empty = _mm256_setzero_si256();
	    __m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(bucket[loc].fingerprints));
	    __m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	    uint32_t bitfield = _mm256_movemask_epi8(cmp);
	    for(int i=0; i<32; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){
		    if(high_key < bucket[loc].entry[i].key){ // migrate key-value from current node's bucket to right node's bucket
			right_bucket->fingerprints[i] = bucket[loc].fingerprints[i];
			memcpy(&right_bucket->entry[i], &bucket[loc].entry[i], sizeof(entry_t<Key_t, Value_t>));
			bucket[loc].fingerprints[i] = 0;
		    }
		    // else stays in current node's bucket
		}
	    }
	    #elif defined AVX2
	    __m128i empty = _mm_setzero_si128();
	    for(int m=0; m<2; m++){
		__m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(bucket[loc].fingerprints + m*16));
		__m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
		uint16_t bitfield = _mm_movemask_epi8(cmp);
		for(int i=0; i<16; i++){
		    auto bit = (bitfield >> i);
		    if((bit & 0x1) == 0){
			auto idx = m*16 + i;
			if(lnode_t<Key_t, Value_t>::high_key < bucket[loc].entry[idx].key){ // migrate key-value from current node's bucket to right node's bucket
			    right_bucket->fingerprints[idx] = bucket[loc].fingerprints[idx];
			    memcpy(&right_bucket->entry[idx], &bucket[loc].entry[idx], sizeof(entry_t<Key_t, Value_t>));
			    bucket[loc].fingerprints[idx] = 0;
			}
			// else stays in current node's bucket
		    }
		}
	    }
	    #else
	    for(int i=0; i<entry_num; i++){
		if(bucket[loc].fingerprints[i] != 0){
		    if(high_key < bucket[loc].entry[i].key){ // migrate key-value from current node's bucket to right node's bucket
			right_bucket->fingerprints[i] = bucket[loc].fingerprints[i];
			memcpy(&right_bucket->entry[i], &bucket[loc].entry[i], sizeof(entry_t<Key_t, Value_t>));
			bucket[loc].fingerprints[i] = 0;
		    }
		    // else stays in current node's bucket
		}
	    }
	    #endif
	    bucket[loc].state = bucket_t<Key_t, Value_t>::STABLE;
	    right_bucket->state = bucket_t<Key_t, Value_t>::STABLE;
	    right_bucket->unlock();
	}
	else{
	    std::cout << "[" << __func__ << "]: something wrong!" << std::endl;
	    std::cout << "\t current bucket state: " << bucket[loc].state << ", \t right bucket state: " << right_bucket->state << std::endl;
	    return false;
	}
    }
    if(bucket_version)
	bucket[loc].unlock();
    return true;
}

template <typename Key_t, typename Value_t>
inline void lnode_hash_t<Key_t, Value_t>::swap(Key_t* a, Key_t* b){
    Key_t temp;
    memcpy(&temp, a, sizeof(Key_t));
    memcpy(a, b, sizeof(Key_t));
    memcpy(b, &temp, sizeof(Key_t));
}

template <typename Key_t, typename Value_t>
inline int lnode_hash_t<Key_t, Value_t>::partition(Key_t* keys, int left, int right){
    Key_t last = keys[right];
    int i = left, j = left;
    while(j < right){
	if(keys[j] < last){
	    swap(&keys[i], &keys[j]);
	    i++;
	}
	j++;
    }
    swap(&keys[i], &keys[right]);
    return i;
}

template <typename Key_t, typename Value_t>
inline int lnode_hash_t<Key_t, Value_t>::random_partition(Key_t* keys, int left, int right){
    int n = right - left + 1;
    int pivot = rand() % n;
    swap(&keys[left+pivot], &keys[right]);
    return partition(keys, left, right);
}

template <typename Key_t, typename Value_t>
inline void lnode_hash_t<Key_t, Value_t>::median_util(Key_t* keys, int left, int right, int k, int& a, int& b){
    if(left <= right){
	int partition_idx = random_partition(keys, left, right);
	if(partition_idx == k){
	    b = partition_idx;
	    if(a != -1)
		return;
	}
	else if(partition_idx == k-1){
	    a = partition_idx;
	    if(b != -1)
		return;
	}

	if(partition_idx >= k){
	    return median_util(keys, left, partition_idx-1, k, a, b);
	}
	else{
	    return median_util(keys, partition_idx+1, right, k, a, b);
	}
    }
}

template <typename Key_t, typename Value_t>
inline int lnode_hash_t<Key_t, Value_t>::find_median(Key_t* keys, int n){
    int ret;
    int a = -1, b = -1;
    if(n % 2 == 1){
	median_util(keys, 0, n-1, n/2-1, a, b);
	ret = b;
    }
    else{
	median_util(keys, 0, n-1, n/2, a, b);
	ret = (a+b)/2;
    }
    return ret;
}

template <typename Key_t, typename Value_t>
inline void lnode_hash_t<Key_t, Value_t>::prefetch_range(void* addr, size_t len){
    void* cp;
    void* end = addr + len;
    typedef struct {char x[64]; } cacheline_t;
    for(cp=addr; cp<end; cp+=64){
	asm volatile("prefetcht0 %0" : : "m" (*(const cacheline_t*)cp));
    }
}

template <typename Key_t, typename Value_t>
inline void lnode_hash_t<Key_t, Value_t>::prefetch(const void* addr){
    __builtin_prefetch(addr);
    //_mm_prefetch(addr, _MM_HINT_T0);
}

template class lnode_hash_t<uint64_t, uint64_t>;
}
