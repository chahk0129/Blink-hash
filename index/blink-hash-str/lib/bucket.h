#ifndef BLINK_HASH_BUCKET_H__
#define BLINK_HASH_BUCKET_H__

#include <atomic>
#include <cstdint>

#include "entry.h"

namespace BLINK_HASH{

template <typename Key_t, typename Value_t>
struct bucket_t{
    enum state_t{
	STABLE = 0,
	LINKED_LEFT,
	LINKED_RIGHT
    };

    std::atomic<uint32_t> lock;
    #ifdef LINKED
    state_t state;
    #endif
    #ifdef FINGERPRINT
    uint8_t fingerprints[entry_num];
    #endif
    entry_t<Key_t, Value_t> entry[entry_num];

    bucket_t(): lock(0){
	#ifdef FINGERPRINT
	memset(fingerprints, 0, sizeof(uint8_t)*entry_num);
	#else
	memset(entry, 0, sizeof(entry_t<Key_t, Value_t>)*entry_num);
	//memset(entry, EMPTY<Key_t>, sizeof(entry_t<Key_t, Value_t>)*entry_num);
	#endif
    }

    bool is_locked(uint32_t version){
	if((version & 0b10) == 0b10)
	    return true;
	return false;
    }

    bool try_lock(){
	auto version = lock.load();
	if(is_locked(version))
	    return false;

	if(!lock.compare_exchange_strong(version, version + 0b10)){
	    _mm_pause();
	    return false;
	}

	return true;
    }

    bool upgrade_lock(uint32_t version){
	auto _version = lock.load();
	if(_version != version)
	    return false;

	if(!lock.compare_exchange_strong(_version, _version + 0b10)){
	    _mm_pause();
	    return false;
	}

	return true;
    }

    void unlock(){
	lock.fetch_add(0b10);
    }

    uint32_t get_version(bool& need_restart){
	auto version = lock.load();
	if(is_locked(version))
	    need_restart = true;

	return version;
    }


#ifdef FINGERPRINT
    #ifdef AVX_256
    bool insert(Key_t key, Value_t value, uint8_t fingerprint, __m256i empty){
	__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(fingerprints));
	__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	uint32_t bitfield = _mm256_movemask_epi8(cmp);
	for(int i=0; i<32; i++){
	    auto bit = (bitfield >> i);
	    if((bit & 0x1) == 1){
		fingerprints[i] = fingerprint;
		entry[i].key = key;
		entry[i].value = value;
		return true;
	    }
	}
	return false;
    }
    #elif defined AVX_128
    bool insert(Key_t key, Value_t value, uint8_t fingerprint, __m128i empty){
	for(int m=0; m<2; m++){
	    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
	    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
	    uint32_t bitfield = _mm_movemask_epi8(cmp);
	    for(int i=0; i<16; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 1){
		    auto idx = m*16 + i;
		    fingerprints[idx] = fingerprint;
		    entry[idx].key = key;
		    entry[idx].value = value;
		    return true;
		}
	    }
	}
	return false;
    }
    #else
    bool insert(Key_t key, Value_t value, uint8_t fingerprint, uint8_t empty){
	for(int i=0; i<entry_num; i++){
	    if(fingerprints[i] == empty){
		fingerprints[i] = fingerprint;
		entry[i].key = key;
		entry[i].value = value;
		return true;
	    }
	}
	return false;
    }
    #endif
#else
    bool insert(Key_t key, Value_t value){
	for(int i=0; i<entry_num; i++){
	    if(entry[i].key == EMPTY<Key_t>){
		entry[i].key = key;
		entry[i].value = value;
		return true;
	    }
	}
	return false;
    }
#endif


#ifdef FINGERPRINT
    #ifdef AVX_256
    bool find(Key_t key, Value_t& value, __m256i fingerprint){
	__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(fingerprints));
	__m256i cmp = _mm256_cmpeq_epi8(fingerprint, fingerprints_);
	uint32_t bitfield = _mm256_movemask_epi8(cmp);
	for(int i=0; i<32; i++){
	    auto bit = (bitfield >> i);
	    if((bit & 0x1) == 1){
		if(entry[i].key == key){
		    value = entry[i].value;
		    return true;
		}
	    }
	}
	return false;
    }
    #elif defined AVX_128
    bool find(Key_t key, Value_t& value, __m128i fingerprint){
	for(int m=0; m<2; m++){
	    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
	    __m128i cmp = _mm_cmpeq_epi8(fingerprint, fingerprints_);
	    uint32_t bitfield = _mm_movemask_epi8(cmp);
	    for(int i=0; i<16; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 1){
		    auto idx = m*16 + i;
		    if(entry[idx].key == key){
			value = entry[idx].value;
			return true;
		    }
		}
	    }
	}
	return false;
    }
    #else
    bool find(Key_t key, Value_t& value, uint8_t fingerprint){
	for(int i=0; i<entry_num; i++){
	    if(fingerprints[i] == fingerprint){
		if(entry[i].key == key){
		    value = entry[i].value;
		    return true;
		}
	    }
	}
	return false;
    }
    #endif
#else
    bool find(Key_t key, Value_t& value){
	for(int i=0; i<entry_num; i++){
	    if(entry[i].key == key){
		value = entry[i].value;
		return true;
	    }
	}
	return false;
    }
#endif

#ifdef FINGERPRINT
    #ifdef AVX_256
    void collect(Key_t key, entry_t<Key_t, Value_t>* buf, int& num, __m256i empty){
	__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(fingerprints));
	__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	uint32_t bitfield = _mm256_movemask_epi8(cmp);
	for(int i=0; i<32; i++){
	    auto bit = (bitfield >> i);
	    if((bit & 0x1) == 0){
		if(entry[i].key >= key){
		    memcpy(&buf[num++], &entry[i], sizeof(entry_t<Key_t, Value_t>));
		}
	    }
	}
    }
    #elif defined AVX_128
    void collect(Key_t key, entry_t<Key_t, Value_t>* buf, int& num, __m128i empty){
	for(int m=0; m<2; m++){
	    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
	    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
	    uint32_t bitfield = _mm_movemask_epi8(cmp);
	    for(int i=0; i<16; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){
		    auto idx = m*16 + i;
		    if(key <= entry[idx].key){
			memcpy(&buf[num++], &entry[idx], sizeof(entry_t<Key_t, Value_t>));
		    }
		}
	    }
	}
    }
    #else
    void collect(Key_t key, entry_t<Key_t, Value_t>* buf, int& num, uint8_t empty){
	for(int i=0; i<entry_num; i++){
	    if(fingerprints[i] != empty){
		if(entry[i].key >= key){
		    memcpy(&buf[num++], &entry[i], sizeof(entry_t<Key_t, Value_t>));
		}
	    }
	}
    }
    #endif
#else
    void collect(Key_t key, entry_t<Key_t, Value_t>* buf, int& num){
	for(int i=0; i<entry_num; i++){
	    if(entry[i].key != EMPTY<Key_t>){
		if(entry[i].key >= key){
		    memcpy(&buf[num++], &entry[i], sizeof(entry_t<Key_t, Value_t>));
		}
	    }
	}
    }
#endif

#ifdef FINGERPRINT
    #ifdef AVX_256
    void collect(entry_t<Key_t, Value_t>* buf, int& num, __m256i empty){
	__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(fingerprints));
	__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	uint32_t bitfield = _mm256_movemask_epi8(cmp);
	for(int i=0; i<32; i++){
	    auto bit = (bitfield >> i);
	    if((bit & 0x1) == 0){
		memcpy(&buf[num++], &entry[i], sizeof(entry_t<Key_t, Value_t>));
	    }
	}
    }
    #elif defined AVX_128
    void collect(entry_t<Key_t, Value_t>* buf, int& num, __m128i empty){
	for(int m=0; m<2; m++){
	    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
	    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
	    uint32_t bitfield = _mm_movemask_epi8(cmp);
	    for(int i=0; i<16; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){
		    auto idx = m*16 + i;
		    memcpy(&buf[num++], &entry[idx], sizeof(entry_t<Key_t, Value_t>));
		}
	    }
	}
    }
    #else
    void collect(entry_t<Key_t, Value_t>* buf, int& num, uint8_t empty){
	for(int i=0; i<entry_num; i++){
	    if(fingerprints[i] != empty){
		memcpy(&buf[num++], &entry[i], sizeof(entry_t<Key_t, Value_t>));
	    }
	}
    }
    #endif
#else
    void collect(entry_t<Key_t, Value_t>* buf, int& num){
	for(int i=0; i<entry_num; i++){
	    if(entry[i].key != EMPTY<Key_t>){
		memcpy(&buf[num++], &entry[i], sizeof(entry_t<Key_t, Value_t>));
	    }
	}
    }
#endif

#ifdef FINGERPRINT
    #ifdef AVX_256
    bool update(Key_t key, Value_t value, __m256i fingerprint){
	__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(fingerprints));
	__m256i cmp = _mm256_cmpeq_epi8(fingerprint, fingerprints_);
	uint32_t bitfield = _mm256_movemask_epi8(cmp);
	for(int i=0; i<32; i++){
	    auto bit = (bitfield >> i);
	    if((bit & 0x1) == 1){
		if(entry[i].key == key){
		    entry[i].value = value;
		    return true;
		}
	    }
	}
	return false;
    }
    #elif defined AVX_128
    bool update(Key_t key, Value_t value, __m128i fingerprint){
	for(int m=0; m<2; m++){
	    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
	    __m128i cmp = _mm_cmpeq_epi8(fingerprint, fingerprints_);
	    uint32_t bitfield = _mm_movemask_epi8(cmp);
	    for(int i=0; i<16; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 1){
		    auto idx = m*16 + i;
		    if(entry[idx].key == key){
			entry[idx].value = value;
			return true;
		    }
		}
	    }
	}
	return false;
    }
    #else
    bool update(Key_t key, Value_t value, uint8_t fingerprint){
	for(int i=0; i<entry_num; i++){
	    if(fingerprints[i] == fingerprint){
		if(entry[i].key == key){
		    entry[i].value = value;
		    return true;
		}
	    }
	}
	return false;
    }
    #endif
#else
    bool update(Key_t key, Value_t value){
	for(int i=0; i<entry_num; i++){
	    if(entry[i].key == key){
		entry[i].value = value;
		return true;
	    }
	}
	return false;
    }
#endif

#ifdef FINGERPRINT
    #ifdef AVX_256
    bool collect_keys(Key_t* keys, int& num, int cardinality, __m256i empty){
	__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(fingerprints));
	__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	uint32_t bitfield = _mm256_movemask_epi8(cmp);
	for(int i=0; i<32; i++){
	    auto bit = (bitfield >> i);
	    if((bit & 0x1) == 0){
		keys[num++] = entry[i].key;
		if(num == cardinality)
		    return true;
	    }
	}
	return false;
    }
    #elif defined AVX_128
    bool collect_keys(Key_t* keys, int& num, int cardinality, __m128i empty){
	for(int m=0; m<2; m++){
	    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
	    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
	    uint16_t bitfield = _mm_movemask_epi8(cmp);
	    for(int i=0; i<16; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){
		    auto idx = m*16 + i;
		    keys[num++] = entry[idx].key;
		    if(num == cardinality)
			return true;
		}
	    }
	}
	return false;
    }
    #else
    bool collect_keys(Key_t* keys, int& num, int cardinality, uint8_t empty){
	for(int i=0; i<entry_num; i++){
	    if(fingerprints[i] != empty){
		keys[num++] = entry[i].key;
		if(num == cardinality)
		    return true;
	    }
	}
	return false;
    }
    #endif
#else
    bool collect_keys(Key_t* keys, int& num, int cardinality){
	for(int i=0; i<entry_num; i++){
	    if(entry[i].key != EMPTY<Key_t>){
		keys[num++] = entry[i].key;
		if(num == cardinality)
		    return true;
	    }
	}
	return false;
    }
#endif

#ifdef FINGERPRINT
    #ifdef AVX_256
    void collect_all_keys(Key_t* keys, int& num, __m256i empty){
	__m256i fingerprints_ = _mm256_loadu_si256(reinterpret_cast<__m256i*>(fingerprints));
	__m256i cmp = _mm256_cmpeq_epi8(empty, fingerprints_);
	uint32_t bitfield = _mm256_movemask_epi8(cmp);
	for(int i=0; i<32; i++){
	    auto bit = (bitfield >> i);
	    if((bit & 0x1) == 0){
		keys[num++] = entry[i].key;
	    }
	}
    }
    #elif defined AVX_128
    void collect_all_keys(Key_t* keys, int& num, __m128i empty){
	for(int m=0; m<2; m++){
	    __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
	    __m128i cmp = _mm_cmpeq_epi8(empty, fingerprints_);
	    uint16_t bitfield = _mm_movemask_epi8(cmp);
	    for(int i=0; i<16; i++){
		auto bit = (bitfield >> i);
		if((bit & 0x1) == 0){
		    auto idx = m*16 + i;
		    keys[num++] = entry[idx].key;
		}
	    }
	}
    }
    #else
    void collect_all_keys(Key_t* keys, int& num, uint8_t empty){
	for(int i=0; i<entry_num; i++){
	    if(fingerprints[i] != empty){
		keys[num++] = entry[i].key;
	    }
	}
    }
    #endif
#else
    void collect_all_keys(Key_t* keys, int& num){
	for(int i=0; i<entry_num; i++){
	    if(entry[i].key != EMPTY<Key_t>){
		keys[num++] = entry[i].key;
	    }
	}
    }
#endif

    void print(){
    }

};

}

#endif
