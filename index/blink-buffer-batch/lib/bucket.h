#ifndef BLINK_BUFFER_BATCH_BUCKET_H__
#define BLINK_BUFFER_BATCH_BUCKET_H__

#include <atomic>
#include <cstdint>
#include <immintrin.h>
#include <cstring>
#include <iostream>

#include "entry.h"

namespace BLINK_BUFFER_BATCH{

template <typename Key_t, typename Value_t>
struct bucket_t{
    std::atomic<uint32_t> lock;
    uint8_t fingerprints[entry_num];
    entry_t<Key_t, Value_t> entry[entry_num];

    bucket_t(): lock(0){
	memset(fingerprints, 0, sizeof(uint8_t)*entry_num);
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

    bool remove(Key_t key, __m128i fingerprint){
	for(int m=0; m<2; m++){
            __m128i fingerprints_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(fingerprints + m*16));
            __m128i cmp = _mm_cmpeq_epi8(fingerprint, fingerprints_);
            uint32_t bitfield = _mm_movemask_epi8(cmp);
            for(int i=0; i<16; i++){
                auto bit = (bitfield >> i);
                if((bit & 0x1) == 1){
                    auto idx = m*16 + i;
                    if(entry[idx].key == key){
                        fingerprints[idx] = 0;
                        return true;
                    }
                }
            }
        }
        return false;
    }

    void clear(){
	memset(fingerprints, 0, sizeof(uint8_t)*entry_num);
    }

    void print(){
	for(int i=0; i<entry_num; i++){
	    if((fingerprints[i] & 0x1) == 1){
		std::cout << "key: " << entry[i].key << ", ";
	    }
	}
    }
};
}
#endif
