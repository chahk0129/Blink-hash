#ifndef BLINK_HASHED_BUCKET_H__
#define BLINK_HASHED_BUCKET_H__

#include <atomic>
#include <cstdint>

#include "entry.h"

namespace BLINK_HASHED{

template <typename Key_t, typename Value_t>
struct bucket_t{
    enum state_t{
	STABLE = 0,
	LINKED_LEFT,
	LINKED_RIGHT
    };

    std::atomic<uint32_t> lock;
    state_t state;
    uint8_t fingerprints[entry_num];
    entry_t<Key_t, Value_t> entry[entry_num];

    bucket_t(){
	memset(fingerprints, 0, sizeof(uint8_t)*entry_num);
    }
    bool try_lock(){
	auto lock_ = lock.load();
	if((lock_ & 0b10) == 0b10){
	    return false;
	}

	if(!lock.compare_exchange_strong(lock_, lock_ + 0b10)){
	    return false;
	}

	return true;
    }

    bool upgrade_lock(uint64_t version){
	auto lock_ = lock.load();
	if(lock_ != version){
	    return false;
	}

	if(!lock.compare_exchange_strong(lock_, lock_ + 0b10)){
	    return false;
	}

	return true;
    }

    void unlock(){
	lock.fetch_add(0b10);
    }

    uint64_t get_version(bool& need_restart){
	auto lock_ = lock.load();
	if((lock_ & 0b10) == 0b10){
	    need_restart = true;
	}

	return lock_;
    }

};

}

#endif
