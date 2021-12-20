#ifndef BLINK_HASHED_ALLOCATOR_H__
#define BLINK_HASHED_ALLOCATOR_H__

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <unistd.h>
#include <assert.h>

namespace BLINK_HASHED{

class leaf_allocator_t{
    public:
	uint64_t end;
	std::atomic<uint64_t> offset;
	void* ptr;

	leaf_allocator_t(uint64_t chunk_size, int num) {
	    uint64_t size = chunk_size * num;
	    assert(size > 0);
	    auto ret = posix_memalign(&ptr, 64, size);
	    if(ret == ENOMEM){
		std::cerr << "Insufficient memory error" << std::endl;
		exit(0);
	    }
	    else if(ret == EINVAL){
		std::cerr << "Incorrect alginment unit error" << std::endl;
		exit(0);
	    }
	    offset.store((uint64_t)ptr, std::memory_order_relaxed);
	    end = (uint64_t)ptr + chunk_size * num;
	}

	uint64_t request_mem(uint64_t size){
	    auto offset_ = offset.load();
	    if(offset_ + size > end)
		return 0;

	    while(!offset.compare_exchange_strong(offset_, offset_ + size)){
		_mm_pause();
		offset_ = offset.load();
		if(offset_ + size > end)
		    return 0;
	    }
	    return offset_;
	}

};

}
#endif


