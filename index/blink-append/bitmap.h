#ifndef BITMAP_H__
#define BITMAP_H__

#include <cstdint>
#include <atomic>
#include <cassert>

#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

#define BITS_MAX 64

class bitmap_t{
    private:
	std::atmoic<uint64_t> bits;

	int get_next_zero_bit(){
	    uint64_t bit* = bits.load();  



#endif
