#ifndef BLINK_HASHED_THREADINFO_H__
#define BLINK_HASHED_THREADINFO_H__

#include <cstdlib>
#include <unistd.h>
#include <cstdint>
#include <iostream>

namespace BLINK_HASHED{

class threadinfo{
    public:
    void* mem;

    threadinfo(){ }
    ~threadinfo(){
	if(mem != nullptr)
	    delete mem;
    }

    static threadinfo* make(int purpose, int index){
	threadinfo* ti = new threadinfo();
	ti->mem = nullptr;
	return ti;
    }

    void* allocate(size_t size){
	if(mem != nullptr){
	    auto ptr = mem;
	    mem = nullptr;
	    return ptr;
	}

	void* ptr;
	auto ret = posix_memalign(&ptr, 64, size);
	return ptr;
	/*
	auto ptr = new char[size];
	return ptr;
	*/
    }

    void deallocate(void* ptr){
	//if(mem == nullptr)
	    mem = ptr;
    }

   /* 
    void* operator new[] (size_t size){
	void* ptr;
	auto ret = posix_memalign(&ptr, 64, size);
	return ptr;
    }
    */

};

}

#endif
