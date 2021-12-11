#ifndef SPINLOCK_H__
#define SPINLOCK_H__

#include <atomic>
namespace BLINK_HASHED{

class spinlock_t{
    std::atomic_flag flag;

    public:
      spinlock_t(): flag(ATOMIC_FLAG_INIT) { }

      void lock(){
	  bool before;
	  while((before = flag.test_and_set())){
	      #if (__x86__ || __x86_64__)
	      _mm_pause();
    	      #else
	      asm volatile("yield");
	      #endif
	  }
      }

      bool try_lock(){
	  bool before = flag.test_and_set();
	  return !before;
      }

      void lock_non_atomic(){
	  bool before;
	  while((before = flag.test_and_set(std::memory_order_relaxed))){
	      ;
	  }
      }

      void unlock(){
	  flag.clear(std::memory_order_release);
      }

      void unlock_non_atomic(){
	  flag.clear(std::memory_order_relaxed);
      }
};
}
#endif
