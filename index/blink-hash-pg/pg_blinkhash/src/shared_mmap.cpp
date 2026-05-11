#include "shared_mmap.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <atomic>
#include <unistd.h>
#include <iostream>

struct SharedGlobal {
    std::atomic_flag lock;
    void* tree_root;
    std::atomic<size_t> offset;
    size_t max_size;
};

static SharedGlobal* global = nullptr;
static void* shared_base = nullptr;

extern "C" {

void* bh_shared_mmap_init(size_t size) {
    if (shared_base) return shared_base;
    
    int fd = shm_open("/blinkhash_shm", O_CREAT | O_RDWR, 0666);
    if (fd < 0) return nullptr;
    ftruncate(fd, size);
    
    void* addr = (void*)0x6F0000000000ULL;
    shared_base = mmap(addr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0);
    close(fd);
    
    if (shared_base == MAP_FAILED) {
        shared_base = nullptr;
        return nullptr;
    }
    
    global = (SharedGlobal*)shared_base;
    
    // Initialize headers if offset is 0
    size_t expected = 0;
    if (global->offset.compare_exchange_strong(expected, sizeof(SharedGlobal) + 64)) {
        global->max_size = size;
        global->tree_root = nullptr;
        global->lock.clear();
    }
    return shared_base;
}

void* bh_shared_malloc(size_t size) {
    if (!global) return malloc(size); // Fallback
    size_t off = global->offset.fetch_add(size + 64);
    if (off + size > global->max_size) return nullptr; // OOM in arena
    return (char*)shared_base + off;
}

void bh_shared_free(void* ptr) {
}

void* bh_get_global_tree(void) {
    if (!global) return nullptr;
    return global->tree_root;
}

void bh_set_global_tree(void* tree) {
    if (global) global->tree_root = tree;
}

void bh_global_lock(void) {
    if (global) while(global->lock.test_and_set(std::memory_order_acquire));
}

void bh_global_unlock(void) {
    if (global) global->lock.clear(std::memory_order_release);
}

}

#ifdef BH_USE_SHARED_MEMORY
void* operator new(size_t size) {
    if (shared_base) return bh_shared_malloc(size);
    return malloc(size);
}
void operator delete(void* ptr) noexcept {
    if (shared_base && ptr >= shared_base && ptr < (char*)shared_base + (global ? global->max_size : 0)) return;
    free(ptr);
}
void* operator new[](size_t size) {
    if (shared_base) return bh_shared_malloc(size);
    return malloc(size);
}
void operator delete[](void* ptr) noexcept {
    if (shared_base && ptr >= shared_base && ptr < (char*)shared_base + (global ? global->max_size : 0)) return;
    free(ptr);
}
#endif
