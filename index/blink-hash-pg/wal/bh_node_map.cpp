
#include "bh_node_map.h"
#include "node.h"

#include <cassert>
#include <cstdio>

namespace BLINK_HASH {
namespace WAL {


NodeMap* g_node_map = nullptr;

void node_map_init(size_t num_shards) {
    if (!g_node_map)
        g_node_map = new NodeMap(num_shards);
}

void node_map_destroy() {
    delete g_node_map;
    g_node_map = nullptr;
}



NodeMap::NodeMap(size_t num_shards)
    : num_shards_(num_shards), shards_(num_shards) {
    for (auto& s : shards_) {
        s.head  = nullptr;
        s.count = 0;
    }
}

NodeMap::~NodeMap() {
    clear();
}



void NodeMap::register_node(uint64_t node_id, node_t* ptr) {
    size_t idx = shard_for(node_id);
    auto& shard = shards_[idx];

    std::lock_guard<std::mutex> lk(shard.mu);


    for (Entry* e = shard.head; e; e = e->next) {
        if (e->node_id == node_id) {
            e->ptr = ptr;
            return;
        }
    }

    Entry* entry = new Entry{node_id, ptr, shard.head};
    shard.head = entry;
    shard.count++;
}



node_t* NodeMap::resolve(uint64_t node_id) const {
    size_t idx = shard_for(node_id);
    const auto& shard = shards_[idx];

    std::lock_guard<std::mutex> lk(shard.mu);

    for (Entry* e = shard.head; e; e = e->next) {
        if (e->node_id == node_id)
            return e->ptr;
    }
    return nullptr;
}



node_t* NodeMap::remove(uint64_t node_id) {
    size_t idx = shard_for(node_id);
    auto& shard = shards_[idx];

    std::lock_guard<std::mutex> lk(shard.mu);

    Entry** pp = &shard.head;
    while (*pp) {
        Entry* e = *pp;
        if (e->node_id == node_id) {
            node_t* ptr = e->ptr;
            *pp = e->next;
            delete e;
            shard.count--;
            return ptr;
        }
        pp = &e->next;
    }
    return nullptr;
}



size_t NodeMap::size() const {
    size_t total = 0;
    for (const auto& s : shards_) {
        std::lock_guard<std::mutex> lk(s.mu);
        total += s.count;
    }
    return total;
}



void NodeMap::clear() {
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> lk(shard.mu);
        Entry* e = shard.head;
        while (e) {
            Entry* next = e->next;
            delete e;
            e = next;
        }
        shard.head  = nullptr;
        shard.count = 0;
    }
}



void NodeMap::build_from_tree(node_t* root) {
    if (!root) return;

    /*
     * Level-order traversal via leftmost_ptr + sibling chains.
     * Same pattern as btree_t::destroy() and footprint().
     */
    node_t* level_head = root;
    uint64_t count = 0;

    while (level_head) {
        node_t* next_level_head = nullptr;
        if (level_head->level > 0)
            next_level_head = level_head->leftmost_ptr;

        node_t* cur = level_head;
        while (cur) {
            if (cur->node_id != 0) {
                register_node(cur->node_id, cur);
                count++;
            }
            cur = cur->sibling_ptr;
        }

        level_head = next_level_head;
    }

    printf("[node_map] built from tree: %llu nodes registered\n",
           (unsigned long long)count);
}


void NodeMap::for_each(std::function<void(uint64_t, node_t*)> fn) const {
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lk(shard.mu);
        for (Entry* e = shard.head; e; e = e->next) {
            fn(e->node_id, e->ptr);
        }
    }
}

} 
} 