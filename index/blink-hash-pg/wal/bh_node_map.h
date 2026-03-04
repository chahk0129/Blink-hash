#ifndef BLINK_HASH_BH_NODE_MAP_H__
#define BLINK_HASH_BH_NODE_MAP_H__

/*
 * bh_node_map.h — Node ID → node_t* runtime map
 *
 * Used during recovery to resolve WAL records that reference nodes
 * by their persistent IDs (splits, converts, new-root).
 *
 * Also available at runtime for structural operations that need
 * to find nodes by ID (e.g., checkpoint CoW tracking).
 *
 * Thread-safety: sharded mutexes for writes, lock-free reads via
 * atomic slot states.
 */

#include <atomic>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <vector>
#include <functional>

namespace BLINK_HASH {

class node_t;

namespace WAL {

class NodeMap {
public:
    /*
     * Construct a NodeMap with `num_shards` shards.
     * Each shard is a bucket chained hash table.
     * Default: 256 shards — up to 256 concurrent writers.
     */
    explicit NodeMap(size_t num_shards = 256);
    ~NodeMap();

  
    NodeMap(const NodeMap&) = delete;
    NodeMap& operator=(const NodeMap&) = delete;


    void register_node(uint64_t node_id, node_t* ptr);


    node_t* resolve(uint64_t node_id) const;
    node_t* remove(uint64_t node_id);

    size_t size() const;


    void clear();

    void build_from_tree(node_t* root);


    void for_each(std::function<void(uint64_t, node_t*)> fn) const;

private:
    struct Entry {
        uint64_t  node_id;
        node_t*   ptr;
        Entry*    next;
    };

    struct Shard {
        mutable std::mutex mu;
        Entry*  head;
        size_t  count;
    };

    size_t              num_shards_;
    std::vector<Shard>  shards_;

    size_t shard_for(uint64_t node_id) const {
        /* Fibonacci hashing for good distribution */
        return static_cast<size_t>(
            (node_id * 11400714819323198485ULL) >> (64 - 8)
        ) % num_shards_;
    }
};


extern NodeMap* g_node_map;


void node_map_init(size_t num_shards = 256);


void node_map_destroy();

} 
} 

#endif