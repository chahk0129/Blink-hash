#include <iostream>
#include <byteswap.h>
#include "include/indexkey.h"
#include "index/ARTOLC/Tree.h"
#include "index/ARTROWEX/Tree.h"
#include "index/blink/tree_optimized.h"
#include "index/libcuckoo/libcuckoo/cuckoohash_map.hh"
#include "index/BTreeOLC/BTreeOLC_adjacent_layout.h"
#include "index/BwTree/bwtree.h"
#include "index/masstree/mtIndexAPI.hh"
#include "index/hot/src/wrapper.h"
#ifdef STRING_KEY
#include "index/blink-hash-str/lib/tree.h"
#else
#include "index/blink-hash/lib/tree.h"
#endif
#ifndef _INDEX_H
#define _INDEX_H

using namespace wangziqi2013;
using namespace bwtree;

template<typename KeyType, class KeyComparator>
class Index
{
    public:

	virtual bool insert(KeyType key, uint64_t value, threadinfo *ti) = 0;
	virtual uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) = 0;
	virtual bool upsert(KeyType key, uint64_t value, threadinfo *ti) = 0;
	virtual uint64_t scan(KeyType key, int range, threadinfo *ti) = 0;

	virtual uint64_t find_bwtree_fast(KeyType key, std::vector<uint64_t> *v) {};

	// Used for bwtree only
	virtual bool insert_bwtree_fast(KeyType key, uint64_t value) {};

	virtual void getMemory() = 0;
	virtual void find_depth() = 0;
	virtual void convert() = 0;

	// This initializes the thread pool
	virtual void UpdateThreadLocal(size_t thread_num) = 0;
	virtual void AssignGCID(size_t thread_id) = 0;
	virtual void UnregisterThread(size_t thread_id) = 0;

	// After insert phase perform this action
	// By default it is empty
	// This will be called in the main thread
	virtual void AfterLoadCallback() {}

	// This is called after threads finish but before the thread local are
	// destroied by the thread manager
	virtual void CollectStatisticalCounter(int) {}
	virtual size_t GetIndexSize() { return 0UL; }

	// Destructor must also be virtual
	virtual ~Index() {}

	#ifdef BREAKDOWN
	virtual void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation) = 0;
	#endif
};


/////////////////////////////////////////////////////////////////////
// ARTOLC
/////////////////////////////////////////////////////////////////////

template<typename KeyType, class KeyComparator>
class ArtOLCIndex : public Index<KeyType, KeyComparator>
{
    public:

	~ArtOLCIndex() {
	    delete idx;
	}

	void UpdateThreadLocal(size_t thread_num) {}
	void AssignGCID(size_t thread_id) {}
	void UnregisterThread(size_t thread_id) {}

	void setKey(Key& k, uint64_t key) { k.setInt(key); }
	void setKey(Key& k, GenericKey<32> key) { k.set(key.data,32); }
	void setKey(Key& k, GenericKey<128> key) { k.set(key.data,128); }


	bool insert(KeyType key, uint64_t value, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    Key k; setKey(k, key);
	    idx->insert(k, value, t);
	    return true;
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    Key k; setKey(k, key);
	    uint64_t result=idx->lookup(k, t);

	    v->clear();
	    v->push_back(result);
	    return 0;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    Key k; setKey(k, key);
	    idx->insert(k, value, t);
	    return 0;
	}

	uint64_t scan(KeyType key, int range, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    Key startKey; setKey(startKey, key);

	    TID results[range];
	    size_t resultCount = 0;
	    Key continueKey;
	    idx->lookupRange(startKey, maxKey, continueKey, results, range, resultCount, t);

	    return resultCount;
	}

	void getMemory() {
	    uint64_t meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied;
	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    idx->footprint(meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint]" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;
	}

	void find_depth(){
	    idx->find_depth();
	}

	void convert(){ }

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){
	    time_consolidation = 0;
	    idx->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
	}
	#endif

	ArtOLCIndex(uint64_t kt) {
	    if (sizeof(KeyType)==8) {
		idx = new ART_OLC::Tree([](TID tid, Key &key) { key.setInt(*reinterpret_cast<uint64_t*>(tid)); });
		maxKey.setInt(~0ull);
	    }
	    else if(sizeof(KeyType) == 32){
		idx = new ART_OLC::Tree([](TID tid, Key &key) { key.set(reinterpret_cast<char*>(tid), 32); });
		uint8_t m[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
		maxKey.set((char*)m,32);
	    }
	    else{
		idx = new ART_OLC::Tree([](TID tid, Key &key) { key.set(reinterpret_cast<char*>(tid), 128); });
		uint8_t m[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
		maxKey.set((char*)m,128);
	    }
	}

    private:
	Key maxKey;
	ART_OLC::Tree *idx;
};

/////////////////////////////////////////////////////////////////////
// ARTROWEX
/////////////////////////////////////////////////////////////////////

template<typename KeyType, class KeyComparator>
class ArtROWEXIndex : public Index<KeyType, KeyComparator>
{
    public:

	~ArtROWEXIndex() {
	    delete idx;
	}

	void UpdateThreadLocal(size_t thread_num) {}
	void AssignGCID(size_t thread_id) {}
	void UnregisterThread(size_t thread_id) {}

	void setKey(Key& k, uint64_t key) { k.setInt(key); }
	void setKey(Key& k, GenericKey<32> key) { k.set(key.data,32); }
	void setKey(Key& k, GenericKey<128> key) { k.set(key.data,128); }

	bool insert(KeyType key, uint64_t value, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    Key k; setKey(k, key);
	    idx->insert(k, value, t);
	    return true;
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    Key k; setKey(k, key);
	    uint64_t result=idx->lookup(k, t);
	    v->clear();
	    v->push_back(result);
	    return 0;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    Key k; setKey(k, key);
	    idx->insert(k, value, t);
	    return 0;
	}

	uint64_t scan(KeyType key, int range, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    Key startKey; setKey(startKey, key);

	    TID results[range];
	    size_t resultCount;
	    Key continueKey;
	    idx->lookupRange(startKey, maxKey, continueKey, results, range, resultCount, t);

	    return resultCount;
	}

	void getMemory() {
	    uint64_t meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied;
	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    idx->footprint(meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint]" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;
	}
	void find_depth(){ }

	void convert(){ }

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){
	    time_consolidation = 0;
	    idx->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
	}
	#endif


	ArtROWEXIndex(uint64_t kt) {
	    if (sizeof(KeyType) == 8) {
		idx = new ART_ROWEX::Tree([](TID tid, Key &key) { key.setInt(*reinterpret_cast<uint64_t*>(tid)); });
		maxKey.setInt(~0ull);
	    } 
	    else if(sizeof(KeyType) == 32){
		idx = new ART_ROWEX::Tree([](TID tid, Key &key) { key.set(reinterpret_cast<char*>(tid), 32); });
		uint8_t m[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
		maxKey.set((char*)m,32);
	    }
	    else{
		idx = new ART_ROWEX::Tree([](TID tid, Key &key) { key.set(reinterpret_cast<char*>(tid), 128); });
		uint8_t m[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
		    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
		maxKey.set((char*)m,128);
	    }
	}

    private:
	Key maxKey;
	ART_ROWEX::Tree *idx;
};

////////////////////////////////////
///////////// Bw-tree //////////////
////////////////////////////////////

template<typename KeyType, 
    typename KeyComparator,
    typename KeyEqualityChecker=std::equal_to<KeyType>,
    typename KeyHashFunc=std::hash<KeyType>>
    class BwTreeIndex : public Index<KeyType, KeyComparator>
{
    public:
	using index_type = BwTree<KeyType, uint64_t, KeyComparator, KeyEqualityChecker, KeyHashFunc>;
	using BaseNode = typename index_type::BaseNode;

	BwTreeIndex(uint64_t kt) {
	    index_p = new index_type{};
	    assert(index_p != nullptr);
	    (void)kt;

	    // Print the size of preallocated storage
	    fprintf(stderr, "Inner prealloc size = %lu; Leaf prealloc size = %lu\n",
		    index_type::INNER_PREALLOCATION_SIZE,
		    index_type::LEAF_PREALLOCATION_SIZE);

	    return;
	}

	~BwTreeIndex() {
	    delete index_p;
	    return;
	}

#ifdef BWTREE_COLLECT_STATISTICS
	void CollectStatisticalCounter(int thread_num) {
	    static constexpr int counter_count = \
						 BwTreeBase::GCMetaData::CounterType::COUNTER_COUNT;
	    int counters[counter_count];

	    // Aggregate on the array of counters
	    memset(counters, 0x00, sizeof(counters));

	    for(int i = 0;i < thread_num;i++) {
		for(int j = 0;j < counter_count;j++) {
		    counters[j] += index_p->GetGCMetaData(i)->counters[j];
		}
	    }

	    fprintf(stderr, "Statistical counters:\n");
	    for(int j = 0;j < counter_count;j++) {
		fprintf(stderr,
			"    counter %s = %d\n",
			BwTreeBase::GCMetaData::COUNTER_NAME_LIST[j],
			counters[j]);
	    }

	    return;
	}
#endif

	void AfterLoadCallback() {
	    int inner_depth_total = 0,
		leaf_depth_total = 0,
		inner_node_total = 0,
		leaf_node_total = 0;
	    int inner_size_total = 0, leaf_size_total = 0;
	    size_t inner_alloc_total = 0, inner_used_total = 0;
	    size_t leaf_alloc_total = 0, leaf_used_total = 0;

	    uint64_t index_root_id = index_p->root_id.load();
	    fprintf(stderr, "BwTree - Start consolidating delta chains...\n");
	    int ret = index_p->DebugConsolidateAllRecursive(
		    index_root_id,
		    &inner_depth_total,
		    &leaf_depth_total,
		    &inner_node_total,
		    &leaf_node_total,
		    &inner_size_total,
		    &leaf_size_total,
		    &inner_alloc_total,
		    &inner_used_total,
		    &leaf_alloc_total,
		    &leaf_used_total);
	    fprintf(stderr, "BwTree - Finished consolidating %d delta chains\n", ret);

	    fprintf(stderr,
		    "    Inner Avg. Depth: %f (%d / %d)\n",
		    (double)inner_depth_total / (double)inner_node_total,
		    inner_depth_total,
		    inner_node_total);
	    fprintf(stderr,
		    "    Inner Avg. Size: %f (%d / %d)\n",
		    (double)inner_size_total / (double)inner_node_total,
		    inner_size_total,
		    inner_node_total);
	    fprintf(stderr,
		    "    Leaf Avg. Depth: %f (%d / %d)\n",
		    (double)leaf_depth_total / (double)leaf_node_total,
		    leaf_depth_total,
		    leaf_node_total);
	    fprintf(stderr,
		    "    Leaf Avg. Size: %f (%d / %d)\n",
		    (double)leaf_size_total / (double)leaf_node_total,
		    leaf_size_total,
		    leaf_node_total);

	    fprintf(stderr,
		    "Inner Alloc. Util: %f (%lu / %lu)\n",
		    (double)inner_used_total / (double)inner_alloc_total,
		    inner_used_total,
		    inner_alloc_total);

	    fprintf(stderr,
		    "Leaf Alloc. Util: %f (%lu / %lu)\n",
		    (double)leaf_used_total / (double)leaf_alloc_total,
		    leaf_used_total,
		    leaf_alloc_total);

	    // Only do thid after the consolidation, because the mapping will change
	    // during the consolidation

#ifndef BWTREE_USE_MAPPING_TABLE
	    fprintf(stderr, "Replacing all NodeIDs to BaseNode *\n");
	    BaseNode *node_p = (BaseNode *)index_p->GetNode(index_p->root_id.load());
	    index_p->root_id = reinterpret_cast<NodeID>(node_p);
	    index_p->DebugReplaceNodeIDRecursive(node_p);
#endif
	    return;
	}

	void UpdateThreadLocal(size_t thread_num) { 
	    index_p->UpdateThreadLocal(thread_num); 
	}

	void AssignGCID(size_t thread_id) {
	    index_p->AssignGCID(thread_id); 
	}

	void UnregisterThread(size_t thread_id) {
	    index_p->UnregisterThread(thread_id); 
	}

	bool insert(KeyType key, uint64_t value, threadinfo *) {
	    return index_p->Insert(key, value);
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *) {
	    v->clear();
	    index_p->GetValue(key, *v);
	    return 0UL;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *) {
	    index_p->Upsert(key, value);
	    return true;
	}

	uint64_t scan(KeyType key, int range, threadinfo *) {
	    auto it = index_p->Begin(key);

	    if(it.IsEnd() == true) {
		std::cout << "Iterator reaches the end\n";
		return 0UL;
	    }

	    uint64_t sum = 0;
	    for(int i = 0;i < range;i++) {
		if(it.IsEnd() == true) {
		    return sum;
		}
		sum += it->second;
		it++;
	    }

	    return sum;
	}

#ifndef BWTREE_USE_MAPPING_TABLE
	uint64_t find_bwtree_fast(KeyType key, std::vector<uint64_t> *v) {
	    index_p->GetValueNoMappingTable(key, *v);

	    return 0UL;
	}
#endif

#ifndef BWTREE_USE_DELTA_UPDATE
	bool insert_bwtree_fast(KeyType key, uint64_t value) {
	    index_p->InsertInPlace(key, value);
	    return true;
	}
#endif

	void getMemory() {
	    uint64_t meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied;
	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    auto root_id = index_p->root_id.load();
	    index_p->getMemory(root_id, meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint] - before delta chain consolidation" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;
	    std::cout << "\n" << std::endl;

	    int inner_depth_total = 0,
		leaf_depth_total = 0,
		inner_node_total = 0,
		leaf_node_total = 0;
	    int inner_size_total = 0, leaf_size_total = 0;
	    size_t inner_alloc_total = 0, inner_used_total = 0;
	    size_t leaf_alloc_total = 0, leaf_used_total = 0;

	    uint64_t index_root_id = index_p->root_id.load();
	    int ret = index_p->DebugConsolidateAllRecursive(
		    index_root_id,
		    &inner_depth_total,
		    &leaf_depth_total,
		    &inner_node_total,
		    &leaf_node_total,
		    &inner_size_total,
		    &leaf_size_total,
		    &inner_alloc_total,
		    &inner_used_total,
		    &leaf_alloc_total,
		    &leaf_used_total);

	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    root_id = index_p->root_id.load();
	    index_p->getMemory(root_id, meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint] - after delta chain consolidation" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;

	}

	void find_depth(){
	    auto index_root_id = index_p->root_id.load();
	    int depth = 0;
	    int depth_with_delta = 0;
	    index_p->findDepth(index_root_id, depth, depth_with_delta);
	    std::cout << "Depth: " << depth << " , depth with deta records: " << depth_with_delta << std::endl;
	}
	void convert(){ }

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){
	    index_p->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split, time_consolidation);
	}
	#endif


    private:
	BwTree<KeyType, uint64_t, KeyComparator, KeyEqualityChecker, KeyHashFunc> *index_p;

};


/////////////////////////////////
/////////// Masstree ////////////
/////////////////////////////////

template<typename KeyType, class KeyComparator>
class MassTreeIndex : public Index<KeyType, KeyComparator>
{
    public:

	typedef mt_index<Masstree::default_table> MapType;

	~MassTreeIndex() {
	    delete idx;
	}

	inline void swap_endian(uint64_t &i) {
	    // Note that masstree internally treat input as big-endian
	    // integer values, so we need to swap here
	    // This should be just one instruction
	    i = __bswap_64(i);
	}

	inline void swap_endian(GenericKey<32> &) { return; }
	inline void swap_endian(GenericKey<128> &) { return; }

	void UpdateThreadLocal(size_t thread_num) {}
	void AssignGCID(size_t thread_id) {}
	void UnregisterThread(size_t thread_id) {}

	bool insert(KeyType key, uint64_t value, threadinfo *ti) {
	    swap_endian(key);
	    idx->put((const char*)&key, sizeof(KeyType), (const char*)&value, 8, ti);
	    return true;
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
	    Str val;
	    swap_endian(key);
	    idx->get((const char*)&key, sizeof(KeyType), val, ti);

	    v->clear();
	    if (val.s)
		v->push_back(*(uint64_t *)val.s);

	    return 0;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
	    swap_endian(key);
	    idx->put((const char*)&key, sizeof(KeyType), (const char*)&value, 8, ti);
	    return true;
	}

	uint64_t scan(KeyType key, int range, threadinfo *ti) {
	    Str results[range];

	    swap_endian(key);
	    int key_len = sizeof(KeyType);

	    int resultCount = idx->get_next_n(results, (char *)&key, &key_len, range, ti);
	    return resultCount;
	}

	void getMemory() {
	    uint64_t meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied;
	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    idx->get_footprint_without_suffix(meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint] - without suffix" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;
	    std::cout << "\n" << std::endl;

	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    idx->get_footprint_with_suffix(meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint] - with suffix]" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;

	    double inode_util = 0;
	    double lnode_util = 0;
	    idx->utilization(inode_util, lnode_util);
	    std::cout << "---------------------------------" << std::endl;
	    std::cout << "inode util(%): " << inode_util << std::endl;
	    std::cout << "lnode util(%): " << lnode_util << std::endl;
	    std::cout << "---------------------------------" << std::endl;

	}

	void find_depth(){
	    uint64_t layer_num = 0;
	    uint64_t max_depth = 0;
	    idx->find_depth(layer_num, max_depth);
	    std::cout << "Layer num: " << layer_num << std::endl;
	    std::cout << "Max depth : " << max_depth << std::endl;
	}
	void convert(){ }

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){
	    time_consolidation = 0;
	    idx->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
	}
	#endif


	MassTreeIndex(uint64_t kt) {
	    idx = new MapType{};

	    threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
	    idx->setup(main_ti);

	    return;
	}

	MapType *idx;
};

/////////////////
///// HOT ///////
/////////////////
template<typename KeyType, class KeyComparator>
class HOTIndex : public Index<KeyType, KeyComparator>
{
    public:
	bool insert(KeyType key, uint64_t value, threadinfo *ti) {
	    #ifdef STRING_KEY
	    int keylen = sizeof(KeyType);
	    idx->insert(key.data, keylen, value);
	    #else
	    idx->insert(key, value);
	    #endif
	    return 0;
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
	    #ifdef STRING_KEY
	    auto ret = idx->find(key.data);
	    #else
	    auto ret = idx->find(key);
	    #endif
	    v->clear();
	    v->push_back(ret);
	    return 0;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
	    #ifdef STRING_KEY
	    int keylen = sizeof(KeyType);
	    return idx->upsert(key.data, keylen, value);
	    #else
	    return idx->upsert(key, value);
	    #endif
	}

	uint64_t scan(KeyType key, int range, threadinfo *ti) {
	    #ifdef STRING_KEY
	    idx->scan(key.data, range);
	    #else
	    idx->scan(key, range);
	    #endif
	    return 0;
	}

	HOTIndex(uint64_t kt){
	    idx = new HOT_wrapper();
	}

	void getMemory() {
	    uint64_t meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied;
	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    idx->get_memory(meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint]" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;
	}

	void find_depth(){
	    idx->find_depth();
	}
	void convert(){ }

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){
	    time_consolidation = 0;
	    idx->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
	}
	#endif


	void UpdateThreadLocal(size_t thread_num){ }
	void AssignGCID(size_t thread_id){ }
	void UnregisterThread(size_t thread_id) { }

	HOT_wrapper* idx;
};

//////////////////
/// Blink-tree ///
//////////////////

template<typename KeyType, class KeyComparator>
class BlinkIndex: public Index<KeyType, KeyComparator>
{
    public:

	bool insert(KeyType key, uint64_t value, threadinfo *ti) {
	    idx->insert(key, value);
	    return 0;
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
	    auto ret = idx->lookup(key);
	    v->clear();
	    v->push_back(ret);
	    return 0;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
	    return idx->update(key, value);
	}

	uint64_t scan(KeyType key, int range, threadinfo *ti) {
	    uint64_t buf[range];
	    auto ret = idx->range_lookup(key, range, buf);
	    return ret;
	}

	BlinkIndex(uint64_t kt){
	    idx = new BLINK_OPTIMIZED::btree_t<KeyType>();
	}

	void getMemory() {
	    uint64_t meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied;
	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    idx->footprint(meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint]" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;
	}
	void find_depth(){
	    auto ret = idx->check_height();
	    std::cout << "height of blink-tree: \t" << ret << std::endl;
	}
	void convert(){ }

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){
	    time_consolidation = 0;
	    idx->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
	}
	#endif

	void UpdateThreadLocal(size_t thread_num){ }
	void AssignGCID(size_t thread_id){ }
	void UnregisterThread(size_t thread_id) { }

    private:
	BLINK_OPTIMIZED::btree_t<KeyType>* idx;
};


//////////////////
/// BTree-OLC  ///
//////////////////

template<typename KeyType, class KeyComparator>
class BTreeOLCIndex: public Index<KeyType, KeyComparator>
{
    public:
	bool insert(KeyType key, uint64_t value, threadinfo *ti) {
	    idx->insert(key, value);
	    return 0;
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
	    uint64_t val;
	    auto ret = idx->lookup(key, val);
	    if(!ret)
		return 1;
	    v->clear();
	    v->push_back(val);
	    return 0;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
	    return idx->update(key, value);
	}

	uint64_t scan(KeyType key, int range, threadinfo *ti) {
	    uint64_t buf[range];
	    auto ret = idx->scan(key, range, buf);
	    return ret;
	}

	BTreeOLCIndex(uint64_t kt){
	    idx = new btreeolc::BTree<KeyType, uint64_t>();
	}

	void getMemory() {
	    uint64_t meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied;
	    meta_size = structural_data_occupied = structural_data_unoccupied = key_data_occupied = key_data_unoccupied = 0;

	    idx->footprint(meta_size, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
	    std::cout << "[Memory Footprint]" << std::endl;
	    std::cout << "Metadata: \t" << meta_size << std::endl;
	    std::cout << "Structural_data_occupied: \t" << structural_data_occupied << std::endl;
	    std::cout << "Structural_data_unoccupied: \t" << structural_data_unoccupied << std::endl;
	    std::cout << "Key_data_occupied: \t" << key_data_occupied << std::endl;
	    std::cout << "Key_data_unoccupied: \t" << key_data_unoccupied << std::endl;
	}

	void find_depth(){
	    std::cout << "depth: " << idx->find_depth() << std::endl;
	}

	void convert(){ }
	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){
	    time_consolidation = 0;
	    idx->get_breakdown(time_traversal, time_abort, time_latch, time_node, time_split);
	}
	#endif

	void UpdateThreadLocal(size_t thread_num){ }
	void AssignGCID(size_t thread_id){ }
	void UnregisterThread(size_t thread_id) { }

    private:
	btreeolc::BTree<KeyType, uint64_t>* idx;
};


/////////////////////////
/////// libcuckoo  //////
/////////////////////////
template<typename KeyType, class KeyComparator>
class CuckooIndex : public Index<KeyType, KeyComparator>
{
    public:
	~CuckooIndex() { }

	void UpdateThreadLocal(size_t thread_num) { }
	void AssignGCID(size_t thread_id) { }
	void UnregisterThread(size_t thread_id) { }

	bool insert(KeyType key, uint64_t value, threadinfo *ti) {
	    #ifdef STRING_KEY
	    idx->insert(key.data, value);
	    #else
	    idx->insert(key, value);
	    #endif
	    return true;
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
	    uint64_t out;
	    #ifdef STRING_KEY
	    idx->find(key.data, out);
	    #else
	    idx->find(key, out);
	    #endif
	    return out;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
	    #ifdef STRING_KEY
	    return idx->insert(key.data, value);
	    #else
	    return idx->insert(key, value);
	    #endif
	}

	uint64_t scan(KeyType key, int range, threadinfo *ti) {
	    return 0;
	}

	void getMemory() { }

	void find_depth(){ }
	void convert(){ }

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){ }
	#endif


	CuckooIndex(uint64_t kt) {
	    #ifdef STRING_KEY
	    idx = new cuckoohash_map<std::string, uint64_t>;
	    #else
	    idx = new cuckoohash_map<KeyType, uint64_t>;
	    #endif
	    idx->reserve(200000000u);
	}

    private:
	#ifdef STRING_KEY
	cuckoohash_map<std::string, uint64_t> *idx;
	#else
	cuckoohash_map<KeyType, uint64_t> *idx;
	#endif

};

/////////////////////////
/////// Blink-hash //////
/////////////////////////
template<typename KeyType, class KeyComparator>
class BlinkHashIndex: public Index<KeyType, KeyComparator>
{
    public:

	bool insert(KeyType key, uint64_t value, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    idx->insert(key, value, t);
	    return 0;
	}

	uint64_t find(KeyType key, std::vector<uint64_t> *v, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    auto ret = idx->lookup(key, t);
	    v->clear();
	    v->push_back(ret);
	    return 0;
	}

	bool upsert(KeyType key, uint64_t value, threadinfo *ti) {
	    auto t = idx->getThreadInfo();
	    return idx->update(key, value, t);
	}

	uint64_t scan(KeyType key, int range, threadinfo *ti) {
	    uint64_t buf[range];
	    auto t = idx->getThreadInfo();
	    auto ret = idx->range_lookup(key, range, buf, t);
	    return ret;
	}

	BlinkHashIndex(uint64_t kt){
	    idx = new BLINK_HASH::btree_t<KeyType, uint64_t>();
	}

	void getMemory() { }

	void convert(){
	    auto t = idx->getThreadInfo();
	    idx->convert_all(t);
	}

	void find_depth(){
	    auto ret = idx->check_height();
	    std::cout << "height of blink-tree hashed: \t" << ret << std::endl;
	}

	#ifdef BREAKDOWN
	void get_breakdown(uint64_t& time_traversal, uint64_t& time_abort, uint64_t& time_latch, uint64_t& time_node, uint64_t& time_split, uint64_t& time_consolidation){
	}
	#endif


	void UpdateThreadLocal(size_t thread_num){ }
	void AssignGCID(size_t thread_id){ }
	void UnregisterThread(size_t thread_id) { }

    private:
	BLINK_HASH::btree_t<KeyType, uint64_t>* idx;
};
#endif
