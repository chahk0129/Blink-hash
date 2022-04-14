#ifndef BLINK_HASH_LNODE_H__
#define BLINK_HASH_LNODE_H__

#include "node.h"
#include "bucket.h"

#define LEAF_BTREE_SIZE (PAGE_SIZE)
//#define LEAF_BTREE_SIZE (4096)
//#define LEAF_HASH_SIZE (1024*16)
#define LEAF_HASH_SIZE (1024 * 256)
//#define LEAF_HASH_SIZE (262144)
#define SEED (0xc70697UL)
#define HASH_FUNCS_NUM (2)
#define NUM_SLOT (4)

namespace BLINK_HASH{

    /*
template <typename Key_t, typename Value_t>
class lnode_btree_t<Key_t, Value_t>;

template <typename Key_t, typename Value_t>
class lnode_hash_t<Key_t, Value_t>;
*/
template <typename Key_t, typename Value_t>
class lnode_t : public node_t{
    public:
	enum node_type_t{
	    BTREE_NODE = 0,
	    HASH_NODE
	};

	node_type_t type;
	char dummy[4];
	Key_t high_key;

	// initial constructor
	lnode_t(node_type_t _type): node_t(0), type(_type) { }

	// constructor when leaf split
	lnode_t(node_t* sibling, int _cnt, uint32_t _level, node_type_t _type): node_t(sibling, nullptr, _cnt, _level, true), type(_type){ }

	//bool is_full();

	inline void write_unlock();

	int insert(Key_t key, Value_t value, uint64_t version);

	node_t* split(Key_t& split_key, Key_t key, Value_t value, uint64_t version);

	int update(Key_t key, Value_t value, uint64_t version);

	Value_t find(Key_t key, bool& need_restart);

	int range_lookup(Key_t key, Value_t* buf, int count, int range, bool continued);

	void sanity_check(Key_t key, bool first);

	void print();

	double utilization();
	
};

template <typename Key_t, typename Value_t>
class lnode_btree_t : public lnode_t<Key_t, Value_t>{
    public:
	static constexpr size_t cardinality = (LEAF_BTREE_SIZE - sizeof(lnode_t<Key_t, Value_t>) - sizeof(size_t)) / sizeof(entry_t<Key_t, Value_t>);
    private:
	entry_t<Key_t, Value_t> entry[cardinality];

    public:

	// initial constructor
	lnode_btree_t(): lnode_t<Key_t, Value_t>(lnode_t<Key_t, Value_t>::BTREE_NODE){ }

	// constructor when leaf splits
	lnode_btree_t(node_t* sibling, int _cnt, uint32_t _level): lnode_t<Key_t, Value_t>(sibling, _cnt, _level, lnode_t<Key_t, Value_t>::BTREE_NODE){ }

	inline void write_unlock();

	inline bool is_full();

	int find_lowerbound(Key_t key);

	Value_t find(Key_t key);

	int insert(Key_t key, Value_t value, uint64_t version);

	lnode_btree_t<Key_t, Value_t>* split(Key_t& split_key, Key_t key, Value_t value);
	
	void insert_after_split(Key_t key, Value_t value);

	void batch_insert(entry_t<Key_t, Value_t>* buf, size_t batch_size, int& from, int to);

	int remove(Key_t key, uint64_t version);

        int update(Key_t key, Value_t value, uint64_t version);

        int range_lookup(Key_t key, Value_t* buf, int count, int range, bool continued);

	void print();

        void sanity_check(Key_t _high_key, bool first);

        int get_cnt();

        double utilization();

    private:
        int lowerbound_linear(Key_t key);

	int lowerbound_binary(Key_t key);

        bool update_linear(Key_t key, uint64_t value);

        Value_t find_linear(Key_t key);

        Value_t find_binary(Key_t key);

	int find_pos_linear(Key_t key);

	int find_pos_binary(Key_t key);
};

template <typename Key_t, typename Value_t>
static constexpr size_t FILL_SIZE = lnode_btree_t<Key_t, Value_t>::cardinality * FILL_FACTOR;

template <typename Key_t, typename Value_t>
class lnode_hash_t : public lnode_t<Key_t, Value_t>{
    public:
	static constexpr size_t cardinality = (LEAF_HASH_SIZE - sizeof(lnode_t<Key_t, Value_t>) - sizeof(lnode_t<Key_t, Value_t>*)) / sizeof(bucket_t<Key_t, Value_t>);

	lnode_hash_t<Key_t, Value_t>* left_sibling_ptr;

    private:
	bucket_t<Key_t, Value_t> bucket[cardinality];

    public:
	inline bool _try_splitlock(uint64_t version);

        inline void _split_unlock();

        inline void bucket_acquire();

        inline void bucket_release();

        // initial constructor
        lnode_hash_t(): lnode_t<Key_t, Value_t>(lnode_t<Key_t, Value_t>::HASH_NODE) { }

        // constructor when leaf splits
        lnode_hash_t(node_t* sibling, int _cnt, uint32_t _level): lnode_t<Key_t, Value_t>(sibling, 0, _level, lnode_t<Key_t, Value_t>::HASH_NODE){
            for(int i=0; i<cardinality; i++){
                bucket[i].state = bucket_t<Key_t, Value_t>::LINKED_LEFT;
            }
        }

	inline uint8_t _hash(size_t key);

	int insert(Key_t key, Value_t value, uint64_t version);

	lnode_hash_t<Key_t, Value_t>* split(Key_t& split_key, Key_t key, Value_t value, uint64_t version);

	int update(Key_t key, Value_t value, uint64_t vstart);

	Value_t find(Key_t key, bool& need_restart);

	int range_lookup(Key_t key, Value_t* buf, int count, int range);

	// need to use structure to return output
	lnode_btree_t<Key_t, Value_t>** convert(int& num, uint64_t version);

	void print();

	void sanity_check(Key_t _high_key, bool first);

        double utilization();


    private:
	bool stabilize_all(uint64_t version);

	bool stabilize_bucket(int loc);

	inline void swap(Key_t* a, Key_t* b);

        inline int partition(Key_t* keys, int left, int right);

        inline int random_partition(Key_t* keys, int left, int right);

        inline void median_util(Key_t* keys, int left, int right, int k, int& a, int& b);

        inline int find_median(Key_t* keys, int n);

	inline void prefetch_range(void* addr, size_t len);

        inline void prefetch(const void* addr);
};

}
#endif
