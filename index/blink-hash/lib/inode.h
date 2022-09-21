#ifndef BLINK_HASH_INODE_H__
#define BLINK_HASH_INODE_H__

#include "node.h"
#include "entry.h"

namespace BLINK_HASH{
    
template <typename Key_t>
class inode_t : public node_t{
    public:
        static constexpr size_t cardinality = (PAGE_SIZE - sizeof(node_t)- sizeof(Key_t)) / sizeof(entry_t<Key_t, node_t*>);
	Key_t high_key;
    private:
        entry_t<Key_t, node_t*> entry[cardinality];
    public:

        inode_t() { }

	// constructor when inserts in batch
	inode_t(int _level): node_t(_level){ }

	 // constructor when inode needs to split
        inode_t(node_t* sibling, int _cnt, node_t* left, int _level, Key_t _high_key): node_t(sibling, left, _cnt, _level), high_key(_high_key){ }

        // constructor when tree height grows
        inode_t(Key_t split_key, node_t* left, node_t* right, node_t* sibling, int _level, Key_t _high_key): node_t(sibling, left, 1, _level){
            high_key = _high_key;
            entry[0].value = right;
            entry[0].key = split_key;
        }

	/*
        void* operator new(size_t size) {
            void* mem;
            auto ret = posix_memalign(&mem, 64, size);
            return mem;
        }*/

        bool is_full();

        int find_lowerbound(Key_t& key);

        node_t* scan_node(Key_t key);

	void insert(Key_t key, node_t* value);

        void insert(Key_t key, node_t* value, node_t* left);

	inode_t<Key_t>* split(Key_t& split_key);

	void insert_for_root(Key_t* key, node_t** value, node_t* left, int num);

	void batch_insert_last_level(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num);

	void batch_insert_last_level(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num);

	void batch_insert(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num);

	void batch_insert(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num);

	void move_normal_insertion(int pos, int num, int move_num);
		
	inode_t<Key_t>** batch_insert_last_level(Key_t* key, node_t** value, int num, int& new_num);

	inode_t<Key_t>** batch_insert(Key_t* key, node_t** value, int num, int& new_num);

	node_t* rightmost_ptr();

        void print();

	void sanity_check(Key_t _high_key, bool first);

    private:

	int lowerbound_linear(Key_t key);

        int lowerbound_binary(Key_t key);
};
}
#endif
