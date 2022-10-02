#ifndef BETREE_NODE_H__
#define BETREE_NODE_H__
//#define NODE_SIZE (8192)
//#define NODE_SIZE (4096)
#define NODE_SIZE (2048)
//#define NODE_SIZE (1024)
//#define NODE_SIZE (512)
//#define NODE_SIZE (256)

#include "common.h"
#include <utility>
#include <vector>
#include <map>


namespace B_EPSILON_TREE{

#define RETRY_THRESHOLD 100
class node_t{
    public:
	node_t(): lock(0), level(0){ }
	node_t(int _level): lock(0), level(_level){ }

	std::atomic<uint64_t> lock;
	int level;

	bool is_locked(){
	    auto version = lock.load();
	    return ((version & 0b10) == 0b10);
	}

	bool is_locked(uint64_t version){
            return ((version & 0b10) == 0b10);
        }

        bool is_obsolete(uint64_t version){
            return (version & 1) == 1;
        }

        uint64_t get_version(bool& need_restart){
            uint64_t version = lock.load();
            if(is_locked(version) || is_obsolete(version)){
                _mm_pause();
                need_restart = true;
            }
            return version;
        }


        uint64_t try_readlock(bool& need_restart){
            uint64_t version = (uint64_t)lock.load();
            if(is_locked(version) || is_obsolete(version)){
                _mm_pause();
                need_restart = true;
            }
            return version;
        }


        bool try_writelock(){ // exclusive lock
            uint64_t version = lock.load();
            if(is_locked(version) || is_obsolete(version)){
                _mm_pause();
                return false;
            }

	    if(lock.compare_exchange_strong(version, version + 0b10)){
                version += 0b10;
                return true;
            }
            else{
                _mm_pause();
                return false;
            }
        }

        void try_upgrade_writelock(uint64_t& version, bool& need_restart){
            uint64_t _version = lock.load();
            if(version != _version){
                _mm_pause();
                need_restart = true;
                return;
            }

            if(lock.compare_exchange_strong(version, version + 0b10)){
                version += 0b10;
            }
            else{
                _mm_pause();
                need_restart =true;
            }
        }

        void write_unlock(){
            lock.fetch_add(0b10);
        }

        void write_unlock_obsolete(){
            lock.fetch_add(0b11);
        }

        void read_unlock(uint64_t before, bool& need_restart) const{
            need_restart = (before != lock.load());
        }

};

struct child_info_t{
    node_t* ptr;
    int from;
    int to;
};


template <typename Key_t, typename Value_t>
class inode_t: public node_t{
    public:
	static constexpr size_t pivot_cardinality = (NODE_SIZE - sizeof(node_t) - sizeof(int)*2) / sizeof(entry_t<Key_t, node_t*>) / 2;
	static constexpr size_t msg_cardinality = (NODE_SIZE - sizeof(node_t) - sizeof(int)*2 - sizeof(entry_t<Key_t, node_t*>)*pivot_cardinality) / sizeof(entry_t<Key_t, Value_t>);

	inode_t() { }
	inode_t(int _level, int _pivot_cnt, node_t* left): node_t(_level), pivot_cnt(_pivot_cnt), msg_cnt(0) {
	    pivot[0].value = left;
	}

	inode_t(int _level, node_t* left, node_t* right, Key_t split_key): node_t(_level), pivot_cnt(1), msg_cnt(0){
	    pivot[0].key = split_key;
	    pivot[0].value = left;
	    pivot[1].value = right;
	}

	bool is_pivot_full(){
	    return ((size_t)pivot_cnt == (pivot_cardinality-1));
	}

	bool is_pivot_full(int num){
	    return ((size_t)pivot_cnt+num >= (pivot_cardinality-1));
	}

	bool is_msg_full(){
	    return ((size_t)msg_cnt == msg_cardinality);
	}

	bool is_msg_full(int num){
	    return ((size_t)msg_cnt+num > msg_cardinality);
	    //return ((size_t)msg_cnt+num >= msg_cardinality);
	}

	int find_lowerbound_pivot(Key_t& key){
	    if constexpr(NODE_SIZE > 1024)
		return lowerbound_binary_pivot(key);
	    else
		return lowerbound_linear_pivot(key);
	}

	int find_lowerbound_msg(Key_t& key){
	    if constexpr(NODE_SIZE > 1024)
		return lowerbound_binary_msg(key);
	    else
		return lowerbound_linear_msg(key);
	}

	node_t* leftmost_ptr(){
	    return pivot[0].vlaue;
	}

	node_t* scan_pivot(Key_t key){
	    return pivot[find_lowerbound_pivot(key)].value;
	}

	void scan_msg(Key_t key, std::map<Key_t, Value_t>& buf){
	    int pos = find_lowerbound_msg(key);
	    for(int i=pos; i<msg_cnt; i++){
		buf.insert(std::make_pair(msg[i].key, msg[i].value));
	    }
	}

	bool scan_msg(Key_t key, Value_t& value){
	    int pos = find_matching(key);
	    if(pos != -1){
		value = msg[pos].value;
		return true;
	    }
	    return false;
	}

	void put(Key_t key, Value_t value){
	    int pos = find_matching(key);
	    if(pos == -1){
		pos = find_lowerbound_msg(key);
		memmove(msg+pos+1, msg+pos, sizeof(entry_t<Key_t, Value_t>)*(msg_cnt-pos));
		msg[pos].key = key;
		msg[pos].value = value;
		msg_cnt++;
	    }
	    else{
		msg[pos].value = value;
	    }
	}

	void put_msg(entry_t<Key_t, Value_t>* buf, int buf_cnt){
	    for(int i=0; i<buf_cnt; i++){
		int pos = find_matching(buf[i].key);
		if(pos == -1){
		    pos = find_lowerbound_msg(buf[i].key);
		    memmove(msg+pos+1, msg+pos, sizeof(entry_t<Key_t, Value_t>)*(msg_cnt - pos));
		    memcpy(msg+pos, &buf[i], sizeof(entry_t<Key_t, Value_t>));
		    msg_cnt++;
		}
		else
		    msg[pos].value = buf[i].value;
	    }
	}


	void put_msg(Key_t key, Value_t value, opcode_t op){
	    if(op == OP_INSERT){
		int pos = find_lowerbound_msg(key);
		memmove(msg+pos+1, msg+pos, sizeof(entry_t<Key_t, Value_t>)*(msg_cnt-pos));
		msg[pos].key = key;
		msg[pos].value = value;
		msg_cnt++;
	    }
	    else{
		int pos = find_matching(key);
		if(pos == -1){
		    pos = find_lowerbound_msg(key);
		    memmove(msg+pos+1, msg+pos, sizeof(entry_t<Key_t, Value_t>)*(msg_cnt-pos));
		    msg[pos].key = key;
		    msg[pos].value = value;
		    msg_cnt++;
		}
		else{
		    msg[pos].value = value;
		}
	    }
	}

	void insert_pivot(Key_t key, node_t* value){
	    int pos = find_lowerbound_pivot(key);
	    memmove(pivot+pos+1, pivot+pos, sizeof(entry_t<Key_t, node_t*>)*(pivot_cnt-pos+1));
	    pivot[pos].key = key;
	    pivot[pos].value = value;
	    std::swap(pivot[pos].value, pivot[pos+1].value);
	    pivot_cnt++;
	}

	inode_t* split(Key_t& split_key){
	    int half = pivot_cnt / 2;
	    split_key = pivot[half].key;

	    int new_cnt = pivot_cnt - half - 1;
	    auto new_node = new inode_t<Key_t, Value_t>(level, new_cnt, pivot[half].value);
	    memcpy(new_node->pivot, pivot+half+1, sizeof(entry_t<Key_t, node_t*>)*(new_cnt + 1));
	    pivot_cnt = half;

	    int from = msg_cnt;
	    for(int i=0; i<msg_cnt; i++){
		if(msg[i].key > split_key){
		    from = i;
		    break;
		}
	    }
	    memcpy(new_node->msg, msg+from, sizeof(entry_t<Key_t, Value_t>)*(msg_cnt - from));
	    new_node->msg_cnt = msg_cnt - from;
	    msg_cnt = from;

	    return new_node;
	}

	int get_msg_cnt(){
	    return msg_cnt;
	}

	int get_pivot_cnt(){
	    return pivot_cnt;
	}

	void key_range(Key_t& min_key, Key_t& max_key){
	    min_key = pivot[0].key;
	    max_key = pivot[pivot_cnt].key;
	}

	Key_t get_msg_key(int idx){
	    return msg[idx].key;
	}

	Value_t get_msg_value(int idx){
	    return msg[idx].value;
	}

	Key_t get_key(int idx){
	    return pivot[idx].key;
	}

	node_t* get_child(int idx){
	    return pivot[idx].value;
	}

	void move_msg(int num){
	    memmove(msg, msg+num, sizeof(entry_t<Key_t, Value_t>)*(msg_cnt - num));
	    msg_cnt -= num;
	}

	void inspect(child_info_t* child_info, int& child_info_size){
	//void inspect(std::vector<std::pair<int, std::pair<int, int>>>& child_info){
	    int pivot_idx = 0;
	    int msg_idx = 0;
	    int from = 0;
	    bool flag = false;
	    while((msg_idx < msg_cnt) && (pivot_idx < pivot_cnt)){
		if(msg[msg_idx].key <= pivot[pivot_idx].key){
		    if(!flag){
			from = msg_idx;
			flag = true;
		    }
		    msg_idx++;
		}
		else{
		    if(flag){
			child_info[child_info_size].ptr = pivot[pivot_idx].value;
			child_info[child_info_size].from = from;
			child_info[child_info_size].to = msg_idx;
			child_info_size++;
			flag = false;
			from = msg_idx;
		    }
		    pivot_idx++;
		}
	    }

	    if(from < msg_cnt){ // belongs to last pivot
		child_info[child_info_size].ptr = pivot[pivot_idx].value;
		child_info[child_info_size].from = from;
		child_info[child_info_size].to = msg_cnt;
		child_info_size++;
		//child_info.push_back(std::make_pair(pivot_idx, std::make_pair(msg_idx, msg_cnt)));
	    }
	}


	void print(){
	    std::cout << "inode " << this << " at lv " << level << std::endl;
	    std::cout << "  msg cnt (" << msg_cnt << "): ";
	    for(int i=0; i<msg_cnt; i++){
		std::cout<< msg[i].key << ", ";
	    }
	    std::cout << "\n pivot cnt (" << pivot_cnt << "): ";
	    for(int i=0; i<pivot_cnt; i++){
		std::cout << pivot[i].value << " " << pivot[i].key << ", ";
	    }
	    std::cout << pivot[pivot_cnt].value << std::endl;
	}


    private:

	int find_matching(Key_t key){
	    if constexpr(NODE_SIZE > 1024)
		return find_binary_matching(key);
	    else
		return find_linear_matching(key);
	}

	int find_binary_matching(Key_t key){
	    int lower = 0;
	    int upper = msg_cnt;
	    while(lower < upper){
		int mid = ((upper - lower)/2) + lower;
		if(key < msg[mid].key)
		    upper = mid;
		else if(key > msg[mid].key)
		    lower = mid + 1;
		else
		    return mid;
	    }

	    if(msg[lower].key == key)
		return lower;
	    return -1;
	}

	int find_linear_matching(Key_t key){
	    for(int i=0; i<msg_cnt; i++){
		if(key == msg[i].key)
		    return i;
	    }
	    return -1;
	}

	int lowerbound_binary_pivot(Key_t key){
	    int lower = 0;
	    int upper = pivot_cnt;
	    while(lower < upper){
		int mid = ((upper - lower)/2) + lower;
		if(key < pivot[mid].key)
		    upper = mid;
		else if(key > pivot[mid].key)
		    lower = mid + 1;
		else
		    return mid;
	    }
	    return lower;
	}

	int lowerbound_linear_pivot(Key_t key){
	    for(int i=0; i<pivot_cnt; i++){
		if(key <= pivot[i].key)
		    return i;
	    }
	    return pivot_cnt;
	}

	int lowerbound_binary_msg(Key_t key){
	    int lower = 0;
	    int upper = msg_cnt;
	    while(lower < upper){
		int mid = ((upper - lower)/2) + lower;
		if(key < msg[mid].key)
		    upper = mid;
		else if(key > msg[mid].key)
		    lower = mid + 1;
		else
		    return mid;
	    }
	    return lower;
	}

	int lowerbound_linear_msg(Key_t key){
	    for(int i=0; i<msg_cnt; i++){
		if(key <= msg[i].key)
		    return i;
	    }
	    return msg_cnt;
	}

        int pivot_cnt;
	int msg_cnt;
	entry_t<Key_t, node_t*> pivot[pivot_cardinality];
	entry_t<Key_t, Value_t> msg[msg_cardinality];
};


template <typename Key_t, typename Value_t>
class lnode_t: public node_t{
    public:
	static constexpr size_t cardinality = (NODE_SIZE - sizeof(node_t) - sizeof(int) - sizeof(lnode_t<Key_t, Value_t>*)) / sizeof(entry_t<Key_t, Value_t>);

	lnode_t(): node_t(0), cnt(0), sibling_ptr(nullptr) { }// initial constructor
	lnode_t(int _cnt, lnode_t* _sibling_ptr): node_t(0), cnt(_cnt), sibling_ptr(_sibling_ptr) { } // split constructor

	bool is_full(){
	    return ((size_t)cnt == cardinality);
	}

	bool is_full(int num){
	    return ((size_t)cnt+num >= cardinality);
	}

	int get_cnt(){
	    return cnt;
	}

	int find_lowerbound(Key_t key){
	    if constexpr(NODE_SIZE > 1024)
		return lowerbound_binary(key);
	    else
		return lowerbound_linear(key);
        }

	void scan_node(Key_t key, std::map<Key_t, Value_t>& buf){
	    int pos = find_lowerbound(key);
	    for(int i=pos; i<cnt; i++){
		buf.insert(std::make_pair(entry[i].key, entry[i].value));
	    }
	}

        Value_t scan_node(Key_t key){
            return entry[find_lowerbound(key)].value;
        }

        Value_t find(Key_t key){
	    if constexpr(NODE_SIZE > 1024)
		return find_binary(key);
	    else
		return find_linear(key);
        }

	void put(entry_t<Key_t, Value_t>* buf, int buf_cnt){
	    for(int i=0; i<buf_cnt; i++){
		auto ret = update(buf[i].key, buf[i].value);
		if(!ret){
		    if(cnt){
			int pos = find_lowerbound(buf[i].key);
			memmove(&entry[pos+1], &entry[pos], sizeof(entry_t<Key_t, uint64_t>)*(cnt-pos));
			memcpy(&entry[pos], &buf[i], sizeof(entry_t<Key_t, Value_t>));
		    }
		    else{
			memcpy(&entry[0], &buf[i], sizeof(entry_t<Key_t, Value_t>));
		    }
		    cnt++;
		}
	    }
	}
 
        void put(Key_t key, Value_t value){
	    auto ret = update(key, value);
	    if(!ret){
		if(cnt){
		    int pos = find_lowerbound(key);
		    memmove(&entry[pos+1], &entry[pos], sizeof(entry_t<Key_t, uint64_t>)*(cnt-pos));
		    entry[pos].key = key;
		    entry[pos].value = value;
		}
		else{
		    entry[0].key = key;
		    entry[0].value= value;
		}

		cnt++;
	    }
        }

        void put(Key_t key, Value_t value, opcode_t op){
	    if(op == OP_INSERT){
		if(cnt){
		    int pos = find_lowerbound(key);
		    memmove(&entry[pos+1], &entry[pos], sizeof(entry_t<Key_t, uint64_t>)*(cnt-pos));
		    entry[pos].key = key;
		    entry[pos].value = value;
		}
		else{
		    entry[0].key = key;
		    entry[0].value= value;
		}

		cnt++;
	    }
	    else{
		if constexpr(NODE_SIZE > 1024) 
		    auto ret = update_binary(key, value);
		else
		    auto ret = update_linear(key, value);
	    }
        }

	lnode_t* split(Key_t& split_key){
            int half = cnt/2;
            int new_cnt = cnt - half;
            split_key = entry[half-1].key;

            auto new_leaf = new lnode_t<Key_t, Value_t>(new_cnt, sibling_ptr);
            memcpy(new_leaf->entry, entry+half, sizeof(entry_t<Key_t, Value_t>)*new_cnt);

            sibling_ptr = new_leaf;
            cnt = half;
            return new_leaf;
        }

        bool update(Key_t& key, uint64_t value){
	    if constexpr(NODE_SIZE > 1024) 
		return update_binary(key, value);
	    else
		return update_linear(key, value);
        }

        int range_lookup(int idx, uint64_t* buf, int count, int range){
            auto _count = count;
            for(int i=idx; i<cnt; i++){
                buf[_count++] = entry[i].value;
                if(_count == range) return _count;
            }
            return _count;
        }

	void print(){
	    std::cout << "lnode " << this << " (cnt: " << cnt << ")" << std::endl;
	    std::cout << "  ";
	    for(int i=0; i<cnt; i++){
		std::cout << entry[i].key << ", ";
	    }
	    std::cout << "\n";
	}



    private:
	int lowerbound_binary(Key_t key){
	    int lower = 0;
	    int upper = cnt;
	    while(lower < upper){
		int mid = ((upper - lower)/2) + lower;
		if(key < entry[mid].key)
		    upper = mid;
		else if(key > entry[mid].key)
		    lower = mid + 1;
		else
		    return mid;
	    }
	    return lower;
	}


	int lowerbound_linear(Key_t key){
            for(int i=0; i<cnt; i++){
                if(key <= entry[i].key)
                    return i;
            }
            return cnt;
        }

	bool update_binary(Key_t key, Value_t value){
	    int lower = 0;
	    int upper = cnt;
	    while(lower < upper){
		int mid = ((upper - lower)/2) + lower;
		if(key < entry[mid].key)
		    upper = mid;
		else if(key > entry[mid].key)
		    lower = mid + 1;
		else{
		    entry[mid].value = value;
		    return true;
		}
	    }

	    if(key == entry[lower].key){
		entry[lower].value = value;
		return true;
	    }
	    return false;
	}

        bool update_linear(Key_t key, Value_t value){
            for(int i=0; i<cnt; i++){
                 if(key == entry[i].key){
                    entry[i].value = value;
                    return true;
                }
            }
            return false;
        }

	Value_t find_binary(Key_t key){
	    int lower = 0;
	    int upper = cnt;
	    while(lower < upper){
		int mid = ((upper - lower)/2) + lower;
		if(key < entry[mid].key)
		    upper = mid;
		else if(key > entry[mid].key)
		    lower = mid + 1;
		else{
		    auto ret = entry[mid].value;
		    return ret;
		}
	    }

	    if(key == entry[lower].key){
		auto ret = entry[lower].value;
		return ret;
	    }
	    return 0;
	}

        Value_t find_linear(Key_t key){
            for(int i=0; i<cnt; i++){
                if(key == entry[i].key){
                    auto ret = entry[i].value;
                    return ret;
                }
            }
            return 0;
        }



	int cnt;
    public:
	lnode_t* sibling_ptr;
    private:
	entry_t<Key_t, Value_t> entry[cardinality];
};


void yield(int& cnt){
    /*
    for(int i=0; i<cnt*4; i++)
	_mm_pause();
	*/
    //cnt = 0;
}

}
#endif
