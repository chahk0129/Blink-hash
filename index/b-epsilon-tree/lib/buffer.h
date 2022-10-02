#ifndef BETREE_BUFFER_H__
#define BETREE_BUFFER_H__

#include "common.h"

namespace B_EPSILON_TREE{

template <typename Key_t, typename Value_t>
class buffer_t{
    public:
	static constexpr size_t cardinality = (BUFFER_SIZE - sizeof(uint64_t) - sizeof(int)) / sizeof(entry_t<Key_t, Value_t>);

	buffer_t(): lock(0), cnt(0){ }

	std::atomic<uint64_t> lock;
	int cnt;

	bool is_full(){
	    return ((size_t)cnt == cardinality);
	}

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

	int get_cnt(){
            return cnt;
        }

	void reset(){
	    cnt = 0;
	}

	entry_t<Key_t, Value_t>* get_entry(){
	    return entry;
	}

        int find_lowerbound(Key_t key){
            if constexpr(BUFFER_SIZE > 1024)
                return lowerbound_binary(key);
            else
                return lowerbound_linear(key);
        }

	bool find(Key_t key, Value_t& value){
            if constexpr(BUFFER_SIZE > 1024){
		value = find_binary(key);
		if(value)
		    return true;
	    }
            else{
		value = find_linear(key);
		if(value)
		    return true;
	    }
	    return false;
        }

	bool update(Key_t key, Value_t value){
	    if constexpr(BUFFER_SIZE > 1024)
		return update_binary(key, value);
	    else
		return update_linear(key, value);
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
		update(key, value);
            }
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


	entry_t<Key_t, Value_t> entry[cardinality];
};

}

#endif
