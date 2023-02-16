#include "inode.h"

namespace BLINK_HASH{

template <typename Key_t>
inline bool inode_t<Key_t>::is_full(){
    return (cnt == cardinality);
}

template <typename Key_t>
inline int inode_t<Key_t>::find_lowerbound(Key_t& key){
    return lowerbound_linear(key);
}

template <typename Key_t>
inline node_t* inode_t<Key_t>::scan_node(Key_t key){
    if(sibling_ptr && (high_key < key))
	return sibling_ptr;
    else{
	int idx = find_lowerbound(key);
	if(idx > -1)
	    return entry[idx].value;
	else
	    return leftmost_ptr;
    }
}

template <typename Key_t>
void inode_t<Key_t>::insert(Key_t key, node_t* value){
    int pos = find_lowerbound(key);
    memmove(&entry[pos+2], &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*(cnt-pos-1));
    entry[pos+1].key = key;
    entry[pos+1].value = value;
    cnt++;

}

template <typename Key_t>
void inode_t<Key_t>::insert(Key_t key, node_t* value, node_t* left){
    int pos = find_lowerbound(key);
    memmove(&entry[pos+2], &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*(cnt-pos-1));
    entry[pos].value = left;
    entry[pos+1].key = key;
    entry[pos+1].value = value;
}

template <typename Key_t>
inode_t<Key_t>* inode_t<Key_t>::split(Key_t& split_key){
    int half = cnt/2;
    split_key = entry[half].key;

    int new_cnt = cnt-half-1;
    auto new_node = new inode_t<Key_t>(sibling_ptr, new_cnt, entry[half].value, level, high_key);
    memcpy(new_node->entry, entry+half+1, sizeof(entry_t<Key_t, node_t*>)*new_cnt);

    sibling_ptr = static_cast<node_t*>(new_node);
    high_key = entry[half].key;
    cnt = half;
    return new_node;
}

template <typename Key_t>
void inode_t<Key_t>::batch_migrate(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num){
    leftmost_ptr = migrate[migrate_idx++].value;
    int copy_num = migrate_num - migrate_idx;
    memcpy(entry, &migrate[migrate_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
    cnt += copy_num;
    migrate_idx += copy_num;
}

template <typename Key_t>
bool inode_t<Key_t>::batch_kvpair(Key_t* key, node_t** value, int& idx, int num, int batch_size){
    for(; cnt<batch_size && idx<num-1; cnt++, idx++){
	entry[cnt].key = key[idx];
	entry[cnt].value = value[idx];
    }

    if(cnt == batch_size){ // insert in the next node
	high_key = key[idx];
	return true;
    }
    
    entry[cnt].key = key[idx];
    entry[cnt].value = value[idx];
    cnt++, idx++;
    return false;
}

template <typename Key_t>
void inode_t<Key_t>::batch_buffer(entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num, int batch_size){
    for(; cnt<batch_size && buf_idx<buf_num-1; cnt++, buf_idx++){
	entry[cnt].key = buf[buf_idx].key;
	entry[cnt].value = buf[buf_idx].value;
    }

    if(cnt == batch_size){ // insert in the next node
	high_key = buf[buf_idx].key;
	return;
    }
    
    entry[cnt].key = buf[buf_idx].key;
    entry[cnt].value = buf[buf_idx].value;
    cnt++, buf_idx++;
}

// batch insert with migration and movement
template <typename Key_t>
void inode_t<Key_t>::batch_insert_last_level(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
    bool from_start = true;
    if(migrate_idx < migrate_num){
	from_start = false;
	batch_migrate(migrate, migrate_idx, migrate_num);
    }

    if(idx < num && cnt < batch_size){
	if(from_start)
	    leftmost_ptr = value[idx++];
	from_start = false;
	if(idx < num){
	    if(batch_kvpair(key, value, idx, num, batch_size))
		return;

	    if(idx == num && cnt == batch_size && buf_num != 0){
		high_key = buf[buf_idx].key;
		return;
	    }
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start)
	    leftmost_ptr = buf[buf_idx++].value;
	batch_buffer(buf, buf_idx, buf_num, batch_size);
    }
}

// batch insert with and movement
template <typename Key_t>
void inode_t<Key_t>::batch_insert_last_level(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
    bool from_start = true;
    if(idx < num){
	leftmost_ptr = value[idx++];
	from_start = false;
	if(idx < num){
	    if(batch_kvpair(key, value, idx, num, batch_size))
		return;

	    if(idx == num && cnt == batch_size && buf_num != 0){
		high_key = buf[buf_idx].key;
		return;
	    }
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start)
	    leftmost_ptr = buf[buf_idx++].value;
	batch_buffer(buf, buf_idx, buf_num, batch_size);
    }
}

template <typename Key_t>
void inode_t<Key_t>::calculate_node_num(int total_num, int& numerator, int& remains, int& last_chunk, int& new_num, int batch_size){
    if(numerator == 0){ // need only one new node
	new_num = 1;
	last_chunk = remains;
	return;
    }
    // multiple new nodes
    if(remains == 0){ // exact match
	new_num = numerator;
	last_chunk = batch_size;
    }
    else{
	if(remains < cardinality - batch_size){ // can be squeezed into the last new node
	    new_num = numerator;
	    last_chunk = batch_size + remains;
	}
	else{ // need extra new node
	    new_num = numerator + 1;
	    last_chunk = remains;
	}
    }
}

template <typename Key_t>
inode_t<Key_t>** inode_t<Key_t>::batch_insert_last_level(Key_t* key, node_t** value, int num, int& new_num){
    int pos = find_lowerbound(key[0]);
    int batch_size = cardinality * FILL_FACTOR;
    bool inplace = (cnt + num) < cardinality ? 1 : 0;
    int move_num = 0;
    int idx = 0;
    if(pos < 0)
	move_num = cnt;
    else
	move_num = cnt-pos-1;

    if(inplace){ // normal insertion
	move_normal_insertion(pos, num, move_num);
	if(pos < 0) // leftmost ptr
	    leftmost_ptr = value[idx++];
	else
	    entry[pos].value = value[idx++];

	for(int i=pos+1; i<pos+num+1; i++, idx++){
	    entry[i].key = key[idx];
	    entry[i].value = value[idx];
	}
	cnt += num-1;
	return nullptr;
    }
    else{
	auto prev_high_key = high_key;
	if(pos < 0) // leftmost ptr
	    leftmost_ptr = value[idx++];
	else
	    entry[pos].value = value[idx++];

	if(batch_size < pos){ // need insert in the middle (migrated + new kvs + moved)
	    int migrate_num = pos - batch_size;
	    entry_t<Key_t, node_t*> migrate[migrate_num];
	    memcpy(migrate, &entry[batch_size], sizeof(entry_t<Key_t, node_t*>) * migrate_num);

	    entry_t<Key_t, node_t*> buf[move_num];
	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);
	    cnt = batch_size;

	    int total_num = num + move_num + migrate_num;
	    int last_chunk = 0;
	    int numerator = total_num / (batch_size+1);
	    int remains = total_num % (batch_size+1);
	    calculate_node_num(total_num, numerator, remains, last_chunk, new_num, batch_size);

	    auto new_nodes = new inode_t<Key_t>*[new_num];
	    for(int i=0; i<new_num; i++)
		new_nodes[i] = new inode_t<Key_t>(level);

	    auto old_sibling = sibling_ptr;
	    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	    int migrate_idx = 0;
	    int move_idx = 0;

	    high_key = migrate[migrate_idx].key;
	    for(int i=0; i<new_num-1; i++){
		new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
		new_nodes[i]->batch_insert_last_level(migrate, migrate_idx, migrate_num, key, value, idx, num, batch_size, buf, move_idx, move_num);
	    }
	    new_nodes[new_num-1]->sibling_ptr = old_sibling;
	    new_nodes[new_num-1]->batch_insert_last_level(migrate, migrate_idx, migrate_num, key, value, idx, num, last_chunk, buf, move_idx, move_num);
	    new_nodes[new_num-1]->high_key = prev_high_key;
	    return new_nodes;
	}
	else{ // need insert in the middle (new_kvs + moved)
	    int move_idx = 0;
	    entry_t<Key_t, node_t*> buf[move_num];
	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);

	    for(int i=pos+1; i<batch_size && idx<num; i++, idx++){
		entry[i].key = key[idx];
		entry[i].value = value[idx];
	    }

	    cnt += (idx - move_num - 1);
	    for(; cnt<batch_size; cnt++, move_idx++){
		entry[cnt].key = buf[move_idx].key;
		entry[cnt].value = buf[move_idx].value;
	    }

	    if(idx < num)
		high_key = key[idx];
	    else
		high_key = buf[move_idx].key;

	    int total_num = num - idx + move_num - move_idx;
	    int last_chunk = 0;
	    int numerator = total_num / (batch_size+1);
	    int remains = total_num % (batch_size+1);
	    calculate_node_num(total_num, numerator, remains, last_chunk, new_num, batch_size);

	    auto new_nodes = new inode_t<Key_t>*[new_num];
	    for(int i=0; i<new_num; i++)
		new_nodes[i] = new inode_t<Key_t>(level);

	    auto old_sibling = sibling_ptr;
	    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	    for(int i=0; i<new_num-1; i++){
		new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
		new_nodes[i]->batch_insert_last_level(key, value, idx, num, batch_size, buf, move_idx, move_num);
	    }
	    new_nodes[new_num-1]->sibling_ptr = old_sibling;
	    new_nodes[new_num-1]->batch_insert_last_level(key, value, idx, num, last_chunk, buf, move_idx, move_num);
	    new_nodes[new_num-1]->high_key = prev_high_key;
	    return new_nodes;
	}
    }
}

// batch insert with migration and movement
template <typename Key_t>
void inode_t<Key_t>::batch_insert(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
    bool from_start = true;
    if(migrate_idx < migrate_num){
	from_start = false;
	batch_migrate(migrate, migrate_idx, migrate_num);
    }

    if(idx < num && cnt < batch_size){
	if(from_start)
	    leftmost_ptr = value[idx++];
	from_start = false;
	if(idx < num){
	    if(batch_kvpair(key, value, idx, num, batch_size))
		return;
	    if(idx == num && cnt == batch_size && buf_num != 0)
		high_key = buf[buf_idx].key;
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start)
	    leftmost_ptr = buf[buf_idx++].value;
	batch_buffer(buf, buf_idx, buf_num, batch_size);
    }
}

// batch insert with and movement
template <typename Key_t>
void inode_t<Key_t>::batch_insert(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
    bool from_start = true;
    if(idx < num){
	from_start = false;
	leftmost_ptr = value[idx++];
	if(idx < num){
	    if(batch_kvpair(key, value, idx, num, batch_size))
		return;

	    if(idx == num && cnt == batch_size && buf_num != 0)
		high_key = buf[buf_idx].key;
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start)
	    leftmost_ptr = buf[buf_idx++].value;
	batch_buffer(buf, buf_idx, buf_num, batch_size);
    }
}

template <typename Key_t>
inode_t<Key_t>** inode_t<Key_t>::batch_insert(Key_t* key, node_t** value, int num, int& new_num){
    int pos = find_lowerbound(key[0]);
    int batch_size = cardinality * FILL_FACTOR;
    bool inplace = (cnt + num) < cardinality ? 1 : 0;
    int move_num = 0;
    int idx = 0;
    if(pos < 0)
	move_num = cnt;
    else
	move_num = cnt - pos - 1;

    if(inplace){
	move_normal_insertion(pos, num, move_num);
	for(int i=pos+1; i<pos+num+1; i++, idx++){
	    entry[i].key = key[idx];
	    entry[i].value = value[idx];
	}
	cnt += num;
	return nullptr;
    }
    else{
	if(batch_size < pos){ // need insert in the middle (migrated + new kvs + moved)
	    int migrate_num = pos - batch_size;
	    entry_t<Key_t, node_t*> migrate[migrate_num];
	    memcpy(migrate, &entry[batch_size], sizeof(entry_t<Key_t, node_t*>) * migrate_num);

	    entry_t<Key_t, node_t*> buf[move_num];
	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);
	    cnt = batch_size;

	    int total_num = num + move_num + migrate_num;
	    int last_chunk = 0;
	    int numerator = total_num / (batch_size+1);
	    int remains = total_num % (batch_size+1);
	    calculate_node_num(total_num, numerator, remains, last_chunk, new_num, batch_size);

	    auto new_nodes = new inode_t<Key_t>*[new_num];
	    for(int i=0; i<new_num; i++)
		new_nodes[i] = new inode_t<Key_t>(level);

	    auto old_sibling = sibling_ptr;
	    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	    int migrate_idx = 0;
	    int move_idx = 0;

	    auto prev_high_key = high_key;
	    high_key = migrate[migrate_idx].key;
	    for(int i=0; i<new_num-1; i++){
		new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
		new_nodes[i]->batch_insert(migrate, migrate_idx, migrate_num, key, value, idx, num, batch_size, buf, move_idx, move_num);
	    }
	    new_nodes[new_num-1]->sibling_ptr = old_sibling;
	    new_nodes[new_num-1]->high_key = prev_high_key;
	    new_nodes[new_num-1]->batch_insert(migrate, migrate_idx, migrate_num, key, value, idx, num, last_chunk, buf, move_idx, move_num);
	    return new_nodes;
	}
	else{ // need insert in the middle (new_kvs + moved)
	    int move_idx = 0;
	    entry_t<Key_t, node_t*> buf[move_num];
	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*move_num);

	    for(int i=pos+1; i<batch_size && idx<num; i++, idx++){
		entry[i].key = key[idx];
		entry[i].value = value[idx];
	    }

	    cnt += (idx - move_num);
	    for(; cnt<batch_size; cnt++, move_idx++){
		entry[cnt].key = buf[move_idx].key;
		entry[cnt].value = buf[move_idx].value;
	    }
	    auto prev_high_key = high_key;

	    if(idx < num)
		high_key = key[idx];
	    else
		high_key = buf[move_idx].key;

	    int total_num = num - idx + move_num - move_idx;
	    int last_chunk = 0;
	    int numerator = total_num / (batch_size+1);
	    int remains = total_num % (batch_size+1);
	    calculate_node_num(total_num, numerator, remains, last_chunk, new_num, batch_size);

	    auto new_nodes = new inode_t<Key_t>*[new_num];
	    for(int i=0; i<new_num; i++)
		new_nodes[i] = new inode_t<Key_t>(level);

	    auto old_sibling = sibling_ptr;
	    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	    for(int i=0; i<new_num-1; i++){
		new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
		new_nodes[i]->batch_insert(key, value, idx, num, batch_size, buf, move_idx, move_num);
	    }
	    new_nodes[new_num-1]->sibling_ptr = old_sibling;
	    new_nodes[new_num-1]->high_key = prev_high_key;
	    new_nodes[new_num-1]->batch_insert(key, value, idx, num, last_chunk, buf, move_idx, move_num);
	    return new_nodes;
	}
    }
}

template <typename Key_t>
void inode_t<Key_t>::insert_for_root(Key_t* key, node_t** value, node_t* left, int num){
    leftmost_ptr = left;
    for(int i=0; i<num; i++, cnt++){
	entry[cnt].key = key[i];
	entry[cnt].value = value[i];
    }
}

template <typename Key_t>
void inode_t<Key_t>::move_normal_insertion(int pos, int num, int move_num){
    memmove(&entry[pos+num+1], &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*move_num);
}

template <typename Key_t>
node_t* inode_t<Key_t>::rightmost_ptr(){
    return entry[cnt-1].value;
}

template <typename Key_t>
void inode_t<Key_t>::print(){
    std::cout << leftmost_ptr;
    for(int i=0; i<cnt; i++){
	std::cout << " [" << i << "]" << entry[i].key << " " << entry[i].value << ", ";
    }
    std::cout << "  high_key: " << high_key << "\n\n";
}

template <typename Key_t>
void inode_t<Key_t>::sanity_check(Key_t _high_key, bool first){
    for(int i=0; i<cnt-1; i++){
	for(int j=i+1; j<cnt; j++){
	    if(entry[i].key > entry[j].key){
		std::cout << "inode_t::key order is not preserved!!" << std::endl;
		std::cout << "[" << i << "].key: " << entry[i].key << "\t[" << j << "].key: " << entry[j].key << " at node " << this << std::endl;
	    }
	}
    }
    for(int i=0; i<cnt; i++){
	if(sibling_ptr && (entry[i].key > high_key)){
	    std::cout << "inode_t:: " << i << "(" << entry[i].key << ") is higher than high key " << high_key << "at node " << this << std::endl;
	}
	if(!first){
	    if(sibling_ptr && (entry[i].key <= _high_key)){
		std::cout << "inode_t:: " << i << "(" << entry[i].key << ") is smaller than previous high key " << _high_key << std::endl;
		std::cout << "--------- node_address " << this << " , current high_key " << high_key << std::endl;
	    }
	}
    }
    if(sibling_ptr != nullptr)
	(static_cast<inode_t<Key_t>*>(sibling_ptr))->sanity_check(high_key, false);
}

template <typename Key_t>
inline int inode_t<Key_t>::lowerbound_linear(Key_t key){
    int count = cnt;
    for(int i=0; i<count; i++){
	if(key <= entry[i].key){
	    return i-1;
	}
    }
    return count-1;
}

template <typename Key_t>
inline int inode_t<Key_t>::lowerbound_binary(Key_t key){
    int lower = 0;
    int upper = cnt;
    do{
	int mid = ((upper - lower)/2) + lower;
	if(key <= entry[mid].key)
	    upper = mid;
	else
	    lower = mid+1;
    }while(lower < upper);
    return lower-1;
}

template class inode_t<key64_t>;
}
