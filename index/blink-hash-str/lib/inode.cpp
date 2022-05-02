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
    if(sibling_ptr && (high_key < key)){
	return sibling_ptr;
    }
    else{
	int idx = find_lowerbound(key);
	if(idx > -1){
	    return entry[idx].value;
	}
	else{
	    return leftmost_ptr;
	}
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

// batch insert with migration and movement
template <typename Key_t>
void inode_t<Key_t>::batch_insert_last_level(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
    //std::cout << "inode_t::" << __func__ << "1: size " << batch_size << ", migrate idx " << migrate_idx << " / " << migrate_num << ", kv idx " << idx << " / " << num << ", buf idx " << buf_idx << " / " << buf_num << " in line " << __LINE__ << " at " << this << std::endl;
    bool from_start = true;
    if(migrate_idx < migrate_num){
	from_start = false;
	leftmost_ptr = migrate[migrate_idx++].value;
	//std::cout << "    leftmost_ptr: " << leftmost_ptr << " ---- from migrate[" << migrate_idx-1 << "]" << std::endl;
	int copy_num = migrate_num - migrate_idx;
	memcpy(entry, &migrate[migrate_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
	/*
	for(int i=0; i<copy_num; i++){
	    std::cout << "    entry[" << i << "].key: " << entry[i].key << " ---- from migrate[" << migrate_idx+i << "]" << std::endl;
	    std::cout << "    entry[" << i << "].value: " << entry[i].value << " ---- from migrate[" << migrate_idx+i << "]" << std::endl;
	}
	 */
	cnt += copy_num;
	migrate_idx += copy_num;
    }

    if(idx < num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = value[idx++];
	    //std::cout << "    leftmost ptr: " << leftmost_ptr << " ---- from value[" << idx-1 << "]" << std::endl;
	}
	from_start = false;
	if(idx < num){
	    for(; cnt<batch_size && idx<num-1; cnt++, idx++){
		entry[cnt].key = key[idx];
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
		//std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
	    }

	    if(cnt == batch_size){ // insert in next node
		high_key = key[idx];
		//std::cout << "    high_key: " << high_key << " ---- from key[" << idx << "]" << std::endl;
		return;
	    }
	    else{
		entry[cnt].key = key[idx];
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
		//std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
		cnt++, idx++;
		if(idx == num && cnt == batch_size && buf_num != 0){
		    high_key = buf[buf_idx].key;
		    //std::cout << "    high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
		    return;
		}
	    }
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = buf[buf_idx++].value;
	    //std::cout << "    leftmost_ptr: " << leftmost_ptr << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
	}
	for(; cnt<batch_size && buf_idx<buf_num-1; cnt++, buf_idx++){
	    entry[cnt].key = buf[buf_idx].key;
	    //std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    entry[cnt].value = buf[buf_idx].value;
	    //std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	}

	if(cnt == batch_size){ // insert in next node
	    high_key = buf[buf_idx].key;
	    //std::cout << "    high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    return;
	}
	else{
	    entry[cnt].key = buf[buf_idx].key;
	    entry[cnt].value = buf[buf_idx].value;
	    //std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    //std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	    cnt++, buf_idx++;
	}
    }
    //std::cout << "\n";
}



// batch insert with and movement
template <typename Key_t>
void inode_t<Key_t>::batch_insert_last_level(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
    //std::cout << "inode_t::" << __func__ << "1.5: size " << batch_size << ", kv idx " << idx << " / " << num << ", buf idx " << buf_idx << " / " << buf_num << " in line " << __LINE__ << " at " << this << std::endl;
    bool from_start = true;
    if(idx < num){
	leftmost_ptr = value[idx++];
	//std::cout << "    leftmost ptr: " << leftmost_ptr << " ---- from value[" << idx << "]" << std::endl;
	from_start = false;
	if(idx < num){
	    for(; cnt<batch_size && idx<num-1; cnt++, idx++){
		entry[cnt].key = key[idx];
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
		//std::cout << "    entry[" << cnt << "].value : " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
	    }

	    if(cnt == batch_size){ // insert in next node
		high_key = key[idx];
		//std::cout << "    high_key: " << high_key << " ---- from key[" << idx << "]" << std::endl;
		return;
	    }
	    else{
		entry[cnt].key = key[idx];
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
		//std::cout << "    entry[" << cnt << "].value : " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
		idx++, cnt++;
		if(idx == num && cnt == batch_size && buf_num != 0){
		    high_key = buf[buf_idx].key;
		    //std::cout << "    high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
		    return;
		}
	    }
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = buf[buf_idx++].value;
	    //std::cout << "    leftmost_ptr: " << leftmost_ptr << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
	}

	for(; cnt<batch_size && buf_idx<buf_num-1; cnt++, buf_idx++){
	    entry[cnt].key = buf[buf_idx].key;
	    //std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    entry[cnt].value = buf[buf_idx].value;
	    //std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	}
	if(cnt == batch_size){ // insert in next node
	    high_key = buf[buf_idx].key;
	    //std::cout << "    high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    return;
	}
	else{
	    entry[cnt].key = buf[buf_idx].key;
	    entry[cnt].value = buf[buf_idx].value;
	    //std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    //std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	    cnt++, buf_idx++;
	}
    }
    //std::cout << "\n";
}


template <typename Key_t>
inode_t<Key_t>** inode_t<Key_t>::batch_insert_last_level(Key_t* key, node_t** value, int num, int& new_num){
    //std::cout << "inode_t::" << __func__ << ": inserting " << num << " records at " << this << " in line " << __LINE__ << std::endl;
    int pos = find_lowerbound(key[0]);
    //std::cout << "    key[0]: " << key[0] << ", key[1]: " << key[1];
    //std::cout << ", found pos: " << pos << " , upper key: " << entry[pos+1].key;
    //if(pos < 0) std::cout << ", leftmost" << std::endl;
    //else std::cout << ", pos key: " << entry[pos].key << std::endl;
    int batch_size = cardinality * FILL_FACTOR;
    bool inplace = (cnt + num) < cardinality ? 1 : 0;
    int move_num = 0;
    int idx = 0;
    if(pos < 0)
	move_num = cnt;
    else
	move_num = cnt-pos-1;

    if(inplace){ // normal insertion
	//std::cout << "  inplace moving " << move_num << " records" << std::endl;
	move_normal_insertion(pos, num, move_num);
	if(pos < 0){ // leftmost ptr
	    leftmost_ptr = value[idx++];
	    //std::cout << "leftmost_ptr: " << leftmost_ptr << " ----- from value[" << idx-1 << "]: " << value[idx-1] << std::endl;
	}
	else{
	    entry[pos].value = value[idx++];
	    //std::cout << "entry[" << pos << "].value: " << entry[pos].value << " ----- from value[" << idx-1 << "]: " << value[idx-1] << std::endl;
	}

	for(int i=pos+1; i<pos+num+1; i++, idx++){
	    entry[i].key = key[idx];
	    //std::cout << "entry[" << i << "].key : " << entry[i].key << " ----- from key[" << idx << "]: " << key[idx] << std::endl;
	    entry[i].value = value[idx];
	    //std::cout << "entry[" << i << "].value: " << entry[i].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	}

	cnt += num-1;
	return nullptr;
    }
    else{
	auto prev_high_key = high_key;
	//std::cout << "  split, batch_size: " << batch_size << ", prev highkey: " << prev_high_key << std::endl;
	if(pos < 0){ // leftmost ptr
	    leftmost_ptr = value[idx++];
	    //std::cout << "    leftmost ptr: " << leftmost_ptr << " ---- from value[" << idx-1 << "]" << std::endl;
	}
	else{
	    entry[pos].value = value[idx++];
	    //std::cout << "    entry[" << pos << "].value: " << entry[pos].value << " ---- from value[" << idx-1 << "]" << std::endl;
	}

	if(batch_size < pos){ // need insert in the middle (migrated + new kvs + moved)
	    int migrate_num = pos - batch_size;
	    //std::cout << "    migrate: " << migrate_num << ", move_num: " << move_num << ", pos: " << pos << ", batch_size: " << batch_size << ", cnt: " << cnt << std::endl;
	    entry_t<Key_t, node_t*> migrate[migrate_num];
	    memcpy(migrate, &entry[batch_size], sizeof(entry_t<Key_t, node_t*>) * migrate_num);
	    /*
	    for(int i=batch_size; i<pos; i++){
		std::cout << "    copied for migrate entry[" << i << "].key " << entry[i].key << std::endl;
		std::cout << "    copied for migrate entry[" << i << "].value " << entry[i].value << std::endl;
	    }
	    */

	    entry_t<Key_t, node_t*> buf[move_num];
	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);
	    /*
	    for(int i=pos+1; i<cnt; i++){
		std::cout << "    copied for move entry[" << i << "].key " << entry[i].key << std::endl;
		std::cout << "    copied for move entry[" << i << "].value " << entry[i].value << std::endl;
	    }
	    */
	    cnt = batch_size;
	    //std::cout << "    cnt: " << cnt << std::endl;

	    int total_num = num + move_num + migrate_num;
	    int last_chunk = 0;
	    int numerator = total_num / (batch_size+1);
	    int remains = total_num % (batch_size+1);
	    if(numerator == 0){ // need only one new node
		new_num = 1;
		last_chunk = remains;
	    }
	    else{ // multiple new nodes
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

	    auto new_nodes = new inode_t<Key_t>*[new_num];
	    for(int i=0; i<new_num; i++)
		new_nodes[i] = new inode_t<Key_t>(level);

	    auto old_sibling = sibling_ptr;
	    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	    int migrate_idx = 0;
	    int move_idx = 0;

	    high_key = migrate[migrate_idx].key;
	    //std::cout << "    high_key: " << high_key << " ---- from migrate[" << migrate_idx << "]" << std::endl;
	    for(int i=0; i<new_num-1; i++){
		new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
		new_nodes[i]->batch_insert_last_level(migrate, migrate_idx, migrate_num, key, value, idx, num, batch_size, buf, move_idx, move_num);
	    }
	    new_nodes[new_num-1]->sibling_ptr = old_sibling;
	    new_nodes[new_num-1]->batch_insert_last_level(migrate, migrate_idx, migrate_num, key, value, idx, num, last_chunk, buf, move_idx, move_num);
	    new_nodes[new_num-1]->high_key = prev_high_key;
	    //std::cout << "    high_key: " << new_nodes[new_num-1]->high_key << " ---- from prev_high_key" << std::endl;

	    return new_nodes;
	}
	else{ // need insert in the middle (new_kvs + moved)
	    //std::cout << "    move_num: " << move_num << ", pos: " << pos << ", batch_size: " << batch_size << ", cnt: " << cnt << std::endl;
	    int move_idx = 0;
	    entry_t<Key_t, node_t*> buf[move_num];
	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);
	    /*
	    for(int i=pos+1; i<cnt; i++){
		std::cout << "    copied for move entry[" << i << "].key " << entry[i].key << std::endl;
		std::cout << "    copied for move entry[" << i << "].value " << entry[i].value << std::endl;
	    }
	    */

	    for(int i=pos+1; i<batch_size && idx<num; i++, idx++){
		entry[i].key = key[idx];
		//std::cout << "    entry[" << i << "].key: " << entry[i].key << " ---- from key[" << idx << "]" << std::endl;
		entry[i].value = value[idx];
		//std::cout << "    entry[" << i << "].value: " << entry[i].value << " ---- from value[" << idx << "]" << std::endl;
	    }

	    cnt += (idx - move_num - 1);
	    for(; cnt<batch_size; cnt++, move_idx++){
		entry[cnt].key = buf[move_idx].key;
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << move_idx << "]" << std::endl;
		entry[cnt].value = buf[move_idx].value;
		//std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << move_idx << "]" << std::endl;
	    }

	    if(idx < num){
		high_key = key[idx];
		//std::cout << "    high_key: " << high_key << " --- from key[" << idx << "]" << std::endl;
	    }
	    else{
		high_key = buf[move_idx].key;
		//std::cout << "    high_key: " << high_key << " --- from buf[" << move_idx << "]" << std::endl;
	    }

	    int total_num = num - idx + move_num - move_idx;
	    int last_chunk = 0;
	    int numerator = total_num / (batch_size+1);
	    int remains = total_num % (batch_size+1);
	    if(numerator == 0){ // need only one new node
		new_num = 1;
		last_chunk = remains;
	    }
	    else{ // multiple new nodes
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
	    //std::cout << "    high_key: " << new_nodes[new_num-1]->high_key << " ---- from prev_high_key" << std::endl;

	    return new_nodes;
	}
    }
}

// batch insert with migration and movement
template <typename Key_t>
void inode_t<Key_t>::batch_insert(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
    //std::cout << "inode_t::" << __func__ << "1: migrate idx " << migrate_idx << " / " << migrate_num << ", kv idx " << idx << " / " << num << ", buf idx " << buf_idx << " / " << buf_num << " in line " << __LINE__ << " at " << this << std::endl;
    bool from_start = true;
    if(migrate_idx < migrate_num){
	from_start = false;
	leftmost_ptr = migrate[migrate_idx++].value;
	//std::cout << "    leftmost_ptr: " << leftmost_ptr << " ---- from migrate[" << migrate_idx-1 << "]" << std::endl;
	int copy_num = migrate_num - migrate_idx;
	memcpy(entry, &migrate[migrate_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
	/*
	for(int i=0; i<copy_num; i++){
	    std::cout << "    entry[" << i << "].key: " << entry[i].key << " ---- from migrate[" << migrate_idx+i << "]" << std::endl;
	    std::cout << "    entry[" << i << "].value: " << entry[i].value << " ---- from migrate[" << migrate_idx+i << "]" << std::endl;
	}
	*/
	cnt += copy_num;
	migrate_idx += copy_num;
    }

    if(idx < num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = value[idx++];
	    //std::cout << "    leftmost ptr: " << leftmost_ptr << " ---- from value[" << idx-1 << "]" << std::endl;
	}
	from_start = false;
	if(idx < num){
	    for(; cnt<batch_size && idx<num-1; cnt++, idx++){
		entry[cnt].key = key[idx];
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
		//std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
	    }

	    if(cnt == batch_size){ // insert in next node
		high_key = key[idx];
		//std::cout << "    high_key: " << high_key << " ---- from key[" << idx << "]" << std::endl;
		return;
	    }
	    else{
		entry[cnt].key = key[idx];
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
		//std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
		idx++, cnt++;
		if(idx == num && cnt == batch_size && buf_num != 0){
		    high_key = buf[buf_idx].key;
		    //std::cout << "    high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
		}
	    }
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = buf[buf_idx++].value;
	    //std::cout << "    leftmost_ptr: " << leftmost_ptr << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
	}
	for(; cnt<batch_size && buf_idx<buf_num-1; cnt++, buf_idx++){
	    entry[cnt].key = buf[buf_idx].key;
	    //std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    entry[cnt].value = buf[buf_idx].value;
	    //std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	}

	if(cnt == batch_size){ // insert in next node
	    high_key = buf[buf_idx].key;
	    //std::cout << "    high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    return;
	}
	else{
	    entry[cnt].key = buf[buf_idx].key;
	    entry[cnt].value = buf[buf_idx].value;
	    //std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    //std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	    cnt++, buf_idx++;
	}
    }
    //std::cout << "\n";
}

// batch insert with and movement
template <typename Key_t>
void inode_t<Key_t>::batch_insert(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num){
    //std::cout << "inode_t::" << __func__ << "1.5: kv idx " << idx << " / " << num << ", buf idx " << buf_idx << " / " << buf_num << " in line " << __LINE__ << " at " << this << std::endl;
    bool from_start = true;
    if(idx < num){
	leftmost_ptr = value[idx++];
	//std::cout << "    leftmost ptr: " << leftmost_ptr << " ---- from value[" << idx-1 << "]" << std::endl;
	from_start = false;
	if(idx < num){
	    for(; cnt<batch_size && idx<num-1; cnt++, idx++){
		entry[cnt].key = key[idx];
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
		//std::cout << "    entry[" << cnt << "].value : " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
	    }

	    if(cnt == batch_size){ // insert in next node
		high_key = key[idx];
		//std::cout << "    high_key: " << high_key << " ---- from key[" << idx << "]" << std::endl;
		return;
	    }
	    else{
		entry[cnt].key = key[idx];
		//std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
		//std::cout << "    entry[" << cnt << "].value : " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
		idx++, cnt++;
		if(idx == num && cnt == batch_size && buf_num != 0){
		    high_key = buf[buf_idx].key;
		    //std::cout << "    high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
		}
	    }
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = buf[buf_idx++].value;
	    //std::cout << "    leftmost_ptr: " << leftmost_ptr << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
	}

	for(; cnt<batch_size && buf_idx<buf_num-1; cnt++, buf_idx++){
	    entry[cnt].key = buf[buf_idx].key;
	    //std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    entry[cnt].value = buf[buf_idx].value;
	    //std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	}
	if(cnt == batch_size){ // insert in next node
	    high_key = buf[buf_idx].key;
	    //std::cout << "    high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    return;
	}
	else{
	    entry[cnt].key = buf[buf_idx].key;
	    entry[cnt].value = buf[buf_idx].value;
	    //std::cout << "    entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    //std::cout << "    entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	    cnt++, buf_idx++;
	}
    }
    //std::cout << "\n";
}

template <typename Key_t>
inode_t<Key_t>** inode_t<Key_t>::batch_insert(Key_t* key, node_t** value, int num, int& new_num){
    //std::cout << "inode_t::" << __func__ << "3: inserting " << num << " records at " << this << " in line " << __LINE__ << std::endl;
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
	//std::cout << "  inplace " << std::endl;
	move_normal_insertion(pos, num, move_num);
	for(int i=pos+1; i<pos+num+1; i++, idx++){
	    entry[i].key = key[idx];
	    //std::cout << "    entry[" << i << "].key : " << entry[i].key << " ----- from key[" << idx << "]: " << key[idx] << std::endl;
	    entry[i].value = value[idx];
	    //std::cout << "    entry[" << i << "].value: " << entry[i].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	}
	cnt += num;
	return nullptr;
    }
    else{
	//std::cout << "  split, batch_size: " << batch_size << std::endl;
	if(batch_size < pos){ // need insert in the middle (migrated + new kvs + moved)
	    int migrate_num = pos - batch_size;
	    //std::cout << "    migrate: " << migrate_num << ", move_num: " << move_num << ", pos: " << pos << ", batch_size: " << batch_size << ", cnt: " << cnt << std::endl;
	    entry_t<Key_t, node_t*> migrate[migrate_num];
	    memcpy(migrate, &entry[batch_size], sizeof(entry_t<Key_t, node_t*>) * migrate_num);
	    /*
	    for(int i=batch_size; i<pos; i++){
		std::cout << "    copied for migrate entry[" << i << "].key " << entry[i].key << std::endl;
		std::cout << "    copied for migrate entry[" << i << "].value " << entry[i].value << std::endl;
	    }
	    */

	    entry_t<Key_t, node_t*> buf[move_num];
	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);
	    /*
	    for(int i=pos+1; i<pos+move_num+1; i++){
		std::cout << "    copied for move entry[" << i << "].key " << entry[i].key << std::endl;
		std::cout << "    copied for move entry[" << i << "].value " << entry[i].value << std::endl;
	    }
	    */
	    cnt = batch_size;

	    int total_num = num + move_num + migrate_num;
	    int last_chunk = 0;
	    int numerator = total_num / (batch_size+1);
	    int remains = total_num % (batch_size+1);
	    if(numerator == 0){ // need only one new node
		new_num = 1;
		last_chunk = remains;
	    }
	    else{ // multiple new nodes
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

	    auto new_nodes = new inode_t<Key_t>*[new_num];
	    for(int i=0; i<new_num; i++)
		new_nodes[i] = new inode_t<Key_t>(level);

	    auto old_sibling = sibling_ptr;
	    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	    int migrate_idx = 0;
	    int move_idx = 0;

	    auto prev_high_key = high_key;
	    high_key = migrate[migrate_idx].key;
	    //std::cout << "    high_key: " << high_key << " ---- from migrate[" << migrate_idx << "]" << std::endl;
	    for(int i=0; i<new_num-1; i++){
		new_nodes[i]->sibling_ptr = static_cast<node_t*>(new_nodes[i+1]);
		new_nodes[i]->batch_insert(migrate, migrate_idx, migrate_num, key, value, idx, num, batch_size, buf, move_idx, move_num);
	    }
	    new_nodes[new_num-1]->sibling_ptr = old_sibling;
	    new_nodes[new_num-1]->high_key = prev_high_key;
	    //std::cout << "    high_key: " << prev_high_key << " ---- from prev high key" << std::endl;
	    new_nodes[new_num-1]->batch_insert(migrate, migrate_idx, migrate_num, key, value, idx, num, last_chunk, buf, move_idx, move_num);

	    return new_nodes;
	}
	else{ // need insert in the middle (new_kvs + moved)
	    //std::cout << "    move_num: " << move_num << ", pos: " << pos << ", batch_size: " << batch_size << ", cnt: " << cnt << std::endl;
	    int move_idx = 0;
	    entry_t<Key_t, node_t*> buf[move_num];
	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*move_num);

	    /*
	    for(int i=pos+1; i<pos+1+move_num; i++){
		std::cout << "    copied for move entry[" << i << "].key " << entry[i].key << std::endl;
		std::cout << "    copied for move entry[" << i << "].value " << entry[i].value << std::endl;
	    }
	    */

	    for(int i=pos+1; i<batch_size && idx<num; i++, idx++){
		entry[i].key = key[idx];
		//std::cout << "    entry[" << i << "].key : " << entry[i].key << " ----- from key[" << idx << "]: " << key[idx] << std::endl;
		entry[i].value = value[idx];
		//std::cout << "    entry[" << i << "].value : " << entry[i].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	    }

	    cnt += (idx - move_num);
	    for(; cnt<batch_size; cnt++, move_idx++){
		entry[cnt].key = buf[move_idx].key;
		//std::cout << "    entry[" << cnt << "].key : " << entry[cnt].key << " ----- from buf[" << move_idx << std::endl;
		entry[cnt].value = buf[move_idx].value;
		//std::cout << "    entry[" << cnt << "].value : " << entry[cnt].value << " ----- from buf[" << move_idx << std::endl;
	    }
	    auto prev_high_key = high_key;

	    if(idx < num){
		high_key = key[idx];
		//std::cout << "    high_key: " << high_key << " ---- from key[" << idx  << "]" << std::endl;
	    }
	    else{
		high_key = buf[move_idx].key;
		//std::cout << "    high_key: " << high_key << " ---- from buf[" << move_idx  << "]" << std::endl;
	    }

	    int total_num = num - idx + move_num - move_idx;
	    int last_chunk = 0;
	    int numerator = total_num / (batch_size+1);
	    int remains = total_num % (batch_size+1);
	    if(numerator == 0){ // need only one new node
		new_num = 1;
		last_chunk = remains;
	    }
	    else{ // multiple new nodes
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
	    //std::cout << "    high_key: " << prev_high_key << " ---- from prev high key" << std::endl;
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
void inode_t<Key_t>::print(){
}

template <typename Key_t>
void inode_t<Key_t>::sanity_check(Key_t _high_key, bool first){
    for(int i=0; i<cnt-1; i++){
	for(int j=i+1; j<cnt; j++){
	    if(entry[i].key > entry[j].key){
		std::cout << "inode_t::key order is not preserved!!" << std::endl;
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

template class inode_t<StringKey>;
}
