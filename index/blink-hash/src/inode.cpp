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

template <typename Key_t>
void inode_t<Key_t>::batch_insert(Key_t* key, node_t** value, node_t* left, int& idx, int num, int batch_size){
    std::cout << "inode_t::" << __func__ << ": this should not be called" << std::endl;
    leftmost_ptr = left;
    for(; cnt<batch_size, idx < num-1; cnt++, idx++){
	entry[cnt].key = key[idx];
	entry[cnt].value = value[idx];
	//if(idx == num-1) return;
    }
    high_key = key[idx++];
}


template <typename Key_t>
void inode_t<Key_t>::insert_for_root(Key_t* key, node_t** value, node_t* left, int num){
//    std::cout << "inode_t::" << __func__ << ": inserting in root " << num << " records at " << this << std::endl;
    leftmost_ptr = left;
//    std::cout << "leftmost_ptr: " << left << std::endl;
    for(int i=0; i<num; i++, cnt++){
	entry[cnt].key = key[i];
//	std::cout << "entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << i << "]: " << key[i] << std::endl;
	entry[cnt].value = value[i];
//	std::cout << "entry[" << cnt << "].value : " << entry[cnt].value << " ---- from value[" << i << "]: " << value[i] << std::endl;
    }
//    if(leftmost_ptr == nullptr) std::cout << "assigned null for root!! " <<std::endl;
}

// batch insert
template <typename Key_t>
void inode_t<Key_t>::batch_insert_last_level(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num, Key_t prev_high_key){
//    std::cout << "inode_t::" << __func__ << "1: inserting from " << idx << " out of " << num << " with buf_idx " << buf_idx << " out of " << buf_num << " until " << batch_size << " at " << this << std::endl;
    bool from_start = true;
    if(idx < num){
	from_start = false;
	leftmost_ptr = value[idx];
//	std::cout << "leftmost_ptr: " << leftmost_ptr << " ---- from value[" << idx << "]" << std::endl;
	for(; cnt<batch_size && idx<num-1; cnt++){
	    entry[cnt].key = key[idx++];
//	    std::cout << "entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx-1 << "]" << std::endl;
	    entry[cnt].value = value[idx];
//	    std::cout << "entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
	}

	if(cnt == batch_size){
	    if(sibling_ptr != nullptr){
		high_key = key[idx++];
//		std::cout << "high_key: " << high_key << " ---- from key[" << idx-1 << "]" << std::endl;
	    }
	    else{
//		std::cout << "just increasing idx" << std::endl;
		idx++;
	    }
	}
	else{
	    if(buf_num == 0){
		high_key = key[idx++];
//		std::cout << "high_key: " << high_key << " ---- from key[" << idx-1 << "]" << std::endl;
	    }
	    else{
		entry[cnt].key = key[idx++];
//		std::cout << "entry[" << cnt << "].key: " <<entry[cnt].key << " ---- from key[" << idx-1 << "]" << std::endl;
	    }
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = buf[buf_idx++].value;
//	    std::cout << "leftmost_ptr: " << buf[buf_idx].value << " ---- from buf[" << buf_idx-1 << "]" << std::endl;

	    int copy_num = batch_size;
	    if(buf_num - buf_idx < batch_size - cnt)
		copy_num = buf_num - buf_idx;
	    memcpy(entry, &buf[buf_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
	    //memcpy(entry, &buf[buf_idx], sizeof(entry_t<Key_t, node_t*>) * batch_size);
//	    for(int i=cnt, j=0; i<copy_num; i++, j++){
//		std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//		std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//	    }
	    buf_idx += copy_num;
	    cnt += copy_num;

	    /*
	    //if(buf_idx == 0) buf_idx++;
	    for(; cnt<batch_size && buf_idx<buf_num-1; cnt++){
		entry[cnt].key = buf[buf_idx++].key;
		std::cout << "entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
		entry[cnt].value = buf[buf_idx].value;
		std::cout << "entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	    }

	    */
	    if(buf_idx < buf_num){
//		high_key = buf[buf_idx++].key;
		high_key = buf[buf_idx].key;
//		std::cout << "high_key: " << high_key << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
	    }
	    else{
		high_key = prev_high_key;
//		std::cout << "high_key: " << prev_high_key << " ---- from prev_high_key" << std::endl;
		//std::cout << "just increasing buf_idx" << std::endl;
		//buf_idx++;
	    }
	}
	else{
	    int copy_num = batch_size - cnt;
	    if(buf_num - buf_idx < batch_size - cnt)
		copy_num = buf_num - buf_idx;
	    memcpy(&entry[cnt], &buf[buf_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
//	    for(int i=cnt, j=0; i<cnt+copy_num; i++, j++){
//		std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//		std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//	    }
	    buf_idx += copy_num;
	    cnt += copy_num;
	    /*
	    entry[cnt++].value = buf[buf_idx].value;
//	    entry[cnt].key = prev_high_key;
	    std::cout << "entry[" << cnt-1 << "].value: " << entry[cnt-1].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	    if(buf_idx == 0) buf_idx++;
	    for(; cnt<batch_size && buf_idx<buf_num-1; cnt++){
		entry[cnt].key = buf[buf_idx++].key;
		std::cout << "entry[" << cnt << "].key: " << entry[cnt].key << " ---- from buf[" << buf_idx << "]" << std::endl;
		entry[cnt].value = buf[buf_idx].value;
		std::cout << "entry[" << cnt << "].value: " << entry[cnt].value << " ---- from buf[" << buf_idx << "]" << std::endl;
	    }*/

	    if(buf_idx < buf_num){
		high_key = buf[buf_idx].key;
//		std::cout << "high_key: " << high_key << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
	    }
	    else{
		high_key = prev_high_key;
//		std::cout << "high_key: " << prev_high_key << " ---- from prev_high_key" << std::endl;
		//buf_idx++;
	    }
	}
    }

//    std::cout << "\n";
}

template <typename Key_t>
inode_t<Key_t>** inode_t<Key_t>::batch_insert_last_level(Key_t* key, node_t** value, int num, int& new_num){
//    std::cout << "inode_t::" << __func__ << ": inserting " << num << " records at " << this << std::endl;
//    std::cout << "previous high key: " << high_key << std::endl;
    int pos = find_lowerbound(key[0]);
    int batch_size = cardinality * FILL_FACTOR;
    //int batch_size = cardinality * FILL_FACTOR + 1;
    bool inplace = (cnt + num) < cardinality ? 1 : 0;
    int move_num = 0;
    int idx = 0;
    if(pos < 0)
	move_num = cnt;
    else
	move_num = cnt-pos-1;
//    std::cout << "Moving " << move_num << " records! " << std::endl;

    if(inplace){ // normal insertion
	move_normal_insertion(pos, num, move_num);
	if(pos < 0){ // leftmost ptr
	    leftmost_ptr = value[idx];
//	    std::cout << "leftmost_ptr: " << leftmost_ptr << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	}
	else{
	    entry[pos].value = value[idx];
//	    std::cout << "entry[" << pos << "].value: " << entry[pos].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	}


	for(int i=pos+1; i<pos+num; i++){
	    entry[i].key = key[idx++];
//	    std::cout << "entry[" << i << "].key : " << entry[i].key << " ----- from key[" << idx-1 << "]: " << key[idx-1] << std::endl;
	    entry[i].value = value[idx];
//	    std::cout << "entry[" << i << "].value: " << entry[i].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	}
	if(move_num == 0){
	    cnt += num-1;
	    high_key = key[idx++];
//	    std::cout << "high_key: " << high_key << " ---- from key[" << idx-1 << " <<\n"<< std::endl;
	}
	else{
	    cnt += num;
	    entry[pos+num].key = key[idx++];
//	    std::cout << "entry[" << pos+num << "].key: " << entry[pos+num].key << " ---- from key[" << idx-1 << "]" << std::endl;
	}
	//if(!sibling_ptr) high_key = entry[cnt-1].key;
	return nullptr;
    }
    else{
//	std::cout << " split, batch_size: " << batch_size << std::endl;
	entry_t<Key_t, node_t*> buf[move_num];
	//auto buf = new entry_t<Key_t, node_t*>[move_num];
	Key_t prev_high_key = high_key;

	memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*move_num);
	for(int i=pos+1; i<pos+1+move_num; i++){
//	    std::cout << "copied entry[" << i << "].key " << entry[i].key << std::endl;
//	    std::cout << "copied entry[" << i << "].value " << entry[i].value << std::endl;
	}

	int written_num = 0;
	if(pos < 0){ // leftmost ptr
	    leftmost_ptr = value[idx];
//	    std::cout << "leftmost_ptr = " << leftmost_ptr << " ---- from value[" << idx << "]: " << value[idx] << std::endl;
	}
	else{
	    entry[pos].value = value[idx];
//	    std::cout << "entry[" << pos << "].value : " << entry[pos].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	}

	for(int i=pos+1; i<batch_size && idx<num-1; i++){
	    entry[i].key = key[idx++];
//	    std::cout << "entry[" << i << "].key : " << entry[i].key << " ----- from key[" << idx-1 << "]: " << key[idx-1] << std::endl;
	    entry[i].value = value[idx];
//	    std::cout << "entry[" << i << "].value : " << entry[i].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	}

	cnt += (idx - move_num);
	int from = 0;
	int total = num - idx + move_num;
	if(cnt < batch_size){
	    idx++;
	    memcpy(&entry[cnt], buf, sizeof(entry_t<Key_t, node_t*>) * (batch_size - cnt));
	    int _from = from;
//	    for(int i=cnt; i<batch_size; i++, _from++){
//		std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << _from << "]" << std::endl;
//		std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << _from << "]" << std::endl;
//	    }
	    from += (batch_size - cnt);
	    cnt = batch_size;
	    //from = _from;
	    //from += (batch_size - cnt);
	    high_key = buf[from].key;
//	    std::cout << "high_key: " << high_key << " ---- from buf[" << from << "]" << std::endl;
	}
	else{
	    high_key = key[idx++];
//	    std::cout << "high_key: " << high_key << " ---- from key[" << idx-1 << "]: " << key[idx-1] << std::endl;
	}
//	std::cout << "\n" << std::endl;

	int left_num = num - idx + move_num - from;
	int last_chunk = 0;
	int remains = left_num % batch_size;
	if(left_num / batch_size == 0){
	    new_num = 1;
	    last_chunk = remains;
	}
	else{
	    if(remains == 0){
		new_num = left_num / batch_size;
		last_chunk = batch_size;
	    }
	    else{
		if(remains < cardinality - batch_size){
		    new_num = left_num / batch_size;
		    last_chunk = batch_size + remains;
		}
		else{
		    new_num = left_num / batch_size + 1;
		    last_chunk = remains;
		}
	    }
	}

	//inode_t<Key_t>* new_nodes[new_num]; 
	auto new_nodes = new inode_t<Key_t>*[new_num];
	for(int i=0; i<new_num; i++)
	    new_nodes[i] = new inode_t<Key_t>(level);

	auto old_sibling = sibling_ptr;
	sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	int node_id = 0;
	for(; node_id<new_num-1; node_id++){
	    new_nodes[node_id]->sibling_ptr = static_cast<node_t*>(new_nodes[node_id+1]);
	    new_nodes[node_id]->batch_insert_last_level(key, value, idx, num, batch_size, buf, from, move_num, prev_high_key);
	}
	new_nodes[node_id]->sibling_ptr = old_sibling;
	new_nodes[node_id]->batch_insert_last_level(key, value, idx, num, last_chunk, buf, from, move_num, prev_high_key);
	return new_nodes;
    }
}

// batch insert
template <typename Key_t>
void inode_t<Key_t>::batch_insert(entry_t<Key_t, node_t*>* migrate, int& migrate_idx, int migrate_num, Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num, Key_t prev_high_key){
//    std::cout << "inode_t::" << __func__ << "1: migrating from " << migrate_idx << " out of " << migrate_num << " and inserting from " << idx << " out of " << num << " with buf_idx " << buf_idx << " out of " << buf_num << " until " << batch_size << " at " << this << std::endl;
    bool from_start = true;
    if(migrate_idx < migrate_num){
	from_start = false;
	leftmost_ptr = migrate[migrate_idx++].value;
//	std::cout << "leftmost_ptr: " << leftmost_ptr << " ---- from migrate[" << migrate_idx-1 << "]" << std::endl;
	int copy_num = migrate_num - migrate_idx;
	memcpy(entry, &migrate[migrate_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
	for(int i=0; i<copy_num; i++){
//	    std::cout << " entry[" << i << "].key: " << entry[i].key << " ---- from migrate[" << migrate_idx+i << "]" << std::endl;
//	    std::cout << " entry[" << i << "].value: " << entry[i].value << " ---- from migrate[" << migrate_idx+i << "]" << std::endl;
	}
	cnt += copy_num;
	migrate_idx += copy_num;
    }

    if(idx < num && cnt < batch_size){
	if(from_start){
	    from_start = false;
	    leftmost_ptr = value[idx++];
//	    std::cout << "leftmost_ptr: " << leftmost_ptr << " ---- from value[" << idx-1 << "]" << std::endl;
	    for(; cnt<batch_size && idx<num-1; cnt++, idx++){
		entry[cnt].key = key[idx];
//		std::cout << "entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
//		std::cout << "entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
	    }

	    if(cnt == batch_size){
		if(sibling_ptr != nullptr){
		    high_key = key[idx];
//		    std::cout << "high_key: " << high_key << " ---- from key[" << idx << "]" << std::endl;
		}
		else{
//		    std::cout << "just increasing idx" << std::endl;
		    idx++;
		}
	    }
	    else{
		entry[cnt].key = key[idx];
//		std::cout << "entry[" << cnt << "].key: " <<entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
	    }
	}
	else{
	    for(; cnt<batch_size && idx<num-1; cnt++, idx++){
		entry[cnt].key = key[idx];
//		std::cout << "entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		entry[cnt].value = value[idx];
//		std::cout << "entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
	    }

	    if(cnt == batch_size){
		if(sibling_ptr != nullptr){
		    high_key = key[idx];
//		    std::cout << "high_key: " << high_key << " ---- from key[" << idx << "]" << std::endl;
		}
		else{
//		    std::cout << "just increasing idx" << std::endl;
		    idx++;
		}
	    }
	    else{
		if(buf_num == 0){
		    high_key = prev_high_key;
//		    std::cout << "high_key: " << high_key << " ---- from prev_high_key" << std::endl;
		}
		else{
		    entry[cnt].key = key[idx];
//		    std::cout << "entry[" << cnt << "].key: " <<entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
		}
	    }
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = buf[buf_idx++].value;
//	    std::cout << "leftmost_ptr: " << leftmost_ptr << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
	   
	    int copy_num = batch_size;
	    if(buf_num - buf_idx < batch_size - cnt)
		copy_num = buf_num - buf_idx;
	    memcpy(entry, &buf[buf_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
//	    for(int i=cnt, j=0; i<copy_num; i++, j++){
//		std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//		std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//	    }
	    buf_idx += copy_num;
	    cnt += copy_num;

	    if(buf_idx < buf_num){
		high_key = buf[buf_idx].key;
//		std::cout << "high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    }
	    else{
		high_key = prev_high_key;
//		std::cout << "high_key: " << high_key << " ---- from prev_high_key" << std::endl;
	    }
	}
	else{
	    int copy_num = batch_size - cnt;
	    if(buf_num - buf_idx < batch_size - cnt)
		copy_num = buf_num - buf_idx;
	    memcpy(&entry[cnt], &buf[buf_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
//	    for(int i=cnt, j=0; i<copy_num; i++, j++){
//		std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//		std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//	    }
	    buf_idx += copy_num;
	    cnt += copy_num;

	    if(buf_idx < buf_num){
		high_key = buf[buf_idx].key;
//		std::cout << "high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    }
	    else{
		high_key = prev_high_key;
//		std::cout << "high_key: " << high_key << " ---- from prev_high_key" << std::endl;
	    }
	}
    }

//    std::cout << "\n";
}



// batch insert
template <typename Key_t>
void inode_t<Key_t>::batch_insert(Key_t* key, node_t** value, int& idx, int num, int batch_size, entry_t<Key_t, node_t*>* buf, int& buf_idx, int buf_num, Key_t prev_high_key){
//    std::cout << "inode_t::" << __func__ << "1: inserting from " << idx << " out of " << num << " with buf_idx " << buf_idx << " out of " << buf_num << " until " << batch_size << " at " << this << std::endl;
    bool from_start = true;
    if(idx < num){
	from_start = false;
	leftmost_ptr = value[idx++];
//	std::cout << "leftmost_ptr: " << leftmost_ptr << " ---- from value[" << idx-1 << "]" << std::endl;
	for(; cnt<batch_size && idx<num-1; cnt++, idx++){
	    entry[cnt].key = key[idx];
//	    std::cout << "entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
	    entry[cnt].value = value[idx];
//	    std::cout << "entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]" << std::endl;
	}

	if(idx < num){
	    entry[cnt].key = key[idx];
//	    std::cout << "entry[" << cnt << "].key: " <<entry[cnt].key << " ---- from key[" << idx << "]" << std::endl;
	    entry[cnt++].value = value[idx++];
//	    std::cout << "entry[" << cnt-1 << "].key: " <<entry[cnt-1].value << " ---- from value[" << idx-1 << "]" << std::endl;
	}
	else{
	    high_key = key[idx];
//	    std::cout << "high_key: " << high_key << " ---- from key[" << idx << "]" << std::endl;
	}
    }

    if(buf_idx < buf_num && cnt < batch_size){
	if(from_start){
	    leftmost_ptr = buf[buf_idx++].value;
//	    std::cout << "leftmost_ptr: " << leftmost_ptr << " ---- from buf[" << buf_idx-1 << "]" << std::endl;
	   
	    int copy_num = batch_size;
	    if(buf_num - buf_idx < batch_size - cnt)
		copy_num = buf_num - buf_idx;
	    memcpy(entry, &buf[buf_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
	    for(int i=cnt, j=0; i<copy_num; i++, j++){
//		std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//		std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << buf_idx+j << "]" << std::endl;
	    }
	    buf_idx += copy_num;
	    cnt += copy_num;

	    if(buf_idx < buf_num){
		high_key = buf[buf_idx].key;
//		std::cout << "high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    }
	    else{
		high_key = prev_high_key;
//		std::cout << "high_key: " << high_key << " ---- from prev_high_key" << std::endl;
	    }
	}
	else{
	    int copy_num = batch_size - cnt;
	    if(buf_num - buf_idx < batch_size - cnt)
		copy_num = buf_num - buf_idx;
	    memcpy(&entry[cnt], &buf[buf_idx], sizeof(entry_t<Key_t, node_t*>) * copy_num);
//	    for(int i=cnt, j=0; i<copy_num; i++, j++){
//		std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//		std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << buf_idx+j << "]" << std::endl;
//	    }
	    buf_idx += copy_num;
	    cnt += copy_num;

	    if(buf_idx < buf_num){
		high_key = buf[buf_idx].key;
//		std::cout << "high_key: " << high_key << " ---- from buf[" << buf_idx << "]" << std::endl;
	    }
	    else{
		high_key = prev_high_key;
//		std::cout << "high_key: " << high_key << " ---- from prev_high_key" << std::endl;
	    }
	}
    }

//    std::cout << "\n";
}

template <typename Key_t>
inode_t<Key_t>** inode_t<Key_t>::batch_insert(Key_t* key, node_t** value, int num, int& new_num){
//    std::cout << "inode_t::" << __func__ << "3: inserting " << num << " records at " << this << std::endl;
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
	for(int i=pos+1; i<pos+num; i++, idx++){
	//for(int i=pos+1; i<pos+num+1; i++, idx++){
	    entry[i].key = key[idx];
//	    std::cout << "entry[" << i << "].key : " << entry[i].key << " ----- from key[" << idx << "]: " << key[idx] << std::endl;
	    entry[i].value = value[idx];
//	    std::cout << "entry[" << i << "].value: " << entry[i].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	}
	if(move_num == 0){
	    if(sibling_ptr){
		entry[pos+num].key = key[idx];
		//entry[pos+num].key = high_key;
//		std::cout << "entry[" << pos+num << "].key: " << entry[pos+num].key << " ---- from key[" << idx << "]" << std::endl;
		entry[pos+num].value = value[idx];
//		std::cout << "entry[" << pos+num << "].value: " << entry[pos+num].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
		//high_key = key[idx];
		//std::cout << "high_key: " << high_key << std::endl;
		cnt += num;
	    }
	    else{
		entry[pos+num].key = key[idx];
//		std::cout << "entry[" << pos+num << "].key: " << entry[pos+num].key << " ----- from key[" << idx << "]: " << key[idx] << std::endl;
		entry[pos+num].value = value[idx];
//		std::cout << "entry[" << pos+num << "].value: " << entry[pos+num].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
		cnt += num;
	    }
	}
	else{
	    entry[pos+num].key = key[idx];
//	    std::cout << "entry[" << pos+num << "].key: " << entry[pos+num].key << " ----- from key[" << idx << "]: " << key[idx] << std::endl;
	    entry[pos+num].value = value[idx];
//	    std::cout << "entry[" << pos+num << "].value: " << entry[pos+num].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	    cnt += num;
	    //high_key = entry[cnt-1].key;
	    //std::cout << "high_key: " << high_key << " ---- from entry[" << cnt-1 << "]" << std::endl;
	}
//	std::cout << "\n";
	return nullptr;
    }
    else{
//	std::cout << " split, batch_size: " << batch_size << std::endl;
	if(batch_size < pos){
	    int migrate_num = pos - batch_size;
//	    std::cout << "migrate: " << migrate_num << ", pos: " << pos << ", batch_size: " << batch_size << ", cnt: " << cnt << std::endl;
	    entry_t<Key_t, node_t*> migrate[migrate_num];
	    memcpy(migrate, &entry[batch_size], sizeof(entry_t<Key_t, node_t*>) * migrate_num);
//	    for(int i=batch_size; i<pos; i++){
//		std::cout << "copied entry[" << i << "].key:  " << entry[i].key << " for migration" << std::endl;
//		std::cout << "copied entry[" << i << "].value:  " << entry[i].value << " for migration" << std::endl;
//	    }
	    //cnt -= migrate_num;

	    entry_t<Key_t, node_t*> buf[move_num];
	    //auto buf = new entry_t<Key_t, node_t*>[move_num];
	    Key_t prev_high_key = high_key;

	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>) * move_num);
//	    for(int i=pos+1; i<pos+1+move_num; i++){
//		std::cout << "copied entry[" << i << "].key " << entry[i].key << std::endl;
//		std::cout << "copied entry[" << i << "].value " << entry[i].value << std::endl;
//	    }

	    cnt = batch_size;

	    int left_num = num + move_num + migrate_num;
	    int last_chunk = 0;
	    int remains = left_num % batch_size;
	    if(left_num / batch_size == 0){
		new_num = 1;
		last_chunk = remains;
	    }
	    else{
		if(remains == 0){
		    new_num = left_num / batch_size;
		    last_chunk = batch_size;
		}
		else{
		    if(remains < cardinality - batch_size){
			new_num = left_num / batch_size;
			last_chunk = batch_size + remains;
		    }
		    else{
			new_num = left_num / batch_size + 1;
			last_chunk = remains;
		    }
		}
	    }


	    auto new_nodes = new inode_t<Key_t>*[new_num];
	    for(int i=0; i<new_num; i++)
		new_nodes[i] = new inode_t<Key_t>(level);

	    auto old_sibling = sibling_ptr;
	    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	    int node_id = 0;
	    int migrate_from = 0;
	    int from = 0;

	    high_key = migrate[migrate_from].key;
//	    std::cout << "high_key: " << high_key << " ---- from migrate[" << migrate_from << "]" << std::endl;
	    for(; node_id<new_num-1; node_id++){
		new_nodes[node_id]->sibling_ptr = static_cast<node_t*>(new_nodes[node_id+1]);
		new_nodes[node_id]->batch_insert(migrate, migrate_from, migrate_num, key, value, idx, num, batch_size, buf, from, move_num, prev_high_key);
	    }
	    new_nodes[node_id]->sibling_ptr = old_sibling;
	    new_nodes[node_id]->batch_insert(migrate, migrate_from, migrate_num, key, value, idx, num, batch_size, buf, from, move_num, prev_high_key);

	    return new_nodes;
	}
	else{
	    entry_t<Key_t, node_t*> buf[move_num];
//	    auto buf = new entry_t<Key_t, node_t*>[move_num];
	    Key_t prev_high_key = high_key;

	    memcpy(buf, &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*move_num);
//	    for(int i=pos+1; i<pos+1+move_num; i++){
//		std::cout << "copied entry[" << i << "].key " << entry[i].key << std::endl;
//		std::cout << "copied entry[" << i << "].value " << entry[i].value << std::endl;
//	    }

	    for(int i=pos+1; i<batch_size && idx<num; i++, idx++){
		entry[i].key = key[idx];
//		std::cout << "entry[" << i << "].key : " << entry[i].key << " ----- from key[" << idx << "]: " << key[idx] << std::endl;
		entry[i].value = value[idx];
//		std::cout << "entry[" << i << "].value : " << entry[i].value << " ----- from value[" << idx << "]: " << value[idx] << std::endl;
	    }

	    cnt += (idx - move_num);
	    int total = num - idx + move_num;
	    int from = 0;
	    if(cnt < batch_size){
		memcpy(&entry[cnt], buf, sizeof(entry_t<Key_t, node_t*>) * (batch_size - cnt));
		int _from = from;
//		for(int i=cnt; i<batch_size; i++, _from++){
//		    std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << _from << "] " << std::endl;
//		    std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << _from << "] " << std::endl;
//		}
		from += (batch_size - cnt);
		cnt = batch_size;
	    }

	    if(idx < num){
		high_key = key[idx];
//		std::cout << "high_key: " << high_key << " ---- from key[" << idx << "]" << std::endl;
	    }
	    else{
		high_key = buf[from].key;
//		std::cout << "high_key: " << high_key << " ---- from buf[" << from << "]" << std::endl;
	    }


	    int left_num = num - idx + move_num - from;
	    int last_chunk = 0;
	    int remains = left_num % batch_size;
	    if(left_num / batch_size == 0){ 
		new_num = 1;
		last_chunk = remains;
	    }
	    else{
		if(remains == 0){
		    new_num = left_num / batch_size;
		    last_chunk = batch_size;
		}
		else{
		    if(remains < cardinality - batch_size){
			new_num = left_num / batch_size;
			last_chunk = batch_size + remains;
		    }
		    else{
			new_num = left_num / batch_size + 1;
			last_chunk =remains;
		    }
		}
	    }


	    //inode_t<Key_t>* new_nodes[new_num]; 
	    auto new_nodes = new inode_t<Key_t>*[new_num];
	    for(int i=0; i<new_num; i++)
		new_nodes[i] = new inode_t<Key_t>(level);

	    auto old_sibling = sibling_ptr;
	    sibling_ptr = static_cast<node_t*>(new_nodes[0]);

	    int node_id = 0;
	    for(; node_id<new_num-1; node_id++){
		new_nodes[node_id]->sibling_ptr = static_cast<node_t*>(new_nodes[node_id+1]);
		new_nodes[node_id]->batch_insert(key, value, idx, num, batch_size, buf, from, move_num, prev_high_key);
	    }
	    new_nodes[node_id]->sibling_ptr = old_sibling;
	    new_nodes[node_id]->batch_insert(key, value, idx, num, last_chunk, buf, from, move_num, prev_high_key);

	    return new_nodes;
	}
    }
}




// last batch insert
template <typename Key_t>
void inode_t<Key_t>::batch_insert(Key_t* key, node_t** value, int& idx, int num, entry_t<Key_t, node_t*>* buf, int buf_num, Key_t _high_key){
//    std::cout << "inode_t::" << __func__ << "1.5: inserting from " << idx << " out of " << num << " and buf size of " << buf_num << " at " << this << std::endl;
    leftmost_ptr = value[idx];
    for(; idx<num; cnt++){
	entry[cnt].key = key[idx++];
//	std::cout << "entry[" << cnt << "].key: " << entry[cnt].key << " ---- from key[" << idx-1 << "]: " << key[idx-1] << std::endl;
	entry[cnt].value = value[idx];
//	std::cout << "entry[" << cnt << "].value: " << entry[cnt].value << " ---- from value[" << idx << "]: " << value[idx] << std::endl;
    }

    if(buf_num == 0){
	high_key = entry[cnt-1].key;
//	std::cout << "high key: " << high_key << " ---- from entry[" << cnt-1 << "].key" << std::endl;
    }
    else{
	memcpy(&entry[cnt], buf, sizeof(entry_t<Key_t, node_t*>)*buf_num);
	high_key = _high_key;
//	std::cout << "high key: " << high_key << " ---- from prev high_key" << std::endl;
    }
//    std::cout << "\n";
}

template <typename Key_t>
void inode_t<Key_t>::batch_insert(entry_t<Key_t, node_t*>* buf, int num, Key_t _high_key){
//    std::cout << "inode_t::" << __func__ << "2: inserting " << num << " records at " << this << std::endl;
    if(leftmost_ptr == nullptr) std::cout << "leftmost_ptr is null!! inserting " <<num << std::endl;
    high_key = _high_key;
    memcpy(&entry[cnt], buf, sizeof(entry_t<Key_t, node_t*>)*num);
//    for(int i=cnt, j=0; i<cnt+num; i++, j++){
//	std::cout << "entry[" << i << "].key: " << entry[i].key << " ---- from buf[" << j << "]: " << buf[j].key << std::endl;
//	std::cout << "entry[" << i << "].value: " << entry[i].value << " ---- from buf[" << j << "]: " << buf[j].value << std::endl;
//    }
//    std::cout << "\n";
    cnt += num;
}


template <typename Key_t>
void inode_t<Key_t>::move_normal_insertion(int pos, int num, int move_num){
//    std::cout << "inode_t::" << __func__ << ": moving " << move_num << " records" << std::endl;
    memmove(&entry[pos+num+1], &entry[pos+1], sizeof(entry_t<Key_t, node_t*>)*move_num);
//    for(int i=pos+num+1; i<pos+num+1+move_num; i++){
//	std::cout << "moved entry[" << i << "].key: " << entry[i].key << std::endl;
//	std::cout << "moved entry[" << i << "].value: " << entry[i].value << std::endl;
//    }
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

template class inode_t<uint64_t>;
}
