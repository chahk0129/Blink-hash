/*
 * BTreeOLC_child_layout.h - This file contains a modified version that
 *                           uses the key-value pair layout
 * 
 * We use this to test whether child node layout will affect performance
 */


#pragma once

#include <cassert>
#include <cstring>
#include <atomic>
#include <immintrin.h>
#include <sched.h>
#include <functional>
// std::pair
#include <utility>

#define LINEAR_SEARCH
#define CAS(_p, _u, _v) (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

#ifdef URL_KEYS
#define pageSize (4096)
#elif defined STRING_KEY
#define pageSize (1024)
#else
#define pageSize (512)
#endif

namespace btreeolc {

#ifdef BREAKDOWN
static thread_local uint64_t time_traversal;
static thread_local uint64_t time_abort;
static thread_local uint64_t time_latch;
static thread_local uint64_t time_node;
static thread_local uint64_t time_split;
static thread_local bool abort;
#endif

    enum class PageType : uint8_t { BTreeInner=1, BTreeLeaf=2 };

    struct OptLock {
	std::atomic<uint64_t> typeVersionLockObsolete{0};
	//std::atomic<uint64_t> typeVersionLockObsolete{0b100};

	bool isLocked(uint64_t version) {
	    return ((version & 0b10) == 0b10);
	}

	uint64_t readLockOrRestart(bool &needRestart) {
	    uint64_t version;
	    version = typeVersionLockObsolete.load();
	    if (isLocked(version) || isObsolete(version)) {
		_mm_pause();
		needRestart = true;
	    }
	    return version;
	}

	#ifdef UPDATE_LOCK
	void writeLockOrRestart(bool &needRestart) {
	    uint64_t version = typeVersionLockObsolete.load();
	    if (isLocked(version) || isObsolete(version) || ((version >> 32) > 0)){
		_mm_pause();
		needRestart = true;
		return;
	    }

	    if(typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
		version = version + 0b10;
	    }
	    else{
		_mm_pause();
		needRestart = true;
	    }
	}

	void upgradeToWriteLockOrRestart(uint64_t version, bool &needRestart) {
	    uint64_t _version = typeVersionLockObsolete.load();
	    if(((_version & (~0u)) != (version & (~0u))) || ((_version >> 32) > 0)){
		_mm_pause();
		needRestart = true;
		return;
	    }

	    if(!typeVersionLockObsolete.compare_exchange_strong(_version, _version + 0b10)){
		_mm_pause();
		needRestart = true;
	    }
	}

	void upgradeToUpdateLockOrRestart(uint64_t version, bool& needRestart){
	    uint64_t _version = typeVersionLockObsolete.load();
	    if((_version & (~0u)) != (version & (~0u))){
		_mm_pause();
		needRestart = true;
		return;
	    }

	    if(!typeVersionLockObsolete.compare_exchange_strong(_version, _version + ((uint64_t)1 << 32))){
		_mm_pause();
		needRestart = true;
	    }
	}

	void writeUnlock() {
	    uint64_t version = typeVersionLockObsolete.load();
	    if((version & (~0u)) == UINT32_MAX-1)
		typeVersionLockObsolete.store(0, std::memory_order_relaxed);
	    else
		typeVersionLockObsolete.fetch_add(0b10);
	}

	void writeUnlockObsolete() {
	    uint64_t version = typeVersionLockObsolete.load();
	    if((version & (~0u)) == UINT32_MAX-1)
		typeVersionLockObsolete.store(1, std::memory_order_relaxed);
	    else
		typeVersionLockObsolete.fetch_add(0b11);
	}

	void updateUnlock() {
	    typeVersionLockObsolete.fetch_sub(((uint64_t)1) << 32);
	}

	/*
	void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
	    needRestart = ((startRead & (~0u)) != (typeVersionLockObsolete.load() & (~0u));
	}*/

	uint64_t getVersion(bool &needRestart){
	    uint64_t version = typeVersionLockObsolete.load();
	    if(isLocked(version) || isObsolete(version)){
	        _mm_pause();
		needRestart = true;
	    }
	    return version;
	}

	#else
	void writeLockOrRestart(bool &needRestart) {
	    uint64_t version;
	    version = readLockOrRestart(needRestart);
	    if (needRestart) return;

	    upgradeToWriteLockOrRestart(version, needRestart);
	    if (needRestart) return;
	}

	void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
	    if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
		version = version + 0b10;
	    } else {
		_mm_pause();
		needRestart = true;
	    }
	}

	void writeUnlock() {
	    typeVersionLockObsolete.fetch_add(0b10);
	}

	void writeUnlockObsolete() {
	    typeVersionLockObsolete.fetch_add(0b11);
	}

	#endif

	void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
	    needRestart = (startRead != typeVersionLockObsolete.load());
	}

	bool isObsolete(uint64_t version) {
	    return (version & 1) == 1;
	}

	void checkOrRestart(uint64_t startRead, bool &needRestart) const {
	    readUnlockOrRestart(startRead, needRestart);
	}



    };

    struct NodeBase : public OptLock{
	PageType type;
	uint16_t count;
    };

    struct BTreeLeafBase : public NodeBase {
	static const PageType typeMarker=PageType::BTreeLeaf;
    };

    template<class Key,class Payload>
	struct BTreeLeaf : public BTreeLeafBase {
	    // This is the element type of the leaf node
	    using KeyValueType = std::pair<Key, Payload>;
	    static const uint64_t maxEntries=(pageSize-sizeof(NodeBase) - sizeof(BTreeLeaf*))/(sizeof(KeyValueType));

	    BTreeLeaf* sibling_ptr;
	    // This is the array that we perform search on
	    KeyValueType data[maxEntries];


	    BTreeLeaf() {
		count=0;
		type=typeMarker;
		sibling_ptr = nullptr;
	    }

	    BTreeLeaf(BTreeLeaf* _sibling_ptr){
		count = 0;
		type = typeMarker;
		sibling_ptr = _sibling_ptr;
	    }

	    bool isFull() { return count==maxEntries; };

	    unsigned lowerBound(Key k) {
		#ifdef LINEAR_SEARCH
		return linear_search(k);
		#else
		return binary_search(k);
		#endif
	    }

	    unsigned binary_search(Key k){
		unsigned lower=0;
		unsigned upper=count;
		do {
		    unsigned mid=((upper-lower)/2)+lower;
		    // This is the key at the pivot position
		    const Key &middle_key = data[mid].first;

		    if (k<middle_key) {
			upper=mid;
		    } else if (k>middle_key) {
			lower=mid+1;
		    } else {
			return mid;
		    }
		} while (lower<upper);
		return lower;
	    }
	    
	    unsigned linear_search(Key k){
		for(unsigned i=0; i<count; i++){
		    if(k <= data[i].first)
			return i;
		}
		return count;
	    }


	    void insert(Key k,Payload p) {
		assert(count<maxEntries);
		if (count) {
		    unsigned pos=lowerBound(k);
		    if ((pos<count) && (data[pos].first==k)) {
			// Upsert
			data[pos].second = p;
			return;
		    }
		    memmove(data+pos+1,data+pos,sizeof(KeyValueType)*(count-pos));
		    //memmove(payloads+pos+1,payloads+pos,sizeof(Payload)*(count-pos));
		    data[pos].first=k;
		    data[pos].second=p;
		} else {
		    data[0].first=k;
		    data[0].second=p;
		}
		count++;
	    }

	    BTreeLeaf* split(Key& sep) {
		BTreeLeaf* newLeaf = new BTreeLeaf(sibling_ptr);
		newLeaf->count = count-(count/2);
		count = count-newLeaf->count;
		memcpy(newLeaf->data, data+count, sizeof(KeyValueType)*newLeaf->count);
		//memcpy(newLeaf->payloads, payloads+count, sizeof(Payload)*newLeaf->count);
		sep = data[count-1].first;
		sibling_ptr = newLeaf;
		return newLeaf;
	    }
	};

    struct BTreeInnerBase : public NodeBase {
	static const PageType typeMarker=PageType::BTreeInner;
    };

    template<class Key>
	struct BTreeInner : public BTreeInnerBase {
	    using KeyValueType = std::pair<Key, NodeBase*>;
	    static const uint64_t maxEntries=(pageSize-sizeof(NodeBase))/(sizeof(KeyValueType));
	    KeyValueType data[maxEntries];



	    BTreeInner() {
		count=0;
		type=typeMarker;
	    }


	    bool isFull() { return count==(maxEntries-1); };

	    unsigned lowerBound(Key k) {
		#ifdef LINEAR_SEARCH
		return linear_search(k);
		#else
		return binary_search(k);
		#endif
	    }

	    unsigned binary_search(Key k){
		unsigned lower = 0;
		unsigned upper = count;
		do{
		    unsigned mid = ((upper-lower) / 2) + lower;
		    if(k < data[mid].first){
			upper = mid;
		    }
		    else if(k > data[mid].first){
			lower = mid+1;
		    }
		    else{
			return mid;
		    }
		}while(lower < upper);
		return lower;
	    }

	    unsigned linear_search(Key k){
		for(int i=0; i<count; i++){
		    if(k <= data[i].first){
			return i;
		    }
		}
		return count;
	    }

	    BTreeInner* split(Key& sep) {
		BTreeInner* newInner=new BTreeInner();
		newInner->count=count-(count/2);
		count=count-newInner->count-1;
		sep=data[count].first;
		memcpy(newInner->data,data+count+1,sizeof(KeyValueType)*(newInner->count+1));
		//memcpy(newInner->keys,keys+count+1,sizeof(Key)*(newInner->count+1));
		//memcpy(newInner->children,children+count+1,sizeof(NodeBase*)*(newInner->count+1));
		return newInner;
	    }

	    void insert(Key k,NodeBase* child) {
		assert(count<maxEntries-1);
		unsigned pos=lowerBound(k);
		memmove(data+pos+1,data+pos,sizeof(KeyValueType)*(count-pos+1));
		//memmove(keys+pos+1,keys+pos,sizeof(Key)*(count-pos+1));
		//memmove(children+pos+1,children+pos,sizeof(NodeBase*)*(count-pos+1));
		data[pos].first = k;
		data[pos].second = child;
		//keys[pos]=k;
		//children[pos]=child;
		std::swap(data[pos].second,data[pos+1].second);
		//std::swap(children[pos],children[pos+1]);
		count++;
	    }

	};


    template<class Key,class Value>
	struct BTree {
	    std::atomic<NodeBase*> root;

	    inline uint64_t _rdtsc(){
		uint32_t lo, hi;
		asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
		return (((uint64_t)hi << 32) | lo);
	    }

	    BTree() {
		root = new BTreeLeaf<Key,Value>();
	    }

	    #ifdef BREAKDOWN
	    void get_breakdown(uint64_t& _time_traversal, uint64_t& _time_abort, uint64_t& _time_latch, uint64_t& _time_node, uint64_t& _time_split){
		_time_traversal = time_traversal;
		_time_abort = time_abort;
		_time_latch = time_latch;
		_time_node = time_node;
		_time_split = time_split;
	    }
	    #endif

	    void makeRoot(Key k,NodeBase* leftChild,NodeBase* rightChild) {
		auto inner = new BTreeInner<Key>();
		inner->count = 1;
		inner->data[0].first = k;
		inner->data[0].second = leftChild;
		inner->data[1].second = rightChild;
		root = inner;
	    }

	    void backoff (int count){
		#ifdef BACKOFF
		if(count > 20){
		    for(int i=0; i<count*5; i++){
                    asm volatile("pause\n" : : : "memory");
                    std::atomic_thread_fence(std::memory_order_seq_cst);
		    }
		}
		else{
		    if(count > 5)
			sched_yield();
		}
	        #endif
	    }

	    void insert(Key k, Value v) {
		int restartCount = 0;
		#ifdef BREAKDOWN
		uint64_t start, end;
		abort = false;
		#endif
		restart:
		bool needRestart = false;
		#ifdef BREAKDOWN
		start = _rdtsc();
		#endif
		// Current node
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node!=root)){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
		    #endif
		    goto restart;
		}

		// Parent of current node
		BTreeInner<Key>* parent = nullptr;
		uint64_t versionParent;

		while (node->type==PageType::BTreeInner) {
		    auto inner = static_cast<BTreeInner<Key>*>(node);

		    // Split eagerly if full
		    if (inner->isFull()) {
		    	#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			start = _rdtsc();
		    	#endif
			// Lock
			if (parent) {
			    parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
			    if (needRestart) {
		    		#ifdef BREAKDOWN
				end = _rdtsc();
				time_latch += (end - start);
				abort = true;
		    		#endif
				goto restart;
			    }
			}
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if (needRestart) {
			    if (parent){
				parent->writeUnlock();
			    }
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    time_latch += (end - start);
			    abort = true;
			    #endif
			    goto restart;
			}
			if (!parent && (node != root)) { // there's a new parent
			    node->writeUnlock();
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    time_latch += (end - start);
			    abort = true;
			    #endif
			    goto restart;
			}
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_latch += (end - start);
			start = _rdtsc();
			#endif
			// Split
			Key sep; BTreeInner<Key>* newInner = inner->split(sep);
			if (parent)
			    parent->insert(sep,newInner);
			else
			    makeRoot(sep,inner,newInner);
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_split += (end - start);
			start = _rdtsc();
			#endif
			// Unlock and restart
			node->writeUnlock();
			if (parent)
			    parent->writeUnlock();
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_latch += (end - start);
			#endif
			goto restart;
		    }

		    if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if (needRestart){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    if(abort) time_abort += (end - start);
			    else time_traversal += (end - start);
			    abort = true;
			    #endif
			    goto restart;
			}
		    }

		    parent = inner;
		    versionParent = versionNode;

		    node = inner->data[inner->lowerBound(k)].second;
		    //node = inner->children[inner->lowerBound(k)];
		    inner->checkOrRestart(versionNode, needRestart);
		    if (needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		    versionNode = node->readLockOrRestart(needRestart);
		    if (needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		}

		auto leaf = static_cast<BTreeLeaf<Key,Value>*>(node);

		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		start = _rdtsc();
		#endif
		// Split leaf if full
		if (leaf->count==leaf->maxEntries) {
		    // Lock
		    if (parent) {
			parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
			if (needRestart){ 
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    time_latch += (end - start);
			    abort = true;
			    #endif
			    goto restart;
			}
		    }
		    node->upgradeToWriteLockOrRestart(versionNode, needRestart);
		    if (needRestart) {
			if (parent) parent->writeUnlock();
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_latch += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		    if (!parent && (node != root)) { // there's a new parent
			node->writeUnlock();
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_latch += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_latch += (end - start);
		    start = _rdtsc();
	 	    #endif
		    // Split
		    Key sep; BTreeLeaf<Key,Value>* newLeaf = leaf->split(sep);
		    if (parent)
			parent->insert(sep, newLeaf);
		    else
			makeRoot(sep, leaf, newLeaf);
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_split += (end - start);
		    start = _rdtsc();
	 	    #endif
		    // Unlock and restart
		    node->writeUnlock();
		    if (parent)
			parent->writeUnlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_latch += (end - start);
	 	    #endif
		    goto restart;
		} else {
		    // only lock leaf node
		    node->upgradeToWriteLockOrRestart(versionNode, needRestart);
		    if (needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_latch += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		    if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if (needRestart) {
			    node->writeUnlock();
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    time_latch += (end - start);
			    abort = true;
			    #endif
			    goto restart;
			}
		    }
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_latch += (end - start);
		    start = _rdtsc();
	  	    #endif
		    leaf->insert(k, v);
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_node += (end - start);
		    start = _rdtsc();
	  	    #endif
		    node->writeUnlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_latch += (end - start);
	  	    #endif
		    return; // success
		}
	    }


	    bool update(Key k, Value v) {
		int restartCount = -1;
		#ifdef BREAKDOWN
		uint64_t start, end;
		abort = false;
		#endif
		restart:
		#ifdef BREAKDOWN
		start = _rdtsc();
		#endif
		backoff(restartCount++);

		bool needRestart = false;

		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node!=root)){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
		    #endif
		    goto restart;
		}

		// Parent of current node
		BTreeInner<Key>* parent = nullptr;
		uint64_t versionParent;

		while (node->type==PageType::BTreeInner) {
		    auto inner = static_cast<BTreeInner<Key>*>(node);

		    if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if (needRestart){
		    	    #ifdef BREAKDOWN
			    end = _rdtsc();
			    if(abort) time_abort += (end - start);
			    else time_traversal += (end - start);
			    abort = true;
		    	    #endif
			    goto restart;
			}
		    }

		    parent = inner;
		    versionParent = versionNode;

		    node = inner->data[inner->lowerBound(k)].second;
		    //node = inner->children[inner->lowerBound(k)];
		    inner->checkOrRestart(versionNode, needRestart);
		    if (needRestart){
		    	#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
		    	#endif
			goto restart;
		    }
		    versionNode = node->readLockOrRestart(needRestart);
		    if (needRestart){
		    	#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
		    	#endif
			goto restart;
		    }
		}
		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		start = _rdtsc();
		#endif

		BTreeLeaf<Key,Value>* leaf = static_cast<BTreeLeaf<Key,Value>*>(node);
		#ifdef UPDATE_LOCK
		leaf->upgradeToUpdateLockOrRestart(versionNode, needRestart);
		#else
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		#endif
		#ifdef BREAKDOWN
		end = _rdtsc();
		time_latch += (end - start);
		start = _rdtsc(); 
		#endif
		if(needRestart){
		    #ifdef BREAKDOWN
		    abort = true;
		    #endif
		    goto restart;
		}

		unsigned pos = leaf->lowerBound(k);
		bool success = false;
		if ((pos<leaf->count) && (leaf->data[pos].first==k)) {
		    #ifdef UPDATE_LOCK
		    auto _v = leaf->data[pos].second;
		    while(!CAS(&leaf->data[pos].second, &_v, v)){
			_mm_pause();
			_v = leaf->data[pos].second;
		    }
		    #else
		    leaf->data[pos].second = v;
		    #endif
		    success = true;
		}

		#ifdef BREAKDOWN
		end = _rdtsc();
		time_node += (end - start);
		start = _rdtsc();
		#endif

		#ifdef UPDATE_LOCK
		leaf->updateUnlock();
		#else
		leaf->writeUnlock();
		#endif

		#ifdef BREAKDOWN
		end = _rdtsc();
		time_latch += (end - start);
		start = _rdtsc();
		#endif
		return success;

	    }

	    bool remove(Key k){
		restart:
		bool needRestart = false;
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root)) goto restart;

		BTreeInner<Key>* parent = nullptr;
		uint64_t versionParent;

		while (node->type == PageType::BTreeInner) {
		    auto inner = static_cast<BTreeInner<Key>*>(node);
		    if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if(needRestart) goto restart;
		    }
		}
		return false;
	    }

	    bool lookup(Key k, Value& result) {
		#ifdef BREAKDOWN
		uint64_t start, end;
		abort = false;
		#endif
		restart:
		#ifdef BREAKDOWN
		start = _rdtsc();
		#endif
		bool needRestart = false;

		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node!=root)){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
		    #endif
		    goto restart;
		}

		// Parent of current node
		BTreeInner<Key>* parent = nullptr;
		uint64_t versionParent;

		while (node->type==PageType::BTreeInner) {
		    auto inner = static_cast<BTreeInner<Key>*>(node);

		    if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if (needRestart) {
		    	    #ifdef BREAKDOWN
			    end = _rdtsc();
			    if(abort) time_abort += (end - start);
			    else time_traversal += (end - start);
			    abort = true;
		    	    #endif
			    goto restart;
			}
		    }

		    parent = inner;
		    versionParent = versionNode;

		    node = inner->data[inner->lowerBound(k)].second;
		    //node = inner->children[inner->lowerBound(k)];
		    inner->checkOrRestart(versionNode, needRestart);
		    if (needRestart){
		    	#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
		    	#endif
			goto restart;
		    }
		    versionNode = node->readLockOrRestart(needRestart);
		    if (needRestart){
		    	#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
		    	#endif
			goto restart;
		    }
		}

		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		start = _rdtsc();
		#endif

		BTreeLeaf<Key,Value>* leaf = static_cast<BTreeLeaf<Key,Value>*>(node);
		unsigned pos = leaf->lowerBound(k);
		bool success = false;
		if ((pos<leaf->count) && (leaf->data[pos].first==k)) {
		    success = true;
		    result = leaf->data[pos].second;
		}

		if (parent) {
		    parent->readUnlockOrRestart(versionParent, needRestart);
		    if (needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_node += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		}

		#ifdef UPDATE_LOCK
		auto _versionNode = node->getVersion(needRestart);
		if(needRestart || ((versionNode & (~0u)) != (_versionNode & (~0u)))){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_node += (end - start);
		    abort = true;
		    #endif
		    goto restart;
		}
		#else
		node->readUnlockOrRestart(versionNode, needRestart);
		if (needRestart){
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_node += (end - start);
		    abort = true;
		    #endif
		    goto restart;
		}
		#endif

		#ifdef BREAKDOWN
		end = _rdtsc();
		time_node += (end - start);
		#endif
		return success;
	    }

	    uint64_t scan(Key k, int range, Value* output) {
		#ifdef BREAKDOWN
		uint64_t start, end;
		abort = false;
		#endif
		restart:
		#ifdef BREAKDOWN
		start = _rdtsc();
		#endif
		bool needRestart = false;

		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node!=root)) {
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
		    #endif
		    goto restart;
		}

		// Parent of current node
		BTreeInner<Key>* parent = nullptr;
		uint64_t versionParent;

		while (node->type==PageType::BTreeInner) {
		    auto inner = static_cast<BTreeInner<Key>*>(node);

		    if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if (needRestart){
		    	    #ifdef BREAKDOWN
			    end = _rdtsc();
			    if(abort) time_abort += (end - start);
			    else time_traversal += (end - start);
			    abort = true;
		    	    #endif
			    goto restart;
			}
		    }

		    parent = inner;
		    versionParent = versionNode;

		    node = inner->data[inner->lowerBound(k)].second;
		    inner->checkOrRestart(versionNode, needRestart);
		    if (needRestart){
		    	#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
		    	#endif
			goto restart;
		    }
		    versionNode = node->readLockOrRestart(needRestart);
		    if (needRestart){ 
		    	#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
		    	#endif
			goto restart;
		    }
		}

		BTreeLeaf<Key,Value>* leaf = static_cast<BTreeLeaf<Key,Value>*>(node);

		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		start = _rdtsc();
		#endif

		int count = 0;
		unsigned pos = leaf->lowerBound(k);
		while(count < range){
		    for(unsigned i=pos; i<leaf->count; i++){
			output[count++] = leaf->data[i].second;
			if(count == range) break;
		    }

		    auto sibling = leaf->sibling_ptr;
		    if((count == range) || !sibling){
			#ifdef UPDATE_LOCK
			auto _versionNode = leaf->getVersion(needRestart);
			if(needRestart || ((versionNode & (~0u)) != (_versionNode & (~0u)))){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    time_node += (end - start);
			    abort = true;
			    #endif
			    goto restart;
			}
			#else
			leaf->readUnlockOrRestart(versionNode, needRestart);
			if(needRestart){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    time_node += (end - start);
			    abort = true;
			    #endif
			    goto restart;
			}
			#endif
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_node += (end - start);
			#endif
			return count;
		    }

		    auto versionSibling = sibling->readLockOrRestart(needRestart);
		    if(needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_node += (end - start);
			abort = true;
			#endif
			goto restart;
		    }

		    #ifdef UPDATE_LOCK
		    auto _versionNode = leaf->getVersion(needRestart);
		    if(needRestart || ((versionNode & (~0u)) != (_versionNode & (~0u)))){
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_node += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		    #else
		    leaf->readUnlockOrRestart(versionNode, needRestart);
		    if(needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_node += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		    #endif

		    leaf = sibling;
		    versionNode = versionSibling;
		    pos = 0;
		}
		
		#ifdef BREAKDOWN
		end = _rdtsc();
		time_node += (end - start);
		#endif
		return count;
	    }

	    void footprint(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
		std::function<void(NodeBase*)> func = [&meta, &structural_data_occupied, &structural_data_unoccupied, &key_data_occupied, &key_data_unoccupied, &func](NodeBase* node){
		    if(node->type == PageType::BTreeInner){
			meta += sizeof(BTreeInnerBase);
			auto empty = BTreeInner<Key>::maxEntries - node->count;
			structural_data_occupied += sizeof(std::pair<Key,NodeBase*>)*node->count + sizeof(NodeBase*);
			structural_data_unoccupied += empty*sizeof(std::pair<Key,NodeBase*>);
			auto inner = static_cast<BTreeInner<Key>*>(node);
			for(int i=0; i<inner->count+1; i++){
			    func(inner->data[i].second);
			}
		    }
		    else{
			meta += sizeof(BTreeLeafBase) + sizeof(NodeBase*);
			auto empty = BTreeLeaf<Key,Value>::maxEntries - node->count;
			key_data_occupied += sizeof(std::pair<Key,Value>)*node->count;
			key_data_unoccupied += empty*sizeof(std::pair<Key,Value>);
		    }
		};

		NodeBase* node = root;
		func(node);
	    }

	    int find_depth(){
		NodeBase* node = root;
		int depth = 0;
		while(node->type == PageType::BTreeInner){
		    depth++;
		    auto inner = static_cast<BTreeInner<Key>*>(node);
		    node = inner->data[0].second;
		}
		depth++;
		return depth;
	    }

	};

}
