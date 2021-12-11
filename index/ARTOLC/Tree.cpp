#include <assert.h>
#include <algorithm>
#include <functional>
#include "Tree.h"
#include "N.cpp"
#include "Epoche.cpp"
#include "Key.h"

#include <ctime>

#include <cstdio>
#include <map>

namespace ART_OLC {

    Tree::Tree(LoadKeyFunction loadKey) : root(new N256( nullptr, 0)), loadKey(loadKey) { }

    Tree::~Tree() {
        N::deleteChildren(root);
        N::deleteNode(root);
    }

    ThreadInfo Tree::getThreadInfo() {
        return ThreadInfo(this->epoche);
    }

    #ifdef BREAKDOWN
    void Tree::get_breakdown(uint64_t& _time_traversal, uint64_t& _time_abort, uint64_t& _time_latch, uint64_t& _time_node, uint64_t& _time_split){
	_time_traversal = time_traversal;
	_time_abort = time_abort;
	_time_latch = time_latch;
	_time_node = time_node;
	_time_split = time_split;
    }
    #endif


    void yield(int count) {
       if (count>3)
          sched_yield();
    }

    TID Tree::lookup(const Key &k, ThreadInfo &threadEpocheInfo) const {
        #ifdef BREAKDOWN
	uint64_t start, end;
	abort = false;
	#endif
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);
    restart:
	#ifdef BREAKDOWN
	start = _rdtsc();
	#endif
        bool needRestart = false;

        N *node;
        N *parentNode = nullptr;
        uint64_t v;
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;

        node = root;
        v = node->readLockOrRestart(needRestart);
        if (needRestart){
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    if(abort) time_abort += (end - start);
	    else time_traversal += (end - start);
	    abort = true;
	    #endif
	    goto restart;
	}

        while (true) {
            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
			#endif
			goto restart;
		    }
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    #endif
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match:
                    if (k.getKeyLen() <= level) {
			#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			#endif
                        return 0;
                    }
                    parentNode = node;
		    node = N::getChild(k[level], parentNode);
                    parentNode->checkOrRestart(v,needRestart);
                    if (needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			abort = true;
			#endif
			goto restart;
		    }

                    if (node == nullptr) {
			#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			#endif
                        return 0;
                    }

                    if (N::isLeaf(node)) {
                        parentNode->readUnlockOrRestart(v, needRestart);
                        if (needRestart){
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    if(abort) time_abort += (end - start);
			    else time_traversal += (end - start);
			    abort = true;
			    #endif
			    goto restart;
			}
			#ifdef BREAKDOWN
			end = _rdtsc();
			if(abort) time_abort += (end - start);
			else time_traversal += (end - start);
			start = _rdtsc();
			#endif
                        TID tid = N::getLeaf(node);
                        if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
			    #ifdef BREAKDOWN
			    auto ret = checkKey(tid, k);
			    end = _rdtsc();
			    time_node += (end - start);
			    return ret;
			    #else
                            return checkKey(tid, k);
			    #endif
                        }
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_node += (end - start);
			#endif
                        return tid;
                    }
                    level++;
            }
            uint64_t nv = node->readLockOrRestart(needRestart);
            if (needRestart){
	        #ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		abort = true;
	        #endif
		goto restart;
	    }

            parentNode->readUnlockOrRestart(v, needRestart);
            if (needRestart){
	        #ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		abort = true;
	        #endif
		goto restart;
	    }
            v = nv;
        }
    }

    bool Tree::lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[],
                                std::size_t resultSize, std::size_t &resultsFound, ThreadInfo &threadEpocheInfo) const {
        for (uint32_t i = 0; i < std::min(start.getKeyLen(), end.getKeyLen()); ++i) {
            if (start[i] > end[i]) {
                resultsFound = 0;
                return false;
            } else if (start[i] < end[i]) {
                break;
            }
        }
        EpocheGuard epocheGuard(threadEpocheInfo);
        TID toContinue = 0;
	bool isObsolete = false;
	#ifdef BREAKDOWN
	abort = false;
	uint64_t _start, _end;
	#endif

        std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy, &isObsolete](const N *node) {
	    #ifdef BREAKDOWN
	    uint64_t start, end;
	    start = _rdtsc();
	    #endif
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_node += (end - start);
		    #endif
                    return;
                }
                result[resultsFound] = N::getLeaf(node);
		#ifdef BREAKDOWN
		end = _rdtsc();
		time_node += (end - start);
		#endif
                resultsFound++;
            } 
	    else {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount, isObsolete);
		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		#endif
		if (isObsolete){
		    #ifdef BREAKDOWN
		    abort = true;
		    #endif
		    return;
		}

                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const N *n = std::get<1>(children[i]);
                    copy(n);
                    if (toContinue != 0) {
                        break;
                    }
                }
            }
        };

        std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findStart = [&copy, &start, &findStart, &toContinue, this, &isObsolete](
                N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
	    #ifdef BREAKDOWN
	    uint64_t _start, _end;
	    #endif
            if (N::isLeaf(node)) {
                copy(node);
                return;
            }

            uint64_t v;
            PCCompareResults prefixResult;
            {
                readAgain:
		#ifdef BREAKDOWN
		_start = _rdtsc();
		#endif
                bool needRestart = false;
                v = node->readLockOrRestart(needRestart, isObsolete);
		if (isObsolete){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
		    return;
		}
		else if (needRestart){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
		    goto readAgain;
		}

                prefixResult = checkPrefixCompare(node, start, 0, level, loadKey, needRestart);
                if (needRestart){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
		    goto readAgain;
		}

                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) {
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    readParentAgain:
		    #ifdef BREAKDOWN
		    abort = true; 
		    _start = _rdtsc();
		    #endif
		    needRestart = false;
                    vp = parentNode->readLockOrRestart(needRestart, isObsolete);
		    if (isObsolete){
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			#endif
			return;
		    }
		    else if (needRestart){
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			#endif
			goto readParentAgain;
		    }

                    node = N::getChild(nodeK, parentNode);

                    parentNode->readUnlockOrRestart(vp, needRestart);
                    if (needRestart){
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			#endif
			goto readParentAgain;
		    }

                    if (node == nullptr) {
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			#endif
                        return;
                    }
                    if (N::isLeaf(node)) {
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			#endif
                        copy(node);
                        return;
                    }
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    goto readAgain;
                }
                node->readUnlockOrRestart(v, needRestart);
                if (needRestart){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
		    goto readAgain;
		}
            }

            switch (prefixResult) {
                case PCCompareResults::Bigger:
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren(node, startLevel, 255, children, childrenCount, isObsolete);
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
		    if (isObsolete){
			#ifdef BREAKDOWN
			abort = true;
			#endif
			return;
		    }

                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);
                        if (k == startLevel) {
                            findStart(n, k, level + 1, node, v);
                        } else if (k > startLevel) {
                            copy(n);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Smaller:
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    break;
            }
        };

        std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findEnd = [&copy, &end, &toContinue, &findEnd, this, &isObsolete](
                N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
	    #ifdef BREAKDOWN
	    uint64_t _start, _end;
	    #endif
            if (N::isLeaf(node)) {
                return;
            }
            uint64_t v;
            PCCompareResults prefixResult;
            {
                readAgain:
		#ifdef BREAKDOWN
		_start = _rdtsc();
		#endif
                bool needRestart = false;
                v = node->readLockOrRestart(needRestart, isObsolete);
		if (isObsolete){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
		    return;
		}
		else if (needRestart){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
		    goto readAgain;
		}

                prefixResult = checkPrefixCompare(node, end, 255, level, loadKey, needRestart);
                if (needRestart){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
		    goto readAgain;
		}

                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) {
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    readParentAgain:
		    #ifdef BREAKDOWN
		    abort = true;
		    _start = _rdtsc();
		    #endif
                    vp = parentNode->readLockOrRestart(needRestart, isObsolete);
		    if (isObsolete){
		        #ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
		    	#endif
			return;
		    }
		    else if (needRestart){
		    	#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
		    	#endif
			goto readParentAgain;
		    }
                    node = N::getChild(nodeK, parentNode);

                    parentNode->readUnlockOrRestart(vp, needRestart);
                    if (needRestart){
		    	#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
		    	#endif
			goto readParentAgain;
		    }

                    if (node == nullptr) {
		    	#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
		    	#endif
                        return;
                    }
                    if (N::isLeaf(node)) {
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			#endif
                        return;
                    }
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    goto readAgain;
                }
                node->readUnlockOrRestart(v, needRestart);
                if (needRestart){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
		    goto readAgain;
		}
            }
            switch (prefixResult) {
                case PCCompareResults::Smaller:
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren(node, 0, endLevel, children, childrenCount, isObsolete);
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
		    if (isObsolete){
			#ifdef BREAKDOWN
			abort = true;
			#endif
			return;
		    }
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);
                        if (k == endLevel) {
                            findEnd(n, k, level + 1, node, v);
                        } else if (k < endLevel) {
                            copy(n);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Bigger:
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    break;
            }
        };


    restart:
	#ifdef BREAKDOWN
	_start = _rdtsc();
	#endif
        bool needRestart = false;
	isObsolete = false;

        resultsFound = 0;

        uint32_t level = 0;
        N *node = nullptr;
        N *nextNode = root;
        N *parentNode;
        uint64_t v = 0;
        uint64_t vp;

        while (true) {
            parentNode = node;
            vp = v;
            node = nextNode;
            PCEqualsResults prefixResult;
            v = node->readLockOrRestart(needRestart);
            if (needRestart){
		#ifdef BREAKDOWN
		_end = _rdtsc();
		if(abort) time_abort += (_end - _start);
		else time_traversal += (_end - _start);
		abort = true;
		#endif
		goto restart;
	    }
            prefixResult = checkPrefixEquals(node, level, start, end, loadKey, needRestart);
            if (needRestart){
		#ifdef BREAKDOWN
		_end = _rdtsc();
		if(abort) time_abort += (_end - _start);
		else time_traversal += (_end - _start);
		abort = true;
		#endif
		goto restart;
	    }
            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart){
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
		    goto restart;
		}
            }
            node->readUnlockOrRestart(v, needRestart);
            if (needRestart){
		#ifdef BREAKDOWN
		_end = _rdtsc();
		if(abort) time_abort += (_end - _start);
		else time_traversal += (_end - _start);
		abort = true;
		#endif
		goto restart;
	    }

            switch (prefixResult) {
                case PCEqualsResults::NoMatch: {
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    return false;
                }
                case PCEqualsResults::Contained: {
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    copy(node);
		    if (isObsolete){
			#ifdef BREAKDOWN
			abort = true;
			#endif
			goto restart;
		    }
		    #ifdef BREAKDOWN
		    _start = _rdtsc();
		    #endif
                    break;
                }
                case PCEqualsResults::BothMatch: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
                    if (startLevel != endLevel) {
                        std::tuple<uint8_t, N *> children[256];
                        uint32_t childrenCount = 0;
                        v = N::getChildren(node, startLevel, endLevel, children, childrenCount, isObsolete);
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			#endif
			if (isObsolete){
			    #ifdef BREAKDOWN
			    abort = true;
			    #endif
			    goto restart;
			}

                        for (uint32_t i = 0; i < childrenCount; ++i) {
                            const uint8_t k = std::get<0>(children[i]);
                            N *n = std::get<1>(children[i]);
                            if (k == startLevel) {
                                findStart(n, k, level + 1, node, v);
				if (isObsolete){
				    #ifdef BREAKDOWN
				    abort = true;
				    #endif
				    goto restart;
				}
                            } else if (k > startLevel && k < endLevel) {
                                copy(n);
				if (isObsolete){
				    #ifdef BREAKDOWN
				    abort = true;
				    #endif
				    goto restart;
				}
                            } else if (k == endLevel) {
                                findEnd(n, k, level + 1, node, v);
				if (isObsolete){
				    #ifdef BREAKDOWN
				    abort = true;
				    #endif
				    goto restart;
				}
                            }
                            if (toContinue) {
                                break;
                            }
                        }
			#ifdef BREAKDOWN
			_start = _rdtsc();
			#endif
                    } else {
                        nextNode = N::getChild(startLevel, node);
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart){
			    #ifdef BREAKDOWN
			    _end = _rdtsc();
			    if(abort) time_abort += (_end - _start);
			    else time_traversal += (_end - _start);
			    abort = true;
			    #endif
			    goto restart;
			}
                        level++;
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			_start = _rdtsc();
			#endif
                        continue;
                    }
                    break;
                }
            }
            break;
        }
        if (toContinue != 0) {
	    #ifdef BREAKDOWN
	    _end = _rdtsc();
	    if(abort) time_abort += (_end - _start);
	    else time_traversal += (_end - _start);
	    _start = _rdtsc();
	    #endif
            loadKey(toContinue, continueKey);
	    #ifdef BREAKDOWN
	    _end = _rdtsc();
	    time_node += (_end - _start);
	    #endif
            return true;
        }
	else {
	    #ifdef BREAKDOWN
	    _end = _rdtsc();
	    if(abort) time_abort += (_end - _start);
	    else time_traversal += (_end - _start);
	    #endif
            return false;
        }
    }


    TID Tree::checkKey(const TID tid, const Key &k) const {
        Key kt;
        this->loadKey(tid, kt);
        if (k == kt) {
            return tid;
        }
        return 0;
    }

    void Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo) {
        EpocheGuard epocheGuard(epocheInfo);
	#ifdef BREAKDOWN
	abort = false;
	uint64_t start, end;
	#endif
    restart:
        bool needRestart = false;
	#ifdef BREAKDOWN
	start = _rdtsc();
	#endif

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart){
		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		abort = true;
		#endif
		goto restart;
	    }

            uint32_t nextLevel = level;
            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                   this->loadKey, needRestart); // increases level
            if (needRestart){
		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		abort = true;
		#endif
		goto restart;
	    }

            switch (res) {
                case CheckPrefixPessimisticResult::NoMatch: {
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    start = _rdtsc();
		    #endif
                    parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                    if (needRestart){
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_latch += (end - start);
			abort = true;
			#endif
			goto restart;
		    }

                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart) {
                        parentNode->writeUnlock();
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
                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    auto newNode = new N4(node->getPrefix(), nextLevel - level);
                    // 2)  add node and (tid, *k) as children
                    newNode->insert(k[nextLevel], N::setLeaf(tid));
                    newNode->insert(nonMatchingKey, node);

                    // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                    N::change(parentNode, parentKey, newNode);

		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_split += (end - start);
		    start = _rdtsc();
		    #endif
                    parentNode->writeUnlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_latch += (end - start);
		    start= _rdtsc();
		    #endif

                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix,
                                    node->getPrefixLength() - ((nextLevel - level) + 1));
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_split += (end - end);
		    start = _rdtsc();
		    #endif

                    node->writeUnlock();
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_latch += (end - start);
		    #endif
                    return;
                }
                case CheckPrefixPessimisticResult::Match:
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    start = _rdtsc();
		    #endif
                    break;
            }
            level = nextLevel;
            nodeKey = k[level];
            nextNode = N::getChild(nodeKey, node);
            node->checkOrRestart(v,needRestart);
            if (needRestart){
		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		abort = true;
		#endif
		goto restart;
	    }

	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    if(abort) time_abort += (end - start);
	    else time_traversal += (end - start);
	    start = _rdtsc();
	    #endif
            if (nextNode == nullptr) {
                N::insertAndUnlock(node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid), needRestart, epocheInfo);
                if (needRestart){
		    #ifdef BREAKDOWN
		    abort = true;
		    #endif
		    goto restart;
		}
                return;
            }

            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(parentVersion, needRestart);
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
            if (N::isLeaf(nextNode)) {
                node->upgradeToWriteLockOrRestart(v, needRestart);
		#ifdef BREAKDOWN
		end = _rdtsc();
		time_latch += (end - start);
		start = _rdtsc();
		#endif
                if (needRestart){
		    #ifdef BREAKDOWN
		    abort = true;
		    #endif
		    goto restart;
		}
                Key key;
                loadKey(N::getLeaf(nextNode), key);
		#ifdef BREAKDOWN
		end = _rdtsc();
		time_node += (end - start);
		start = _rdtsc();
		#endif

		if (key == k) {
		  // upsert
		  N::change(node, k[level], N::setLeaf(tid));
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
		  return;
		}

                level++;
                uint32_t prefixLength = 0;
                while (key[level + prefixLength] == k[level + prefixLength]) {
                    prefixLength++;
                }

                auto n4 = new N4(&k[level], prefixLength);
                n4->insert(k[level + prefixLength], N::setLeaf(tid));
                n4->insert(key[level + prefixLength], nextNode);
                N::change(node, k[level - 1], n4);
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
                return;
            }
            level++;
            parentVersion = v;
        }
    }

    void Tree::remove(const Key &k, TID tid, ThreadInfo &threadInfo) {
        EpocheGuard epocheGuard(threadInfo);
        int restartCount = 0;
    restart:
        if (restartCount++)
           yield(restartCount);
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = k[level];
                    nextNode = N::getChild(nodeKey, node);

                    node->checkOrRestart(v, needRestart);
                    if (needRestart) goto restart;

                    if (nextNode == nullptr) {
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        return;
                    }
                    if (N::isLeaf(nextNode)) {
                        if (N::getLeaf(nextNode) != tid) {
                            return;
                        }
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && parentNode != nullptr) {
                            parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                            if (needRestart) goto restart;

                            node->upgradeToWriteLockOrRestart(v, needRestart);
                            if (needRestart) {
                                parentNode->writeUnlock();
                                goto restart;
                            }
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            } else {
                                secondNodeN->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    parentNode->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
                                parentNode->writeUnlock();

                                secondNodeN->addPrefixBefore(node, secondNodeK);
                                secondNodeN->writeUnlock();

                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            }
                        } else {
                            N::removeAndUnlock(node, v, k[level], parentNode, parentVersion, parentKey, needRestart, threadInfo);
                            if (needRestart) goto restart;
                        }
                        return;
                    }
                    level++;
                    parentVersion = v;
                }
            }
        }
    }

    inline typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
        if (n->hasPrefix()) {
            if (k.getKeyLen() <= level + n->getPrefixLength()) {
                return CheckPrefixResult::NoMatch;
            }
            for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
                if (n->getPrefix()[i] != k[level]) {
                    return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (n->getPrefixLength() > maxStoredPrefixLength) {
                level = level + (n->getPrefixLength() - maxStoredPrefixLength);
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        return CheckPrefixResult::Match;
    }

    typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                        uint8_t &nonMatchingKey,
                                                                        Prefix &nonMatchingPrefix,
                                                                        LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            uint32_t prevLevel = level;
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart){
			#ifdef BREAKDOWN
			abort = true;
			#endif
			return CheckPrefixPessimisticResult::Match;
		    }
                    loadKey(anyTID, kt);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey != k[level]) {
                    nonMatchingKey = curKey;
                    if (n->getPrefixLength() > maxStoredPrefixLength) {
                        if (i < maxStoredPrefixLength) {
                            auto anyTID = N::getAnyChildTid(n, needRestart);
                            if (needRestart){
				#ifdef BREAKDOWN
				abort = true;
				#endif
			        return CheckPrefixPessimisticResult::Match;
			    }
                            loadKey(anyTID, kt);
                        }
                        memcpy(nonMatchingPrefix, &kt[0] + level + 1, std::min((n->getPrefixLength() - (level - prevLevel) - 1),
                                                                           maxStoredPrefixLength));
                    } else {
                        memcpy(nonMatchingPrefix, n->getPrefix() + i + 1, n->getPrefixLength() - i - 1);
                    }
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    typename Tree::PCCompareResults Tree::checkPrefixCompare(const N *n, const Key &k, uint8_t fillKey, uint32_t &level,
                                                        LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart){
			#ifdef BREAKDOWN
			abort = true;
			#endif
			return PCCompareResults::Equal;
		    }
                    loadKey(anyTID, kt);
                }
                uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : fillKey;
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey < kLevel) {
                    return PCCompareResults::Smaller;
                } else if (curKey > kLevel) {
                    return PCCompareResults::Bigger;
                }
                ++level;
            }
        }
        return PCCompareResults::Equal;
    }

    typename Tree::PCEqualsResults Tree::checkPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end,
                                                      LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart){
			#ifdef BREAKDOWN
			abort = true;
			#endif
			return PCEqualsResults::BothMatch;
		    }
                    loadKey(anyTID, kt);
                }
                uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey > startLevel && curKey < endLevel) {
                    return PCEqualsResults::Contained;
                } else if (curKey < startLevel || curKey > endLevel) {
                    return PCEqualsResults::NoMatch;
                }
                ++level;
            }
        }
        return PCEqualsResults::BothMatch;
    }

    void Tree::footprint(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied){
	std::function<void(N*)> traverse = [&traverse, &meta, &structural_data_occupied, &structural_data_unoccupied, &key_data_occupied, &key_data_unoccupied, this](N* node){
	    if(N::isLeaf(node)){
		structural_data_occupied -= sizeof(N*);
		key_data_occupied += sizeof(N*);
		return;
	    }

	    N* children[256];
	    uint32_t childrenCount = 0;
	    N::getChildren(node, children, childrenCount);

	    meta += (sizeof(N) - sizeof(Prefix));
	    key_data_occupied += sizeof(Prefix);
	    structural_data_occupied += sizeof(N*)*childrenCount;

	    auto type = node->getType();
	    if(type == NTypes::N4){
		key_data_occupied += sizeof(uint8_t)*childrenCount;
		auto invalid_num = 4 - childrenCount;
		key_data_unoccupied += sizeof(uint8_t)*invalid_num;
		structural_data_unoccupied += sizeof(N*)*invalid_num;
	    }
	    else if(type == NTypes::N16){
		key_data_occupied += sizeof(uint8_t)*childrenCount;
		auto invalid_num = 16 - childrenCount;
		key_data_unoccupied += sizeof(uint8_t)*invalid_num;
		structural_data_unoccupied += sizeof(N*)*invalid_num;
	    }
	    else if(type == NTypes::N48){
		key_data_occupied += sizeof(uint8_t)*childrenCount;
		auto invalid_key = 256 - childrenCount;
		auto invalid_ptr = 48 - childrenCount;
		key_data_unoccupied += sizeof(uint8_t)*invalid_key;
		structural_data_unoccupied += sizeof(N*)*invalid_ptr;
	    }
	    else if(type == NTypes::N256){
		auto invalid_num = 256 - childrenCount;
		structural_data_unoccupied += sizeof(N*)*invalid_num;
	    }
	    else{
		std::cout << __func__ << " invalid NType (" << (int)type << ")" << std::endl;
		exit(0);
	    }

	    for(uint32_t i=0; i<childrenCount; i++){
		traverse(children[i]);
	    }
	};
	
	traverse(root);
    }

    void Tree::find_depth(){
	std::map<uint64_t, uint64_t> depths;
	std::function<void(N*, uint64_t)> traverse = [&traverse, &depths, this](N* node, uint64_t cur_depth){
	    if(N::isLeaf(node)){
		depths[cur_depth]++;
	    }
	    else{
		N* children[256];
		uint32_t childrenCount = 0;
		N::getChildren(node, children, childrenCount);

		for(auto i=0; i<childrenCount; i++)
		    traverse(children[i], cur_depth+1);
	    }
	};

	traverse(root, 1);
	uint64_t leaf_cnt = 0;
	uint64_t leaf_depths = 0;
	uint64_t max_depth = 0;
	for(auto it: depths){
	    leaf_cnt += it.second;
	    leaf_depths += (it.first * it.second);
	    if(max_depth < it.first)
		max_depth = it.first;
	}

	std::cout << "Total number of nodes: \t" << leaf_cnt << std::endl;
	std::cout << "Total depth: \t" << leaf_depths << std::endl;
	std::cout << "Max depth: \t" << max_depth << std::endl;
	std::cout << "Average depth: \t" << (double)leaf_depths / leaf_cnt << std::endl;
    }


}
