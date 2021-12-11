#include <assert.h>
#include <algorithm>
#include <functional>
#include "Tree.h"
#include "N.cpp"
#include "Epoche.cpp"

namespace ART_ROWEX {

    Tree::Tree(LoadKeyFunction loadKey) : root(new N256(0, {})), loadKey(loadKey) { } 
    
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

    TID Tree::lookup(const Key &k, ThreadInfo &threadEpocheInfo) const {
	#ifdef BREAKDOWN
	uint64_t start, end;
	abort = false;
	start = _rdtsc();
	#endif
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);
        N *node = root;
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;


        while (true) {
            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_traversal += (end - start);
		    #endif
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match: {
                    if (k.getKeyLen() <= level) {
		        #ifdef BREAKDOWN
			end = _rdtsc();
			time_traversal += (end - start);
		        #endif
                        return 0;
                    }
                    node = N::getChild(k[level], node);
                    if (node == nullptr) {
		        #ifdef BREAKDOWN
			end = _rdtsc();
			time_traversal += (end - start);
		        #endif
                        return 0;
                    }
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_traversal += (end - start);
		    start = _rdtsc();
		    #endif
                    if (N::isLeaf(node)) {
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
			else {
			    #ifdef BREAKDOWN
			    end = _rdtsc();
			    time_node += (end - start);
			    #endif
                            return tid;
                        }
                    }
                }
            }
            level++;
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
        bool restart;
	#ifdef BREAKDOWN
	uint64_t _start, _end;
	abort = false;
	_start = _rdtsc();
	#endif

        std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy](const N *node) {
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
                N::getChildren(node, 0u, 255u, children, childrenCount);
		#ifdef BREAKDOWN
		end = _rdtsc();
		if(abort) time_abort += (end - start);
		else time_traversal += (end - start);
		#endif
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const N *n = std::get<1>(children[i]);
                    copy(n);
                    if (toContinue != 0) {
                        break;
                    }
                }
            }
        };
        std::function<void(const N *, uint32_t)> findStart = [&copy, &start, &findStart, &toContinue, &restart, this](
                const N *node, uint32_t level) {
	    #ifdef BREAKDOWN
	    uint64_t _start, _end;
	    #endif
            if (N::isLeaf(node)) {
                copy(node);
                return;
            }
	    #ifdef BREAKDOWN
	    _start = _rdtsc();
	    #endif
            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level, loadKey);
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
                    N::getChildren(node, startLevel, 255, children, childrenCount);
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        const N *n = std::get<1>(children[i]);
                        if (k == startLevel) {
                            findStart(n, level + 1);
                        } else if (k > startLevel) {
                            copy(n);
                        }
			#ifdef BREAKDOWN
			if (toContinue != 0) break;
			if (restart){ abort = true; break; }
			#else
                        if (toContinue != 0 || restart) {
                            break;
                        }
			#endif
                    }
                    break;
                }
                case PCCompareResults::SkippedLevel:{
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
                    restart = true;
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
        std::function<void(const N *, uint32_t)> findEnd = [&copy, &end, &toContinue, &restart, &findEnd, this](
                const N *node, uint32_t level) {
	    #ifdef BREAKDOWN
	    uint64_t _start, _end;
	    #endif
            if (N::isLeaf(node)) {
                return;
            }
	    #ifdef BREAKDOWN
	    _start = _rdtsc();
	    #endif
            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, end, level, loadKey);

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
                    N::getChildren(node, 0, endLevel, children, childrenCount);
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    #endif
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        const N *n = std::get<1>(children[i]);
                        if (k == endLevel) {
                            findEnd(n, level + 1);
                        } else if (k < endLevel) {
                            copy(n);
                        }
			#ifdef BREAKDOWN
			if (toContinue != 0) break;
			if (restart){ abort = true; break; }
			#else
                        if (toContinue != 0 || restart) {
                            break;
                        }
			#endif
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
                case PCCompareResults::SkippedLevel:
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
                    restart = true;
                    break;
            }
        };

        restart:
        restart = false;
        resultsFound = 0;
	#ifdef BREAKDOWN
	_start = _rdtsc();
	#endif

        uint32_t level = 0;
        N *node = nullptr;
        N *nextNode = root;

        while (true) {
            node = nextNode;
            PCEqualsResults prefixResult;
            prefixResult = checkPrefixEquals(node, level, start, end, loadKey);
            switch (prefixResult) {
                case PCEqualsResults::SkippedLevel:
		    #ifdef BREAKDOWN
		    _end = _rdtsc();
		    if(abort) time_abort += (_end - _start);
		    else time_traversal += (_end - _start);
		    abort = true;
		    #endif
                    goto restart;
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
                        N::getChildren(node, startLevel, endLevel, children, childrenCount);
			#ifdef BREAKDOWN
			_end = _rdtsc();
			if(abort) time_abort += (_end - _start);
			else time_traversal += (_end - _start);
			#endif
                        for (uint32_t i = 0; i < childrenCount; ++i) {
                            const uint8_t k = std::get<0>(children[i]);
                            const N *n = std::get<1>(children[i]);
                            if (k == startLevel) {
                                findStart(n, level + 1);
                            } else if (k > startLevel && k < endLevel) {
                                copy(n);
                            } else if (k == endLevel) {
                                findEnd(n, level + 1);
                            }
                            if (restart) {
				#ifdef BREAKDOWN
				abort = true;
				#endif
                                goto restart;
                            }
                            if (toContinue) {
                                break;
                            }
                        }
			#ifdef BREAKDOWN
			_start = _rdtsc();
			#endif
                    } 
		    else {
                        nextNode = N::getChild(startLevel, node);
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

    void yield(int count){
	if(count > 3)
	    sched_yield();
    }

    void Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo) {
	#ifdef BREAKDOWN
	uint64_t start, end;
	abort = false;
	#endif

        EpocheGuard epocheGuard(epocheInfo);
        restart:
        bool needRestart = false;
	#ifdef BREAKDOWN
	start = _rdtsc();
	#endif

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->getVersion();

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            switch (checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                                           this->loadKey)) { // increases level
                case CheckPrefixPessimisticResult::SkippedLevel:
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    abort = true;
		    #endif
                    goto restart;
                case CheckPrefixPessimisticResult::NoMatch: {
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    if(abort) time_abort += (end - start);
		    else time_traversal += (end - start);
		    start = _rdtsc();
		    #endif
                    assert(nextLevel < k.getKeyLen()); //prevent duplicate key
                    node->lockVersionOrRestart(v, needRestart);
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

                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    Prefix prefi = node->getPrefi();
                    prefi.prefixCount = nextLevel - level;
                    auto newNode = new N4(nextLevel, prefi);
                    // 2)  add node and (tid, *k) as children
                    newNode->insert(k[nextLevel], N::setLeaf(tid));
                    newNode->insert(nonMatchingKey, node);
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_split += (end - start);
		    start = _rdtsc();
		    #endif
                    // 3) lockVersionOrRestart, update parentNode to point to the new node, unlock
                    parentNode->writeLockOrRestart(needRestart);
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_latch += (end - start);
		    start = _rdtsc();
		    #endif
                    if (needRestart) {
			#ifdef BREAKDOWN
			abort = true;
			start = _rdtsc();
			#endif
                        delete newNode;
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_split += (end - start);
			start = _rdtsc();
			#endif
                        node->writeUnlock();
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_latch += (end - end);
			#endif
                        goto restart;
                    }
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
		    start = _rdtsc();
		    #endif

                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix.prefix,
                                    node->getPrefi().prefixCount - ((nextLevel - level) + 1));
		    #ifdef BREAKDOWN
		    end = _rdtsc();
		    time_split += (end - start);
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
            assert(nextLevel < k.getKeyLen()); //prevent duplicate key
            level = nextLevel;
            nodeKey = k[level];
            nextNode = N::getChild(nodeKey, node);
	    #ifdef BREAKDOWN
	    end = _rdtsc();
	    if(abort) time_abort += (end - start);
	    else time_traversal += (end - start);
	    start = _rdtsc();
	    #endif
            if (nextNode == nullptr) {
                node->lockVersionOrRestart(v, needRestart);
		#ifdef BREAKDOWN
		end = _rdtsc();
		time_latch += (end - start);
		#endif
                if (needRestart){
		    #ifdef BREAKDOWN
		    abort = true;
		    #endif
		    goto restart;
		}

                N::insertAndUnlock(node, parentNode, parentKey, nodeKey, N::setLeaf(tid), epocheInfo, needRestart);
                if (needRestart){
		    #ifdef BREAKDOWN
		    abort = true;
		    #endif
		    goto restart;
		}
                return;
            }

            if (N::isLeaf(nextNode)) {
                node->lockVersionOrRestart(v, needRestart);
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

		if(key == k){
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
                assert(level < key.getKeyLen()); //prevent inserting when prefix of key exists already
                uint32_t prefixLength = 0;
                while (key[level + prefixLength] == k[level + prefixLength]) {
                    prefixLength++;
                }

                auto n4 = new N4(level + prefixLength, &k[level], prefixLength);
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
        }
    }


    void Tree::remove(const Key &k, TID tid, ThreadInfo &threadInfo) {
        EpocheGuard epocheGuard(threadInfo);
        restart:
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;
        //bool optimisticPrefixMatch = false;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->getVersion();

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
                        goto restart;
                    }
                    return;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = k[level];
                    nextNode = N::getChild(nodeKey, node);

                    if (nextNode == nullptr) {
                        if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {//TODO benötigt??
                            goto restart;
                        }
                        return;
                    }
                    if (N::isLeaf(nextNode)) {
                        node->lockVersionOrRestart(v, needRestart);
                        if (needRestart) goto restart;

                        if (N::getLeaf(nextNode) != tid) {
                            node->writeUnlock();
                            return;
                        }
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && node != root) {
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
                                parentNode->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            } else {
                                uint64_t vChild = secondNodeN->getVersion();
                                secondNodeN->lockVersionOrRestart(vChild, needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    goto restart;
                                }
                                parentNode->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    secondNodeN->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
                                secondNodeN->addPrefixBefore(node, secondNodeK);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                                secondNodeN->writeUnlock();
                            }
                        } else {
                            N::removeAndUnlock(node, k[level], parentNode, parentKey, threadInfo, needRestart);
                            if (needRestart) goto restart;
                        }
                        return;
                    }
                    level++;
                }
            }
        }
    }


    typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
        if (k.getKeyLen() <= n->getLevel()) {
            return CheckPrefixResult::NoMatch;
        }
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
            level = n->getLevel();
            return CheckPrefixResult::OptimisticMatch;
        }
        if (p.prefixCount > 0) {
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
                 i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
                if (p.prefix[i] != k[level]) {
                    return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (p.prefixCount > maxStoredPrefixLength) {
                level += p.prefixCount - maxStoredPrefixLength;
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        return CheckPrefixResult::Match;
    }

    typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                        uint8_t &nonMatchingKey,
                                                                        Prefix &nonMatchingPrefix,
                                                                        LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
	    #ifdef BREAKDOWN
	    abort = true;
	    #endif
            return CheckPrefixPessimisticResult::SkippedLevel;
        }
        if (p.prefixCount > 0) {
            uint32_t prevLevel = level;
            Key kt;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    loadKey(N::getAnyChildTid(n), kt);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : p.prefix[i];
                if (curKey != k[level]) {
                    nonMatchingKey = curKey;
                    if (p.prefixCount > maxStoredPrefixLength) {
                        if (i < maxStoredPrefixLength) {
                            loadKey(N::getAnyChildTid(n), kt);
                        }
                        for (uint32_t j = 0; j < std::min((p.prefixCount - (level - prevLevel) - 1),
                                                          maxStoredPrefixLength); ++j) {
                            nonMatchingPrefix.prefix[j] = kt[level + j + 1];
                        }
                    } else {
                        for (uint32_t j = 0; j < p.prefixCount - i - 1; ++j) {
                            nonMatchingPrefix.prefix[j] = p.prefix[i + j + 1];
                        }
                    }
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    typename Tree::PCCompareResults Tree::checkPrefixCompare(const N *n, const Key &k, uint32_t &level,
                                                        LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
	    #ifdef BREAKDOWN
	    abort = true;
	    #endif
            return PCCompareResults::SkippedLevel;
        }
        if (p.prefixCount > 0) {
            Key kt;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    loadKey(N::getAnyChildTid(n), kt);
                }
                uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : 0;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : p.prefix[i];
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
                                                      LoadKeyFunction loadKey) {
        Prefix p = n->getPrefi();
        if (p.prefixCount + level < n->getLevel()) {
	    #ifdef BREAKDOWN
	    abort = true;
	    #endif
            return PCEqualsResults::SkippedLevel;
        }
        if (p.prefixCount > 0) {
            Key kt;
            for (uint32_t i = ((level + p.prefixCount) - n->getLevel()); i < p.prefixCount; ++i) {
                if (i == maxStoredPrefixLength) {
                    loadKey(N::getAnyChildTid(n), kt);
                }
                uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 0;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : p.prefix[i];
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
}
