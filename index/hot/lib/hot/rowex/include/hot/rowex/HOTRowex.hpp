#ifndef __HOT__ROWEX__HOT_ROWEX__
#define __HOT__ROWEX__HOT_ROWEX__

#include <array>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <utility>
#include <set>
#include <map>
#include <numeric>
#include <cstring>
#include <functional>

#include <hot/commons/Algorithms.hpp>
#include <hot/commons/BiNode.hpp>
#include <hot/commons/DiscriminativeBit.hpp>
#include <hot/commons/InsertInformation.hpp>
#include <hot/commons/TwoEntriesNode.hpp>

#include "hot/rowex/HOTRowexChildPointer.hpp"
#include "hot/rowex/HOTRowexFirstInsertLevel.hpp"
#include "hot/rowex/HOTRowexInsertStackEntry.hpp"
#include "hot/rowex/HOTRowexInsertStack.hpp"
#include "hot/rowex/HOTRowexInterface.hpp"
#include "hot/rowex/HOTRowexIterator.hpp"
#include "hot/rowex/HOTRowexNode.hpp"
#include "hot/rowex/EpochBasedMemoryReclamationStrategy.hpp"
#include "hot/rowex/MemoryGuard.hpp"

#include "idx/contenthelpers/KeyUtilities.hpp"
#include "idx/contenthelpers/TidConverters.hpp"
#include "idx/contenthelpers/ContentEquals.hpp"
#include "idx/contenthelpers/KeyComparator.hpp"
#include "idx/contenthelpers/OptionalValue.hpp"

namespace hot { namespace rowex {

template<typename ValueType, template <typename> typename KeyExtractor> KeyExtractor<ValueType> HOTRowex<ValueType, KeyExtractor>::extractKey;
template<typename ValueType, template <typename> typename KeyExtractor>
	typename idx::contenthelpers::KeyComparator<typename  HOTRowex<ValueType, KeyExtractor>::KeyType>::type
	HOTRowex<ValueType, KeyExtractor>::compareKeys;

template<typename ValueType, template <typename> typename KeyExtractor> HOTRowex<ValueType, KeyExtractor>::HOTRowex() : mRoot {}, mMemoryReclamation(EpochBasedMemoryReclamationStrategy::getInstance()) {
}

template<typename ValueType, template <typename> typename KeyExtractor> HOTRowex<ValueType, KeyExtractor>::HOTRowex(HOTRowex && other) : mRoot(other.mRoot), mMemoryReclamation(other.mMemoryReclamation) {
	other.mRoot = {};
}

template<typename ValueType, template <typename> typename KeyExtractor> HOTRowex<ValueType, KeyExtractor> & HOTRowex<ValueType, KeyExtractor>::operator=(HOTRowex && other) {
	mMemoryReclamation = other.mMemoryReclamation;
	mRoot = other.mRoot;
	other.mRoot = {};
	return *this;
}

template<typename ValueType, template <typename> typename KeyExtractor> HOTRowex<ValueType, KeyExtractor>::~HOTRowex() {
	mRoot.deleteSubtree();
}

template<typename ValueType, template <typename> typename KeyExtractor> inline idx::contenthelpers::OptionalValue<ValueType> HOTRowex<ValueType, KeyExtractor>::lookup(HOTRowex<ValueType, KeyExtractor>::KeyType const &key) const {
	MemoryGuard memoryGuard(mMemoryReclamation);
	auto const & fixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(key));
	uint8_t const* byteKey = idx::contenthelpers::interpretAsByteArray(fixedSizeKey);

	#ifdef BREAKDOWN
	abort = false;
	uint64_t start, end;
	start = _rdtsc();
	#endif
	HOTRowexChildPointer current =  mRoot;
	while(!current.isLeaf()) {
		current = *(current.search(byteKey));
	}
	#ifdef BREAKDOWN
	end = _rdtsc();
	time_traversal += (end - start);
	start = _rdtsc();
	#endif
	ValueType const & value = idx::contenthelpers::tidToValue<ValueType>(current.getTid());
	#ifdef BREAKDOWN
	auto ret = idx::contenthelpers::OptionalValue<ValueType> {idx::contenthelpers::contentEquals(extractKey(value), key), value};
	end = _rdtsc();
	time_node += (end - start);
	return ret;
	#else
	return { idx::contenthelpers::contentEquals(extractKey(value), key), value };
	#endif
}

template<typename ValueType, template <typename> typename KeyExtractor> inline idx::contenthelpers::OptionalValue<ValueType> HOTRowex<ValueType, KeyExtractor>::scan(HOTRowex<ValueType, KeyExtractor>::KeyType const &key, size_t numberValues) const {
	const_iterator iterator = lower_bound(key);
	for(size_t i = 0u; i < numberValues && iterator != end(); ++i) {
		++iterator;
	}
	return iterator == end() ? idx::contenthelpers::OptionalValue<ValueType>({}) : idx::contenthelpers::OptionalValue<ValueType>({ true, *iterator });
}


template<typename ValueType, template <typename> typename KeyExtractor> inline bool HOTRowex<ValueType, KeyExtractor>::insert(ValueType const & value) {
	MemoryGuard guard(mMemoryReclamation);
	#ifdef BREAKDOWN
	abort = false;
	#endif
	return insertGuarded(value);
}

template<typename ValueType, template <typename> typename KeyExtractor> inline bool HOTRowex<ValueType, KeyExtractor>::insertGuarded(ValueType const & value) {
        #ifdef BREAKDOWN
	uint64_t start, end;
	start = _rdtsc();
	#endif
	idx::contenthelpers::OptionalValue<bool> insertionResult;

	auto const & fixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(extractKey(value)));
	uint8_t const* keyBytes = idx::contenthelpers::interpretAsByteArray(fixedSizeKey);

	while(!insertionResult.mIsValid) {
		//This temporary variable is important to prevent race conditions, which can occur
		// in case the root pointer is directly used and can be dereference to two different values
		HOTRowexChildPointer currentRoot = mRoot;
		if (currentRoot.isAValidNode()) {
			InsertStackType insertStack{ currentRoot, &mRoot, keyBytes};
			idx::contenthelpers::OptionalValue<hot::commons::DiscriminativeBit> const &mismatchingBit = insertStack.getMismatchingBit(keyBytes);
			if (mismatchingBit.mIsValid) {
			        #ifdef BREAKDOWN
			    	end = _rdtsc();
				if(!abort) time_traversal += (end - start);
				else time_abort += (end - start);
				#endif
				insertionResult = insertNewValue(insertStack, mismatchingBit.mValue, value);
				#ifdef BREAKDOWN
				start = _rdtsc();
				#endif
			} else {
			        #ifdef BREAKDOWN
			    	end = _rdtsc();
				if(!abort) time_traversal += (end - start);
				else time_abort += (end - start);
				#endif
				insertionResult = {true, false };
			}
		} else if (currentRoot.isLeaf()) {
			HOTRowexChildPointer valueToInsert(idx::contenthelpers::valueToTid(value));
			ValueType const &currentLeafValue = idx::contenthelpers::tidToValue<ValueType>(currentRoot.getTid());
			auto const &existingFixedSizeKey = idx::contenthelpers::toFixSizedKey(
				idx::contenthelpers::toBigEndianByteOrder(extractKey(currentLeafValue)));
			uint8_t const *existingKeyBytes = idx::contenthelpers::interpretAsByteArray(existingFixedSizeKey);
			idx::contenthelpers::OptionalValue<hot::commons::DiscriminativeBit> const &mismatchingBit = hot::commons::getMismatchingBit(existingKeyBytes, keyBytes, static_cast<uint16_t>(idx::contenthelpers::getMaxKeyLength<KeyType>()));

			if (mismatchingBit.mIsValid) {
			    	#ifdef BREAKDOWN
			    	end = _rdtsc();
				if(!abort) time_traversal += (end - start);
				else time_abort += (end - start);
				start = _rdtsc();
				#endif
				HOTRowexChildPointer const & newRoot = hot::commons::createTwoEntriesNode<HOTRowexChildPointer, HOTRowexNode>(hot::commons::BiNode<HOTRowexChildPointer>::createFromExistingAndNewEntry(mismatchingBit.mValue, mRoot, valueToInsert))->toChildPointer();
				#ifdef BREAKDOWN
				auto ret = mRoot.compareAndSwap(currentRoot, newRoot);
				end = _rdtsc();
				time_split += (end - start);
				insertionResult = {ret, true};
				start = _rdtsc();
				#else
				insertionResult = { mRoot.compareAndSwap(currentRoot, newRoot) , true};
				#endif
			} else {
			        #ifdef BREAKDOWN
			    	end = _rdtsc();
				if(!abort) time_traversal += (end - start);
				else time_abort += (end - start);
				#endif
				insertionResult = {true, false };
			}
		} else {
		    	#ifdef BREAKDOWN
		    	end = _rdtsc();
			if(!abort) time_traversal += (end - start);
			else time_abort += (end - start);
			start = _rdtsc();
			#endif
			HOTRowexChildPointer newValue(idx::contenthelpers::valueToTid(value));
			#ifdef BREAKDOWN
			auto ret = mRoot.compareAndSwap(currentRoot, newValue);
			end = _rdtsc();
			time_node += (end - start);
			insertionResult = { ret, true};
			start = _rdtsc();
			#else
			insertionResult = { mRoot.compareAndSwap(currentRoot, newValue), true };
			#endif
		}
		#ifdef BREAKDOWN
		abort = true;
		#endif
	}
	return insertionResult.mValue;
}

template<typename ValueType, template <typename> typename KeyExtractor> inline idx::contenthelpers::OptionalValue<ValueType> HOTRowex<ValueType, KeyExtractor>::upsert(ValueType newValue) {
        #ifdef BREAKDOWN
	abort = false;
	uint64_t start, end;
	start = _rdtsc();
	#endif
	MemoryGuard guard(mMemoryReclamation);
	KeyType newKey = extractKey(newValue);
	auto const & fixedSizeKey = idx::contenthelpers::toFixSizedKey(idx::contenthelpers::toBigEndianByteOrder(extractKey(newValue)));
	uint8_t const* keyBytes = idx::contenthelpers::interpretAsByteArray(fixedSizeKey);
	idx::contenthelpers::OptionalValue<ValueType> upsertResult;
	bool upsertCompleted = false;

	while(!upsertCompleted) {
		HOTRowexChildPointer currentRoot = mRoot;
		upsertResult = {};
		if (currentRoot.isAValidNode()) {
			InsertStackType insertStack{currentRoot, &mRoot, keyBytes};
			idx::contenthelpers::OptionalValue<hot::commons::DiscriminativeBit> const &mismatchingBit = insertStack.getMismatchingBit(
				keyBytes);

			if (mismatchingBit.mIsValid) {
				//insert
			    	#ifdef BREAKDOWN
			    	end = _rdtsc();
				if(!abort) time_traversal += (end - start);
				else time_abort += (end - start);
				#endif
				const idx::contenthelpers::OptionalValue<bool> &insertResult = insertNewValue(insertStack, mismatchingBit.mValue, newValue);
			    	#ifdef BREAKDOWN
				start = _rdtsc();
				#endif
				upsertCompleted = insertResult.mIsValid;
			} else {
				InsertStackEntryType &leafEntry = *insertStack.mLeafEntry;
				InsertStackEntryType* parentEntry = insertStack.mLeafEntry - 1;
				#ifdef BREAKDOWN
				end = _rdtsc();
				if(!abort) time_traversal += (end - start);
				else time_abort += (end - start);
				start = _rdtsc();
				#endif
				if(parentEntry->tryLock()) { //Check for consistency
				    	#ifdef BREAKDOWN
				    	end = _rdtsc();
					time_latch += (end - start);
					start = _rdtsc();
					#endif
					ValueType const &existingValue = idx::contenthelpers::tidToValue<ValueType>(
						leafEntry.getChildPointer().getTid());
					leafEntry.updateChildPointer(HOTRowexChildPointer(idx::contenthelpers::valueToTid(newValue)));
					upsertResult = idx::contenthelpers::OptionalValue<ValueType> {true, existingValue};
					upsertCompleted = true;
					#ifdef BREAKDOWN
					end = _rdtsc();
					time_node += (end - start);
					start = _rdtsc();
					#endif
					parentEntry->unlock();
					#ifdef BREAKDOWN
					end = _rdtsc();
					time_latch += (end - start);
					start = _rdtsc();
					#endif
				}
				#ifdef BREAKDOWN
				else{
				    end = _rdtsc();
				    time_latch += (end - start);
				    start = _rdtsc();
				}
				#endif
			}
		} else if (currentRoot.isLeaf()) {
		    	#ifdef BREAKDOWN
		    	end = _rdtsc();
			if(!abort) time_traversal += (end - start);
			else time_abort += (end - start);
			start = _rdtsc();
			#endif
			ValueType existingValue = idx::contenthelpers::tidToValue<ValueType>(currentRoot.getTid());
			if (idx::contenthelpers::contentEquals(extractKey(existingValue), newKey)) {
				upsertCompleted = mRoot.compareAndSwap(currentRoot, HOTRowexChildPointer(idx::contenthelpers::valueToTid(newValue)));
				upsertResult = { true, existingValue };
			    	#ifdef BREAKDOWN
				end = _rdtsc();
				time_node += (end - start);
				start = _rdtsc();
				#endif
			} else {
				upsertCompleted = insertGuarded(newValue);
			}
		} else {
		    	#ifdef BREAKDOWN
		    	end = _rdtsc();
			if(!abort) time_traversal += (end - start);
			else time_abort += (end - start);
			start = _rdtsc();
			#endif
			HOTRowexChildPointer newRootPointer(idx::contenthelpers::valueToTid(newValue));
			upsertCompleted = mRoot.compareAndSwap(currentRoot, newRootPointer);
			#ifdef BREAKDOWN
			end = _rdtsc();
			time_node += (end - start);
			start = _rdtsc();
			#endif
		}
		#ifdef BREAKDOWN
		abort = true;
		#endif
	}
	return upsertResult;
}

template<typename ValueType, template <typename> typename KeyExtractor> inline idx::contenthelpers::OptionalValue<bool> HOTRowex<ValueType, KeyExtractor>::insertNewValue(typename HOTRowex<ValueType, KeyExtractor>::InsertStackType & insertStack, hot::commons::DiscriminativeBit const & newBit, ValueType const & value) {
    	#ifdef BREAKDOWN
    	uint64_t start, end;
	start = _rdtsc();
	#endif
	const HOTRowexFirstInsertLevel<InsertStackEntryType> & insertLevel = insertStack.determineInsertLevel(newBit);
	#ifdef BREAKDOWN
	end = _rdtsc();
	if(!abort) time_traversal += (end - start);
	else time_abort += (end - start);
	start = _rdtsc();
	#endif
	unsigned int numberLockedEntries = insertStack.tryLock(&mRoot, insertLevel);
	#ifdef BREAKDOWN
	end = _rdtsc();
	time_latch += (end - start);
	#endif
	bool aquiredLocks = numberLockedEntries > 0;
	#ifdef BREAKDOWN
	if(!aquiredLocks) abort = true;
	#endif
	return (aquiredLocks) ? insertForStackRange(insertStack, insertLevel, numberLockedEntries, value) : idx::contenthelpers::OptionalValue<bool> { };
};

template<typename ValueType, template <typename> typename KeyExtractor> inline idx::contenthelpers::OptionalValue<bool> HOTRowex<ValueType, KeyExtractor>::insertForStackRange(typename HOTRowex<ValueType, KeyExtractor>::InsertStackType & insertStack, const HOTRowexFirstInsertLevel<InsertStackEntryType> & insertLevel, unsigned int numberLockedEntries, ValueType const & valueToInsert) {
     	#ifdef BREAKDOWN
    	uint64_t start, end;
	start = _rdtsc();
	#endif
	InsertStackEntryType * firstStackEntry = insertLevel.mFirstEntry;
	InsertStackEntryType * currentStackEntry = firstStackEntry;
	HOTRowexChildPointer childPointerToValue(idx::contenthelpers::valueToTid(valueToInsert));

	if (insertLevel.mIsLeafNodePushdown) {
		leafNodePushDown(*insertStack.mLeafEntry, insertLevel.mInsertInformation, childPointerToValue);
	     	#ifdef BREAKDOWN
	    	end = _rdtsc();
		time_split += (end - start);
		start = _rdtsc();
		#endif
		//std::cout << "Leaf HOTRowexNode pushdown " << valueToInsert << " with " << numberLockedEntries << "locked entries " <<std::endl;
	} else if (!currentStackEntry->getChildPointer().getNode()->isFull()) {
		normalInsert(*currentStackEntry, insertLevel.mInsertInformation, childPointerToValue);
		currentStackEntry->markAsObsolete(*mMemoryReclamation);
	    	#ifdef BREAKDOWN
	    	end = _rdtsc();
		time_node += (end - start);
		start = _rdtsc();
		#endif
		//std::cout << "Normal Insert: " << valueToInsert << std::endl;
	} else {
		//initial parent pull up or create new bi node with sibling
		assert(insertLevel.mInsertInformation.mKeyInformation.mAbsoluteBitIndex != currentStackEntry->mSearchResultForInsert.mMostSignificantBitIndex);
		hot::commons::BiNode<HOTRowexChildPointer> currentSplitEntries;
		if((insertLevel.mInsertInformation.mKeyInformation.mAbsoluteBitIndex > currentStackEntry->mSearchResultForInsert.mMostSignificantBitIndex)) {
			currentSplitEntries = split(*currentStackEntry, insertLevel.mInsertInformation, childPointerToValue);
			currentStackEntry->markAsObsolete(*mMemoryReclamation);
			//std::cout << "Initial split: " << valueToInsert << std::endl;
		} else {
			currentSplitEntries = hot::commons::BiNode<HOTRowexChildPointer>::createFromExistingAndNewEntry(insertLevel.mInsertInformation.mKeyInformation, currentStackEntry->getChildPointer(), childPointerToValue);
			//std::cout << "Create Sibling HOTRowexNode: " << valueToInsert << std::endl;
		}

		InsertStackEntryType* nextStackEntry = currentStackEntry - 1;
		//recursive parent pull ups
		while(nextStackEntry >= insertStack.getRawStack() && ((currentSplitEntries.mHeight == nextStackEntry->getChildPointer().getHeight()) & (nextStackEntry->getChildPointer().getNode()->isFull()))) {
			currentStackEntry = nextStackEntry;
			nextStackEntry = currentStackEntry - 1;
			currentSplitEntries = integrateAndSplit(*currentStackEntry, currentSplitEntries);
			currentStackEntry->markAsObsolete(*mMemoryReclamation);
			//std::cout << "Recusive splits: " << valueToInsert << std::endl;
		}

		//normal parent pull up
		if(nextStackEntry >= insertStack.getRawStack() && currentSplitEntries.mHeight == nextStackEntry->getChildPointer().getHeight()) {
			finalParentPullUp(*nextStackEntry, currentSplitEntries);
			nextStackEntry->markAsObsolete(*mMemoryReclamation);
			//std::cout << "Final parent pullup: " << valueToInsert << std::endl;
		} //Either hasSpaceAboveForIntermediateNode or requires a new root node
		else {
			currentStackEntry->updateChildPointer(hot::commons::createTwoEntriesNode<HOTRowexChildPointer, HOTRowexNode>(currentSplitEntries)->toChildPointer());
			//std::cout << "Intermediate HOTRowexNode creation: " << valueToInsert << std::endl;
		}
		#ifdef BREAKDOWN
		end = _rdtsc();
		time_split += (end - start);
		start = _rdtsc();
		#endif
	}

	//unlock top down
	for(int i = numberLockedEntries - 1; i >= 0; --i) {
		InsertStackEntryType *stackEntry = (firstStackEntry - i);
		stackEntry->unlock();
	}
	#ifdef BREAKDOWN
	end = _rdtsc();
	time_latch += (end - start);
 	#endif

	return { true, true };
}

template<typename ValueType, template <typename> typename KeyExtractor> inline void HOTRowex<ValueType, KeyExtractor>::leafNodePushDown(typename HOTRowex<ValueType, KeyExtractor>::InsertStackEntryType & leafEntry, hot::commons::InsertInformation const & insertInformation, HOTRowexChildPointer const & valueToInsert) {
	leafEntry.updateChildPointer(hot::commons::createTwoEntriesNode<HOTRowexChildPointer, HOTRowexNode>(hot::commons::BiNode<HOTRowexChildPointer>::createFromExistingAndNewEntry(insertInformation.mKeyInformation, leafEntry.getChildPointer(), valueToInsert))->toChildPointer());
}

template<typename ValueType, template <typename> typename KeyExtractor> inline void HOTRowex<ValueType, KeyExtractor>::normalInsert(typename HOTRowex<ValueType, KeyExtractor>::InsertStackEntryType & currentNodeStackEntry, hot::commons::InsertInformation const & insertInformation, HOTRowexChildPointer const & valueToInsert) {
	currentNodeStackEntry.updateChildPointer(
		currentNodeStackEntry.getChildPointer().executeForSpecificNodeType(false, [&](auto & currentNode) -> HOTRowexChildPointer {
			return currentNode.addEntry(insertInformation, valueToInsert);
		})
	);
}

template<typename ValueType, template <typename> typename KeyExtractor> inline hot::commons::BiNode<HOTRowexChildPointer> HOTRowex<ValueType, KeyExtractor>::split(
	typename HOTRowex<ValueType, KeyExtractor>::InsertStackEntryType &currentInsertStackEntry,
	hot::commons::InsertInformation const &insertInformation, HOTRowexChildPointer const &valueToInsert) {

	return currentInsertStackEntry.getChildPointer().executeForSpecificNodeType(false, [&](auto & currentNode) -> hot::commons::BiNode<HOTRowexChildPointer> {
				return currentNode.split(insertInformation, valueToInsert);
	});
}

template<typename ValueType, template <typename> typename KeyExtractor> inline hot::commons::BiNode<HOTRowexChildPointer> HOTRowex<ValueType, KeyExtractor>::integrateAndSplit(typename HOTRowex<ValueType, KeyExtractor>::InsertStackEntryType & currentInsertStackEntry, hot::commons::BiNode<HOTRowexChildPointer> const & splitEntries) {
	//parent pull up
	return currentInsertStackEntry.getChildPointer().executeForSpecificNodeType(false, [&](auto & currentNode) -> hot::commons::BiNode<HOTRowexChildPointer> {
		//the right branch of the new bi node is always the one to insert, as this was not inserted before....
		hot::commons::DiscriminativeBit discriminativeBit = { splitEntries.mDiscriminativeBitIndex, true };
		hot::commons::InsertInformation const & insertInformation = currentInsertStackEntry.getInsertInformation(discriminativeBit);

		HOTRowexChildPointer const & valueToInsert = splitEntries.mRight;
		HOTRowexChildPointer const & valueToReplace = splitEntries.mLeft;

		//propagate recursively up
		//The diffing Bit index cannot be larger as the parents mostSignificantBitIndex. the reason is that otherwise
		//the trie condition would be violated
		assert(currentInsertStackEntry.mSearchResultForInsert.mMostSignificantBitIndex < splitEntries.mDiscriminativeBitIndex);
		//Furthermore due to the trie condition it is safe to assume that both the existing entry and the new entry will be part of the same subtree
		hot::commons::BiNode<HOTRowexChildPointer> const & newSplitEntries = currentNode.split(insertInformation, valueToInsert);
		//Detect subtree side
		//This newSplitEntries.mLeft.getHeight() == currentNodePointer.getHeight() check is important because in case of a split with 1:31 it can happen that if
		//the 1 entry is not a leaf node the node it is pointing to will be pulled up, which implies that the numberEntriesInLowerPart are not correct anymore.
		unsigned int numberEntriesInLowerPart = newSplitEntries.mLeft.getHeight() == currentNode.mHeight ? newSplitEntries.mLeft.getNumberEntries() : 1;
		bool isInUpperPart = numberEntriesInLowerPart <= currentInsertStackEntry.mSearchResultForInsert.mEntryIndex;

		unsigned int correspondingEntryIndexInPart = currentInsertStackEntry.mSearchResultForInsert.mEntryIndex - (isInUpperPart * numberEntriesInLowerPart);
		HOTRowexChildPointer const & nodePointerContainingSplitEntries = (isInUpperPart) ? newSplitEntries.mRight : newSplitEntries.mLeft;
		nodePointerContainingSplitEntries.getNode()->getPointers()[correspondingEntryIndexInPart] = valueToReplace;
		return newSplitEntries;
	});
}

template<typename ValueType, template <typename> typename KeyExtractor> inline void HOTRowex<ValueType, KeyExtractor>::finalParentPullUp(typename HOTRowex<ValueType, KeyExtractor>::InsertStackEntryType & currentNodeStackEntry, hot::commons::BiNode<HOTRowexChildPointer> const & splitEntries) {
	//the right branch of the new bi node is always the one to insert, as this discriminative bit was not contained (otherwise the branch node would be impossible...
	hot::commons::DiscriminativeBit const discriminativeBit { splitEntries.mDiscriminativeBitIndex, true };
	HOTRowexChildPointer const & currentNodePointer = currentNodeStackEntry.getChildPointer();
	uint32_t entryIndex = currentNodeStackEntry.mSearchResultForInsert.mEntryIndex;

	HOTRowexChildPointer const & newNode = currentNodePointer.executeForSpecificNodeType(false, [&](auto & parentNode) -> HOTRowexChildPointer {
		hot::commons::InsertInformation const &insertInformation = parentNode.getInsertInformation(entryIndex, discriminativeBit);
		//simple parent pullup
		return parentNode.addEntry(insertInformation, splitEntries.mRight);
	});

	newNode.getNode()->getPointers()[entryIndex] = splitEntries.mLeft;
	currentNodeStackEntry.updateChildPointer(newNode);
}


template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTRowex<ValueType, KeyExtractor>::const_iterator HOTRowex<ValueType, KeyExtractor>::begin() const {
	return HOTRowexSynchronizedIterator<ValueType, KeyExtractor>::begin(&mRoot, const_cast<EpochBasedMemoryReclamationStrategy*>(mMemoryReclamation));
}

template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTRowex<ValueType, KeyExtractor>::const_iterator const & HOTRowex<ValueType, KeyExtractor>::end() const {
	return HOTRowexSynchronizedIterator<ValueType, KeyExtractor>::end();
}

template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTRowex<ValueType, KeyExtractor>::const_iterator HOTRowex<ValueType, KeyExtractor>::find(typename HOTRowex<ValueType, KeyExtractor>::KeyType const & searchKey) const {
	return HOTRowexSynchronizedIterator<ValueType, KeyExtractor>::find(&mRoot, searchKey, const_cast<EpochBasedMemoryReclamationStrategy*>(mMemoryReclamation));
}

template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTRowex<ValueType, KeyExtractor>::const_iterator HOTRowex<ValueType, KeyExtractor>::lower_bound(typename HOTRowex<ValueType, KeyExtractor>::KeyType const & searchKey) const {
	return HOTRowexSynchronizedIterator<ValueType, KeyExtractor>::getBounded(&mRoot, searchKey, true, const_cast<EpochBasedMemoryReclamationStrategy*>(mMemoryReclamation));
}

template<typename ValueType, template <typename> typename KeyExtractor> inline typename HOTRowex<ValueType, KeyExtractor>::const_iterator HOTRowex<ValueType, KeyExtractor>::upper_bound(typename HOTRowex<ValueType, KeyExtractor>::KeyType const & searchKey) const {
	return HOTRowexSynchronizedIterator<ValueType, KeyExtractor>::getBounded(&mRoot, searchKey, false, const_cast<EpochBasedMemoryReclamationStrategy*>(mMemoryReclamation));
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline HOTRowexChildPointer HOTRowex<ValueType, KeyExtractor>::getNodeAtPath(std::initializer_list<unsigned int> path) {
	HOTRowexChildPointer current = mRoot;
	for(unsigned int entryIndex : path) {
		assert(!current.isLeaf());
		current = current.getNode()->getPointers()[entryIndex];
	}
	return current;
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline void HOTRowex<ValueType, KeyExtractor>::collectStatsForSubtree(
	HOTRowexChildPointer const & subTreeRoot, std::map<std::string, double> & stats
) const {
	if(!subTreeRoot.isLeaf()) {
		subTreeRoot.executeForSpecificNodeType(true, [&, this](auto & node) -> void {
			std::string nodeType = nodeAlgorithmToString(node.mNodeType);
			stats["total"] += node.getNodeSizeInBytes();
			stats[nodeType] += 1.0;
			for(HOTRowexChildPointer const & childPointer : node) {
				this->collectStatsForSubtree(childPointer, stats);
			}
		});
	}
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline void HOTRowex<ValueType, KeyExtractor>::findDepth(uint64_t& leaf_cnt, uint64_t& leaf_depth, uint64_t& max_depth) const{
    std::map<uint64_t, uint64_t> depths;
    std::function<void(HOTRowexChildPointer const&, uint64_t)> traverse = [&traverse, &depths, this](HOTRowexChildPointer const& node, uint64_t cur_depth){
	if(node.isLeaf()){
	    depths[cur_depth]++;
	}
	else{
	    for(auto& child: (*node.getNode())){
		traverse(child, cur_depth+1);
	    }
	}
    };
    traverse(mRoot, 1);
    for(auto it: depths){
	leaf_cnt += it.second;
	leaf_depth += (it.first * it.second);
	if(max_depth < it.first)
	    max_depth = it.first;
    }
}

template<typename ValueType, template <typename> typename KeyExtractor>
inline void HOTRowex<ValueType, KeyExtractor>::getMemory(HOTRowexChildPointer const& node, uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied) const {
    if(node.isLeaf()){
	structural_data_occupied -= sizeof(void*);
	key_data_occupied += sizeof(ValueType);
	return;
    }

    if(!node.isAValidNode()){
	structural_data_occupied -= sizeof(void*);
	structural_data_unoccupied += sizeof(void*);
	return;
    }

    node.executeForSpecificNodeType(true, [&, this, &meta, &structural_data_occupied, &structural_data_unoccupied, &key_data_occupied, &key_data_unoccupied](auto& node_)->void{
	    uint64_t meta_ = sizeof(HOTRowexNodeBase) + sizeof(node_.mDiscriminativeBitsRepresentation);
	    meta += meta_;
	    for(auto child=node_.begin(); child!=node_.end(); ++child){
		if(child->isEmpty()){
		    structural_data_unoccupied += sizeof(void*);
		    key_data_unoccupied += sizeof(node_.mPartialKeys.mEntries[0]);
		}
		else{
		    structural_data_occupied += sizeof(void*);
		    key_data_occupied += sizeof(node_.mPartialKeys.mEntries[0]);
		    this->getMemory(*child, meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
		}
	    }
    });
}
		
template<typename ValueType, template <typename> typename KeyExtractor>
inline void HOTRowex<ValueType, KeyExtractor>::getMemory(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied) const {
    getMemory(mRoot, meta, structural_data_occupied, structural_data_unoccupied, key_data_occupied, key_data_unoccupied);
}

template<typename ValueType, template <typename> typename KeyExtractor>
std::pair<size_t, std::map<std::string, double>> HOTRowex<ValueType, KeyExtractor>::getStatistics() const {
	std::map<size_t, size_t> leafNodesPerDepth;
	getValueDistribution(mRoot, 0, leafNodesPerDepth);

	std::map<size_t, size_t> leafNodesPerBinaryDepth;
	getBinaryTrieValueDistribution(mRoot, 0, leafNodesPerBinaryDepth);

	std::map<std::string, double> statistics;
	statistics["height"] = mRoot.getHeight();
	statistics["numberFrees"] = ThreadSpecificEpochBasedReclamationInformation::mNumberFrees;

	size_t overallLeafNodeCount = 0;
	for(auto leafNodesOnDepth : leafNodesPerDepth) {
		std::string statisticsKey { "leafNodesOnDepth_"};
		std::string levelString = std::to_string(leafNodesOnDepth.first);
		statisticsKey += std::string(2 - levelString.length(), '0') + levelString;
		statistics[statisticsKey] = leafNodesOnDepth.second;
		overallLeafNodeCount += leafNodesOnDepth.second;
	}

	for(auto leafNodesOnBinaryDepth : leafNodesPerBinaryDepth) {
		std::string statisticsKey { "leafNodesOnBinaryDepth_"};
		std::string levelString = std::to_string(leafNodesOnBinaryDepth.first);
		statisticsKey += std::string(3 - levelString.length(), '0') + levelString;
		statistics[statisticsKey] = leafNodesOnBinaryDepth.second;
	}

	statistics["numberValues"] = overallLeafNodeCount;
	collectStatsForSubtree(mRoot, statistics);

	size_t totalSize = statistics["total"];
	statistics.erase("total");

	return {totalSize, statistics };
}

/**
 * root has depth 0
 * first Level has depth 1...
 * @param leafNodesPerDepth an output parameter for collecting the number of values aggregated by depth
 * @param currentDepth the current depth to process
 */
template<typename ValueType, template <typename> typename KeyExtractor>
inline void HOTRowex<ValueType, KeyExtractor>::getValueDistribution(
	HOTRowexChildPointer const & childPointer, size_t depth, std::map<size_t, size_t> & leafNodesPerDepth
) const {
	if(childPointer.isLeaf()) {
		++leafNodesPerDepth[depth];
	} else {
		for(HOTRowexChildPointer const & pointer : (*childPointer.getNode())) {
			getValueDistribution(pointer, depth + 1, leafNodesPerDepth);
		}
	}
}

/**
 * root has depth 0
 * first Level has depth 1...
 * @param leafNodesPerDepth an output parameter for collecting the number of values aggregated by depth in a virtual cobtrie
 * @param currentDepth the current depth to process
 */
template<typename ValueType, template <typename> typename KeyExtractor>
inline void HOTRowex<ValueType, KeyExtractor>::getBinaryTrieValueDistribution(
	HOTRowexChildPointer const & childPointer, size_t binaryTrieDepth, std::map<size_t, size_t> & leafNodesPerDepth
) const {
	if(childPointer.isLeaf()) {
		++leafNodesPerDepth[binaryTrieDepth];
	} else {
		childPointer.executeForSpecificNodeType(true, [&, this](auto &node) {
			std::array<uint8_t, 32> binaryEntryDepthsInNode = node.getEntryDepths();
			size_t i=0;
			for(HOTRowexChildPointer const & pointer : node) {
				this->getBinaryTrieValueDistribution(pointer, binaryTrieDepth + binaryEntryDepthsInNode[i], leafNodesPerDepth);
				++i;
			}
		});
	}
}

}}

#endif
