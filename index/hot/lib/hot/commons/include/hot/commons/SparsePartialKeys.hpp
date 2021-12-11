#ifndef __HOT__COMMONS__SPARSE_PARTIAL_KEYS__
#define __HOT__COMMONS__SPARSE_PARTIAL_KEYS__

#if (__x86__ || __x86_64__)
#include <immintrin.h>
#else
#include <arm_neon.h>
#endif

#include <bitset>
#include <cassert>
#include <cstdint>

#include <hot/commons/Algorithms.hpp>

namespace hot { namespace commons {

constexpr uint16_t alignToNextHighestValueDivisableBy8(uint16_t size) {
	return static_cast<uint16_t>((size % 8 == 0) ? size : ((size & (~7)) + 8));
}

/**
 * Sparse Partial Keys are used to store the discriminative bits required to distinguish entries in a linearized binary patricia trie.
 *
 * For a single binary patricia trie the set of discriminative bits consists of all discriminative bit positions used in its BiNodes.
 * Partial Keys therefore consists only of those bits of a the stored key which correspon to a discriminative bit.
 * Sparse partial keys are an optimization of partial keys and can be used to construct linearized representations of binary patricia tries.
 * For each entry only those bits in the sparse partial keys are set, which correspond to a BiNodes along the path from the binary patricia trie root.
 * All other bits are set to 0 and are therefore intentially left undefined.
 *
 * To clarify the notion of sparse partial keys we illustrate the conversion of a binary patricia trie consisting of 7 entries into its linearized representation.
 * This is an ASCII art version of Figure 5 of "HOT: A Height Optimized Trie Index"
 *
 *             (bit 3, 3-bit prefix 011)           |Values |   Raw Key   |Bit Positions|Partial key (dense) |Partial key (sparse)|
 *                    /  \                         |=======|=============|=============|====================|====================|
 *                  /     \                        |  v1   |  0110100101 |  {3,6,8,}   |     0 1 0 0 1      |     0 0 0 0 0      |
 *              010/       \1                      |-------|-------------|-------------|--------------------|--------------------|
 *               /          \                      |  v2   |  0110100110 |  {3,6,8}    |     0 1 0 1 0      |     0 0 0 1 0      |
 *              /            \                     |-------|-------------|-------------|--------------------|--------------------|
 *        (bit 6))          (bit 4)                |  v3   |  0110101010 |  {3,6,9}    |     0 1 1 1 0      |     0 0 1 0 0      |
 *         /     \            /  \                 |-------|-------------|-------------|--------------------|--------------------|
 *      01/    101\    010110/    \ 1010           |  v4   |  0110101011 |  {3,6,9}    |     0 1 1 1 1      |     0 0 1 0 1      |
 *       /        \         /      \               |-------|-------------|-------------|--------------------|--------------------|
 *    (bit 8)  (bit 9)    v5    (bit 8)            |  v5   |  0111010110 |   {3,4}     |     1 0 0 1 0      |     1 0 0 0 0      |
 *      / \      / \              / \              |-------|-------------|-------------|--------------------|--------------------|
 *   01/   \10 0/  \1        01  /   \ 11          |  v6   |  0111101001 |  {3,4,8}    |     1 1 1 0 1      |     1 1 0 0 0      |
 *    /     \  /    \           /     \            |-------|-------------|-------------|--------------------|--------------------|
 *   v1     v2 v3   v4         v6      v7          |  v7   |  0111101011 |  {3,4,8}    |     1 1 1 1 1      |     1 1 0 1 0      |
 *                                                 |=====================|=============|====================|====================|
 */
template<typename PartialKeyType>
struct alignas(8) SparsePartialKeys {
	void *operator new(size_t /* baseSize */, uint16_t const numberEntries);

	void operator delete(void *);

	PartialKeyType mEntries[1];

	/**
	 * Search returns a mask corresponding to all partial keys complying to the dense partial search key.
	 * The bit positions in the result mask are indexed from the least to the most singificant bit
	 *
	 * Compliance for a sparse partial key is defined in the following:
	 *
	 * (densePartialKey & sparsePartialKey) === sparsePartialKey
	 *
	 *
	 * @param densePartialSearchKey the dense partial key of the search key to search matching entries for
	 * @return the resulting mask with each bit representing the result of a single compressed mask. bit 0 (least significant) correspond to the mask 0, bit 1 corresponds to mask 1 and so forth.
	 */
	inline uint32_t search(PartialKeyType const densePartialSearchKey) const;

	/**
	 * Determines all dense partial keys which matche a given partial key pattern.
	 *
	 * This method exectues for each sparse partial key the following operation (sparsePartialKey[i]): sparsePartialKey[i] & partialKeyPattern == partialKeyPattern
	 *
	 * @param partialKeyPattern the pattern to use for searchin compressed mask
	 * @return the resulting mask with each bit representing the result of a single compressed mask. bit 0 (least significant) correspond to the mask 0, bit 1 corresponds to mask 1 and so forth.
	 */
	inline uint32_t findMasksByPattern(PartialKeyType const partialKeyPattern) const {
#if (__x86__ || __x86_64__)
		__m256i searchRegister = broadcastToSIMDRegister(partialKeyPattern);
#else
		auto searchRegister = broadcastToSIMDRegister(partialKeyPattern);
#endif
		return findMasksByPattern(searchRegister, searchRegister);
	}

private:
#if (__x86__ || __x86_64__)
	inline uint32_t findMasksByPattern(__m256i const usedBitsMask, __m256i const expectedBitsMask) const;

	inline __m256i broadcastToSIMDRegister(PartialKeyType const mask) const;
#else
	inline uint32_t findMasksByPattern(uint8x16_t const usedBitsMask, uint8x16_t const expectedBitsMask) const;
	inline uint32_t findMasksByPattern(uint16x8_t const usedBitsMask, uint16x8_t const expectedBitsMask) const;
	inline uint32_t findMasksByPattern(uint32x4_t const usedBitsMask, uint32x4_t const expectedBitsMask) const;

	inline uint8x16_t broadcastToSIMDRegister(uint8_t const mask) const;
	inline uint16x8_t broadcastToSIMDRegister(uint16_t const mask) const;
	inline uint32x4_t broadcastToSIMDRegister(uint32_t const mask) const;
#endif

public:

	/**
	 * This method determines all entries which are contained in a common subtree.
	 * The subtree is defined, by all prefix bits used and the expected prefix bits value
	 *
	 * @param usedPrefixBitsPattern a partial key pattern, which has all bits set, which correspond to parent BiNodes of the  regarding subtree
	 * @param expectedPrefixBits  a partial key pattern, which has all bits set, according to the paths taken from the root node to the parent BiNode of the regarding subtree
	 * @return a resulting bit mask with each bit represent whether the corresponding entry is part of the requested subtree or not.
	 */
	inline uint32_t getAffectedSubtreeMask(PartialKeyType usedPrefixBitsPattern, PartialKeyType const expectedPrefixBits) const {
		//assumed this should be the same as
		//affectedMaskBitsRegister = broadcastToSIMDRegister(affectedBitsMask)
		//expectedMaskBitsRegister = broadcastToSIMDRegister(expectedMaskBits)
		//return findMasksByPattern(affectedMaskBitsRegister, expectedMaskBitsRegister) & usedEntriesMask;

		__m256i prefixBITSSIMDMask = broadcastToSIMDRegister(usedPrefixBitsPattern);
		__m256i prefixMask = broadcastToSIMDRegister(expectedPrefixBits);

		unsigned int affectedSubtreeMask = findMasksByPattern(prefixBITSSIMDMask, prefixMask);

		//uint affectedSubtreeMask = findMasksByPattern(mEntries[entryIndex] & subtreePrefixMask) & usedEntriesMask;
		//at least the zero mask must match
		assert(affectedSubtreeMask != 0);

		return affectedSubtreeMask;
	}

	/**
	 *
	 * in the case of the following tree structure:
	 *               d
	 *            /    \
	 *           b      f
	 *          / \    / \
	 *         a  c   e  g
	 * Index   0  1   2  3
	 *
	 * If the provided index is 2 corresponding the smallest common subtree containing e consists of the nodes { e, f, g }
	 * and therefore the discriminative bit value for this entry is 0 (in the left side of the subtree).
	 *
	 * If the provided index is 1 corresponding to entry c, the smallest common subtree containing c consists of the nodes { a, b, c }
	 * and therefore the discriminative bit value of this entries 1 (in the right side of the subtree).
	 *
	 * in the case of the following tree structure
	 *                    f
	 *                 /    \
	 *                d      g
	 *              /   \
	 *             b     e
	 *            / \
	 *           a   c
	 * Index     0   1   2   3
	 *
	 * If the provided index is 2 correspondig to entry e, the smallest common subtree containing e consists of the nodes { a, b, c, d, e }
	 * As e is on the right side of this subtree, the discriminative bit's value in this case is 1
	 *
	 *
	 * @param indexOfEntry The index of the entry to obtain the discriminative bits value for
	 * @return The discriminative bit value of the discriminative bit discriminating an entry from its direct neighbour (regardless if the direct neighbour is an inner or a leaf node).
	 */
	inline bool determineValueOfDiscriminatingBit(size_t indexOfEntry, size_t mNumberEntries) const {
		bool discriminativeBitValue;

		if(indexOfEntry == 0) {
			discriminativeBitValue = false;
		} else if(indexOfEntry == (mNumberEntries - 1)) {
			discriminativeBitValue = true;
		} else {
			//Be aware that the masks are not order preserving, as the bits may not be in the correct order little vs. big endian and several included bytes
			discriminativeBitValue = (mEntries[indexOfEntry - 1]&mEntries[indexOfEntry]) >= (mEntries[indexOfEntry]&mEntries[indexOfEntry + 1]);
		}
		return discriminativeBitValue;
	}

	/**
	 * Get Relevant bits detects the key bits used for discriminating new entries in the given range.
	 * These bits are determined by comparing successing masks in this range.
	 * Whenever a mask has a bit set which is not set in its predecessor these bit is added to the set of relevant bits.
	 * The reason is that if masks are stored in an orderpreserving way for a mask to be large than its predecessor it has to set
	 * exactly one more bit.
	 * By using this algorithm the bits of the first mask occuring in the range of masks are always ignored.
	 *
	 * @param firstIndexInRange the first index of the range of entries to determine the relevant bits for
	 * @param numberEntriesInRange the number entries in the range of entries to use for determining the relevant bits
	 * @return a mask with only the relevant bits set.
	 */
	inline PartialKeyType getRelevantBitsForRange(uint32_t const firstIndexInRange, uint32_t const numberEntriesInRange) const {
		PartialKeyType relevantBits = 0;

		uint32_t firstIndexOutOfRange = firstIndexInRange + numberEntriesInRange;
		for(uint32_t i = firstIndexInRange + 1; i < firstIndexOutOfRange; ++i) {
			relevantBits |= (mEntries[i] & ~mEntries[i - 1]);
		}
		return relevantBits;
	}

	/**
	 * Gets a partial key which has all discriminative bits set, which are required to distinguish all but the entry, which is intended to be removed.
	 *
	 * @param numberEntries the total number of entries
	 * @param indexOfEntryToIgnore  the index of the entry, which shall be removed
	 * @return partial key which has all neccessary discriminative bits set
	 */
	inline PartialKeyType getRelevantBitsForAllExceptOneEntry(uint32_t const numberEntries, uint32_t indexOfEntryToIgnore) const {
		size_t numberEntriesInFirstRange = indexOfEntryToIgnore + static_cast<size_t>(!determineValueOfDiscriminatingBit(indexOfEntryToIgnore, numberEntries));

		PartialKeyType relevantBitsInFirstPart = getRelevantBitsForRange(0, numberEntriesInFirstRange);
		PartialKeyType relevantBitsInSecondPart = getRelevantBitsForRange(numberEntriesInFirstRange, numberEntries - numberEntriesInFirstRange);
		return relevantBitsInFirstPart | relevantBitsInSecondPart;
	}

	static inline uint16_t estimateSize(uint16_t numberEntries) {
		return alignToNextHighestValueDivisableBy8(numberEntries * sizeof(PartialKeyType));
	}

	/**
	 * @param maskBitMapping maps from the absoluteBitPosition to its maskPosition
	 */
	inline void printMasks(uint32_t maskOfEntriesToPrint, std::map<uint16_t, uint16_t> const & maskBitMapping, std::ostream & outputStream = std::cout) const {
		while(maskOfEntriesToPrint > 0) {
			uint entryIndex = __tzcnt_u32(maskOfEntriesToPrint);
			std::bitset<sizeof(PartialKeyType) * 8> maskBits(mEntries[entryIndex]);
			outputStream << "mask[" << entryIndex << "] = \toriginal: " << maskBits << "\tmapped: ";
			printMaskWithMapping(mEntries[entryIndex], maskBitMapping, outputStream);
			outputStream << std::endl;
			maskOfEntriesToPrint &= (~(1u << entryIndex));
		}
	}

	static inline void printMaskWithMapping(PartialKeyType mask, std::map<uint16_t, uint16_t> const & maskBitMapping, std::ostream & outputStream) {
		std::bitset<convertBytesToBits(sizeof(PartialKeyType))> maskBits(mask);
		for(auto mapEntry : maskBitMapping) {
			uint64_t maskBitIndex = mapEntry.second;
			outputStream << maskBits[maskBitIndex];
		}
	}


	inline void printMasks(uint32_t maskOfEntriesToPrint, std::ostream & outputStream = std::cout) const {
		while(maskOfEntriesToPrint > 0) {
			uint entryIndex = __tzcnt_u32(maskOfEntriesToPrint);

			std::bitset<sizeof(PartialKeyType) * 8> maskBits(mEntries[entryIndex]);
			outputStream << "mask[" << entryIndex << "] = " << maskBits << std::endl;

			maskOfEntriesToPrint &= (~(1u << entryIndex));
		}
	}

private:
	// Prevent heap allocation
	void * operator new   (size_t) = delete;
	void * operator new[] (size_t) = delete;
	void operator delete[] (void*) = delete;
};

template<typename PartialKeyType> void* SparsePartialKeys<PartialKeyType>::operator new (size_t /* baseSize */, uint16_t const numberEntries) {
	assert(numberEntries >= 2);

	constexpr size_t paddingElements = (32 - 8)/sizeof(PartialKeyType);
	size_t estimatedNumberElements = estimateSize(numberEntries)/sizeof(PartialKeyType);


	return new PartialKeyType[estimatedNumberElements + paddingElements];
};
template<typename PartialKeyType> void SparsePartialKeys<PartialKeyType>::operator delete (void * rawMemory) {
	PartialKeyType* masks = reinterpret_cast<PartialKeyType*>(rawMemory);
	delete [] masks;
}

template<>
inline __attribute__((always_inline)) uint32_t SparsePartialKeys<uint8_t>::search(uint8_t const uncompressedSearchMask) const {
#if (__x86__ || __x86_64__)
	__m256i searchRegister = _mm256_set1_epi8(uncompressedSearchMask); //2 instr

	__m256i haystack = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries)); //3 instr
#ifdef USE_AVX512
	uint32_t const resultMask = _mm256_cmpeq_epi8_mask (_mm256_and_si256(haystack, searchRegister), haystack);
#else
	__m256i searchResult = _mm256_cmpeq_epi8(_mm256_and_si256(haystack, searchRegister), haystack);
	uint32_t const resultMask = static_cast<uint32_t>(_mm256_movemask_epi8(searchResult));
#endif
#else
	uint8x16_t searchRegister = vmovq_n_u8(uncompressedSearchMask);
	uint32_t resultMask = 0;
	for(int n=0; n<2; n++){
	    uint8x16_t haystack = vld1q_u8(mEntries + n*16);
	    uint8x16_t searchResult = vceqq_u8(vandq_u8(haystack, searchRegister), haystack);
	    for(int i=0; i<16; i++){
		resultMask |= (searchResult[i] << (sizeof(uint32_t)*8 - n*16 - i - 1));
	    }
	}
#endif
	return resultMask;
}

template<>
inline __attribute__((always_inline)) uint32_t SparsePartialKeys<uint16_t>::search(uint16_t const uncompressedSearchMask) const {
#if (__x86__ || __x86_64__)
#ifdef USE_AVX512
	__m512i searchRegister = _mm512_set1_epi16(uncompressedSearchMask); //2 instr
	__m512i haystack = _mm512_loadu_si512(reinterpret_cast<__m512i const *>(mEntries)); //3 instr
	return static_cast<uint32_t>(_mm512_cmpeq_epi16_mask(_mm512_and_si512(haystack, searchRegister), haystack));
#else
	__m256i searchRegister = _mm256_set1_epi16(uncompressedSearchMask); //2 instr

	__m256i haystack1 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries)); //3 instr
	__m256i haystack2 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 16)); //4 instr

	__m256i const perm_mask = _mm256_set_epi32(7, 6, 3, 2, 5, 4, 1, 0); //35 instr

	__m256i searchResult1 = _mm256_cmpeq_epi16(_mm256_and_si256(haystack1, searchRegister), haystack1);
	__m256i searchResult2 = _mm256_cmpeq_epi16(_mm256_and_si256(haystack2, searchRegister), haystack2);

	__m256i intermediateResult = _mm256_permutevar8x32_epi32(_mm256_packs_epi16( //43 + 6 = 49
		searchResult1, searchResult2
	), perm_mask);

	return static_cast<uint32_t>(_mm256_movemask_epi8(intermediateResult));
#endif
#else
	uint16x8_t searchRegister = vmovq_n_u16(uncompressedSearchMask);
	uint16x8_t haystack1 = vld1q_u16(mEntries);
	uint16x8_t haystack2 = vld1q_u16(mEntries + 8);
	uint16x8_t haystack3 = vld1q_u16(mEntries + 16);
	uint16x8_t haystack4 = vld1q_u16(mEntries + 24);

	uint16x8_t searchResult1 = vceqq_u16(vandq_u16(haystack1, searchRegister), haystack1);
	uint16x8_t searchResult2 = vceqq_u16(vandq_u16(haystack2, searchRegister), haystack2);
	uint16x8_t searchResult3 = vceqq_u16(vandq_u16(haystack3, searchRegister), haystack3);
	uint16x8_t searchResult4 = vceqq_u16(vandq_u16(haystack4, searchRegister), haystack4);

	uint32_t resultMask = 0;
	for(int i=0; i<8; i++){
	    resultMask |= (searchResult4[i] << (sizeof(uint32_t)*8 - i - 1));
	    resultMask |= (searchResult3[i] << (sizeof(uint32_t)*8 - 8 - i - 1));
	    resultMask |= (searchResult2[i] << (sizeof(uint32_t)*8 - 16 - i - 1));
	    resultMask |= (searchResult1[i] << (sizeof(uint32_t)*8 - 24 - i - 1));
	}

	return resultMask;
#endif
}

template<>
inline __attribute__((always_inline)) uint32_t SparsePartialKeys<uint32_t>::search(uint32_t const uncompressedSearchMask) const {
#if (__x86__ || __x86_64__)
	__m256i searchRegister = _mm256_set1_epi32(uncompressedSearchMask); //2 instr

	__m256i haystack1 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries));
	__m256i haystack2 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 8));
	__m256i haystack3 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 16));
	__m256i haystack4 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 24)); //27 instr

	__m256i const perm_mask = _mm256_set_epi32(7, 6, 3, 2, 5, 4, 1, 0); //35 instr

	__m256i searchResult1 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack1, searchRegister), haystack1);
	__m256i searchResult2 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack2, searchRegister), haystack2);
	__m256i searchResult3 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack3, searchRegister), haystack3);
	__m256i searchResult4 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack4, searchRegister), haystack4); //35 + 8 = 43

	__m256i intermediateResult = _mm256_permutevar8x32_epi32(_mm256_packs_epi16( //43 + 6 = 49
		_mm256_permutevar8x32_epi32(_mm256_packs_epi32(searchResult1, searchResult2), perm_mask),
		_mm256_permutevar8x32_epi32(_mm256_packs_epi32(searchResult3, searchResult4), perm_mask)
	), perm_mask);

	return static_cast<uint32_t>(_mm256_movemask_epi8(intermediateResult));
#else
	uint32x4_t searchRegister = vmovq_n_u32(uncompressedSearchMask);

	uint32x4_t haystack1 = vldq_u32(mEntries);
	uint32x4_t haystack2 = vldq_u32(mEntries + 4);
	uint32x4_t haystack3 = vldq_u32(mEntries + 8);
	uint32x4_t haystack4 = vldq_u32(mEntries + 12);
	uint32x4_t haystack5 = vldq_u32(mEntries + 16);
	uint32x4_t haystack6 = vldq_u32(mEntries + 20);
	uint32x4_t haystack7 = vldq_u32(mEntries + 24);
	uint32x4_t haystack8 = vldq_u32(mEntries + 28);
	
	uint32x4_t searchResult1 = vceqq_u32(vandq_u32(haystack1, searchRegister), haystack1);
	uint32x4_t searchResult2 = vceqq_u32(vandq_u32(haystack2, searchRegister), haystack2);
	uint32x4_t searchResult3 = vceqq_u32(vandq_u32(haystack3, searchRegister), haystack3);
	uint32x4_t searchResult4 = vceqq_u32(vandq_u32(haystack4, searchRegister), haystack4);
	uint32x4_t searchResult5 = vceqq_u32(vandq_u32(haystack5, searchRegister), haystack5);
	uint32x4_t searchResult6 = vceqq_u32(vandq_u32(haystack6, searchRegister), haystack6);
	uint32x4_t searchResult7 = vceqq_u32(vandq_u32(haystack7, searchRegister), haystack7);
	uint32x4_t searchResult8 = vceqq_u32(vandq_u32(haystack8, searchRegister), haystack8);

	uint32_t resultMask = 0;
	for(int i=0; i<4; i++){
	    resultMask |= (searchResult8[i] << (sizeof(uint32_t)*8 - i - 1));
	    resultMask |= (searchResult7[i] << (sizeof(uint32_t)*8 - 4 - i - 1));
	    resultMask |= (searchResult6[i] << (sizeof(uint32_t)*8 - 8 - i - 1));
	    resultMask |= (searchResult5[i] << (sizeof(uint32_t)*8 - 12 - i - 1));
	    resultMask |= (searchResult4[i] << (sizeof(uint32_t)*8 - 16 - i - 1));
	    resultMask |= (searchResult3[i] << (sizeof(uint32_t)*8 - 20 - i - 1));
	    resultMask |= (searchResult2[i] << (sizeof(uint32_t)*8 - 24 - i - 1));
	    resultMask |= (searchResult1[i] << (sizeof(uint32_t)*8 - 28 - i - 1));
	}
	
	return resultMask;
#endif	    
}

template<>
#if (__x86__ || __x86_64__)
inline uint32_t SparsePartialKeys<uint8_t>::findMasksByPattern(__m256i consideredBitsRegister, __m256i expectedBitsRegister) const {
	__m256i haystack = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries)); //3 instr
	__m256i searchResult = _mm256_cmpeq_epi8(_mm256_and_si256(haystack, consideredBitsRegister), expectedBitsRegister);
	return static_cast<uint32_t>(_mm256_movemask_epi8(searchResult));
}
#else
inline uint32_t SparsePartialKeys<uint8_t>::findMasksByPattern(uint8x16_t consideredBitsRegister, uint8x16_t expectedBitsRegister) const{
	uint8x16_t haystack1 = vmovq_u8(mEntries);
	uint8x16_t haystack2 = vmovq_u8(mEntries + 16);

	uin8x16_t searchResult1 = vceqq_u8(vandq_u8(haystack1, consideredBitsRegister), expectedBitsRegister);
	uin8x16_t searchResult2 = vceqq_u8(vandq_u8(haystack2, consideredBitsRegister), expectedBitsRegister);
	
	uint32_t resultMask = 0;
	for(int i=0; i<16; i++){
	    resultMask |= (searchResult2[i] << (sizeof(uint32_t)*8 - i - 1));
	    resultMask |= (searchResult1[i] << (sizeof(uint32_t)*8 - 16 - i - 1));
	}
	return resultMask;
}
#endif

template<>
#if (__x86__ || __x86_64__)
inline uint32_t SparsePartialKeys<uint16_t>::findMasksByPattern(__m256i consideredBitsRegister, __m256i expectedBitsRegister) const {
	__m256i haystack1 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries)); //3 instr
	__m256i haystack2 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 16)); //4 instr

	__m256i const perm_mask = _mm256_set_epi32(7, 6, 3, 2, 5, 4, 1, 0); //35 instr

	__m256i searchResult1 = _mm256_cmpeq_epi16(_mm256_and_si256(haystack1, consideredBitsRegister), expectedBitsRegister);
	__m256i searchResult2 = _mm256_cmpeq_epi16(_mm256_and_si256(haystack2, consideredBitsRegister), expectedBitsRegister);

	__m256i intermediateResult = _mm256_permutevar8x32_epi32(_mm256_packs_epi16( //43 + 6 = 49
		searchResult1, searchResult2
	), perm_mask);

	return static_cast<uint32_t>(_mm256_movemask_epi8(intermediateResult));
}
#else
inline uint32_t SparsePartialKeys<uint16_t>::findMasksByPattern(uint16x8_t consideredBitsRegister, uint16x8_t expectedBitsRegister) const{
	uint16x8_t haystack1 = vmovq_u16(mEntries);
	uint16x8_t haystack2 = vmovq_u16(mEntries + 8);
	uint16x8_t haystack3 = vmovq_u16(mEntries + 16);
	uint16x8_t haystack4 = vmovq_u16(mEntries + 24);

	uin16x8_t searchResult1 = vceqq_u16(vandq_u16(haystack1, consideredBitsRegister), expectedBitsRegister);
	uin16x8_t searchResult2 = vceqq_u16(vandq_u16(haystack2, consideredBitsRegister), expectedBitsRegister);
	uin16x8_t searchResult3 = vceqq_u16(vandq_u16(haystack3, consideredBitsRegister), expectedBitsRegister);
	uin16x8_t searchResult4 = vceqq_u16(vandq_u16(haystack4, consideredBitsRegister), expectedBitsRegister);
	
	uint32_t resultMask = 0;
	for(int i=0; i<16; i++){
	    resultMask |= (searchResult4[i] << (sizeof(uint32_t)*8 - i - 1));
	    resultMask |= (searchResult3[i] << (sizeof(uint32_t)*8 - 8 - i - 1));
	    resultMask |= (searchResult2[i] << (sizeof(uint32_t)*8 - 16 - i - 1));
	    resultMask |= (searchResult1[i] << (sizeof(uint32_t)*8 - 24 - i - 1));
	}
	return resultMask;
}
#endif

template<>
#if (__x86__ || __x86_64__)
inline uint32_t SparsePartialKeys<uint32_t>::findMasksByPattern(__m256i consideredBitsRegister, __m256i expectedBitsRegister) const {
	__m256i haystack1 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries));
	__m256i haystack2 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 8));
	__m256i haystack3 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 16));
	__m256i haystack4 = _mm256_loadu_si256(reinterpret_cast<__m256i const *>(mEntries + 24)); //27 instr

	__m256i const perm_mask = _mm256_set_epi32(7, 6, 3, 2, 5, 4, 1, 0); //35 instr

	__m256i searchResult1 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack1, consideredBitsRegister), expectedBitsRegister);
	__m256i searchResult2 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack2, consideredBitsRegister), expectedBitsRegister);
	__m256i searchResult3 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack3, consideredBitsRegister), expectedBitsRegister);
	__m256i searchResult4 = _mm256_cmpeq_epi32(_mm256_and_si256(haystack4, consideredBitsRegister), expectedBitsRegister); //35 + 8 = 43

	__m256i intermediateResult = _mm256_permutevar8x32_epi32(_mm256_packs_epi16( //43 + 6 = 49
		_mm256_permutevar8x32_epi32(_mm256_packs_epi32(searchResult1, searchResult2), perm_mask),
		_mm256_permutevar8x32_epi32(_mm256_packs_epi32(searchResult3, searchResult4), perm_mask)
	), perm_mask);

	return static_cast<uint32_t>(_mm256_movemask_epi8(intermediateResult));
}
#else
inline uint32_t SparsePartialKeys<uint32_t>::findMasksByPattern(uint32x4_t consideredBitsRegister, uint32x4_t expectedBitsRegister) const{
	uint32x4_t haystack1 = vmovq_u32(mEntries);
	uint32x4_t haystack2 = vmovq_u32(mEntries + 4);
	uint32x4_t haystack3 = vmovq_u32(mEntries + 8);
	uint32x4_t haystack4 = vmovq_u32(mEntries + 12);
	uint32x4_t haystack5 = vmovq_u32(mEntries + 16);
	uint32x4_t haystack6 = vmovq_u32(mEntries + 20);
	uint32x4_t haystack7 = vmovq_u32(mEntries + 24);
	uint32x4_t haystack8 = vmovq_u32(mEntries + 28);

	uin32x4_t searchResult1 = vceqq_u32(vandq_u32(haystack1, consideredBitsRegister), expectedBitsRegister);
	uin32x4_t searchResult2 = vceqq_u32(vandq_u32(haystack2, consideredBitsRegister), expectedBitsRegister);
	uin32x4_t searchResult3 = vceqq_u32(vandq_u32(haystack3, consideredBitsRegister), expectedBitsRegister);
	uin32x4_t searchResult4 = vceqq_u32(vandq_u32(haystack4, consideredBitsRegister), expectedBitsRegister);
	uin32x4_t searchResult5 = vceqq_u32(vandq_u32(haystack5, consideredBitsRegister), expectedBitsRegister);
	uin32x4_t searchResult6 = vceqq_u32(vandq_u32(haystack6, consideredBitsRegister), expectedBitsRegister);
	uin32x4_t searchResult7 = vceqq_u32(vandq_u32(haystack7, consideredBitsRegister), expectedBitsRegister);
	uin32x4_t searchResult8 = vceqq_u32(vandq_u32(haystack8, consideredBitsRegister), expectedBitsRegister);
	
	uint32_t resultMask = 0;
	for(int i=0; i<4; i++){
	    resultMask |= (searchResult8[i] << (sizeof(uint32_t)*8 - i - 1));
	    resultMask |= (searchResult7[i] << (sizeof(uint32_t)*8 - 4 - i - 1));
	    resultMask |= (searchResult6[i] << (sizeof(uint32_t)*8 - 8 - i - 1));
	    resultMask |= (searchResult5[i] << (sizeof(uint32_t)*8 - 12 - i - 1));
	    resultMask |= (searchResult4[i] << (sizeof(uint32_t)*8 - 16 - i - 1));
	    resultMask |= (searchResult3[i] << (sizeof(uint32_t)*8 - 20 - i - 1));
	    resultMask |= (searchResult2[i] << (sizeof(uint32_t)*8 - 24 - i - 1));
	    resultMask |= (searchResult1[i] << (sizeof(uint32_t)*8 - 28 - i - 1));
	}
	return resultMask;
}
#endif


template<>
#if (__x86__ || __x86_64__)
inline __m256i SparsePartialKeys<uint8_t>::broadcastToSIMDRegister(uint8_t const mask) const {
	return _mm256_set1_epi8(mask);
}
#else
inline uint8x16_t SparsePartialKeys<uint8_t>::broadcastToSIMDRegister(uint8_t const mask) const {
	return vmovq_u8(mask);
}
#endif

template<>
#if (__x86__ || __x86_64__)
inline __m256i SparsePartialKeys<uint16_t>::broadcastToSIMDRegister(uint16_t const mask) const {
	return _mm256_set1_epi16(mask);
}
#else
inline uint16x8_t SparsePartialKeys<uint8_t>::broadcastToSIMDRegister(uint16_t const mask) const {
	return vmovq_u16(mask);
}
#endif


template<>
#if (__x86__ || __x86_64__)
inline __m256i SparsePartialKeys<uint32_t>::broadcastToSIMDRegister(uint32_t const mask) const {
	return _mm256_set1_epi32(mask);
}
#else
inline uint32x8_t SparsePartialKeys<uint8_t>::broadcastToSIMDRegister(uint32_t const mask) const {
	return vmovq_u32(mask);
}
#endif


}}

#endif
