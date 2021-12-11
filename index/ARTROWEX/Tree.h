//
// Created by florian on 18.11.15.
//

#ifndef ART_ROWEX_TREE_H
#define ART_ROWEX_TREE_H
#include "N.h"

using namespace ART;

namespace ART_ROWEX {

    class Tree {
    public:
        using LoadKeyFunction = void (*)(TID tid, Key &key);

    private:
        N *const root;

        TID checkKey(const TID tid, const Key &k) const;

        LoadKeyFunction loadKey;

        Epoche epoche{256};

    public:
        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
            SkippedLevel
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
            SkippedLevel
        };
        enum class PCEqualsResults : uint8_t {
            BothMatch,
            Contained,
            NoMatch,
            SkippedLevel
        };
        static CheckPrefixResult checkPrefix(N* n, const Key &k, uint32_t &level);


        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   LoadKeyFunction loadKey);

        static PCCompareResults checkPrefixCompare(const N* n, const Key &k, uint32_t &level, LoadKeyFunction loadKey);

        static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key &start, const Key &end, LoadKeyFunction loadKey);

    public:
	#ifdef BREAKDOWN
	void get_breakdown(uint64_t&, uint64_t&, uint64_t&, uint64_t&, uint64_t&);
	#endif

        Tree(LoadKeyFunction loadKey);

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) { }

        ~Tree();

        ThreadInfo getThreadInfo();

        bool lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[], std::size_t resultLen,
                         std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;
        void insert(const Key &k, TID tid, ThreadInfo &epocheInfo);
        TID lookup(const Key &k, ThreadInfo &threadEpocheInfo) const;

        void remove(const Key &k, TID tid, ThreadInfo &epocheInfo);

	void footprint(uint64_t& meta, uint64_t& structural_data_occupied, uint64_t& structural_data_unoccupied, uint64_t& key_data_occupied, uint64_t& key_data_unoccupied);
    };
}
#endif //ART_ROWEX_TREE_H
