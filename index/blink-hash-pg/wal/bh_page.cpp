/*
 * bh_page.cpp — 8 KB page implementation + serialize/deserialize
 *
 * Phase 6.2 of IMPLEMENTATION_SEQUENCE.md
 */

#include "bh_page.h"
#include "bh_node_map.h"

/* Tree node headers — needed for serialization/deserialization */
#include "inode.h"
#include "lnode.h"

#include <algorithm>
#include <cstdio>
#include <cstring>

namespace BLINK_HASH {
namespace WAL {



static uint32_t crc32c_table[256];
static bool crc32c_inited = false;

static void crc32c_init_table() {
    if (crc32c_inited) return;
    constexpr uint32_t POLY = 0x82F63B78;   /* CRC-32C polynomial */
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1)
                crc = (crc >> 1) ^ POLY;
            else
                crc >>= 1;
        }
        crc32c_table[i] = crc;
    }
    crc32c_inited = true;
}

static uint32_t crc32c(const void* data, size_t len) {
    crc32c_init_table();
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++)
        crc = crc32c_table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFF;
}



uint32_t bh_page_t::compute_checksum() const {
    return crc32c(payload, BH_PAGE_PAYLOAD);
}


struct ExtentWriter {
    bh_page_t* pages;
    uint16_t   max_pages;
    uint16_t   cur_page;
    uint16_t   cur_off;

    ExtentWriter(bh_page_t* p, uint16_t mp)
        : pages(p), max_pages(mp), cur_page(0), cur_off(0) {}

    void write(const void* data, size_t len) {
        const char* src = static_cast<const char*>(data);
        size_t written = 0;
        while (written < len) {
            size_t avail = BH_PAGE_PAYLOAD - cur_off;
            size_t chunk = std::min(avail, len - written);
            std::memcpy(pages[cur_page].payload + cur_off,
                        src + written, chunk);
            cur_off += static_cast<uint16_t>(chunk);
            written += chunk;
            if (cur_off == BH_PAGE_PAYLOAD) {
                pages[cur_page].header.payload_used = cur_off;
                cur_page++;
                cur_off = 0;
            }
        }
    }

    uint16_t finalize() {
        if (cur_off > 0) {
            pages[cur_page].header.payload_used = cur_off;
            return cur_page + 1;
        }
        return cur_page;
    }
};

struct ExtentReader {
    const bh_page_t* pages;
    uint16_t num_pages;
    uint16_t cur_page;
    uint16_t cur_off;

    ExtentReader(const bh_page_t* p, uint16_t np)
        : pages(p), num_pages(np), cur_page(0), cur_off(0) {}

    void read(void* dest, size_t len) {
        char* dst = static_cast<char*>(dest);
        size_t done = 0;
        while (done < len) {
            uint16_t avail = pages[cur_page].header.payload_used - cur_off;
            size_t chunk = std::min(static_cast<size_t>(avail), len - done);
            std::memcpy(dst + done,
                        pages[cur_page].payload + cur_off, chunk);
            cur_off += static_cast<uint16_t>(chunk);
            done += chunk;
            if (cur_off >= pages[cur_page].header.payload_used) {
                cur_page++;
                cur_off = 0;
            }
        }
    }
};

/*
 *  serialize_inode
 *
 *  Page payload format:
 *    [cnt:4][key_size:4]
 *    [leftmost_ptr_node_id:8][sibling_ptr_node_id:8]
 *    [high_key: key_size]
 *    for each entry (cnt): [key: key_size][child_node_id:8]
 *
 *  Returns bytes written into payload.
 */

template <typename Key_t>
uint16_t serialize_inode(const void* inode_raw, bh_page_t* page) {
    const auto* node = static_cast<const inode_t<Key_t>*>(inode_raw);
    char* dst = page->payload;
    uint16_t off = 0;

  
    int32_t cnt = node->cnt;
    std::memcpy(dst + off, &cnt, 4); off += 4;

    uint32_t key_size = sizeof(Key_t);
    std::memcpy(dst + off, &key_size, 4); off += 4;

    uint64_t left_id = node->leftmost_ptr
                       ? node->leftmost_ptr->node_id : 0;
    std::memcpy(dst + off, &left_id, 8); off += 8;

    uint64_t sib_id = node->sibling_ptr
                      ? node->sibling_ptr->node_id : 0;
    std::memcpy(dst + off, &sib_id, 8); off += 8;

    std::memcpy(dst + off, &node->high_key, sizeof(Key_t));
    off += sizeof(Key_t);

    for (int i = 0; i < cnt; i++) {
        const auto& e = node->get_entry(i);
        std::memcpy(dst + off, &e.key, sizeof(Key_t));
        off += sizeof(Key_t);

        uint64_t child_id = e.value ? e.value->node_id : 0;
        std::memcpy(dst + off, &child_id, 8); off += 8;
    }

    page->header.payload_used = off;
    return off;
}

/* 
 *  deserialize_inode
 *
 *  Reconstruct an inode_t from a page.
 *  Pointer fields (leftmost_ptr, sibling_ptr, child entries) are
 *  resolved via the NodeMap.  If nmap is null, pointers are left null.
 */

template <typename Key_t>
void* deserialize_inode(const bh_page_t* page, NodeMap* nmap) {
    const char* src = page->payload;
    uint16_t off = 0;

    int32_t cnt;
    std::memcpy(&cnt, src + off, 4); off += 4;

    uint32_t key_size;
    std::memcpy(&key_size, src + off, 4); off += 4;

    uint64_t left_id;
    std::memcpy(&left_id, src + off, 8); off += 8;

    uint64_t sib_id;
    std::memcpy(&sib_id, src + off, 8); off += 8;

    Key_t high_key;
    std::memcpy(&high_key, src + off, sizeof(Key_t)); off += sizeof(Key_t);

  
    node_t* left_ptr = (nmap && left_id) ? nmap->resolve(left_id) : nullptr;
    node_t* sib_ptr  = (nmap && sib_id)  ? nmap->resolve(sib_id)  : nullptr;

    /* Allocate inode using constructor (sibling, cnt, left, level, high_key). */
    auto* node = new inode_t<Key_t>(sib_ptr, cnt, left_ptr,
                                    page->header.level, high_key);
    node->node_id = page->header.node_id;

    /* Populate entries. */
    for (int i = 0; i < cnt; i++) {
        Key_t key;
        std::memcpy(&key, src + off, sizeof(Key_t)); off += sizeof(Key_t);

        uint64_t child_id;
        std::memcpy(&child_id, src + off, 8); off += 8;

        node_t* child_ptr = (nmap && child_id)
                            ? nmap->resolve(child_id) : nullptr;
        node->set_entry(i, key, child_ptr);
    }

    return static_cast<void*>(node);
}

/* 
 *  serialize_btree_leaf
 *
 *  Page payload format:
 *    [cnt:4][high_key: sizeof(Key_t)][sibling_node_id:8]
 *    for each entry (cnt): [key: sizeof(Key_t)][value: sizeof(Value_t)]
 *
 *  Returns bytes written.
 */

template <typename Key_t, typename Value_t>
uint16_t serialize_btree_leaf(const void* leaf_raw, bh_page_t* page) {
    const auto* leaf =
        static_cast<const lnode_btree_t<Key_t, Value_t>*>(leaf_raw);
    char* dst = page->payload;
    uint16_t off = 0;


    int32_t cnt = leaf->get_cnt();
    std::memcpy(dst + off, &cnt, 4); off += 4;


    std::memcpy(dst + off, &leaf->high_key, sizeof(Key_t));
    off += sizeof(Key_t);


    uint64_t sib_id = leaf->sibling_ptr
                      ? leaf->sibling_ptr->node_id : 0;
    std::memcpy(dst + off, &sib_id, 8); off += 8;


    for (int i = 0; i < cnt; i++) {
        const auto& e = leaf->get_entry(i);
        std::memcpy(dst + off, &e.key, sizeof(Key_t));
        off += sizeof(Key_t);
        std::memcpy(dst + off, &e.value, sizeof(Value_t));
        off += sizeof(Value_t);
    }

    page->header.payload_used = off;
    return off;
}


template <typename Key_t, typename Value_t>
void* deserialize_btree_leaf(const bh_page_t* page) {
    const char* src = page->payload;
    uint16_t off = 0;

    int32_t cnt;
    std::memcpy(&cnt, src + off, 4); off += 4;

    Key_t high_key;
    std::memcpy(&high_key, src + off, sizeof(Key_t)); off += sizeof(Key_t);

    uint64_t sib_id;
    std::memcpy(&sib_id, src + off, 8); off += 8;


    auto* leaf = new lnode_btree_t<Key_t, Value_t>();
    leaf->high_key = high_key;
    leaf->node_id  = page->header.node_id;
    leaf->level    = page->header.level;
    leaf->cnt      = cnt;


    for (int i = 0; i < cnt; i++) {
        Key_t key;
        std::memcpy(&key, src + off, sizeof(Key_t)); off += sizeof(Key_t);

        Value_t val;
        std::memcpy(&val, src + off, sizeof(Value_t)); off += sizeof(Value_t);

        leaf->set_entry(i, key, val);
    }

    return static_cast<void*>(leaf);
}

/*
 *  serialize_hash_leaf
 *
 *  A 256 KB hash leaf spans ~32 pages (an "extent").
 *
 *  Byte-stream format (spread across extent pages):
 *    [sibling_node_id:8][left_sibling_node_id:8]
 *    [high_key: sizeof(Key_t)]
 *    [convert_state:1]
 *    For each bucket (cardinality):
 *      #ifdef LINKED:  [state:1]
 *      #ifdef FINGERPRINT: [fingerprints: entry_num]
 *      [entries: entry_num * sizeof(entry_t<K,V>)]
 *
 *  Returns number of pages consumed.
 * */

template <typename Key_t, typename Value_t>
uint16_t serialize_hash_leaf(const void* leaf_raw, bh_page_t* pages,
                             uint16_t max_pages) {
    const auto* leaf =
        static_cast<const lnode_hash_t<Key_t, Value_t>*>(leaf_raw);
    constexpr int num_buckets = lnode_hash_t<Key_t, Value_t>::cardinality;

    ExtentWriter ew(pages, max_pages);

    
    uint64_t sib_id = leaf->sibling_ptr
                      ? leaf->sibling_ptr->node_id : 0;
    uint64_t left_sib_id = leaf->left_sibling_ptr
                           ? leaf->left_sibling_ptr->node_id : 0;
    ew.write(&sib_id, 8);
    ew.write(&left_sib_id, 8);
    ew.write(&leaf->high_key, sizeof(Key_t));

    uint8_t conv_state = leaf->convert_state.load(std::memory_order_relaxed);
    ew.write(&conv_state, 1);

    /* ── Bucket data ── */
    for (int b = 0; b < num_buckets; b++) {
        const auto& bkt = leaf->get_bucket(b);

#ifdef LINKED
        uint8_t state = static_cast<uint8_t>(bkt.state);
        ew.write(&state, 1);
#endif

#ifdef FINGERPRINT
        ew.write(bkt.fingerprints, entry_num);
#endif

        ew.write(bkt.entry,
                 sizeof(entry_t<Key_t, Value_t>) * entry_num);
    }

    return ew.finalize();
}

/*
 *  deserialize_hash_leaf
 *
 *  Reconstruct a lnode_hash_t from an extent of pages.
 *  Sibling pointers must be resolved by the caller via NodeMap.
 **/

template <typename Key_t, typename Value_t>
void* deserialize_hash_leaf(const bh_page_t* pages, uint16_t num_pages) {
    constexpr int num_buckets = lnode_hash_t<Key_t, Value_t>::cardinality;

    ExtentReader er(pages, num_pages);

    /* ── Header ── */
    uint64_t sib_id, left_sib_id;
    er.read(&sib_id, 8);
    er.read(&left_sib_id, 8);

    Key_t high_key;
    er.read(&high_key, sizeof(Key_t));

    uint8_t conv_state;
    er.read(&conv_state, 1);

    /* Allocate hash leaf (constructor zeroes buckets). */
    auto* leaf = new lnode_hash_t<Key_t, Value_t>();
    leaf->high_key       = high_key;
    leaf->node_id        = pages[0].header.node_id;
    leaf->level          = pages[0].header.level;
    leaf->convert_state.store(conv_state, std::memory_order_relaxed);
    /* sibling_ptr/left_sibling_ptr left null — caller resolves */

    /* ── Bucket data ── */
    for (int b = 0; b < num_buckets; b++) {
        auto& bkt = leaf->get_bucket_mut(b);
        bkt.lock.store(0, std::memory_order_relaxed);

#ifdef LINKED
        uint8_t state;
        er.read(&state, 1);
        bkt.state = static_cast<typename bucket_t<Key_t, Value_t>::state_t>(state);
#endif

#ifdef FINGERPRINT
        er.read(bkt.fingerprints, entry_num);
#endif

        er.read(bkt.entry,
                sizeof(entry_t<Key_t, Value_t>) * entry_num);
    }

    return static_cast<void*>(leaf);
}


template uint16_t serialize_inode<key64_t>(const void*, bh_page_t*);
template void* deserialize_inode<key64_t>(const bh_page_t*, NodeMap*);

template uint16_t serialize_btree_leaf<key64_t, value64_t>(
    const void*, bh_page_t*);
template void* deserialize_btree_leaf<key64_t, value64_t>(
    const bh_page_t*);

template uint16_t serialize_hash_leaf<key64_t, value64_t>(
    const void*, bh_page_t*, uint16_t);
template void* deserialize_hash_leaf<key64_t, value64_t>(
    const bh_page_t*, uint16_t);


template uint16_t serialize_inode<StringKey>(const void*, bh_page_t*);
template void* deserialize_inode<StringKey>(const bh_page_t*, NodeMap*);

template uint16_t serialize_btree_leaf<StringKey, value64_t>(
    const void*, bh_page_t*);
template void* deserialize_btree_leaf<StringKey, value64_t>(
    const bh_page_t*);

template uint16_t serialize_hash_leaf<StringKey, value64_t>(
    const void*, bh_page_t*, uint16_t);
template void* deserialize_hash_leaf<StringKey, value64_t>(
    const bh_page_t*, uint16_t);

} 
} 