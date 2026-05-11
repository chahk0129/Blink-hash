/*
 * blinkhash_core.cpp — extern "C" bridge between PostgreSQL callbacks
 *                      and the C++ btree_t engine.
 *
 * All tree handles are type-erased as void*.  The key_class parameter
 * ('i' for integer, 's' for string) selects the correct template
 * specialization.
 */

#include "blinkhash_core.h"

#include "tree.h"
#include "common.h"
#include "bh_key.h"
#include "wal_emitter.h"
#include "bh_node_map.h"
#include "bh_page.h"

#include <cstdio>
#include <cstring>
#include <cassert>

using namespace BLINK_HASH;


extern "C" void* bh_tree_create(char key_class) {
    switch (key_class) {
        case 'i': return new btree_t<key64_t, value64_t>();
        case 's': return new btree_t<StringKey, value64_t>();
        default:
            fprintf(stderr, "bh_tree_create: unknown key_class '%c'\n",
                    key_class);
            return nullptr;
    }
}

extern "C" void bh_tree_destroy(void* tree, char key_class) {
    if (!tree) return;
    switch (key_class) {
        case 'i': delete static_cast<btree_t<key64_t, value64_t>*>(tree); break;
        case 's': delete static_cast<btree_t<StringKey, value64_t>*>(tree); break;
    }
}



extern "C" void* bh_get_thread_info(void* tree, char key_class) {
    /*
     * Return a heap-allocated ThreadInfo.
     * The caller must free it when done.
     * In PG context, this is stored in BHIndexState and freed
     * on relation close.
     */
    switch (key_class) {
        case 'i': {
            auto* t = static_cast<btree_t<key64_t, value64_t>*>(tree);
            auto* ti = new ThreadInfo(t->getThreadInfo());
            return ti;
        }
        case 's': {
            auto* t = static_cast<btree_t<StringKey, value64_t>*>(tree);
            auto* ti = new ThreadInfo(t->getThreadInfo());
            return ti;
        }
        default: return nullptr;
    }
}


static void bh_insert_impl(void* tree, char key_class,
                           const void* key_data, size_t key_len,
                           uint64_t value, void* thread_info,
                           uint64_t* node_id_out,
                           uint32_t* bucket_idx_out) {
    auto* ti = static_cast<ThreadInfo*>(thread_info);

    switch (key_class) {
        case 'i': {
            key64_t key = 0;
            memcpy(&key, key_data,
                   key_len < sizeof(key) ? key_len : sizeof(key));
            static_cast<btree_t<key64_t, value64_t>*>(tree)
                ->insert(key, value, *ti, node_id_out, bucket_idx_out);
            break;
        }
        case 's': {
            StringKey key;
            key.setFromBytes(static_cast<const char*>(key_data), key_len);
            static_cast<btree_t<StringKey, value64_t>*>(tree)
                ->insert(key, value, *ti, node_id_out, bucket_idx_out);
            break;
        }
    }
}

extern "C" void bh_insert(void* tree, char key_class,
                           const void* key_data, size_t key_len,
                           uint64_t value, void* thread_info) {
    bh_insert_impl(tree, key_class, key_data, key_len, value, thread_info,
                   nullptr, nullptr);
}

extern "C" void bh_insert_with_meta(void* tree, char key_class,
                                     const void* key_data, size_t key_len,
                                     uint64_t value, void* thread_info,
                                     uint64_t* node_id_out,
                                     uint32_t* bucket_idx_out) {
    bh_insert_impl(tree, key_class, key_data, key_len, value, thread_info,
                   node_id_out, bucket_idx_out);
}

extern "C" uint64_t bh_lookup(void* tree, char key_class,
                               const void* key_data, size_t key_len,
                               void* thread_info, int* found) {
    auto* ti = static_cast<ThreadInfo*>(thread_info);
    *found = 0;

    switch (key_class) {
        case 'i': {
            key64_t key = 0;
            memcpy(&key, key_data,
                   key_len < sizeof(key) ? key_len : sizeof(key));
            auto val = static_cast<btree_t<key64_t, value64_t>*>(tree)
                ->lookup(key, *ti);
            if (val != 0) { *found = 1; return val; }
            return 0;
        }
        case 's': {
            StringKey key;
            key.setFromBytes(static_cast<const char*>(key_data), key_len);
            auto val = static_cast<btree_t<StringKey, value64_t>*>(tree)
                ->lookup(key, *ti);
            if (val != 0) { *found = 1; return val; }
            return 0;
        }
        default: return 0;
    }
}

extern "C" int bh_update(void* tree, char key_class,
                          const void* key_data, size_t key_len,
                          uint64_t new_value, void* thread_info) {
    auto* ti = static_cast<ThreadInfo*>(thread_info);

    switch (key_class) {
        case 'i': {
            key64_t key = 0;
            memcpy(&key, key_data,
                   key_len < sizeof(key) ? key_len : sizeof(key));
            return static_cast<btree_t<key64_t, value64_t>*>(tree)
                ->update(key, new_value, *ti) ? 1 : 0;
        }
        case 's': {
            StringKey key;
            key.setFromBytes(static_cast<const char*>(key_data), key_len);
            return static_cast<btree_t<StringKey, value64_t>*>(tree)
                ->update(key, new_value, *ti) ? 1 : 0;
        }
        default: return 0;
    }
}

extern "C" int bh_remove(void* tree, char key_class,
                          const void* key_data, size_t key_len,
                          void* thread_info) {
    auto* ti = static_cast<ThreadInfo*>(thread_info);

    switch (key_class) {
        case 'i': {
            key64_t key = 0;
            memcpy(&key, key_data,
                   key_len < sizeof(key) ? key_len : sizeof(key));
            return static_cast<btree_t<key64_t, value64_t>*>(tree)
                ->remove(key, *ti) ? 1 : 0;
        }
        case 's': {
            StringKey key;
            key.setFromBytes(static_cast<const char*>(key_data), key_len);
            return static_cast<btree_t<StringKey, value64_t>*>(tree)
                ->remove(key, *ti) ? 1 : 0;
        }
        default: return 0;
    }
}


extern "C" int bh_range_lookup(void* tree, char key_class,
                                const void* min_key_data, size_t min_key_len,
                                int range,
                                uint64_t* buf,
                                void* thread_info) {
    auto* ti = static_cast<ThreadInfo*>(thread_info);

    switch (key_class) {
        case 'i': {
            key64_t key = 0;
            memcpy(&key, min_key_data,
                   min_key_len < sizeof(key) ? min_key_len : sizeof(key));
            return static_cast<btree_t<key64_t, value64_t>*>(tree)
                ->range_lookup(key, range, buf, *ti);
        }
        case 's': {
            StringKey key;
            key.setFromBytes(static_cast<const char*>(min_key_data),
                             min_key_len);
            return static_cast<btree_t<StringKey, value64_t>*>(tree)
                ->range_lookup(key, range, buf, *ti);
        }
        default: return 0;
    }
}

extern "C" int bh_height(void* tree, char key_class) {
    switch (key_class) {
        case 'i':
            return static_cast<btree_t<key64_t, value64_t>*>(tree)->height();
        case 's':
            return static_cast<btree_t<StringKey, value64_t>*>(tree)->height();
        default: return -1;
    }
}

extern "C" double bh_utilization(void* tree, char key_class) {
    switch (key_class) {
        case 'i':
            return static_cast<btree_t<key64_t, value64_t>*>(tree)->utilization();
        case 's':
            return static_cast<btree_t<StringKey, value64_t>*>(tree)->utilization();
        default: return 0.0;
    }
}

extern "C" void bh_print(void* tree, char key_class) {
    switch (key_class) {
        case 'i':
            static_cast<btree_t<key64_t, value64_t>*>(tree)->print();
            break;
        case 's':
            static_cast<btree_t<StringKey, value64_t>*>(tree)->print();
            break;
    }
}

extern "C" void bh_free_thread_info(void* thread_info) {
    delete static_cast<ThreadInfo*>(thread_info);
}

/* ═══════════════════════════════════════════════════════════════════
 *  bh_serialize_node / bh_deserialize_node
 *
 *  C-callable wrappers around the C++ serialize/deserialize templates
 *  in wal/bh_page.cpp.  Used by blinkhash_page.c to bridge between
 *  PG Page format and the standalone bh_page_t serialization.
 * ═══════════════════════════════════════════════════════════════════ */

using namespace BLINK_HASH::WAL;

extern "C" int bh_serialize_node(const void *node, char key_class,
                                 int node_type, void *page_buf,
                                 int max_pages)
{
    bh_page_t *pages = static_cast<bh_page_t *>(page_buf);

    switch (node_type) {
        case BH_NODE_TYPE_INODE: {
            uint16_t bytes = 0;
            if (key_class == 'i')
                bytes = serialize_inode<key64_t>(node, &pages[0]);
            else
                bytes = serialize_inode<StringKey>(node, &pages[0]);
            return (bytes > 0) ? 1 : 0;
        }
        case BH_NODE_TYPE_LNODE_BTREE: {
            uint16_t bytes = 0;
            if (key_class == 'i')
                bytes = serialize_btree_leaf<key64_t, value64_t>(
                            node, &pages[0]);
            else
                bytes = serialize_btree_leaf<StringKey, value64_t>(
                            node, &pages[0]);
            return (bytes > 0) ? 1 : 0;
        }
        case BH_NODE_TYPE_LNODE_HASH: {
            uint16_t np = 0;
            if (key_class == 'i')
                np = serialize_hash_leaf<key64_t, value64_t>(
                         node, pages, (uint16_t)max_pages);
            else
                np = serialize_hash_leaf<StringKey, value64_t>(
                         node, pages, (uint16_t)max_pages);
            return (int)np;
        }
        default:
            return 0;
    }
}

extern "C" void *bh_deserialize_node(const void *page_buf, int num_pages,
                                     char key_class, int node_type)
{
    const bh_page_t *pages = static_cast<const bh_page_t *>(page_buf);

    switch (node_type) {
        case BH_NODE_TYPE_INODE:
            if (key_class == 'i')
                return deserialize_inode<key64_t>(&pages[0], g_node_map);
            else
                return deserialize_inode<StringKey>(&pages[0], g_node_map);

        case BH_NODE_TYPE_LNODE_BTREE:
            if (key_class == 'i')
                return deserialize_btree_leaf<key64_t, value64_t>(&pages[0]);
            else
                return deserialize_btree_leaf<StringKey, value64_t>(&pages[0]);

        case BH_NODE_TYPE_LNODE_HASH:
            if (key_class == 'i')
                return deserialize_hash_leaf<key64_t, value64_t>(
                           pages, (uint16_t)num_pages);
            else
                return deserialize_hash_leaf<StringKey, value64_t>(
                           pages, (uint16_t)num_pages);

        default:
            return nullptr;
    }
}

/* ═══════════════════════════════════════════════════════════════════
 *  bh_bulk_delete — walk all leaf entries and remove dead tuples
 *
 *  Descends to the leftmost leaf via leftmost_ptr, then walks the
 *  sibling chain.  For each entry whose callback returns true,
 *  the key is collected and removed in a second pass (to avoid
 *  mutating the structure while iterating).
 * ═══════════════════════════════════════════════════════════════════ */

template <typename Key_t, typename Value_t>
static int64_t bulk_delete_impl(btree_t<Key_t, Value_t>* tree,
                                bh_delete_callback_fn callback,
                                void* cb_state,
                                ThreadInfo* ti)
{
    /* Find the leftmost leaf */
    node_t* cur = tree->get_root();
    if (!cur) return 0;

    while (cur->level > 0)
        cur = cur->leftmost_ptr;

    /* Collect keys to delete (can't mutate while scanning) */
    std::vector<Key_t> to_delete;
    to_delete.reserve(1024);

    while (cur != nullptr) {
        auto* leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);

        if (leaf->type == lnode_t<Key_t, Value_t>::BTREE_NODE) {
            auto* bl = static_cast<lnode_btree_t<Key_t, Value_t>*>(leaf);
            int cnt = bl->get_cnt();
            for (int i = 0; i < cnt; i++) {
                const auto& e = bl->get_entry(i);
                if (callback(static_cast<uint64_t>(e.value), cb_state))
                    to_delete.push_back(e.key);
            }
        } else {
            /* HASH_NODE: walk all buckets, all slots */
            auto* hl = static_cast<lnode_hash_t<Key_t, Value_t>*>(leaf);
            constexpr size_t num_buckets = lnode_hash_t<Key_t, Value_t>::cardinality;
            for (size_t b = 0; b < num_buckets; b++) {
                const auto& bkt = hl->get_bucket(b);
                for (int s = 0; s < entry_num; s++) {
#ifdef FINGERPRINT
                    if (bkt.fingerprints[s] == 0)
                        continue;   /* empty slot */
#else
                    if (bkt.entry[s].key == EMPTY<Key_t>)
                        continue;
#endif
                    if (callback(static_cast<uint64_t>(bkt.entry[s].value),
                                 cb_state))
                        to_delete.push_back(bkt.entry[s].key);
                }
            }
        }

        cur = cur->sibling_ptr;
    }

    /* Phase 2: remove collected keys */
    int64_t deleted = 0;
    for (auto& key : to_delete) {
        if (tree->remove(key, *ti))
            deleted++;
    }

    return deleted;
}

extern "C" int64_t bh_bulk_delete(void* tree, char key_class,
                                   bh_delete_callback_fn callback,
                                   void* cb_state,
                                   void* thread_info)
{
    auto* ti = static_cast<ThreadInfo*>(thread_info);
    switch (key_class) {
        case 'i':
            return bulk_delete_impl(
                static_cast<btree_t<key64_t, value64_t>*>(tree),
                callback, cb_state, ti);
        case 's':
            return bulk_delete_impl(
                static_cast<btree_t<StringKey, value64_t>*>(tree),
                callback, cb_state, ti);
        default:
            return 0;
    }
}