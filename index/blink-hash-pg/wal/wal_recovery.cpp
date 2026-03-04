/*

 * Reads WAL segment files, deserializes records, and replays
 * INSERT/DELETE/UPDATE operations into a fresh tree.
 */

#include "wal_recovery.h"
#include "wal_record.h"
#include "wal_emitter.h"    /* for reseed_lsn(), reseed_node_id() */
#include "wal_checkpoint.h"  /* for CheckpointManifest */
#include "bh_key.h"          /* for  StringKey */
#include "tree.h"
#include "common.h"        /* for key64_t, value64_t */
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace BLINK_HASH {
namespace WAL {


std::vector<std::string> find_wal_segments(const std::string& wal_dir) {
    std::vector<std::string> segs;

    DIR* d = ::opendir(wal_dir.c_str());
    if (!d) return segs;

    struct dirent* ent;
    while ((ent = ::readdir(d)) != nullptr) {
        std::string name(ent->d_name);
        /* Match pattern: wal_NNNNNN.seg */
        if (name.size() >= 14 &&
            name.substr(0, 4) == "wal_" &&
            name.substr(name.size() - 4) == ".seg") {
            segs.push_back(wal_dir + "/" + name);
        }
    }
    ::closedir(d);


    std::sort(segs.begin(), segs.end());
    return segs;
}

std::vector<char> read_all_segments(const std::string& wal_dir) {
    auto segs = find_wal_segments(wal_dir);
    std::vector<char> data;

    for (auto& path : segs) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) continue;

        struct stat st;
        if (::fstat(fd, &st) == 0 && st.st_size > 0) {
            size_t old_sz = data.size();
            data.resize(old_sz + st.st_size);
            ssize_t n = ::read(fd, data.data() + old_sz, st.st_size);
            if (n < st.st_size)
                data.resize(old_sz + std::max<ssize_t>(n, 0));
        }
        ::close(fd);
    }
    return data;
}


using RecordVisitor = std::function<bool(const RecordHeader& hdr,
                                         const void* payload,
                                         size_t payload_len)>;

static void scan_records(const char* buf, size_t len,
                         const RecordVisitor& visitor) {
    const char* scan = buf;
    const char* end  = buf + len;

    while (scan + sizeof(RecordHeader) <= end) {
        /*
         * Skip zero-padding from 4KB-aligned ThreadBuf flushes.
         * Same logic as scan_max_lsn() in wal_flusher.cpp.
         */
        RecordHeader hdr;
        std::memcpy(&hdr, scan, sizeof(RecordHeader));

        if (hdr.lsn == 0 && hdr.total_size == 0) {
            scan += sizeof(RecordHeader);
            continue;
        }

        if (hdr.total_size < sizeof(RecordHeader) ||
            hdr.total_size > 256 * 1024 ||
            scan + hdr.total_size > end) {
   
            ++scan;
            continue;
        }

        /* Validate CRC */
        size_t payload_len = hdr.total_size - sizeof(RecordHeader);
        const char* payload_ptr = scan + sizeof(RecordHeader);

        uint16_t actual_crc = wal_crc16(payload_ptr, payload_len);
        if (actual_crc != hdr.crc16) {
        
            ++scan;
            continue;
        }


        if (!visitor(hdr, payload_ptr, payload_len))
            return; 

        scan += hdr.total_size;
    }
}



template <typename Key_t, typename Value_t>
void redo_insert(const void* payload, size_t len,
                 btree_t<Key_t, Value_t>& tree,
                 ThreadInfo& threadInfo) {
    if (len < sizeof(InsertPayload) + sizeof(Key_t) + sizeof(Value_t))
        return;  

    InsertPayload ip;
    std::memcpy(&ip, payload, sizeof(ip));

    const char* p = static_cast<const char*>(payload) + sizeof(ip);

    Key_t key;
    std::memcpy(&key, p, sizeof(Key_t));
    p += ip.key_len;

    Value_t value;
    std::memcpy(&value, p, sizeof(Value_t));

    tree.insert(key, value, threadInfo);
}

template <typename Key_t, typename Value_t>
void redo_delete(const void* payload, size_t len,
                 btree_t<Key_t, Value_t>& tree,
                 ThreadInfo& threadInfo) {
    if (len < sizeof(DeletePayload) + sizeof(Key_t))
        return;

    DeletePayload dp;
    std::memcpy(&dp, payload, sizeof(dp));

    const char* p = static_cast<const char*>(payload) + sizeof(dp);

    Key_t key;
    std::memcpy(&key, p, sizeof(Key_t));

    tree.remove(key, threadInfo);
}


template <typename Key_t, typename Value_t>
void redo_update(const void* payload, size_t len,
                 btree_t<Key_t, Value_t>& tree,
                 ThreadInfo& threadInfo) {
    if (len < sizeof(UpdatePayload) + sizeof(Key_t) + sizeof(Value_t))
        return;

    UpdatePayload up;
    std::memcpy(&up, payload, sizeof(up));

    const char* p = static_cast<const char*>(payload) + sizeof(up);

    Key_t key;
    std::memcpy(&key, p, sizeof(Key_t));
    p += up.key_len;

    Value_t value;
    std::memcpy(&value, p, sizeof(Value_t));

    tree.update(key, value, threadInfo);
}


static uint64_t extract_node_id(const RecordHeader& hdr,
                                const void* payload, size_t len) {
    uint64_t max_nid = 0;

    auto type = static_cast<RecordType>(hdr.type);
    switch (type) {
        case RecordType::INSERT: {
            if (len >= sizeof(InsertPayload)) {
                InsertPayload ip;
                std::memcpy(&ip, payload, sizeof(ip));
                max_nid = ip.node_id;
            }
            break;
        }
        case RecordType::DELETE: {
            if (len >= sizeof(DeletePayload)) {
                DeletePayload dp;
                std::memcpy(&dp, payload, sizeof(dp));
                max_nid = dp.node_id;
            }
            break;
        }
        case RecordType::UPDATE: {
            if (len >= sizeof(UpdatePayload)) {
                UpdatePayload up;
                std::memcpy(&up, payload, sizeof(up));
                max_nid = up.node_id;
            }
            break;
        }
        case RecordType::SPLIT_LEAF: {
            if (len >= sizeof(SplitLeafPayload)) {
                SplitLeafPayload sp;
                std::memcpy(&sp, payload, sizeof(sp));
                max_nid = std::max(sp.old_leaf_id, sp.new_leaf_id);
            }
            break;
        }
        case RecordType::SPLIT_INTERNAL: {
            if (len >= sizeof(SplitInternalPayload)) {
                SplitInternalPayload sp;
                std::memcpy(&sp, payload, sizeof(sp));
                max_nid = std::max(sp.inode_id, sp.new_child_id);
            }
            break;
        }
        case RecordType::NEW_ROOT: {
            if (len >= sizeof(NewRootPayload)) {
                NewRootPayload nr;
                std::memcpy(&nr, payload, sizeof(nr));
                max_nid = std::max({nr.new_root_id,
                                    nr.left_child_id,
                                    nr.right_child_id});
            }
            break;
        }
        case RecordType::CONVERT: {
            if (len >= sizeof(ConvertPayload)) {
                ConvertPayload cp;
                std::memcpy(&cp, payload, sizeof(cp));
                max_nid = cp.old_hash_leaf_id;
                /* Also check new_leaf_ids */
                const char* p = static_cast<const char*>(payload)
                              + sizeof(cp);
                for (uint32_t i = 0; i < cp.num_new_leaves; i++) {
                    if (sizeof(cp) + (i + 1) * 8 <= len) {
                        uint64_t nid;
                        std::memcpy(&nid, p + i * 8, 8);
                        max_nid = std::max(max_nid, nid);
                    }
                }
            }
            break;
        }
        default:
            break;
    }
    return max_nid;
}

template <typename Key_t, typename Value_t>
bool replay_one_record(const RecordHeader& hdr,
                       const void* payload,
                       size_t payload_len,
                       btree_t<Key_t, Value_t>& tree,
                       ThreadInfo& threadInfo,
                       RecoveryStats& stats) {

    /* Track max LSN and node_id regardless of record type */
    if (hdr.lsn > stats.max_lsn)
        stats.max_lsn = hdr.lsn;

    uint64_t nid = extract_node_id(hdr, payload, payload_len);
    if (nid > stats.max_node_id)
        stats.max_node_id = nid;

    auto type = static_cast<RecordType>(hdr.type);

    switch (type) {
        case RecordType::INSERT:
            redo_insert<Key_t, Value_t>(payload, payload_len,
                                        tree, threadInfo);
            stats.inserts_replayed++;
            stats.records_replayed++;
            return true;

        case RecordType::DELETE:
            redo_delete<Key_t, Value_t>(payload, payload_len,
                                        tree, threadInfo);
            stats.deletes_replayed++;
            stats.records_replayed++;
            return true;

        case RecordType::UPDATE:
            redo_update<Key_t, Value_t>(payload, payload_len,
                                        tree, threadInfo);
            stats.updates_replayed++;
            stats.records_replayed++;
            return true;

  
        case RecordType::SPLIT_LEAF:
        case RecordType::SPLIT_INTERNAL:
        case RecordType::NEW_ROOT:
        case RecordType::CONVERT:
        case RecordType::STABILIZE:
        case RecordType::CHECKPOINT_BEGIN:
        case RecordType::CHECKPOINT_END:
            stats.records_skipped++;
            return false;

        default:
            /* Unknown record type — skip */
            stats.records_skipped++;
            return false;
    }
}


template <typename Key_t, typename Value_t>
RecoveryStats recover(const std::string& wal_dir,
                      btree_t<Key_t, Value_t>& tree,
                      ThreadInfo& threadInfo,
                      uint64_t from_lsn) {
    RecoveryStats stats = {};

    auto t0 = std::chrono::steady_clock::now();
    /* Check for checkpoint manifest */
    CheckpointManifest manifest;
    if (from_lsn == 0 && read_manifest(wal_dir, &manifest)) {
      printf("[recovery] found checkpoint: lsn=%llu, snapshot=%s\n",
               (unsigned long long)manifest.checkpoint_lsn,
              manifest.snapshot_file.c_str());

       /* Load snapshot into tree */
        std::string snap_path = wal_dir + "/" + manifest.snapshot_file;
        uint64_t loaded = load_snapshot<Key_t, Value_t>(
            snap_path, tree, threadInfo);

        printf("[recovery] loaded %llu entries from snapshot\n",
               (unsigned long long)loaded);

        
        from_lsn = manifest.checkpoint_lsn;
    }

    auto data = read_all_segments(wal_dir);

    if (data.empty()) {
        auto t1 = std::chrono::steady_clock::now();
        stats.elapsed_sec = std::chrono::duration<double>(t1 - t0).count();
        return stats;
    }

    printf("[recovery] read %zu bytes from WAL segments\n", data.size());

    /*
     * Disable WAL emission during replay.
     * We don't want recovery to re-generate WAL records.
     */
    bool was_enabled = g_wal_enabled;
    g_wal_enabled = false;

    /*
     * Scan all records and sort by LSN.
     *
     * We first collect all records, then sort by LSN to ensure
     * strictly ordered replay.  The ring buffer preserves order
     * within a single producer, but across producers the records
     * may be interleaved in the segments.
     */
    struct RecordRef {
        uint64_t    lsn;
        const char* payload;
        size_t      payload_len;
        RecordHeader hdr;
    };

    std::vector<RecordRef> refs;
    refs.reserve(data.size() / 64);  /* rough estimate */

    scan_records(data.data(), data.size(),
        [&](const RecordHeader& hdr, const void* payload,
            size_t payload_len) -> bool {
            stats.records_total++;

            if (hdr.lsn < from_lsn) {
                stats.records_skipped++;
                return true;  /* continue scanning */
            }

            RecordRef ref;
            ref.lsn         = hdr.lsn;
            ref.payload     = static_cast<const char*>(payload);
            ref.payload_len = payload_len;
            ref.hdr         = hdr;
            refs.push_back(ref);
            return true;
        });

    /* Sort by LSN for deterministic replay */
    std::sort(refs.begin(), refs.end(),
              [](const RecordRef& a, const RecordRef& b) {
                  return a.lsn < b.lsn;
              });

    printf("[recovery] %zu records to replay (from LSN %llu)\n",
           refs.size(), (unsigned long long)from_lsn);

    /* Replay in LSN order */
    for (auto& ref : refs) {
        replay_one_record<Key_t, Value_t>(
            ref.hdr, ref.payload, ref.payload_len,
            tree, threadInfo, stats);
    }

    /*  Re-seed global counters */
    if (stats.max_lsn > 0)
        reseed_lsn(stats.max_lsn);
    if (stats.max_node_id > 0)
        reseed_node_id(stats.max_node_id);

    
    g_wal_enabled = was_enabled;
   
    if (g_node_map) {
        g_node_map->clear();
        g_node_map->build_from_tree(tree.get_root());
    }
    auto t1 = std::chrono::steady_clock::now();
    stats.elapsed_sec = std::chrono::duration<double>(t1 - t0).count();

    printf("[recovery] done: %llu inserts, %llu deletes, %llu updates "
           "in %.3f s (max_lsn=%llu)\n",
           (unsigned long long)stats.inserts_replayed,
           (unsigned long long)stats.deletes_replayed,
           (unsigned long long)stats.updates_replayed,
           stats.elapsed_sec,
           (unsigned long long)stats.max_lsn);

    return stats;
}



template RecoveryStats recover(
    const std::string&, btree_t<key64_t, value64_t>&,
    ThreadInfo&, uint64_t);

template RecoveryStats recover(
    const std::string&, btree_t<StringKey, value64_t>&,
    ThreadInfo&, uint64_t);

template bool replay_one_record(
    const RecordHeader&, const void*, size_t,
    btree_t<key64_t, value64_t>&, ThreadInfo&, RecoveryStats&);

template bool replay_one_record(
    const RecordHeader&, const void*, size_t,
    btree_t<StringKey, value64_t>&, ThreadInfo&, RecoveryStats&);

template void redo_insert(
    const void*, size_t, btree_t<key64_t, value64_t>&, ThreadInfo&);
template void redo_insert(
    const void*, size_t, btree_t<StringKey, value64_t>&, ThreadInfo&);

template void redo_delete(
    const void*, size_t, btree_t<key64_t, value64_t>&, ThreadInfo&);
template void redo_delete(
    const void*, size_t, btree_t<StringKey, value64_t>&, ThreadInfo&);

template void redo_update(
    const void*, size_t, btree_t<key64_t, value64_t>&, ThreadInfo&);
template void redo_update(
    const void*, size_t, btree_t<StringKey, value64_t>&, ThreadInfo&);

}
} 