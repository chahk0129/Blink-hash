

#include "wal_checkpoint.h"
#include "wal_emitter.h"       /* g_lsn, wal_emit() */
#include "wal_record.h"        /* RecordType, CheckpointBeginPayload */
#include "wal_flusher.h"       /* Flusher, WAL_SEGMENT_SIZE */
#include "wal_recovery.h"      /* find_wal_segments() */

#include "tree.h"              /* btree_t<K,V> */
#include "inode.h"             /* inode_t<K> */
#include "lnode.h"             /* lnode_t, lnode_hash_t, lnode_btree_t */

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace BLINK_HASH {
namespace WAL {



std::atomic<bool>         g_checkpoint_active{false};
std::mutex                g_cow_mutex;
std::vector<void*>        g_cow_pending;

/* 
 *
 *  Example:
 *    checkpoint_lsn=1000000
 *    end_lsn=1500000
 *    snapshot_file=snap_000005.dat
 *    num_entries=5000000
*/

static const char* MANIFEST_FILENAME = "checkpoint_manifest";

bool write_manifest(const std::string& wal_dir,
                    const CheckpointManifest& m) {
    std::string path = wal_dir + "/" + MANIFEST_FILENAME;
    std::string tmp  = path + ".tmp";

    FILE* f = ::fopen(tmp.c_str(), "w");
    if (!f) return false;

    ::fprintf(f, "checkpoint_lsn=%llu\n",
              (unsigned long long)m.checkpoint_lsn);
    ::fprintf(f, "end_lsn=%llu\n",
              (unsigned long long)m.end_lsn);
    ::fprintf(f, "snapshot_file=%s\n", m.snapshot_file.c_str());
    ::fprintf(f, "num_entries=%llu\n",
              (unsigned long long)m.num_entries);

    ::fflush(f);
    int fd = ::fileno(f);

#ifdef __APPLE__
    ::fcntl(fd, F_FULLFSYNC);
#else
    ::fdatasync(fd);
#endif

    ::fclose(f);


    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        ::unlink(tmp.c_str());
        return false;
    }
    int dfd = ::open(wal_dir.c_str(), O_RDONLY);
    if (dfd >= 0) {
#ifdef __APPLE__
        ::fcntl(dfd, F_FULLFSYNC);
#else
        ::fdatasync(dfd);
#endif
        ::close(dfd);
    }

    return true;
}

bool read_manifest(const std::string& wal_dir,
                   CheckpointManifest* m_out) {
    std::string path = wal_dir + "/" + MANIFEST_FILENAME;
    FILE* f = ::fopen(path.c_str(), "r");
    if (!f) return false;

    char line[1024];
    CheckpointManifest m = {};

    while (::fgets(line, sizeof(line), f)) {
        unsigned long long val;
        char str_val[512];

        if (::sscanf(line, "checkpoint_lsn=%llu", &val) == 1)
            m.checkpoint_lsn = val;
        else if (::sscanf(line, "end_lsn=%llu", &val) == 1)
            m.end_lsn = val;
        else if (::sscanf(line, "snapshot_file=%511s", str_val) == 1)
            m.snapshot_file = str_val;
        else if (::sscanf(line, "num_entries=%llu", &val) == 1)
            m.num_entries = val;
    }

    ::fclose(f);

    /* Validate: must have all fields */
    if (m.checkpoint_lsn == 0 || m.snapshot_file.empty()) {
        return false;
    }

    *m_out = m;
    return true;
}


/*
 * CRC-64 (ECMA-182) for snapshot integrity — table-driven.
 * The 256-entry lookup table is generated from polynomial 0x42F0E1EBA9EA3693.
 */
static const uint64_t crc64_table[256] = {
    0x0000000000000000ULL, 0x42F0E1EBA9EA3693ULL, 0x85E1C3D753D46D26ULL, 0xC711223CFA3E5BB5ULL,
    0x493366450E42ECDFULL, 0x0BC387AEA7A8DA4CULL, 0xCCD2A5925D9681F9ULL, 0x8E224479F47CB76AULL,
    0x9266CC8A1C85D9BEULL, 0xD0962D61B56FEF2DULL, 0x17870F5D4F51B498ULL, 0x5577EEB6E6BB820BULL,
    0xDB55AACF12C73561ULL, 0x99A54B24BB2D03F2ULL, 0x5EB4691841135847ULL, 0x1C4488F3E8F96ED4ULL,
    0x663D78FF90E185EFULL, 0x24CD9914390BB37CULL, 0xE3DCBB28C335E8C9ULL, 0xA12C5AC36ADFDE5AULL,
    0x2F0E1EBA9EA36930ULL, 0x6DFEFF5137495FA3ULL, 0xAAEFDD6DCD770416ULL, 0xE81F3C86649D3285ULL,
    0xF45BB4758C645C51ULL, 0xB6AB559E258E6AC2ULL, 0x71BA77A2DFB03177ULL, 0x334A9649765A07E4ULL,
    0xBD68D2308226B08EULL, 0xFF9833DB2BCC861DULL, 0x388911E7D1F2DDA8ULL, 0x7A79F00C7818EB3BULL,
    0xCC7AF1FF21C30BDEULL, 0x8E8A101488293D4DULL, 0x499B3228721766F8ULL, 0x0B6BD3C3DBFD506BULL,
    0x854997BA2F81E701ULL, 0xC7B97651866BD192ULL, 0x00A8546D7C558A27ULL, 0x4258B586D5BFBCB4ULL,
    0x5E1C3D753D46D260ULL, 0x1CECDC9E94ACE4F3ULL, 0xDBFDFEA26E92BF46ULL, 0x990D1F49C77889D5ULL,
    0x172F5B3033043EBFULL, 0x55DFBADB9AEE082CULL, 0x92CE98E760D05399ULL, 0xD03E790CC93A650AULL,
    0xAA478900B1228E31ULL, 0xE8B768EB18C8B8A2ULL, 0x2FA64AD7E2F6E317ULL, 0x6D56AB3C4B1CD584ULL,
    0xE374EF45BF6062EEULL, 0xA1840EAE168A547DULL, 0x66952C92ECB40FC8ULL, 0x2465CD79455E395BULL,
    0x3821458AADA7578FULL, 0x7AD1A461044D611CULL, 0xBDC0865DFE733AA9ULL, 0xFF3067B657990C3AULL,
    0x711223CFA3E5BB50ULL, 0x33E2C2240A0F8DC3ULL, 0xF4F3E018F031D676ULL, 0xB60301F359DBE0E5ULL,
    0xDA050215EA6C212FULL, 0x98F5E3FE438617BCULL, 0x5FE4C1C2B9B84C09ULL, 0x1D14202910527A9AULL,
    0x93366450E42ECDF0ULL, 0xD1C685BB4DC4FB63ULL, 0x16D7A787B7FAA0D6ULL, 0x5427466C1E109645ULL,
    0x4863CE9FF6E9F891ULL, 0x0A932F745F03CE02ULL, 0xCD820D48A53D95B7ULL, 0x8F72ECA30CD7A324ULL,
    0x0150A8DAF8AB144EULL, 0x43A04931514122DDULL, 0x84B16B0DAB7F7968ULL, 0xC6418AE602954FFBULL,
    0xBC387AEA7A8DA4C0ULL, 0xFEC89B01D3679253ULL, 0x39D9B93D2959C9E6ULL, 0x7B2958D680B3FF75ULL,
    0xF50B1CAF74CF481FULL, 0xB7FBFD44DD257E8CULL, 0x70EADF78271B2539ULL, 0x321A3E938EF113AAULL,
    0x2E5EB66066087D7EULL, 0x6CAE578BCFE24BEDULL, 0xABBF75B735DC1058ULL, 0xE94F945C9C3626CBULL,
    0x676DD025684A91A1ULL, 0x259D31CEC1A0A732ULL, 0xE28C13F23B9EFC87ULL, 0xA07CF2199274CA14ULL,
    0x167FF3EACBAF2AF1ULL, 0x548F120162451C62ULL, 0x939E303D987B47D7ULL, 0xD16ED1D631917144ULL,
    0x5F4C95AFC5EDC62EULL, 0x1DBC74446C07F0BDULL, 0xDAAD56789639AB08ULL, 0x985DB7933FD39D9BULL,
    0x84193F60D72AF34FULL, 0xC6E9DE8B7EC0C5DCULL, 0x01F8FCB784FE9E69ULL, 0x43081D5C2D14A8FAULL,
    0xCD2A5925D9681F90ULL, 0x8FDAB8CE70822903ULL, 0x48CB9AF28ABC72B6ULL, 0x0A3B7B1923564425ULL,
    0x70428B155B4EAF1EULL, 0x32B26AFEF2A4998DULL, 0xF5A348C2089AC238ULL, 0xB753A929A170F4ABULL,
    0x3971ED50550C43C1ULL, 0x7B810CBBFCE67552ULL, 0xBC902E8706D82EE7ULL, 0xFE60CF6CAF321874ULL,
    0xE224479F47CB76A0ULL, 0xA0D4A674EE214033ULL, 0x67C58448141F1B86ULL, 0x253565A3BDF52D15ULL,
    0xAB1721DA49899A7FULL, 0xE9E7C031E063ACECULL, 0x2EF6E20D1A5DF759ULL, 0x6C0603E6B3B7C1CAULL,
    0xF6FAE5C07D3274CDULL, 0xB40A042BD4D8425EULL, 0x731B26172EE619EBULL, 0x31EBC7FC870C2F78ULL,
    0xBFC9838573709812ULL, 0xFD39626EDA9AAE81ULL, 0x3A28405220A4F534ULL, 0x78D8A1B9894EC3A7ULL,
    0x649C294A61B7AD73ULL, 0x266CC8A1C85D9BE0ULL, 0xE17DEA9D3263C055ULL, 0xA38D0B769B89F6C6ULL,
    0x2DAF4F0F6FF541ACULL, 0x6F5FAEE4C61F773FULL, 0xA84E8CD83C212C8AULL, 0xEABE6D3395CB1A19ULL,
    0x90C79D3FEDD3F122ULL, 0xD2377CD44439C7B1ULL, 0x15265EE8BE079C04ULL, 0x57D6BF0317EDAA97ULL,
    0xD9F4FB7AE3911DFDULL, 0x9B041A914A7B2B6EULL, 0x5C1538ADB04570DBULL, 0x1EE5D94619AF4648ULL,
    0x02A151B5F156289CULL, 0x4051B05E58BC1E0FULL, 0x87409262A28245BAULL, 0xC5B073890B687329ULL,
    0x4B9237F0FF14C443ULL, 0x0962D61B56FEF2D0ULL, 0xCE73F427ACC0A965ULL, 0x8C8315CC052A9FF6ULL,
    0x3A80143F5CF17F13ULL, 0x7870F5D4F51B4980ULL, 0xBF61D7E80F251235ULL, 0xFD913603A6CF24A6ULL,
    0x73B3727A52B393CCULL, 0x31439391FB59A55FULL, 0xF652B1AD0167FEEAULL, 0xB4A25046A88DC879ULL,
    0xA8E6D8B54074A6ADULL, 0xEA16395EE99E903EULL, 0x2D071B6213A0CB8BULL, 0x6FF7FA89BA4AFD18ULL,
    0xE1D5BEF04E364A72ULL, 0xA3255F1BE7DC7CE1ULL, 0x64347D271DE22754ULL, 0x26C49CCCB40811C7ULL,
    0x5CBD6CC0CC10FAFCULL, 0x1E4D8D2B65FACC6FULL, 0xD95CAF179FC497DAULL, 0x9BAC4EFC362EA149ULL,
    0x158E0A85C2521623ULL, 0x577EEB6E6BB820B0ULL, 0x906FC95291867B05ULL, 0xD29F28B9386C4D96ULL,
    0xCEDBA04AD0952342ULL, 0x8C2B41A1797F15D1ULL, 0x4B3A639D83414E64ULL, 0x09CA82762AAB78F7ULL,
    0x87E8C60FDED7CF9DULL, 0xC51827E4773DF90EULL, 0x020905D88D03A2BBULL, 0x40F9E43324E99428ULL,
    0x2CFFE7D5975E55E2ULL, 0x6E0F063E3EB46371ULL, 0xA91E2402C48A38C4ULL, 0xEBEEC5E96D600E57ULL,
    0x65CC8190991CB93DULL, 0x273C607B30F68FAEULL, 0xE02D4247CAC8D41BULL, 0xA2DDA3AC6322E288ULL,
    0xBE992B5F8BDB8C5CULL, 0xFC69CAB42231BACFULL, 0x3B78E888D80FE17AULL, 0x7988096371E5D7E9ULL,
    0xF7AA4D1A85996083ULL, 0xB55AACF12C735610ULL, 0x724B8ECDD64D0DA5ULL, 0x30BB6F267FA73B36ULL,
    0x4AC29F2A07BFD00DULL, 0x08327EC1AE55E69EULL, 0xCF235CFD546BBD2BULL, 0x8DD3BD16FD818BB8ULL,
    0x03F1F96F09FD3CD2ULL, 0x41011884A0170A41ULL, 0x86103AB85A2951F4ULL, 0xC4E0DB53F3C36767ULL,
    0xD8A453A01B3A09B3ULL, 0x9A54B24BB2D03F20ULL, 0x5D45907748EE6495ULL, 0x1FB5719CE1045206ULL,
    0x919735E51578E56CULL, 0xD367D40EBC92D3FFULL, 0x1476F63246AC884AULL, 0x568617D9EF46BED9ULL,
    0xE085162AB69D5E3CULL, 0xA275F7C11F7768AFULL, 0x6564D5FDE549331AULL, 0x279434164CA30589ULL,
    0xA9B6706FB8DFB2E3ULL, 0xEB46918411358470ULL, 0x2C57B3B8EB0BDFC5ULL, 0x6EA7525342E1E956ULL,
    0x72E3DAA0AA188782ULL, 0x30133B4B03F2B111ULL, 0xF7021977F9CCEAA4ULL, 0xB5F2F89C5026DC37ULL,
    0x3BD0BCE5A45A6B5DULL, 0x79205D0E0DB05DCEULL, 0xBE317F32F78E067BULL, 0xFCC19ED95E6430E8ULL,
    0x86B86ED5267CDBD3ULL, 0xC4488F3E8F96ED40ULL, 0x0359AD0275A8B6F5ULL, 0x41A94CE9DC428066ULL,
    0xCF8B0890283E370CULL, 0x8D7BE97B81D4019FULL, 0x4A6ACB477BEA5A2AULL, 0x089A2AACD2006CB9ULL,
    0x14DEA25F3AF9026DULL, 0x562E43B4931334FEULL, 0x913F6188692D6F4BULL, 0xD3CF8063C0C759D8ULL,
    0x5DEDC41A34BBEEB2ULL, 0x1F1D25F19D51D821ULL, 0xD80C07CD676F8394ULL, 0x9AFCE626CE85B507ULL,
};

static uint64_t crc64_update(uint64_t crc, const void* data, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; i++)
        crc = crc64_table[((crc >> 56) ^ p[i]) & 0xFF] ^ (crc << 8);
    return crc;
}

/*
 * Buffered write: accumulate into write_buf, flush to fd when full.
 * Returns bytes added to buffer.
 */
static size_t buf_write(int fd, char* write_buf, size_t buf_cap,
                        size_t& buf_used, const void* data, size_t len) {
    const char* src = static_cast<const char*>(data);
    size_t written = 0;

    while (written < len) {
        size_t space = buf_cap - buf_used;
        size_t chunk = std::min(space, len - written);
        std::memcpy(write_buf + buf_used, src + written, chunk);
        buf_used += chunk;
        written  += chunk;

        if (buf_used == buf_cap) {
            ssize_t n = ::write(fd, write_buf, buf_cap);
            (void)n;  
            buf_used = 0;
        }
    }
    return written;
}


static void buf_flush(int fd, char* write_buf, size_t& buf_used) {
    if (buf_used > 0) {
        ::write(fd, write_buf, buf_used);
        buf_used = 0;
    }
}

/*
 * Serialize entries from a hash leaf into the snapshot file.
 * Hash leaves store entries in buckets with fingerprint-based
 * occupancy.  We iterate all buckets and extract non-empty slots.
 */
template <typename Key_t, typename Value_t>
static uint64_t serialize_hash_leaf(const lnode_hash_t<Key_t, Value_t>* leaf,
                                    int fd, char* write_buf,
                                    size_t& buf_used, size_t buf_cap,
                                    uint64_t& crc) {
    uint64_t count = 0;
    constexpr int num_buckets = lnode_hash_t<Key_t, Value_t>::cardinality;

    for (int b = 0; b < num_buckets; b++) {
        const auto& bkt = leaf->get_bucket(b);
        for (int s = 0; s < entry_num; s++) {
#ifdef FINGERPRINT
            if (bkt.fingerprints[s] == 0)
                continue; 
#else
            /* Without fingerprints, check for zero key */
            Key_t empty_key{};
            if (std::memcmp(&bkt.entry[s].key, &empty_key,
                            sizeof(Key_t)) == 0)
                continue;
#endif
           
            buf_write(fd, write_buf, buf_cap, buf_used,
                      &bkt.entry[s].key, sizeof(Key_t));
            crc = crc64_update(crc, &bkt.entry[s].key, sizeof(Key_t));

           
            buf_write(fd, write_buf, buf_cap, buf_used,
                      &bkt.entry[s].value, sizeof(Value_t));
            crc = crc64_update(crc, &bkt.entry[s].value, sizeof(Value_t));

            count++;
        }
    }
    return count;
}

/*
 * Serialize entries from a btree leaf into the snapshot file.
 * Btree leaves store entries in a sorted array.
 */
template <typename Key_t, typename Value_t>
static uint64_t serialize_btree_leaf(
        const lnode_btree_t<Key_t, Value_t>* leaf,
        int fd, char* write_buf,
        size_t& buf_used, size_t buf_cap,
        uint64_t& crc) {
    uint64_t count = 0;
    int cnt = leaf->get_cnt();

    for (int i = 0; i < cnt; i++) {
        const auto& e = leaf->get_entry(i);

        buf_write(fd, write_buf, buf_cap, buf_used,
                  &e.key, sizeof(Key_t));
        crc = crc64_update(crc, &e.key, sizeof(Key_t));

        buf_write(fd, write_buf, buf_cap, buf_used,
                  &e.value, sizeof(Value_t));
        crc = crc64_update(crc, &e.value, sizeof(Value_t));

        count++;
    }
    return count;
}

/*
 * Walk the tree level-by-level and serialize all leaf entries.
 *
 * Traversal: start at root, descend to leftmost leaf via
 * leftmost_ptr, then walk the sibling chain.  This is the same
 * pattern used by footprint() and destroy().
 */
template <typename Key_t, typename Value_t>
uint64_t Checkpointer::write_snapshot(btree_t<Key_t, Value_t>& tree,
                                      const std::string& snap_path) {
    constexpr size_t BUF_CAP = 256 * 1024;  /* 256 KB write buffer */

    int fd = ::open(snap_path.c_str(),
                    O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("open snapshot");
        return 0;
    }

    char* write_buf = static_cast<char*>(::malloc(BUF_CAP));
    size_t buf_used = 0;
    uint64_t crc = 0;
    uint64_t total_entries = 0;


    SnapHeader hdr = {};
    hdr.magic     = SNAP_MAGIC;
    hdr.version   = SNAP_VERSION;
    hdr.key_size  = sizeof(Key_t);
    hdr.value_size = sizeof(Value_t);
    ::write(fd, &hdr, sizeof(hdr));

    /* Navigate to the leftmost leaf */
    node_t* cur = tree.get_root();
    while (cur && cur->level > 0)
        cur = cur->leftmost_ptr;

    /* Walk all leaves via sibling chain */
    while (cur) {
        auto* leaf = static_cast<lnode_t<Key_t, Value_t>*>(cur);

        if (leaf->type == lnode_t<Key_t, Value_t>::HASH_NODE) {
            auto* h = static_cast<const lnode_hash_t<Key_t, Value_t>*>(leaf);
            total_entries += serialize_hash_leaf<Key_t, Value_t>(
                h, fd, write_buf, buf_used, BUF_CAP, crc);
        } else {
            auto* b = static_cast<const lnode_btree_t<Key_t, Value_t>*>(leaf);
            total_entries += serialize_btree_leaf<Key_t, Value_t>(
                b, fd, write_buf, buf_used, BUF_CAP, crc);
        }

        cur = cur->sibling_ptr;
    }

    /* Flush remaining buffer */
    buf_flush(fd, write_buf, buf_used);

    /* Write footer */
    SnapFooter footer;
    footer.checksum    = crc;
    footer.num_entries = total_entries;
    ::write(fd, &footer, sizeof(footer));

    /* Rewrite header with actual entry count */
    hdr.num_entries = total_entries;
    ::lseek(fd, 0, SEEK_SET);
    ::write(fd, &hdr, sizeof(hdr));

    /* Sync */
#ifdef __APPLE__
    ::fcntl(fd, F_FULLFSYNC);
#else
    ::fdatasync(fd);
#endif

    ::close(fd);
    ::free(write_buf);

    return total_entries;
}


template <typename Key_t, typename Value_t>
uint64_t load_snapshot(const std::string& snap_path,
                       btree_t<Key_t, Value_t>& tree,
                       ThreadInfo& threadInfo) {

    int fd = ::open(snap_path.c_str(), O_RDONLY);
    if (fd < 0) {
        perror("open snapshot for loading");
        return 0;
    }

    
    SnapHeader hdr;
    if (::read(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        ::close(fd);
        return 0;
    }

    /* Validate magic and version */
    if (hdr.magic != SNAP_MAGIC || hdr.version != SNAP_VERSION) {
        fprintf(stderr, "[checkpoint] invalid snapshot magic/version\n");
        ::close(fd);
        return 0;
    }

    /* Validate key/value sizes match the tree template */
    if (hdr.key_size != sizeof(Key_t) || hdr.value_size != sizeof(Value_t)) {
        fprintf(stderr, "[checkpoint] snapshot key/value size mismatch: "
                "snap(%u,%u) vs tree(%zu,%zu)\n",
                hdr.key_size, hdr.value_size,
                sizeof(Key_t), sizeof(Value_t));
        ::close(fd);
        return 0;
    }

    printf("[checkpoint] loading snapshot: %llu entries (%u-byte keys)\n",
           (unsigned long long)hdr.num_entries, hdr.key_size);

    /* Read all entries and insert into tree */
    constexpr size_t READ_BUF_CAP = 256 * 1024;
    char* read_buf = static_cast<char*>(::malloc(READ_BUF_CAP));

    uint64_t loaded = 0;
    uint64_t crc = 0;
    const size_t entry_size = sizeof(Key_t) + sizeof(Value_t);

    size_t buf_avail = 0;
    size_t buf_pos   = 0;

    while (loaded < hdr.num_entries) {
     
        if (buf_pos + entry_size > buf_avail) {
            /* Move remaining bytes to start of buffer */
            size_t remaining = buf_avail - buf_pos;
            if (remaining > 0)
                std::memmove(read_buf, read_buf + buf_pos, remaining);
            buf_pos = 0;
            buf_avail = remaining;

            ssize_t n = ::read(fd, read_buf + buf_avail,
                               READ_BUF_CAP - buf_avail);
            if (n <= 0) break;
            buf_avail += n;
        }

        if (buf_pos + entry_size > buf_avail)
            break; 

        Key_t key;
        Value_t value;
        std::memcpy(&key, read_buf + buf_pos, sizeof(Key_t));
        crc = crc64_update(crc, read_buf + buf_pos, sizeof(Key_t));
        buf_pos += sizeof(Key_t);

        std::memcpy(&value, read_buf + buf_pos, sizeof(Value_t));
        crc = crc64_update(crc, read_buf + buf_pos, sizeof(Value_t));
        buf_pos += sizeof(Value_t);

        tree.insert(key, value, threadInfo);
        loaded++;
    }

  
    off_t expected_footer_pos = sizeof(SnapHeader) +
                                hdr.num_entries * entry_size;
    ::lseek(fd, expected_footer_pos, SEEK_SET);

    SnapFooter footer;
    if (::read(fd, &footer, sizeof(footer)) == sizeof(footer)) {
        if (footer.num_entries != loaded) {
            fprintf(stderr, "[checkpoint] WARNING: footer says %llu entries, "
                    "loaded %llu\n",
                    (unsigned long long)footer.num_entries,
                    (unsigned long long)loaded);
        }
        if (footer.checksum != crc) {
            fprintf(stderr, "[checkpoint] WARNING: CRC mismatch in snapshot\n");
        }
    }

    ::close(fd);
    ::free(read_buf);

    printf("[checkpoint] loaded %llu entries from snapshot\n",
           (unsigned long long)loaded);

    return loaded;
}


void Checkpointer::delete_old_segments(uint64_t before_lsn) {
    auto segs = find_wal_segments(wal_dir_);

    /* Build path of the segment the flusher is actively writing to.
     * We must NEVER unlink this file while the flusher holds the fd. */
    char active_name[64];
    snprintf(active_name, sizeof(active_name),
             "wal_%06llu.seg",
             (unsigned long long)flusher_.current_segment_id());
    std::string active_path = wal_dir_ + "/" + active_name;

    for (auto& seg_path : segs) {
        /* Never delete the active segment — the flusher's fd would be orphaned */
        if (seg_path == active_path)
            continue;

        /*
         * Read the segment and find the MAX record LSN.
         * Only delete the segment if all its records are below
         * the checkpoint LSN — i.e. max_lsn < before_lsn.
         */
        int fd = ::open(seg_path.c_str(), O_RDONLY);
        if (fd < 0) continue;

        off_t sz = ::lseek(fd, 0, SEEK_END);
        if (sz <= 0) { ::close(fd); continue; }
        ::lseek(fd, 0, SEEK_SET);

        std::vector<char> buf(static_cast<size_t>(sz));
        ssize_t n = ::read(fd, buf.data(), buf.size());
        ::close(fd);
        if (n <= 0) continue;

        /* Scan records to find max LSN in this segment */
        uint64_t seg_max_lsn = 0;
        const char* p = buf.data();
        size_t rem = static_cast<size_t>(n);
        while (rem >= sizeof(RecordHeader)) {
            RecordHeader hdr;
            std::memcpy(&hdr, p, sizeof(hdr));

            if (hdr.total_size == 0 && hdr.lsn == 0)
                break;
            if (hdr.total_size < sizeof(RecordHeader) ||
                hdr.total_size > rem)
                break;

            if (hdr.lsn > seg_max_lsn)
                seg_max_lsn = hdr.lsn;

            p   += hdr.total_size;
            rem -= hdr.total_size;
        }

        /* Only delete if EVERY record in the segment is below the checkpoint */
        if (seg_max_lsn > 0 && seg_max_lsn < before_lsn) {
            printf("[checkpoint] deleting old WAL segment: %s "
                   "(max_lsn=%llu < %llu)\n",
                   seg_path.c_str(),
                   (unsigned long long)seg_max_lsn,
                   (unsigned long long)before_lsn);
            ::unlink(seg_path.c_str());
        }
    }
}


Checkpointer::Checkpointer(const std::string& wal_dir,
                           Flusher& flusher)
    : wal_dir_(wal_dir), flusher_(flusher) {

    /* Load snapshot counter from existing manifest (if any) */
    CheckpointManifest m;
    if (read_manifest(wal_dir_, &m)) {
 
        auto pos = m.snapshot_file.find('_');
        auto dot = m.snapshot_file.find('.');
        if (pos != std::string::npos && dot != std::string::npos) {
            snapshot_counter_ = std::stoull(
                m.snapshot_file.substr(pos + 1, dot - pos - 1));
        }
    }
}

Checkpointer::~Checkpointer() {
  
}

template <typename Key_t, typename Value_t>
CheckpointManifest Checkpointer::run_checkpoint(
        btree_t<Key_t, Value_t>& tree,
        ThreadInfo& threadInfo) {

    CheckpointManifest manifest = {};

    active_.store(true, std::memory_order_release);

    auto t0 = std::chrono::steady_clock::now();

    /* Record the checkpoint LSN */
    uint64_t checkpoint_lsn = g_lsn.load(std::memory_order_relaxed);

    /* Emit WAL_CHECKPOINT_BEGIN */
    {
        CheckpointBeginPayload payload;
        payload.checkpoint_lsn = checkpoint_lsn;
        payload.epoch = 0;
        wal_emit(RecordType::CHECKPOINT_BEGIN,
                 &payload, sizeof(payload));
    }

    /* Flush the flusher so all prior records are on disk */
    wal_flush_thread_buf();
    /* Give flusher a moment to drain */
    while (flusher_.flushed_lsn() < checkpoint_lsn) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    /* Flip checkpoint epoch — workers start CoW'ing */
    checkpoint_epoch_set(true);

    /* Write snapshot */
    snapshot_counter_++;
    char snap_name[64];
    snprintf(snap_name, sizeof(snap_name),
             "snap_%06llu.dat", (unsigned long long)snapshot_counter_);

    std::string snap_path = wal_dir_ + "/" + snap_name;

    uint64_t num_entries = write_snapshot<Key_t, Value_t>(tree, snap_path);

    /* Record end LSN */
    uint64_t end_lsn = g_lsn.load(std::memory_order_relaxed);

    /* Emit WAL_CHECKPOINT_END */
    {
        CheckpointEndPayload payload;
        payload.checkpoint_lsn = checkpoint_lsn;
        payload.end_lsn = end_lsn;
        wal_emit(RecordType::CHECKPOINT_END,
                 &payload, sizeof(payload));
        wal_flush_thread_buf();
    }

    /* Write manifest (this is the commit point) */
    manifest.checkpoint_lsn = checkpoint_lsn;
    manifest.end_lsn        = end_lsn;
    manifest.snapshot_file   = snap_name;
    manifest.num_entries     = num_entries;

    bool ok = write_manifest(wal_dir_, manifest);
    assert(ok);

    /* Flip epoch back — end CoW period */
    checkpoint_epoch_set(false);

    /* Free CoW-pending old nodes */
    cow_free_pending();

    /* Delete old WAL segments */
    delete_old_segments(checkpoint_lsn);

    active_.store(false, std::memory_order_release);

    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    printf("[checkpoint] complete: %llu entries, lsn=[%llu, %llu], "
           "%.3f s, file=%s\n",
           (unsigned long long)num_entries,
           (unsigned long long)checkpoint_lsn,
           (unsigned long long)end_lsn,
           elapsed, snap_name);

    return manifest;
}



template CheckpointManifest Checkpointer::run_checkpoint<key64_t, value64_t>(
    btree_t<key64_t, value64_t>&, ThreadInfo&);

template CheckpointManifest Checkpointer::run_checkpoint<StringKey, value64_t>(
    btree_t<StringKey, value64_t>&, ThreadInfo&);

template uint64_t Checkpointer::write_snapshot<key64_t, value64_t>(
    btree_t<key64_t, value64_t>&, const std::string&);

template uint64_t Checkpointer::write_snapshot<StringKey, value64_t>(
    btree_t<StringKey, value64_t>&, const std::string&);

template uint64_t load_snapshot<key64_t, value64_t>(
    const std::string&, btree_t<key64_t, value64_t>&, ThreadInfo&);

template uint64_t load_snapshot<StringKey, value64_t>(
    const std::string&, btree_t<StringKey, value64_t>&, ThreadInfo&);

} 
} 