/*
 * test_wal_record.cpp — Unit tests for WAL record serialize/deserialize
 *
 * Build:
 *   cd index/blink-hash-pg/build && cmake .. && make test_wal_record
 * Run:
 *   ./test/test_wal_record
 */

#include "wal_record.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>

using namespace BLINK_HASH::WAL;

/* ─── Test 1: CRC-16 basic ─────────────────────────────────────── */

static void test_crc16() {
    const char msg[] = "hello world";
    uint16_t c1 = wal_crc16(msg, sizeof(msg) - 1);
    uint16_t c2 = wal_crc16(msg, sizeof(msg) - 1);
    assert(c1 == c2 && "CRC must be deterministic");
    assert(c1 != 0  && "CRC of non-empty data should be non-zero");

    /* Flipping one bit must change the CRC */
    char bad[sizeof(msg)];
    std::memcpy(bad, msg, sizeof(msg) - 1);
    bad[3] ^= 0x01;
    uint16_t c3 = wal_crc16(bad, sizeof(msg) - 1);
    assert(c3 != c1 && "CRC must differ on different data");

    printf("  [PASS] crc16 basic\n");
}

/* ─── Test 2: round-trip serialize/deserialize ─────────────────── */

static void test_roundtrip_insert() {
    /* Construct an InsertPayload followed by key+value */
    InsertPayload ip;
    ip.node_id    = 42;
    ip.bucket_idx = 7;
    ip.key_len    = 8;

    /*
     * Payload on wire: InsertPayload struct + 8 bytes key + 8 bytes value.
     */
    const uint64_t key_val   = 0xDEADBEEFCAFEBABEULL;
    const uint64_t value_val = 0x1234567890ABCDEFULL;

    size_t payload_sz = sizeof(InsertPayload) + 8 + 8;
    std::vector<char> payload_buf(payload_sz);
    std::memcpy(payload_buf.data(), &ip, sizeof(ip));
    std::memcpy(payload_buf.data() + sizeof(ip), &key_val, 8);
    std::memcpy(payload_buf.data() + sizeof(ip) + 8, &value_val, 8);

    /* Serialize */
    std::vector<char> wire(sizeof(RecordHeader) + payload_sz);
    size_t written = wal_record_serialize(RecordType::INSERT, /*lsn=*/100,
                                          payload_buf.data(), payload_sz,
                                          wire.data());
    assert(written == wire.size());

    /* Deserialize */
    RecordHeader hdr;
    const void* p = wal_record_deserialize(wire.data(), wire.size(), &hdr);
    assert(p != nullptr          && "deserialize must succeed");
    assert(hdr.lsn == 100        && "LSN must match");
    assert(hdr.type == static_cast<uint16_t>(RecordType::INSERT));
    assert(hdr.total_size == written);

    /* Verify payload content */
    const InsertPayload* ip2 = static_cast<const InsertPayload*>(p);
    assert(ip2->node_id == 42);
    assert(ip2->bucket_idx == 7);

    printf("  [PASS] roundtrip insert\n");
}

/* ─── Test 3: corrupted CRC → nullptr ─────────────────────────── */

static void test_corrupt_crc() {
    InsertPayload ip = {1, 0, 4};
    uint32_t key = 99;
    uint64_t val = 200;

    size_t payload_sz = sizeof(ip) + 4 + 8;
    std::vector<char> payload_buf(payload_sz);
    std::memcpy(payload_buf.data(), &ip, sizeof(ip));
    std::memcpy(payload_buf.data() + sizeof(ip), &key, 4);
    std::memcpy(payload_buf.data() + sizeof(ip) + 4, &val, 8);

    std::vector<char> wire(sizeof(RecordHeader) + payload_sz);
    wal_record_serialize(RecordType::INSERT, 1,
                         payload_buf.data(), payload_sz, wire.data());

    /* Flip a payload byte */
    wire[sizeof(RecordHeader) + 2] ^= 0xFF;

    RecordHeader hdr;
    const void* p = wal_record_deserialize(wire.data(), wire.size(), &hdr);
    assert(p == nullptr && "corrupt CRC must return nullptr");

    printf("  [PASS] corrupt CRC\n");
}

/* ─── Test 4: truncated buffer → nullptr ──────────────────────── */

static void test_truncated() {
    InsertPayload ip = {1, 0, 4};
    size_t payload_sz = sizeof(ip);
    std::vector<char> wire(sizeof(RecordHeader) + payload_sz);
    wal_record_serialize(RecordType::INSERT, 1, &ip, payload_sz, wire.data());

    RecordHeader hdr;
    /* Pass only half the data */
    const void* p = wal_record_deserialize(wire.data(), wire.size() / 2, &hdr);
    assert(p == nullptr && "truncated data must return nullptr");

    printf("  [PASS] truncated detection\n");
}

/* ─── Test 5: every record type round-trips ───────────────────── */

static void test_all_types() {
    struct { RecordType t; size_t sz; } cases[] = {
        { RecordType::DELETE,           sizeof(DeletePayload) },
        { RecordType::UPDATE,           sizeof(UpdatePayload) },
        { RecordType::SPLIT_LEAF,       sizeof(SplitLeafPayload) },
        { RecordType::SPLIT_INTERNAL,   sizeof(SplitInternalPayload) },
        { RecordType::CONVERT,          sizeof(ConvertPayload) },
        { RecordType::NEW_ROOT,         sizeof(NewRootPayload) },
        { RecordType::STABILIZE,        sizeof(StabilizePayload) },
        { RecordType::CHECKPOINT_BEGIN, sizeof(CheckpointBeginPayload) },
        { RecordType::CHECKPOINT_END,   sizeof(CheckpointEndPayload) },
    };

    for (auto& c : cases) {
        /* zero-filled payload — just testing the container */
        std::vector<char> payload(c.sz, 0);
        std::vector<char> wire(sizeof(RecordHeader) + c.sz);

        size_t w = wal_record_serialize(c.t, 42, payload.data(), c.sz,
                                        wire.data());
        assert(w == wire.size());

        RecordHeader hdr;
        const void* p = wal_record_deserialize(wire.data(), wire.size(), &hdr);
        assert(p != nullptr);
        assert(hdr.type == static_cast<uint16_t>(c.t));
    }
    printf("  [PASS] all record types\n");
}

/* ─── main ─────────────────────────────────────────────────────── */

int main() {
    printf("=== test_wal_record ===\n");
    test_crc16();
    test_roundtrip_insert();
    test_corrupt_crc();
    test_truncated();
    test_all_types();
    printf("All record tests passed.\n");
    return 0;
}