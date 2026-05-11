#!/usr/bin/env bash
#
# bench_timeseries.sh — Benchmark B^link-hash vs B-tree on timeseries workloads
#
# B^link-hash is a single-column index optimised for monotonically-increasing
# keys (timestamps).  All benchmarks index the `ts` column only.
#
# Measures:  index build, point lookup, range scan, recent-window scan,
#            append throughput, WAL generation, crash-recovery, mixed R/W.
#
# Usage:     ./scripts/bench_timeseries.sh [SCALE]
#            SCALE = number of rows (default 1 000 000)
#
# Prerequisites:
#   - PostgreSQL 17 running on port 5432
#   - 'blinkhash' in shared_preload_libraries & extension available
#
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────
PSQL="${PSQL:-/opt/homebrew/opt/postgresql@17/bin/psql}"
PG_CTL="${PG_CTL:-/opt/homebrew/opt/postgresql@17/bin/pg_ctl}"
PG_DATA="${PG_DATA:-/opt/homebrew/var/postgresql@17}"
DB="blinkhash_bench"
USER="${PGUSER:-$(whoami)}"
PORT="${PGPORT:-5432}"
SCALE="${1:-1000000}"

RESULTS_DIR="$(cd "$(dirname "$0")/.." && pwd)/benchmark_results"
mkdir -p "$RESULTS_DIR"
REPORT="$RESULTS_DIR/bench_$(date +%Y%m%d_%H%M%S).txt"

# ── Helpers ────────────────────────────────────────────────────────────
sql() {
    PAGER=cat "$PSQL" -p "$PORT" -U "$USER" -d "$DB" \
        --no-psqlrc -X -q -A -t "$@"
}
sql_pretty() {
    PAGER=cat "$PSQL" -p "$PORT" -U "$USER" -d "$DB" \
        --no-psqlrc -X "$@"
}

log()  { printf '\n\033[1;36m>>> %s\033[0m\n' "$*" | tee -a "$REPORT"; }
info() { printf '    %s\n' "$*" | tee -a "$REPORT"; }
sep()  { printf '    %s\n' "────────────────────────────────────────────────" | tee -a "$REPORT"; }

# macOS-safe millisecond timer (date +%s%N is GNU only)
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# (timing is done with now_ms — no psql \timing needed)

# ── Create database ───────────────────────────────────────────────────
log "Creating benchmark database  db=$DB  scale=$SCALE"

PAGER=cat "$PSQL" -p "$PORT" -U "$USER" -d postgres --no-psqlrc -X -q \
    -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname='$DB' AND pid<>pg_backend_pid();" 2>/dev/null || true

PAGER=cat "$PSQL" -p "$PORT" -U "$USER" -d postgres --no-psqlrc -X -q \
    -c "DROP DATABASE IF EXISTS $DB;" \
    -c "CREATE DATABASE $DB;" 2>&1 | tee -a "$REPORT"

sql -c "CREATE EXTENSION IF NOT EXISTS blinkhash;" 2>&1 | tee -a "$REPORT"

# ══════════════════════════════════════════════════════════════════════
# 1. Schema & Data — monotonic timestamp workload
# ══════════════════════════════════════════════════════════════════════
log "1. Loading $SCALE timeseries rows (monotonic bigint ts)"

sql -c "
CREATE TABLE ts_data (
    ts        bigint           NOT NULL,   -- epoch-micros, monotonic
    device_id integer          NOT NULL,   -- 1..1000 devices
    metric    double precision NOT NULL,
    tag       text             NOT NULL
);
"

sql -c "
INSERT INTO ts_data (ts, device_id, metric, tag)
SELECT
    1000000000 + g,                                -- monotonically increasing
    (g % 1000) + 1,
    random() * 1000.0,
    'dev_' || ((g % 1000) + 1)::text || '_sensor'
FROM generate_series(1, $SCALE) g;
"
sql -c "ANALYZE ts_data;"
info "Loaded $SCALE rows  (ts range 1000000001 .. $((1000000000 + SCALE)))"

# Compute max ts once for later sections
MAX_TS=$((1000000000 + SCALE))

# ══════════════════════════════════════════════════════════════════════
# 2. Index Build — single column on ts (the blink-hash sweet spot)
# ══════════════════════════════════════════════════════════════════════
log "2. Index Build (single-column on ts)"

printf '    %-20s  %12s  %12s\n' "Index" "Build (ms)" "Size" | tee -a "$REPORT"
sep

for IDX_TYPE in blinkhash btree; do
    IDX="idx_${IDX_TYPE}_ts"
    sql -c "DROP INDEX IF EXISTS $IDX;"

    T0=$(now_ms)
    sql -c "CREATE INDEX $IDX ON ts_data USING $IDX_TYPE (ts);"
    T1=$(now_ms)
    BUILD_MS=$((T1 - T0))

    SIZE=$(sql -c "SELECT pg_size_pretty(pg_relation_size('${IDX}'::regclass));")
    printf '    %-20s  %12s  %12s\n' "$IDX" "${BUILD_MS:-N/A} ms" "$SIZE" | tee -a "$REPORT"
done
echo "" | tee -a "$REPORT"

# ══════════════════════════════════════════════════════════════════════
# 3. Point Lookup (equality on ts)
# ══════════════════════════════════════════════════════════════════════
log "3. Point Lookup — 1 000 random  ts = X  queries"

sql -c "DROP TABLE IF EXISTS lookup_keys;"
sql -c "CREATE TABLE lookup_keys AS
        SELECT ts FROM ts_data TABLESAMPLE SYSTEM(0.2) LIMIT 1000;"

for IDX_TYPE in blinkhash btree; do
    IDX="idx_${IDX_TYPE}_ts"
    OTHER="idx_$( [ "$IDX_TYPE" = blinkhash ] && echo btree || echo blinkhash )_ts"

    # Disable competing index so planner must use this one
    sql -c "DROP INDEX IF EXISTS $OTHER;" 2>/dev/null || true
    sql -c "CREATE INDEX IF NOT EXISTS $IDX ON ts_data USING $IDX_TYPE (ts);"

    info "=== $IDX_TYPE ==="
    # Warm-up + EXPLAIN in a SINGLE psql session so blinkhash's per-backend
    # tree is populated before the measured query (mirrors conn-pooled apps).
    "$PSQL" -p "$PORT" -U "$USER" -d "$DB" --no-psqlrc -X <<EOF 2>&1 \
        | tee -a "$REPORT"
\o /dev/null
SELECT count(*) FROM ts_data WHERE ts IN (SELECT ts FROM lookup_keys);
\o
EXPLAIN (ANALYZE, BUFFERS, TIMING)
    SELECT * FROM ts_data WHERE ts IN (SELECT ts FROM lookup_keys);
EOF
    echo "" | tee -a "$REPORT"
done

# Rebuild both
sql -c "CREATE INDEX IF NOT EXISTS idx_blinkhash_ts ON ts_data USING blinkhash (ts);"
sql -c "CREATE INDEX IF NOT EXISTS idx_btree_ts     ON ts_data USING btree (ts);"

# ══════════════════════════════════════════════════════════════════════
# 4. Range Scan — recent 1 % window  (typical "last N minutes")
# ══════════════════════════════════════════════════════════════════════
log "4. Range Scan — recent 1% window  (ts >= $((MAX_TS - SCALE/100)))"

WINDOW_START=$((MAX_TS - SCALE / 100))

for IDX_TYPE in blinkhash btree; do
    IDX="idx_${IDX_TYPE}_ts"
    OTHER="idx_$( [ "$IDX_TYPE" = blinkhash ] && echo btree || echo blinkhash )_ts"

    sql -c "DROP INDEX IF EXISTS $OTHER;" 2>/dev/null || true
    sql -c "CREATE INDEX IF NOT EXISTS $IDX ON ts_data USING $IDX_TYPE (ts);"

    info "=== $IDX_TYPE ==="
    "$PSQL" -p "$PORT" -U "$USER" -d "$DB" --no-psqlrc -X <<EOF 2>&1 \
        | tee -a "$REPORT"
\o /dev/null
SELECT count(*) FROM ts_data WHERE ts >= $WINDOW_START;
\o
EXPLAIN (ANALYZE, BUFFERS, TIMING)
    SELECT * FROM ts_data WHERE ts >= $WINDOW_START AND ts <= $MAX_TS;
EOF
    echo "" | tee -a "$REPORT"
done

sql -c "CREATE INDEX IF NOT EXISTS idx_blinkhash_ts ON ts_data USING blinkhash (ts);"
sql -c "CREATE INDEX IF NOT EXISTS idx_btree_ts     ON ts_data USING btree (ts);"

# ══════════════════════════════════════════════════════════════════════
# 5. Append Throughput — monotonic inserts (timeseries ingestion)
# ══════════════════════════════════════════════════════════════════════
log "5. Append Throughput — 100 000 monotonic rows (ts index only)"

APPEND_ROWS=100000

for IDX_TYPE in blinkhash btree; do
    TBL="ts_append_${IDX_TYPE}"
    IDX="idx_append_${IDX_TYPE}_ts"

    sql -c "DROP TABLE IF EXISTS $TBL CASCADE;"
    sql -c "
        CREATE TABLE $TBL (
            ts        bigint           NOT NULL,
            device_id integer          NOT NULL,
            metric    double precision NOT NULL,
            tag       text             NOT NULL
        );
        CREATE INDEX $IDX ON $TBL USING $IDX_TYPE (ts);
    "

    # Seed with 10 000 rows
    sql -c "
        INSERT INTO $TBL (ts, device_id, metric, tag)
        SELECT 1000000000 + g, (g%1000)+1, random()*1000.0,
               'dev_'||((g%1000)+1)||'_s'
        FROM generate_series(1, 10000) g;
    "

    # Warm + timed append in one connection (tree built once, not re-measured)
    T0=$(now_ms)
    "$PSQL" -p "$PORT" -U "$USER" -d "$DB" --no-psqlrc -X -q -A -t <<EOSQL >/dev/null
SELECT ts FROM $TBL WHERE ts = 1000000001;
INSERT INTO $TBL (ts, device_id, metric, tag)
SELECT $((MAX_TS + 1)) + g, (g%1000)+1, random()*1000.0,
       'dev_'||((g%1000)+1)||'_s'
FROM generate_series(1, $APPEND_ROWS) g;
EOSQL
    T1=$(now_ms)
    APPEND_MS=$((T1 - T0))

    # Compute rows/s safely
    if [ "${APPEND_MS:-0}" -gt 0 ] 2>/dev/null; then
        TPS=$(( APPEND_ROWS * 1000 / APPEND_MS ))
    else
        TPS="N/A"
    fi

    info "=== $IDX_TYPE ==="
    info "Append $APPEND_ROWS rows:  ${APPEND_MS:-N/A} ms   (~$TPS rows/s)"
    echo "" | tee -a "$REPORT"
done

# ══════════════════════════════════════════════════════════════════════
# 6. WAL Generation — bytes per insert row (ts index only)
# ══════════════════════════════════════════════════════════════════════
log "6. WAL Generation (50 000 rows, single ts index)"

WAL_ROWS=50000

printf '    %-12s  %12s  %12s  %14s  %14s\n' \
       "Method" "Insert (ms)" "WAL total" "WAL/row (B)" "Overhead vs heap" \
    | tee -a "$REPORT"
sep

# Baseline: heap-only (no index) to isolate index WAL overhead
TBL_HEAP="ts_wal_heap"
sql -c "DROP TABLE IF EXISTS $TBL_HEAP CASCADE;"
sql -c "
    CREATE TABLE $TBL_HEAP (
        ts bigint NOT NULL, device_id int NOT NULL,
        metric double precision NOT NULL, tag text NOT NULL
    );
"
sql -c "CHECKPOINT;"
WAL_H_START=$(sql -c "SELECT pg_current_wal_lsn();")

T0=$(now_ms)
sql -c "INSERT INTO $TBL_HEAP (ts, device_id, metric, tag)
SELECT 2000000000 + g, (g%1000)+1, random()*1000.0,
       'dev_'||((g%1000)+1)||'_s'
FROM generate_series(1, $WAL_ROWS) g;"
T1=$(now_ms)
HEAP_MS=$((T1 - T0))

WAL_H_END=$(sql -c "SELECT pg_current_wal_lsn();")
WAL_H_BYTES=$(sql -c "SELECT pg_wal_lsn_diff('$WAL_H_END', '$WAL_H_START');")
WAL_H_PRETTY=$(sql -c "SELECT pg_size_pretty($WAL_H_BYTES::bigint);")
WAL_H_PER=$(echo "scale=1; $WAL_H_BYTES / $WAL_ROWS" | bc 2>/dev/null || echo "?")

printf '    %-12s  %12s  %12s  %14s  %14s\n' \
       "heap-only" "${HEAP_MS:-?} ms" "$WAL_H_PRETTY" "$WAL_H_PER" "baseline" \
    | tee -a "$REPORT"

for IDX_TYPE in blinkhash btree; do
    TBL="ts_wal_${IDX_TYPE}"
    IDX="idx_wal_${IDX_TYPE}_ts"

    sql -c "DROP TABLE IF EXISTS $TBL CASCADE;"
    sql -c "
        CREATE TABLE $TBL (
            ts bigint NOT NULL, device_id int NOT NULL,
            metric double precision NOT NULL, tag text NOT NULL
        );
        CREATE INDEX $IDX ON $TBL USING $IDX_TYPE (ts);
    "
    sql -c "CHECKPOINT;"

    WAL_START=$(sql -c "SELECT pg_current_wal_lsn();")

    T0=$(now_ms)
    sql -c "INSERT INTO $TBL (ts, device_id, metric, tag)
SELECT 2000000000 + g, (g%1000)+1, random()*1000.0,
       'dev_'||((g%1000)+1)||'_s'
FROM generate_series(1, $WAL_ROWS) g;"
    T1=$(now_ms)
    INSERT_MS=$((T1 - T0))

    WAL_END=$(sql -c "SELECT pg_current_wal_lsn();")
    WAL_BYTES=$(sql -c "SELECT pg_wal_lsn_diff('$WAL_END', '$WAL_START');")
    WAL_PRETTY=$(sql -c "SELECT pg_size_pretty($WAL_BYTES::bigint);")

    if [ -n "$WAL_BYTES" ] && [ "$WAL_ROWS" -gt 0 ]; then
        WAL_PER=$(echo "scale=1; $WAL_BYTES / $WAL_ROWS" | bc 2>/dev/null || echo "?")
        OVERHEAD=$(echo "scale=1; ($WAL_BYTES - $WAL_H_BYTES) * 100 / $WAL_H_BYTES" | bc 2>/dev/null || echo "?")
    else
        WAL_PER="?"; OVERHEAD="?"
    fi

    printf '    %-12s  %12s  %12s  %14s  %13s%%\n' \
           "$IDX_TYPE" "${INSERT_MS:-?} ms" "$WAL_PRETTY" "$WAL_PER" "$OVERHEAD" \
        | tee -a "$REPORT"
done
echo "" | tee -a "$REPORT"

# ══════════════════════════════════════════════════════════════════════
# 7. Crash Recovery — immediate shutdown + WAL replay timing
# ══════════════════════════════════════════════════════════════════════
log "7. Crash Recovery Simulation (200 000 un-checkpointed rows, both indexes)"

RECOVERY_ROWS=200000
TBL_REC="ts_recovery"

sql -c "DROP TABLE IF EXISTS $TBL_REC CASCADE;"
sql -c "
    CREATE TABLE $TBL_REC (
        ts        bigint           NOT NULL,
        device_id integer          NOT NULL,
        metric    double precision NOT NULL,
        tag       text             NOT NULL
    );
    CREATE INDEX idx_rec_bh ON $TBL_REC USING blinkhash (ts);
    CREATE INDEX idx_rec_bt ON $TBL_REC USING btree     (ts);
"

# Checkpoint → clean baseline
sql -c "CHECKPOINT;"

# Insert rows (un-checkpointed — will need WAL replay)
sql -c "
INSERT INTO $TBL_REC (ts, device_id, metric, tag)
SELECT 3000000000 + g, (g%1000)+1, random()*1000.0,
       'dev_'||((g%1000)+1)||'_s'
FROM generate_series(1, $RECOVERY_ROWS) g;
"

WAL_CRASH=$(sql -c "SELECT pg_current_wal_lsn();")
info "WAL at crash:        $WAL_CRASH"
info "Rows pending replay: $RECOVERY_ROWS"

# Immediate stop (no checkpoint — simulates power loss)
info "Stopping PostgreSQL (immediate mode)..."
"$PG_CTL" stop -D "$PG_DATA" -m immediate -t 10 2>&1 | tee -a "$REPORT" || true
sleep 1

# Fallback: if pg_ctl didn't stop it, kill the postmaster directly
if "$PG_CTL" status -D "$PG_DATA" >/dev/null 2>&1; then
    info "(pg_ctl stop timed out — killing postmaster)"
    PG_PID=$(head -1 "$PG_DATA/postmaster.pid" 2>/dev/null || true)
    if [ -n "$PG_PID" ]; then
        kill -9 "$PG_PID" 2>/dev/null || true
    fi
    sleep 2
fi

# Truncate old logfile so we get a clean redo line
: > "$PG_DATA/logfile" 2>/dev/null || true

# Start & time recovery
info "Starting PostgreSQL (WAL replay)..."
T0=$(now_ms)
"$PG_CTL" start -D "$PG_DATA" -l "$PG_DATA/logfile" -w -t 60 2>&1 | tee -a "$REPORT" || true
T1=$(now_ms)
RECOVERY_MS=$((T1 - T0))

sleep 2

# Verify server is running before querying
if ! "$PG_CTL" status -D "$PG_DATA" >/dev/null 2>&1; then
    info "WARNING: PostgreSQL did not restart — skipping recovery validation"
    info "Check $PG_DATA/logfile for details"
    SURVIVED="?" ; REDO_LINE="(server not running)"
else
    SURVIVED=$(sql -c "SELECT count(*) FROM $TBL_REC;" 2>/dev/null || echo "?")
    REDO_LINE=$(grep "redo done" "$PG_DATA/logfile" 2>/dev/null | tail -1 || echo "(not found)")
fi

info "Recovery time:   ${RECOVERY_MS} ms"
info "Rows after:      $SURVIVED  (expected $RECOVERY_ROWS)"
info "Redo log:        $REDO_LINE"
echo "" | tee -a "$REPORT"

# ══════════════════════════════════════════════════════════════════════
# 8. Mixed Read/Write — interleaved inserts + range scans on ts
# ══════════════════════════════════════════════════════════════════════
log "8. Mixed Workload — 10 batches × (5 000 inserts + range scan on ts)"

for IDX_TYPE in blinkhash btree; do
    TBL="ts_mixed_${IDX_TYPE}"
    IDX="idx_mixed_${IDX_TYPE}_ts"

    sql -c "DROP TABLE IF EXISTS $TBL CASCADE;"
    sql -c "
        CREATE TABLE $TBL (
            ts        bigint           NOT NULL,
            device_id integer          NOT NULL,
            metric    double precision NOT NULL,
            tag       text             NOT NULL
        );
        CREATE INDEX $IDX ON $TBL USING $IDX_TYPE (ts);
    "

    # Seed 100k
    sql -c "
        INSERT INTO $TBL (ts, device_id, metric, tag)
        SELECT 4000000000 + g, (g%1000)+1, random()*1000.0,
               'dev_'||((g%1000)+1)||'_s'
        FROM generate_series(1, 100000) g;
    "
    sql -c "ANALYZE $TBL;"

    # Server-side timing: warm blinkhash tree before the clock starts.
    MIXED_MS=$("$PSQL" -p "$PORT" -U "$USER" -d "$DB" \
        --no-psqlrc -X -q -A -t <<EOSQL 2>&1 \
        | grep 'elapsed_ms=' | sed 's/.*elapsed_ms=//'
DO \$body\$
DECLARE
    batch   int;
    dummy   bigint;
    t0      double precision;
    t1      double precision;
BEGIN
    SELECT ts INTO dummy FROM $TBL WHERE ts = 4000000001;
    t0 := extract(epoch from clock_timestamp()) * 1000.0;
    FOR batch IN 1..10 LOOP
        INSERT INTO $TBL (ts, device_id, metric, tag)
        SELECT 5000000000 + (batch-1)*5000 + g,
               (g%1000)+1, random()*1000.0,
               'dev_'||((g%1000)+1)||'_s'
        FROM generate_series(1, 5000) g;
        SELECT count(*) INTO dummy
        FROM $TBL
        WHERE ts >= 5000000000 + (batch-1)*5000 - 1000;
    END LOOP;
    t1 := extract(epoch from clock_timestamp()) * 1000.0;
    RAISE NOTICE 'elapsed_ms=%', (t1 - t0)::int;
END;
\$body\$;
EOSQL
)
    MIXED_MS=${MIXED_MS:-0}

    FINAL=$(sql -c "SELECT count(*) FROM $TBL;")

    info "=== $IDX_TYPE ==="
    info "50k inserts + 10 range scans:  ${MIXED_MS:-N/A} ms   (final rows: $FINAL)"
    echo "" | tee -a "$REPORT"
done

# ══════════════════════════════════════════════════════════════════════
# 9. EXPLAIN Plans — key timeseries query patterns on ts
# ══════════════════════════════════════════════════════════════════════
log "9. EXPLAIN Plans (both indexes present on ts_data.ts)"

sql -c "CREATE INDEX IF NOT EXISTS idx_blinkhash_ts ON ts_data USING blinkhash (ts);"
sql -c "CREATE INDEX IF NOT EXISTS idx_btree_ts     ON ts_data USING btree (ts);"

MID_TS=$((1000000000 + SCALE / 2))
RECENT_START=$((MAX_TS - SCALE / 1000))

# All queries in one session — blinkhash rebuilds once, then all
# subsequent queries use the cached tree (mirrors conn-pooled apps).
"$PSQL" -p "$PORT" -U "$USER" -d "$DB" --no-psqlrc -X <<EOF 2>&1 \
    | tee -a "$REPORT"
\o /dev/null
SELECT count(*) FROM ts_data WHERE ts = $MID_TS;
\o

\echo --- Point lookup: WHERE ts = $MID_TS ---
EXPLAIN (ANALYZE, BUFFERS)
    SELECT * FROM ts_data WHERE ts = $MID_TS;

\echo
\echo --- Recent 0.1% window: WHERE ts >= $RECENT_START ---
EXPLAIN (ANALYZE, BUFFERS)
    SELECT * FROM ts_data WHERE ts >= $RECENT_START;

\echo
\echo --- Aggregation over recent window (GROUP BY device_id) ---
EXPLAIN (ANALYZE, BUFFERS)
    SELECT device_id, avg(metric)
    FROM ts_data WHERE ts >= $RECENT_START
    GROUP BY device_id;

\echo
\echo --- Latest row per device (DISTINCT ON + ORDER BY ts DESC) ---
EXPLAIN (ANALYZE, BUFFERS)
    SELECT DISTINCT ON (device_id) device_id, ts, metric
    FROM ts_data ORDER BY device_id, ts DESC LIMIT 20;
EOF
echo "" | tee -a "$REPORT"

# ══════════════════════════════════════════════════════════════════════
# 10. Summary — index sizes + usage stats for ts_data
# ══════════════════════════════════════════════════════════════════════
log "10. Summary — index sizes & scan stats (ts_data)"

sql_pretty -c "
SELECT
    i.indexrelid::regclass                          AS index_name,
    am.amname                                       AS method,
    pg_size_pretty(pg_relation_size(i.indexrelid))  AS size,
    s.idx_scan                                      AS scans,
    s.idx_tup_read                                  AS tup_read,
    s.idx_tup_fetch                                 AS tup_fetch
FROM pg_index i
JOIN pg_class c  ON c.oid = i.indrelid
JOIN pg_am   am ON am.oid = (SELECT relam FROM pg_class WHERE oid = i.indexrelid)
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.indexrelid
WHERE c.relname = 'ts_data'
ORDER BY am.amname, i.indexrelid::regclass::text;
" 2>&1 | tee -a "$REPORT"

# ══════════════════════════════════════════════════════════════════════
log "DONE — report saved to $REPORT"
echo ""
echo "Hints:"
echo "  Scale up:            $0 5000000"
echo "  WAL tuning:          SHOW wal_level;  SHOW wal_buffers;  SHOW checkpoint_timeout;"
echo "  Per-query flamegraph: perf record -g -p \$(pgrep -f 'postgres.*blinkhash')"
