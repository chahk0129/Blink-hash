#!/usr/bin/env bash
# btree-only variant of bench_timeseries.sh for safe runs
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
REPORT="$RESULTS_DIR/bench_btree_$(date +%Y%m%d_%H%M%S).txt"

# Helpers
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
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

log "Creating benchmark database  db=$DB  scale=$SCALE"

# Skip blinkhash extension creation for btree-only run
# sql -c "CREATE EXTENSION IF NOT EXISTS blinkhash;" 2>&1 | tee -a "$REPORT" || true

log "1. Loading $SCALE timeseries rows (monotonic bigint ts)"

sql -c "
CREATE TABLE IF NOT EXISTS ts_data (
    ts        bigint           NOT NULL,
    device_id integer          NOT NULL,
    metric    double precision NOT NULL,
    tag       text             NOT NULL
);
"

sql -c "
INSERT INTO ts_data (ts, device_id, metric, tag)
SELECT
    1000000000 + g,
    (g % 1000) + 1,
    random() * 1000.0,
    'dev_' || ((g % 1000) + 1)::text || '_sensor'
FROM generate_series(1, $SCALE) g;
"
sql -c "ANALYZE ts_data;"
info "Loaded $SCALE rows  (ts range 1000000001 .. $((1000000000 + SCALE)))"
MAX_TS=$((1000000000 + SCALE))

log "2. Index Build (single-column on ts)"
printf '    %-20s  %12s  %12s\n' "Index" "Build (ms)" "Size" | tee -a "$REPORT"
sep

# btree-only loop
for IDX_TYPE in btree; do
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

log "3. Point Lookup — 1 000 random  ts = X  queries"

sql -c "DROP TABLE IF EXISTS lookup_keys;"
sql -c "CREATE TABLE lookup_keys AS SELECT ts FROM ts_data TABLESAMPLE SYSTEM(0.2) LIMIT 1000;"

for IDX_TYPE in btree; do
    IDX="idx_${IDX_TYPE}_ts"
    OTHER="idx_$( [ "$IDX_TYPE" = blinkhash ] && echo btree || echo blinkhash )_ts"

    sql -c "DROP INDEX IF EXISTS $OTHER;" 2>/dev/null || true
    sql -c "CREATE INDEX IF NOT EXISTS $IDX ON ts_data USING $IDX_TYPE (ts);"

    info "=== $IDX_TYPE ==="
    "$PSQL" -p "$PORT" -U "$USER" -d "$DB" --no-psqlrc -X <<EOF 2>&1 |
\o /dev/null
SELECT count(*) FROM ts_data WHERE ts IN (SELECT ts FROM lookup_keys);
\o
EXPLAIN (ANALYZE, BUFFERS, TIMING)
    SELECT * FROM ts_data WHERE ts IN (SELECT ts FROM lookup_keys);
EOF
    echo "" | tee -a "$REPORT"
done

# Ensure btree index exists
sql -c "CREATE INDEX IF NOT EXISTS idx_btree_ts ON ts_data USING btree (ts);"

log "4. Range Scan — recent 1% window"
WINDOW_START=$((MAX_TS - SCALE / 100))

for IDX_TYPE in btree; do
    IDX="idx_${IDX_TYPE}_ts"
    OTHER="idx_$( [ "$IDX_TYPE" = blinkhash ] && echo btree || echo blinkhash )_ts"

    sql -c "DROP INDEX IF EXISTS $OTHER;" 2>/dev/null || true
    sql -c "CREATE INDEX IF NOT EXISTS $IDX ON ts_data USING $IDX_TYPE (ts);"

    info "=== $IDX_TYPE ==="
    "$PSQL" -p "$PORT" -U "$USER" -d "$DB" --no-psqlrc -X <<EOF 2>&1 |
\o /dev/null
SELECT count(*) FROM ts_data WHERE ts >= $WINDOW_START;
\o
EXPLAIN (ANALYZE, BUFFERS, TIMING)
    SELECT * FROM ts_data WHERE ts >= $WINDOW_START AND ts <= $MAX_TS;
EOF
    echo "" | tee -a "$REPORT"
done

sql -c "CREATE INDEX IF NOT EXISTS idx_btree_ts ON ts_data USING btree (ts);"

log "5. Append Throughput — 100 000 monotonic rows (ts index only)"
APPEND_ROWS=100000

for IDX_TYPE in btree; do
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

    sql -c "INSERT INTO $TBL (ts, device_id, metric, tag)
        SELECT 1000000000 + g, (g%1000)+1, random()*1000.0,
               'dev_'||((g%1000)+1)||'_s'
        FROM generate_series(1, 10000) g;"

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

    if [ "${APPEND_MS:-0}" -gt 0 ] 2>/dev/null; then
        TPS=$(( APPEND_ROWS * 1000 / APPEND_MS ))
    else
        TPS="N/A"
    fi

    info "=== $IDX_TYPE ==="
    info "Append $APPEND_ROWS rows:  ${APPEND_MS:-N/A} ms   (~$TPS rows/s)"
    echo "" | tee -a "$REPORT"
done

log "DONE — report saved to $REPORT"
