#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_benchmark.sh — averaged throughput comparison: sync vs async
# Usage: ./run_benchmark.sh [runs] [initial_keys] [scan_threads] [insert_threads] [duration_sec]
# Example: ./run_benchmark.sh 7 5000000 2 2 10
# ─────────────────────────────────────────────────────────────────────────────

RUNS="${1:-7}"
INITIAL_KEYS="${2:-5000000}"
SCAN_THREADS="${3:-2}"
INSERT_THREADS="${4:-2}"
DURATION="${5:-10}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
SYNC_BIN="$BUILD_DIR/test/bench_range_throughput_sync"
ASYNC_BIN="$BUILD_DIR/test/bench_range_throughput_async"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; NC='\033[0m'

# ── sanity checks ─────────────────────────────────────────────────────────────
for BIN in "$SYNC_BIN" "$ASYNC_BIN"; do
    if [[ ! -x "$BIN" ]]; then
        echo -e "${RED}ERROR: binary not found: $BIN${NC}"
        echo "  Run: cd $BUILD_DIR && cmake .. && make -j"
        exit 1
    fi
done

# ── check if GNU timeout is available (not default on macOS) ──────────────────
HAS_TIMEOUT=0
if command -v timeout &>/dev/null; then
    HAS_TIMEOUT=1
fi

# ── run one binary, write result to global vars LAST_SCAN / LAST_INSERT ───────
LAST_SCAN="0.0"
LAST_INSERT="0.0"

run_once(){
    local BIN="$1"
    local TMPFILE
    TMPFILE=$(mktemp /tmp/bh_bench_XXXXXX.txt)

    if [[ $HAS_TIMEOUT -eq 1 ]]; then
        timeout "$((DURATION + 30))" \
            "$BIN" "$INITIAL_KEYS" "$SCAN_THREADS" "$INSERT_THREADS" "$DURATION" \
            > "$TMPFILE" 2>&1 || true
    else
        # macOS — no GNU timeout, just run directly
        "$BIN" "$INITIAL_KEYS" "$SCAN_THREADS" "$INSERT_THREADS" "$DURATION" \
            > "$TMPFILE" 2>&1 || true
    fi

    # DEBUG: uncomment to see raw output if all zeros appear
    # echo "=== RAW OUTPUT ===" >&2
    # cat "$TMPFILE" >&2
    # echo "==================" >&2

    # flexible grep: match any line containing "scan" and "throughput"
    # handles:  "scan throughput:   1.41 Mops/sec"
    #           "  scan throughput: 1.41 Mops/sec"
    #           "scan throughput   1.41 Mops/sec"
    LAST_SCAN=$(grep -i "scan.*throughput" "$TMPFILE" \
        | grep -v "insert" \
        | awk '{for(i=1;i<=NF;i++) if($i~/^[0-9]+\.[0-9]+$/) {print $i; exit}}')

    LAST_INSERT=$(grep -i "insert.*throughput" "$TMPFILE" \
        | awk '{for(i=1;i<=NF;i++) if($i~/^[0-9]+\.[0-9]+$/) {print $i; exit}}')

    LAST_SCAN="${LAST_SCAN:-0.0}"
    LAST_INSERT="${LAST_INSERT:-0.0}"

    rm -f "$TMPFILE"
}

# ── awk helpers ───────────────────────────────────────────────────────────────
avg_arr(){
    printf '%s\n' "$@" | awk '{s+=$1; n++} END{printf "%.4f", (n ? s/n : 0)}'
}

std_arr(){
    local AVG="$1"; shift
    printf '%s\n' "$@" | \
        awk -v a="$AVG" '{d=$1-a; s+=d*d; n++} END{printf "%.4f", (n ? sqrt(s/n) : 0)}'
}

pct_change(){
    echo "$1 $2" | awk '{
        if ($2 == 0) { print "0.0"; exit }
        printf "%.1f", (($1 - $2) / $2) * 100
    }'
}

color_pct(){
    local V="$1"
    local NUM="${V#+}"
    if awk "BEGIN{exit !(($NUM) > 2)}"  2>/dev/null; then
        printf "${GREEN}+%s%%${NC}" "$V"
    elif awk "BEGIN{exit !(($NUM) < -2)}" 2>/dev/null; then
        printf "${RED}%s%%${NC}" "$V"
    else
        printf "${YELLOW}%s%% (noise)${NC}" "$V"
    fi
}

# ── debug: show actual binary output format once ──────────────────────────────
echo -e "${YELLOW}Probing binary output format…${NC}"
PROBE=$(mktemp /tmp/bh_probe_XXXXXX.txt)
"$SYNC_BIN" "$INITIAL_KEYS" "$SCAN_THREADS" "$INSERT_THREADS" 3 > "$PROBE" 2>&1 || true
echo -e "${BOLD}--- sample output (3s run) ---${NC}"
cat "$PROBE"
echo -e "${BOLD}------------------------------${NC}"

# auto-detect column position of the number on scan/insert lines
SCAN_COL=$(grep -i "scan.*throughput" "$PROBE" | grep -v insert | \
    awk '{for(i=1;i<=NF;i++) if($i~/^[0-9]+\.[0-9]+$/) {print i; exit}}')
INSERT_COL=$(grep -i "insert.*throughput" "$PROBE" | \
    awk '{for(i=1;i<=NF;i++) if($i~/^[0-9]+\.[0-9]+$/) {print i; exit}}')
rm -f "$PROBE"

echo -e "Detected: scan number is column ${SCAN_COL:-?}, insert number is column ${INSERT_COL:-?}"
if [[ -z "$SCAN_COL" || -z "$INSERT_COL" ]]; then
    echo -e "${RED}ERROR: could not detect number column from binary output.${NC}"
    echo    "  Check that the binary runs correctly and prints throughput lines."
    exit 1
fi
echo ""

# override run_once with detected columns
run_once(){
    local BIN="$1"
    local TMPFILE
    TMPFILE=$(mktemp /tmp/bh_bench_XXXXXX.txt)

    if [[ $HAS_TIMEOUT -eq 1 ]]; then
        timeout "$((DURATION + 30))" \
            "$BIN" "$INITIAL_KEYS" "$SCAN_THREADS" "$INSERT_THREADS" "$DURATION" \
            > "$TMPFILE" 2>&1 || true
    else
        "$BIN" "$INITIAL_KEYS" "$SCAN_THREADS" "$INSERT_THREADS" "$DURATION" \
            > "$TMPFILE" 2>&1 || true
    fi

    LAST_SCAN=$(grep -i "scan.*throughput" "$TMPFILE" | grep -v insert \
        | awk -v col="$SCAN_COL" '{print $col}')
    LAST_INSERT=$(grep -i "insert.*throughput" "$TMPFILE" \
        | awk -v col="$INSERT_COL" '{print $col}')

    LAST_SCAN="${LAST_SCAN:-0.0}"
    LAST_INSERT="${LAST_INSERT:-0.0}"

    rm -f "$TMPFILE"
}

# ── warmup ────────────────────────────────────────────────────────────────────
echo -e "${YELLOW}Warming up (1 throwaway run each)…${NC}"
run_once "$SYNC_BIN"
run_once "$ASYNC_BIN"
echo -e "${GREEN}Warmup done.${NC}\n"

echo -e "${BOLD}Config: initial_keys=$INITIAL_KEYS  scan_threads=$SCAN_THREADS  insert_threads=$INSERT_THREADS  duration=${DURATION}s${NC}"
echo -e "${BOLD}Rounds: $RUNS  (interleaved S→A each round)${NC}\n"

# ── interleaved collection ────────────────────────────────────────────────────
SYNC_SCANS=()
SYNC_INSERTS=()
ASYNC_SCANS=()
ASYNC_INSERTS=()

for i in $(seq 1 "$RUNS"); do
    printf "${YELLOW}[round %d/%d]${NC}\n" "$i" "$RUNS"

    printf "  SYNC  … "
    run_once "$SYNC_BIN"
    printf "scan=%-10s insert=%s Mops/s\n" "$LAST_SCAN" "$LAST_INSERT"
    SYNC_SCANS+=("$LAST_SCAN")
    SYNC_INSERTS+=("$LAST_INSERT")

    printf "  ASYNC … "
    run_once "$ASYNC_BIN"
    printf "scan=%-10s insert=%s Mops/s\n" "$LAST_SCAN" "$LAST_INSERT"
    ASYNC_SCANS+=("$LAST_SCAN")
    ASYNC_INSERTS+=("$LAST_INSERT")
done

# ── stats ─────────────────────────────────────────────────────────────────────
SYNC_AVG_SCAN=$(avg_arr   "${SYNC_SCANS[@]}")
SYNC_AVG_INS=$(avg_arr    "${SYNC_INSERTS[@]}")
ASYNC_AVG_SCAN=$(avg_arr  "${ASYNC_SCANS[@]}")
ASYNC_AVG_INS=$(avg_arr   "${ASYNC_INSERTS[@]}")

SYNC_SD_SCAN=$(std_arr    "$SYNC_AVG_SCAN"  "${SYNC_SCANS[@]}")
SYNC_SD_INS=$(std_arr     "$SYNC_AVG_INS"   "${SYNC_INSERTS[@]}")
ASYNC_SD_SCAN=$(std_arr   "$ASYNC_AVG_SCAN" "${ASYNC_SCANS[@]}")
ASYNC_SD_INS=$(std_arr    "$ASYNC_AVG_INS"  "${ASYNC_INSERTS[@]}")

CV_SS=$(echo "$SYNC_SD_SCAN  $SYNC_AVG_SCAN"  | awk '{printf "%.1f", ($2>0 ? $1/$2*100 : 0)}')
CV_SI=$(echo "$SYNC_SD_INS   $SYNC_AVG_INS"   | awk '{printf "%.1f", ($2>0 ? $1/$2*100 : 0)}')
CV_AS=$(echo "$ASYNC_SD_SCAN $ASYNC_AVG_SCAN" | awk '{printf "%.1f", ($2>0 ? $1/$2*100 : 0)}')
CV_AI=$(echo "$ASYNC_SD_INS  $ASYNC_AVG_INS"  | awk '{printf "%.1f", ($2>0 ? $1/$2*100 : 0)}')

PCT_SCAN=$(pct_change "$ASYNC_AVG_SCAN" "$SYNC_AVG_SCAN")
PCT_INS=$(pct_change  "$ASYNC_AVG_INS"  "$SYNC_AVG_INS")

# ── report ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  FINAL REPORT  ($RUNS interleaved rounds)${NC}"
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
printf "%-24s  %12s  %12s  %s\n" "" "SYNC avg" "ASYNC avg" "delta"
printf "%-24s  %12s  %12s  %s\n" \
    "────────────────────────" "────────────" "────────────" "──────────────"
printf "%-24s  %9s Mops  %9s Mops  " \
    "Scan throughput" "$SYNC_AVG_SCAN" "$ASYNC_AVG_SCAN"
echo -e "$(color_pct "$PCT_SCAN")"
printf "%-24s  %9s Mops  %9s Mops  " \
    "Insert throughput" "$SYNC_AVG_INS" "$ASYNC_AVG_INS"
echo -e "$(color_pct "$PCT_INS")"
printf "%-24s  %10s       %10s\n" \
    "Scan  σ / CV" "$SYNC_SD_SCAN (${CV_SS}%)" "$ASYNC_SD_SCAN (${CV_AS}%)"
printf "%-24s  %10s       %10s\n" \
    "Insert σ / CV" "$SYNC_SD_INS  (${CV_SI}%)" "$ASYNC_SD_INS  (${CV_AI}%)"
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo ""

MAX_CV=$(echo "$CV_SS $CV_SI $CV_AS $CV_AI" | tr ' ' '\n' | \
    awk '{v=$1+0; if(v>m)m=v} END{printf "%.1f",m}')
if awk "BEGIN{exit !(($MAX_CV) > 15)}" 2>/dev/null; then
    echo -e "${YELLOW}⚠  CV > 15% — variance is high. Run with more rounds:${NC}"
    echo    "   $0 $((RUNS + 5)) $INITIAL_KEYS $SCAN_THREADS $INSERT_THREADS $DURATION"
else
    echo -e "${GREEN}✓  CV ≤ 15% — results are stable.${NC}"
fi