#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_latency.sh — averaged latency comparison: sync vs async
# Usage: ./run_latency.sh [runs] [initial_keys] [threads] [queries_per_thread]
# Example: ./run_latency.sh 7 5000000 4 100000
# ─────────────────────────────────────────────────────────────────────────────

RUNS="${1:-7}"
INITIAL_KEYS="${2:-5000000}"
THREADS="${3:-4}"
QUERIES="${4:-100000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
SYNC_BIN="$BUILD_DIR/test/bench_range_latency_sync"
ASYNC_BIN="$BUILD_DIR/test/bench_range_latency_async"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ── sanity checks ─────────────────────────────────────────────────────────────
for BIN in "$SYNC_BIN" "$ASYNC_BIN"; do
    if [[ ! -x "$BIN" ]]; then
        echo -e "${RED}ERROR: binary not found: $BIN${NC}"
        echo "  Run: cd $BUILD_DIR && cmake .. && make -j"
        exit 1
    fi
done

HAS_TIMEOUT=0
if command -v timeout &>/dev/null; then
    HAS_TIMEOUT=1
fi

# ── globals filled by run_once ────────────────────────────────────────────────
LAST_MEAN="0"; LAST_P50="0"; LAST_P90="0"; LAST_P99="0"
LAST_P999="0"; LAST_MAX="0"; LAST_SPIKES="0"; LAST_SPIKE_PCT="0.0"

run_once(){
    local BIN="$1"
    local TMPFILE
    TMPFILE=$(mktemp /tmp/bh_lat_XXXXXX.txt)

    if [[ $HAS_TIMEOUT -eq 1 ]]; then
        timeout 120 "$BIN" "$INITIAL_KEYS" "$THREADS" "$QUERIES" \
            > "$TMPFILE" 2>&1 || true
    else
        "$BIN" "$INITIAL_KEYS" "$THREADS" "$QUERIES" \
            > "$TMPFILE" 2>&1 || true
    fi

    # Parse each metric — hunt for the first floating-point or integer number on matching lines
    extract_num(){
        local PATTERN="$1"
        grep -i "$PATTERN" "$TMPFILE" \
            | awk '{for(i=1;i<=NF;i++) if($i~/^[0-9]+\.?[0-9]*$/) {print $i; exit}}'
    }

    LAST_MEAN=$(extract_num "mean")
    LAST_P50=$(extract_num "p50")
    LAST_P90=$(extract_num "p90")
    LAST_P99=$(extract_num "p99[^.]")        # p99 but not p99.9
    LAST_P999=$(extract_num "p99\\.9")
    LAST_MAX=$(extract_num "max")

    # spikes line: "spikes (>10x median): 1338 (0.3345%)"
    local SPIKE_LINE
    SPIKE_LINE=$(grep -i "spike" "$TMPFILE" || true)
    LAST_SPIKES=$(echo "$SPIKE_LINE" | awk '{for(i=1;i<=NF;i++) if($i~/^[0-9]+$/) {print $i; exit}}')
    LAST_SPIKE_PCT=$(echo "$SPIKE_LINE" | grep -oE '[0-9]+\.[0-9]+%' | tr -d '%' || true)

    # defaults
    LAST_MEAN="${LAST_MEAN:-0}";     LAST_P50="${LAST_P50:-0}"
    LAST_P90="${LAST_P90:-0}";       LAST_P99="${LAST_P99:-0}"
    LAST_P999="${LAST_P999:-0}";     LAST_MAX="${LAST_MAX:-0}"
    LAST_SPIKES="${LAST_SPIKES:-0}"; LAST_SPIKE_PCT="${LAST_SPIKE_PCT:-0.0}"

    rm -f "$TMPFILE"
}

# ── awk helpers ───────────────────────────────────────────────────────────────
avg_arr(){
    printf '%s\n' "$@" | awk '{s+=$1; n++} END{printf "%.1f", (n ? s/n : 0)}'
}

std_arr(){
    local AVG="$1"; shift
    printf '%s\n' "$@" | \
        awk -v a="$AVG" '{d=$1-a; s+=d*d; n++} END{printf "%.1f", (n ? sqrt(s/n) : 0)}'
}

pct_change(){
    # pct_change new old → prints e.g. "-18.2"
    echo "$1 $2" | awk '{
        if ($2 == 0) { print "0.0"; exit }
        printf "%.1f", (($1 - $2) / $2) * 100
    }'
}

color_pct_lower_better(){
    # for latency / spikes: NEGATIVE delta is good (green)
    local V="$1"
    local NUM="${V#+}"
    if awk "BEGIN{exit !(($NUM) < -2)}" 2>/dev/null; then
        printf "${GREEN}%s%%${NC}" "$V"
    elif awk "BEGIN{exit !(($NUM) > 2)}" 2>/dev/null; then
        printf "${RED}+%s%%${NC}" "$V"
    else
        printf "${YELLOW}%s%% (noise)${NC}" "$V"
    fi
}

color_pct_higher_better(){
    # for spike reduction: opposite
    local V="$1"
    local NUM="${V#+}"
    if awk "BEGIN{exit !(($NUM) > 2)}" 2>/dev/null; then
        printf "${GREEN}+%s%%${NC}" "$V"
    elif awk "BEGIN{exit !(($NUM) < -2)}" 2>/dev/null; then
        printf "${RED}%s%%${NC}" "$V"
    else
        printf "${YELLOW}%s%% (noise)${NC}" "$V"
    fi
}

# ── probe output format ──────────────────────────────────────────────────────
echo -e "${YELLOW}Probing binary output format…${NC}"
PROBE=$(mktemp /tmp/bh_lat_probe_XXXXXX.txt)
"$SYNC_BIN" "$INITIAL_KEYS" "$THREADS" 1000 > "$PROBE" 2>&1 || true
echo -e "${BOLD}--- sample output (1000 queries) ---${NC}"
cat "$PROBE"
echo -e "${BOLD}------------------------------------${NC}"
rm -f "$PROBE"
echo ""

# ── warmup ────────────────────────────────────────────────────────────────────
echo -e "${YELLOW}Warming up (1 throwaway run each)…${NC}"
run_once "$SYNC_BIN"
run_once "$ASYNC_BIN"
echo -e "${GREEN}Warmup done.${NC}\n"

TOTAL_QUERIES=$(( THREADS * QUERIES ))
echo -e "${BOLD}Config: initial_keys=$INITIAL_KEYS  threads=$THREADS  queries/thread=$QUERIES  total=$TOTAL_QUERIES${NC}"
echo -e "${BOLD}Rounds: $RUNS  (interleaved S→A each round)${NC}\n"

# ── interleaved collection ────────────────────────────────────────────────────
declare -a S_MEAN S_P50 S_P90 S_P99 S_P999 S_MAX S_SPIKES S_SPCT
declare -a A_MEAN A_P50 A_P90 A_P99 A_P999 A_MAX A_SPIKES A_SPCT

for i in $(seq 1 "$RUNS"); do
    printf "${YELLOW}[round %d/%d]${NC}\n" "$i" "$RUNS"

    printf "  SYNC  … "
    run_once "$SYNC_BIN"
    printf "mean=%-6s p99.9=%-8s spikes=%s (%s%%)\n" \
        "$LAST_MEAN" "$LAST_P999" "$LAST_SPIKES" "$LAST_SPIKE_PCT"
    S_MEAN+=("$LAST_MEAN");   S_P50+=("$LAST_P50");     S_P90+=("$LAST_P90")
    S_P99+=("$LAST_P99");     S_P999+=("$LAST_P999");   S_MAX+=("$LAST_MAX")
    S_SPIKES+=("$LAST_SPIKES"); S_SPCT+=("$LAST_SPIKE_PCT")

    printf "  ASYNC … "
    run_once "$ASYNC_BIN"
    printf "mean=%-6s p99.9=%-8s spikes=%s (%s%%)\n" \
        "$LAST_MEAN" "$LAST_P999" "$LAST_SPIKES" "$LAST_SPIKE_PCT"
    A_MEAN+=("$LAST_MEAN");   A_P50+=("$LAST_P50");     A_P90+=("$LAST_P90")
    A_P99+=("$LAST_P99");     A_P999+=("$LAST_P999");   A_MAX+=("$LAST_MAX")
    A_SPIKES+=("$LAST_SPIKES"); A_SPCT+=("$LAST_SPIKE_PCT")
done

# ── compute stats ─────────────────────────────────────────────────────────────
AVG_S_MEAN=$(avg_arr "${S_MEAN[@]}");       AVG_A_MEAN=$(avg_arr "${A_MEAN[@]}")
AVG_S_P50=$(avg_arr  "${S_P50[@]}");        AVG_A_P50=$(avg_arr  "${A_P50[@]}")
AVG_S_P90=$(avg_arr  "${S_P90[@]}");        AVG_A_P90=$(avg_arr  "${A_P90[@]}")
AVG_S_P99=$(avg_arr  "${S_P99[@]}");        AVG_A_P99=$(avg_arr  "${A_P99[@]}")
AVG_S_P999=$(avg_arr "${S_P999[@]}");       AVG_A_P999=$(avg_arr "${A_P999[@]}")
AVG_S_MAX=$(avg_arr  "${S_MAX[@]}");        AVG_A_MAX=$(avg_arr  "${A_MAX[@]}")
AVG_S_SPIKES=$(avg_arr "${S_SPIKES[@]}");   AVG_A_SPIKES=$(avg_arr "${A_SPIKES[@]}")
AVG_S_SPCT=$(avg_arr "${S_SPCT[@]}");       AVG_A_SPCT=$(avg_arr "${A_SPCT[@]}")

SD_S_SPCT=$(std_arr "$AVG_S_SPCT" "${S_SPCT[@]}")
SD_A_SPCT=$(std_arr "$AVG_A_SPCT" "${A_SPCT[@]}")
SD_S_P999=$(std_arr "$AVG_S_P999" "${S_P999[@]}")
SD_A_P999=$(std_arr "$AVG_A_P999" "${A_P999[@]}")

PCT_MEAN=$(pct_change "$AVG_A_MEAN" "$AVG_S_MEAN")
PCT_P50=$(pct_change  "$AVG_A_P50"  "$AVG_S_P50")
PCT_P90=$(pct_change  "$AVG_A_P90"  "$AVG_S_P90")
PCT_P99=$(pct_change  "$AVG_A_P99"  "$AVG_S_P99")
PCT_P999=$(pct_change "$AVG_A_P999" "$AVG_S_P999")
PCT_MAX=$(pct_change  "$AVG_A_MAX"  "$AVG_S_MAX")
PCT_SPIKES=$(pct_change "$AVG_A_SPIKES" "$AVG_S_SPIKES")
PCT_SPCT=$(pct_change "$AVG_A_SPCT" "$AVG_S_SPCT")

# ── report ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  LATENCY REPORT  ($RUNS interleaved rounds, $TOTAL_QUERIES queries each)${NC}"
echo -e "${BOLD}══════════════════════════════════════════════════════════════════${NC}"
printf "%-20s  %12s  %12s  %s\n" "" "SYNC avg" "ASYNC avg" "delta"
printf "%-20s  %12s  %12s  %s\n" \
    "────────────────────" "────────────" "────────────" "────────────────"

printf "%-20s  %9s ns   %9s ns   " "mean"  "$AVG_S_MEAN"  "$AVG_A_MEAN"
echo -e "$(color_pct_lower_better "$PCT_MEAN")"

printf "%-20s  %9s ns   %9s ns   " "p50"   "$AVG_S_P50"   "$AVG_A_P50"
echo -e "$(color_pct_lower_better "$PCT_P50")"

printf "%-20s  %9s ns   %9s ns   " "p90"   "$AVG_S_P90"   "$AVG_A_P90"
echo -e "$(color_pct_lower_better "$PCT_P90")"

printf "%-20s  %9s ns   %9s ns   " "p99"   "$AVG_S_P99"   "$AVG_A_P99"
echo -e "$(color_pct_lower_better "$PCT_P99")"

printf "%-20s  %9s ns   %9s ns   " "p99.9" "$AVG_S_P999"  "$AVG_A_P999"
echo -e "$(color_pct_lower_better "$PCT_P999")"

printf "%-20s  %9s ns   %9s ns   " "max"   "$AVG_S_MAX"   "$AVG_A_MAX"
echo -e "$(color_pct_lower_better "$PCT_MAX")"

printf "%-20s  %12s  %12s  %s\n" \
    "────────────────────" "────────────" "────────────" "────────────────"

printf "%-20s  %9s       %9s       " "spike count" "$AVG_S_SPIKES" "$AVG_A_SPIKES"
echo -e "$(color_pct_lower_better "$PCT_SPIKES")"

printf "%-20s  %8s%%      %8s%%      " "spike %%" "$AVG_S_SPCT" "$AVG_A_SPCT"
echo -e "$(color_pct_lower_better "$PCT_SPCT")"

echo -e "${BOLD}══════════════════════════════════════════════════════════════════${NC}"
echo ""

# ── stability ─────────────────────────────────────────────────────────────────
echo -e "${BOLD}Stability:${NC}"
printf "  p99.9 σ:    sync=%-10s  async=%s\n" "$SD_S_P999" "$SD_A_P999"
printf "  spike%% σ:   sync=%-10s  async=%s\n" "$SD_S_SPCT" "$SD_A_SPCT"

CV_S_SPCT=$(echo "$SD_S_SPCT $AVG_S_SPCT" | awk '{printf "%.1f", ($2>0 ? $1/$2*100 : 0)}')
CV_A_SPCT=$(echo "$SD_A_SPCT $AVG_A_SPCT" | awk '{printf "%.1f", ($2>0 ? $1/$2*100 : 0)}')
printf "  spike%% CV:  sync=%-10s  async=%s\n" "${CV_S_SPCT}%" "${CV_A_SPCT}%"
echo ""

MAX_CV=$(echo "$CV_S_SPCT $CV_A_SPCT" | tr ' ' '\n' | \
    awk '{v=$1+0; if(v>m)m=v} END{printf "%.1f",m}')
if awk "BEGIN{exit !(($MAX_CV) > 20)}" 2>/dev/null; then
    echo -e "${YELLOW}⚠  CV > 20% — spike variance is high. Run with more rounds:${NC}"
    echo    "   $0 $((RUNS + 5)) $INITIAL_KEYS $THREADS $QUERIES"
else
    echo -e "${GREEN}✓  CV ≤ 20% — results are stable.${NC}"
fi

# ── per-round detail table ────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}Per-round detail:${NC}"
printf "  %-6s  %10s  %10s  %10s  %10s  %10s  %10s\n" \
    "round" "S_p99.9" "A_p99.9" "S_spike%" "A_spike%" "S_max" "A_max"
printf "  %-6s  %10s  %10s  %10s  %10s  %10s  %10s\n" \
    "──────" "──────────" "──────────" "──────────" "──────────" "──────────" "──────────"
for i in $(seq 0 $((RUNS - 1))); do
    printf "  %-6s  %8s ns  %8s ns  %8s%%    %8s%%    %8s ns  %8s ns\n" \
        "$((i+1))" \
        "${S_P999[$i]}" "${A_P999[$i]}" \
        "${S_SPCT[$i]}" "${A_SPCT[$i]}" \
        "${S_MAX[$i]}" "${A_MAX[$i]}"
done
echo ""