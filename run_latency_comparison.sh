#!/bin/bash

get_mem() {
    ps -C postgres -o rss= | awk '{sum+=$1} END {print sum/1024}'
}

rm -f /dev/shm/blinkhash_shm

echo "================================================================"
echo " SCENARIO A: PREVIOUS METHOD (Isolated Multi-Process Memory)"
echo "================================================================"
cd /workspaces/Blink-hash/index/blink-hash-pg/pg_blinkhash
make clean > /dev/null 2>&1
make USE_SHARED_MEMORY=0 > /dev/null 2>&1
sudo make install > /dev/null 2>&1
sudo service postgresql restart > /dev/null 2>&1
sleep 2

psql -U postgres -d testdb -c "DROP TABLE IF EXISTS test_ts;" > /dev/null 2>&1
psql -U postgres -d testdb -c "CREATE TABLE test_ts (id int8, ts int8, val int8);" > /dev/null 2>&1
psql -U postgres -d testdb -c "CREATE INDEX ts_idx ON test_ts USING blinkhash (id);" > /dev/null 2>&1

echo "-> Running pgbench (10 connections, 15 seconds)..."
pgbench -U postgres testdb -c 10 -j 10 -T 15 -f /tmp/pgbench_run.sql > /tmp/result_isolated.txt 2>&1 &
PGBENCH_PID=$!
sleep 14
ISO_MEM=$(get_mem)
wait $PGBENCH_PID

grep -E "tps =|latency average" /tmp/result_isolated.txt
echo "Peak Memory (Postgres Processes): ${ISO_MEM} MB"
echo ""

rm -f /dev/shm/blinkhash_shm

echo "================================================================"
echo " SCENARIO B: NEW NATIVE METHOD (Shared MMAP Arena)"
echo "================================================================"
make clean > /dev/null 2>&1
make USE_SHARED_MEMORY=1 > /dev/null 2>&1
sudo make install > /dev/null 2>&1
sudo service postgresql restart > /dev/null 2>&1
sleep 2

psql -U postgres -d testdb -c "DROP TABLE IF EXISTS test_ts;" > /dev/null 2>&1
psql -U postgres -d testdb -c "CREATE TABLE test_ts (id int8, ts int8, val int8);" > /dev/null 2>&1
psql -U postgres -d testdb -c "CREATE INDEX ts_idx ON test_ts USING blinkhash (id);" > /dev/null 2>&1

echo "-> Running pgbench (10 connections, 15 seconds)..."
pgbench -U postgres testdb -c 10 -j 10 -T 15 -f /tmp/pgbench_run.sql > /tmp/result_shared.txt 2>&1 &
PGBENCH_PID=$!
sleep 14
SHARED_MEM=$(get_mem)
wait $PGBENCH_PID

grep -E "tps =|latency average" /tmp/result_shared.txt
echo "Peak Memory (Postgres Processes): ${SHARED_MEM} MB"
echo "================================================================"
