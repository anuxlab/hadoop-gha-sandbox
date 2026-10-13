#!/usr/bin/env bash
# Validates HiveServer2, metastore connectivity and SQL reads over HDFS output.
set -Eeuo pipefail
INPUT_DIR="${1:-/test/output}"
HDFS_URI="${HDFS_URI:-hdfs://namenode:9000}"
READY_TIMEOUT="${READY_TIMEOUT:-360}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-180}"
log() { printf '[INFO] %s\n' "$*"; }
err() { printf '[ERROR] %s\n' "$*" >&2; }
compose() { docker compose "$@"; }

compose ps --services --filter status=running | grep -Fxq hive-server || { err 'Compose service hive-server is not running.'; exit 2; }
compose exec -T namenode hdfs dfs -test -e "${INPUT_DIR}/_SUCCESS" || { err "MapReduce output missing at ${INPUT_DIR}; run the WordCount test first."; exit 2; }

log "Waiting for HiveServer2 query readiness (timeout ${READY_TIMEOUT}s)..."
elapsed=0
ready=0
while (( elapsed < READY_TIMEOUT )); do
  if compose exec -T hive-server timeout 15 beeline \
      -u 'jdbc:hive2://localhost:10000/;auth=noSasl' -n root \
      --silent=true --showHeader=false --outputformat=tsv2 -e 'SELECT 1;' 2>/dev/null \
      | awk '$0 ~ /^[[:space:]]*1[[:space:]]*$/ {found=1} END {exit !found}'; then
    ready=1; break
  fi
  sleep 10
  elapsed=$((elapsed + 10))
done
if (( ready == 0 )); then
  err 'HiveServer2 did not become query-ready.'
  compose logs --no-color --tail=150 hive-server hive-metastore-postgresql || true
  exit 3
fi

query_file=$(mktemp)
result_file=$(mktemp)
trap 'rm -f "$query_file" "$result_file"' EXIT
cat > "$query_file" <<SQL
DROP TABLE IF EXISTS wordcount;
CREATE EXTERNAL TABLE wordcount (
  word STRING,
  word_count INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '${HDFS_URI}${INPUT_DIR}';
SELECT word, word_count FROM wordcount ORDER BY word;
SQL
compose exec -T hive-server bash -c 'cat > /tmp/ci-wordcount.hql' < "$query_file"

log 'Creating external table and reading MapReduce output through HiveQL...'
compose exec -T hive-server timeout "$QUERY_TIMEOUT" beeline \
  -u 'jdbc:hive2://localhost:10000/;auth=noSasl' -n root \
  --silent=true --showHeader=false --outputformat=tsv2 \
  --hiveconf hive.server2.enable.doAs=false \
  -f /tmp/ci-wordcount.hql > "$result_file" 2>&1 || {
    err 'Hive DDL/query failed or timed out.'
    sed -n '1,160p' "$result_file" >&2
    compose logs --no-color --tail=150 hive-server hive-metastore-postgresql || true
    exit 4
  }

# Beeline versions vary in their banner output. Extract only rows with two TSV fields.
actual=$(mktemp)
expected=$(mktemp)
trap 'rm -f "$query_file" "$result_file" "$actual" "$expected"' EXIT
awk -F '\t' 'NF == 2 && $2 ~ /^[0-9]+$/ && $1 !~ /^(word|INFO|WARN|Connecting|Connected|Closing|No rows)/ {print $1 "\t" $2}' "$result_file" | LC_ALL=C sort > "$actual"
printf 'Action\t1\nBig\t1\nData\t1\nDocker\t2\nExample\t1\nHadoop\t3\nHello\t3\nMapReduce\t2\nWordCount\t1\nYARN\t1\nand\t1\nin\t1\nwith\t1\n' | LC_ALL=C sort > "$expected"
if ! diff -u "$expected" "$actual"; then
  err 'Hive result rows did not match expected WordCount output.'
  sed -n '1,160p' "$result_file" >&2
  exit 5
fi
log 'Hive query returned all expected word/count pairs.'
