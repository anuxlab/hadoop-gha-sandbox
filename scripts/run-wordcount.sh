#!/usr/bin/env bash
# End-to-end HDFS + YARN + MapReduce smoke test.
set -Eeuo pipefail

NAMENODE_SERVICE="${NAMENODE_SERVICE:-namenode}"
INPUT_DIR="${INPUT_DIR:-/test/input}"
OUTPUT_DIR="${OUTPUT_DIR:-/test/output}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-300}"
EXPECTED_DATANODES="${EXPECTED_DATANODES:-1}"
EXPECTED_NODEMANAGERS="${EXPECTED_NODEMANAGERS:-1}"

log() { printf '[INFO] %s\n' "$*"; }
err() { printf '[ERROR] %s\n' "$*" >&2; }
compose() { docker compose "$@"; }

if ! command -v docker >/dev/null 2>&1; then err 'Docker CLI is required.'; exit 1; fi
if ! compose ps --services --filter status=running | grep -Fxq "$NAMENODE_SERVICE"; then
  err "Compose service '$NAMENODE_SERVICE' is not running. Start the selected profile first."
  exit 2
fi

wait_for_cluster() {
  local elapsed=0 live_dn live_nm framework
  log "Waiting for ${EXPECTED_DATANODES} live DataNode(s) and ${EXPECTED_NODEMANAGERS} RUNNING NodeManager(s)..."
  while (( elapsed < MAX_WAIT_SECONDS )); do
    live_dn=$(compose exec -T "$NAMENODE_SERVICE" hdfs dfsadmin -report 2>/dev/null \
      | awk '/Live datanodes \(/ {gsub(/[^0-9]/, "", $3); print $3; exit}' || true)
    live_nm=$(compose exec -T "$NAMENODE_SERVICE" yarn node -list -all 2>/dev/null \
      | awk '/RUNNING/ {n++} END {print n+0}' || true)
    framework=$(compose exec -T "$NAMENODE_SERVICE" hadoop getconf -confKey mapreduce.framework.name 2>/dev/null || true)
    if [[ "$live_dn" =~ ^[0-9]+$ ]] && (( live_dn >= EXPECTED_DATANODES )) \
       && [[ "$live_nm" =~ ^[0-9]+$ ]] && (( live_nm >= EXPECTED_NODEMANAGERS )) \
       && [[ "$framework" == "yarn" ]]; then
      log "Cluster ready: DataNodes=${live_dn}, NodeManagers=${live_nm}, framework=${framework}."
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done
  err "Cluster did not become ready within ${MAX_WAIT_SECONDS}s."
  compose ps -a || true
  compose logs --no-color --tail=100 namenode datanode resourcemanager nodemanager || true
  return 3
}

wait_for_cluster

log 'Preparing HDFS paths...'
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -mkdir -p "$INPUT_DIR"
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -rm -r -f "$OUTPUT_DIR" >/dev/null 2>&1 || true
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -rm -r -f "${INPUT_DIR}/wordcount-input.txt" >/dev/null 2>&1 || true

cat > /tmp/hadoop-gha-wordcount-input.txt <<'DATA'
Hello Hadoop
Hello MapReduce
Hello Docker
Hadoop MapReduce Docker
Big Data with Hadoop and YARN
WordCount Example in Action
DATA

log 'Uploading deterministic fixture...'
compose exec -T "$NAMENODE_SERVICE" bash -c \
  'cat > /tmp/wordcount-input.txt && hdfs dfs -put -f /tmp/wordcount-input.txt "$1"' \
  _ "$INPUT_DIR/wordcount-input.txt" < /tmp/hadoop-gha-wordcount-input.txt

if [[ -n "${EXPECTED_REPLICATION:-}" ]]; then
  log "Setting test-file replication to ${EXPECTED_REPLICATION}..."
  compose exec -T "$NAMENODE_SERVICE" hdfs dfs -setrep -w "$EXPECTED_REPLICATION" "$INPUT_DIR/wordcount-input.txt"
  actual_replication=$(compose exec -T "$NAMENODE_SERVICE" hdfs dfs -stat '%r' "$INPUT_DIR/wordcount-input.txt" | tr -d '\r')
  [[ "$actual_replication" == "$EXPECTED_REPLICATION" ]] || { err "Expected replication ${EXPECTED_REPLICATION}, got ${actual_replication}."; exit 4; }
fi

HADOOP_VERSION="${HADOOP_VERSION:-3.2.1}"
EXAMPLES_JAR="/opt/hadoop-${HADOOP_VERSION}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar"
log 'Verifying MapReduce examples JAR...'
compose exec -T "$NAMENODE_SERVICE" test -r "$EXAMPLES_JAR" || { err "Examples JAR not found: ${EXAMPLES_JAR}"; exit 4; }

job_log=$(mktemp)
actual=$(mktemp)
expected=$(mktemp)
trap 'rm -f "$job_log" "$actual" "$expected" /tmp/hadoop-gha-wordcount-input.txt' EXIT

log 'Submitting WordCount through YARN...'
if ! compose exec -T "$NAMENODE_SERVICE" timeout "${JOB_TIMEOUT:-180}" \
  hadoop jar "$EXAMPLES_JAR" wordcount "$INPUT_DIR" "$OUTPUT_DIR" 2>&1 | tee "$job_log"; then
  err 'MapReduce job command failed.'
  exit 5
fi

job_id=$(grep -Eo 'job_[0-9]+_[0-9]+' "$job_log" | tail -n 1 || true)
if [[ -z "$job_id" ]]; then
  err 'Could not identify the submitted YARN job ID in Hadoop output.'
  exit 5
fi
app_id="application_${job_id#job_}"
app_status=$(compose exec -T "$NAMENODE_SERVICE" yarn application -status "$app_id" 2>&1) || {
  printf '%s\n' "$app_status" >&2
  err "Could not retrieve YARN application status for ${app_id}."
  exit 5
}
printf '%s\n' "$app_status"
grep -Eq 'Final-State[[:space:]]*:[[:space:]]*SUCCEEDED' <<< "$app_status" || {
  err "YARN application ${app_id} did not finish SUCCEEDED."
  exit 5
}

compose exec -T "$NAMENODE_SERVICE" hdfs dfs -test -e "$OUTPUT_DIR/_SUCCESS" || { err 'MapReduce _SUCCESS marker is missing.'; exit 5; }

compose exec -T "$NAMENODE_SERVICE" hdfs dfs -cat "${OUTPUT_DIR}/part-r-*" \
  | awk 'NF >= 2 {print $1 "\t" $2}' | LC_ALL=C sort > "$actual"
printf 'Action\t1\nBig\t1\nData\t1\nDocker\t2\nExample\t1\nHadoop\t3\nHello\t3\nMapReduce\t2\nWordCount\t1\nYARN\t1\nand\t1\nin\t1\nwith\t1\n' > "$expected"
LC_ALL=C sort -o "$expected" "$expected"
if ! diff -u "$expected" "$actual"; then
  err 'MapReduce output differs from expected word counts.'
  exit 5
fi

log 'MapReduce output matches expected counts.'
compose exec -T "$NAMENODE_SERVICE" yarn application -list -appStates FINISHED
log 'Core Hadoop integration test passed.'
