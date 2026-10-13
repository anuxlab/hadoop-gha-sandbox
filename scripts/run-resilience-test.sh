#!/usr/bin/env bash
# Controlled one-DataNode outage: validate HDFS reads and MapReduce while degraded,
# then restore the DataNode and verify the expected replication is healthy again.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
NAMENODE_SERVICE="${NAMENODE_SERVICE:-namenode}"
INPUT_DIR="${INPUT_DIR:-/test/input}"
BASE_OUTPUT_DIR="${OUTPUT_DIR:-/test/output}"
RESILIENCE_OUTPUT_DIR="${RESILIENCE_OUTPUT_DIR:-/test/resilience-output}"
EXPECTED_REPLICATION="${EXPECTED_REPLICATION:-2}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-240}"
POLL_SECONDS="${RESILIENCE_POLL_SECONDS:-5}"
HDFS_REPLICATION_TIMEOUT="${HDFS_REPLICATION_TIMEOUT:-180}"
HADOOP_VERSION="${HADOOP_VERSION:-3.2.1}"
EXAMPLES_JAR="/opt/hadoop-${HADOOP_VERSION}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar"

log() { printf '[RESILIENCE] %s\n' "$*"; }
err() { printf '[RESILIENCE][ERROR] %s\n' "$*" >&2; }
compose() { docker compose "$@"; }

tmp_dir="$(mktemp -d)"
datanode2_stopped=0
cleanup() {
  local rc=$?
  trap - EXIT
  set +e
  if (( datanode2_stopped )); then
    log 'Safety cleanup: attempting to restart datanode2 before exiting.'
    compose start datanode2 >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_dir"
  exit "$rc"
}
trap cleanup EXIT

active_services="$(compose ps --services --filter status=running)"
grep -Fxq "$NAMENODE_SERVICE" <<< "$active_services" || { err "NameNode service '$NAMENODE_SERVICE' is not running."; exit 2; }
grep -Fxq 'datanode2' <<< "$active_services" || { err 'datanode2 must be running before the resilience check.'; exit 2; }
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -test -e "${INPUT_DIR}/wordcount-input.txt" || {
  err "Baseline input is missing: ${INPUT_DIR}/wordcount-input.txt. The profile runner must complete WordCount first."; exit 2;
}
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -test -e "${BASE_OUTPUT_DIR}/_SUCCESS" || {
  err "Baseline MapReduce output is missing: ${BASE_OUTPUT_DIR}/_SUCCESS."; exit 2;
}
compose exec -T "$NAMENODE_SERVICE" test -r "$EXAMPLES_JAR" || { err "MapReduce examples JAR not found: $EXAMPLES_JAR"; exit 2; }

live_datanodes() {
  local report
  report="$(compose exec -T "$NAMENODE_SERVICE" hdfs dfsadmin -report 2>&1)" || {
    printf '%s\n' "$report" >&2
    return 1
  }
  printf '%s\n' "$report" | sed -nE 's/.*Live datanodes \(([0-9]+)\).*/\1/p' | head -n 1
}

wait_for_live_datanodes() {
  local expected="$1" elapsed=0 observed report
  while (( elapsed <= MAX_WAIT_SECONDS )); do
    observed="$(live_datanodes 2>/dev/null || true)"
    if [[ "$observed" =~ ^[0-9]+$ ]] && (( observed == expected )); then
      log "NameNode reports ${observed} live DataNode(s), as expected."
      return 0
    fi
    if (( elapsed == 0 || elapsed % 30 == 0 )); then
      log "Waiting for ${expected} live DataNode(s); current count=${observed:-unknown}; elapsed=${elapsed}s."
    fi
    sleep "$POLL_SECONDS"
    elapsed=$((elapsed + POLL_SECONDS))
  done
  report="$(compose exec -T "$NAMENODE_SERVICE" hdfs dfsadmin -report 2>&1 || true)"
  err "Timed out waiting for exactly ${expected} live DataNode(s). Last report follows."
  printf '%s\n' "$report" >&2
  return 1
}

canonicalize_hadoop_output() {
  local hdfs_path="$1" output_file="$2"
  compose exec -T "$NAMENODE_SERVICE" hdfs dfs -cat "${hdfs_path}/part-r-*" \
    | awk 'NF >= 2 {print $1 "\t" $2}' | LC_ALL=C sort > "$output_file"
}

log 'Confirming the distributed baseline starts with two live DataNodes.'
wait_for_live_datanodes 2
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -cat "${INPUT_DIR}/wordcount-input.txt" > "$tmp_dir/input-before.txt"
[[ -s "$tmp_dir/input-before.txt" ]] || { err 'Baseline input is unexpectedly empty.'; exit 3; }
canonicalize_hadoop_output "$BASE_OUTPUT_DIR" "$tmp_dir/baseline.tsv"

log 'Stopping datanode2 to simulate a single DataNode outage.'
compose stop -t 10 datanode2
# Set the safety flag immediately so any later assertion failure restores the service.
datanode2_stopped=1
wait_for_live_datanodes 1

log 'Reading the previously replicated HDFS input while datanode2 is unavailable.'
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -cat "${INPUT_DIR}/wordcount-input.txt" > "$tmp_dir/input-during-outage.txt"
diff -u "$tmp_dir/input-before.txt" "$tmp_dir/input-during-outage.txt" || {
  err 'HDFS input read during the DataNode outage differs from the baseline.'; exit 4;
}
log 'HDFS read availability assertion passed with one DataNode unavailable.'

log 'Submitting a bounded MapReduce WordCount while one DataNode is unavailable.'
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -rm -r -f "$RESILIENCE_OUTPUT_DIR" >/dev/null 2>&1 || true
if ! compose exec -T "$NAMENODE_SERVICE" timeout "${JOB_TIMEOUT:-240}" \
    hadoop jar "$EXAMPLES_JAR" wordcount \
    -D "dfs.replication=1" -D 'mapreduce.job.reduces=1' \
    "$INPUT_DIR" "$RESILIENCE_OUTPUT_DIR" 2>&1 | tee "$tmp_dir/mapreduce-during-outage.log"; then
  err 'MapReduce failed while one DataNode was unavailable.'
  exit 5
fi

job_id="$(grep -Eo 'job_[0-9]+_[0-9]+' "$tmp_dir/mapreduce-during-outage.log" | tail -n 1 || true)"
[[ -n "$job_id" ]] || { err 'Could not identify the outage-time MapReduce job ID.'; exit 5; }
app_id="application_${job_id#job_}"
app_status="$(compose exec -T "$NAMENODE_SERVICE" yarn application -status "$app_id" 2>&1)" || {
  printf '%s\n' "$app_status" >&2
  err "Could not inspect YARN application ${app_id}."
  exit 5
}
printf '%s\n' "$app_status"
grep -Eq 'Final-State[[:space:]]*:[[:space:]]*SUCCEEDED' <<< "$app_status" || {
  err "YARN application ${app_id} did not succeed during the DataNode outage."; exit 5;
}
compose exec -T "$NAMENODE_SERVICE" hdfs dfs -test -e "${RESILIENCE_OUTPUT_DIR}/_SUCCESS" || {
  err 'Outage-time MapReduce _SUCCESS marker is missing.'; exit 5;
}
canonicalize_hadoop_output "$RESILIENCE_OUTPUT_DIR" "$tmp_dir/outage.tsv"
diff -u "$tmp_dir/baseline.tsv" "$tmp_dir/outage.tsv" || {
  err 'Outage-time MapReduce result differs from the baseline output.'; exit 5;
}
log "MapReduce succeeded during the outage (YARN application ${app_id}); output matches the baseline."

log 'Restarting datanode2 and waiting for both DataNodes to re-register.'
compose start datanode2
datanode2_stopped=0
wait_for_live_datanodes 2

log "Restoring ${EXPECTED_REPLICATION} replicas for outage-time output after recovery."
compose exec -T "$NAMENODE_SERVICE" timeout "$HDFS_REPLICATION_TIMEOUT" \
  hdfs dfs -setrep -w -R "$EXPECTED_REPLICATION" "$RESILIENCE_OUTPUT_DIR"
for hdfs_path in "${INPUT_DIR}/wordcount-input.txt" "$RESILIENCE_OUTPUT_DIR"; do
  fsck_report="$(compose exec -T "$NAMENODE_SERVICE" hdfs fsck "$hdfs_path" -files -blocks -locations 2>&1)" || {
    printf '%s\n' "$fsck_report" >&2
    err "hdfs fsck failed for ${hdfs_path} after recovery."; exit 6;
  }
  printf '%s\n' "$fsck_report" | python3 "$ROOT/scripts/assert-hdfs-replication.py" "$EXPECTED_REPLICATION"
done

log 'PASS: HDFS reads and MapReduce succeeded during one DataNode outage; two-DataNode service and expected block replication recovered.'
