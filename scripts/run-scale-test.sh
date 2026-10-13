#!/usr/bin/env bash
# Multi-file, multi-block HDFS + multi-split YARN/MapReduce + Spark scale test.
set -Eeuo pipefail

SCALE_FILES="${SCALE_FILES:-24}"
SCALE_LINES_PER_FILE="${SCALE_LINES_PER_FILE:-800}"
SCALE_INPUT_DIR="${SCALE_INPUT_DIR:-/test/scale-input}"
SCALE_MR_OUTPUT_DIR="${SCALE_MR_OUTPUT_DIR:-/test/scale-mapreduce-output}"
SCALE_SPARK_OUTPUT_DIR="${SCALE_SPARK_OUTPUT_DIR:-/test/scale-spark-output}"
SCALE_PROBE_HOSTS_DIR="${SCALE_PROBE_HOSTS_DIR:-/test/scale-spark-probe-hosts}"
SCALE_WORDCOUNT_HOSTS_DIR="${SCALE_WORDCOUNT_HOSTS_DIR:-/test/scale-wordcount-hosts}"
EXPECTED_REPLICATION="${EXPECTED_REPLICATION:-2}"
EXPECTED_SPARK_WORKERS="${EXPECTED_SPARK_WORKERS:-2}"
SCALE_REDUCERS="${SCALE_REDUCERS:-2}"
SCALE_BLOCK_SIZE="${SCALE_BLOCK_SIZE:-16384}"

log() { printf '[SCALE] %s\n' "$*"; }
err() { printf '[SCALE][ERROR] %s\n' "$*" >&2; }
compose() { docker compose "$@"; }

is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }
for pair in "SCALE_FILES:$SCALE_FILES" "SCALE_LINES_PER_FILE:$SCALE_LINES_PER_FILE" "EXPECTED_REPLICATION:$EXPECTED_REPLICATION" "EXPECTED_SPARK_WORKERS:$EXPECTED_SPARK_WORKERS" "SCALE_REDUCERS:$SCALE_REDUCERS" "SCALE_BLOCK_SIZE:$SCALE_BLOCK_SIZE"; do
  key="${pair%%:*}"
  value="${pair#*:}"
  if ! is_uint "$value"; then err "$key must be a positive integer; got '$value'."; exit 2; fi
done

# Bound the manual inputs to keep this a CI scale test, not an unbounded load test.
if (( SCALE_FILES < 2 || SCALE_FILES > 128 )); then err 'SCALE_FILES must be between 2 and 128.'; exit 2; fi
if (( SCALE_LINES_PER_FILE < 600 || SCALE_LINES_PER_FILE > 2000 )); then err 'SCALE_LINES_PER_FILE must be between 600 and 2000 so each file crosses the 16 KiB block boundary.'; exit 2; fi
if (( SCALE_FILES * SCALE_LINES_PER_FILE > 128000 )); then err 'SCALE_FILES * SCALE_LINES_PER_FILE must not exceed 128000 records.'; exit 2; fi
if (( EXPECTED_REPLICATION != 2 )); then err 'The scale topology has exactly two DataNodes; EXPECTED_REPLICATION must be 2.'; exit 2; fi
if (( EXPECTED_SPARK_WORKERS != 2 )); then err 'The scale topology currently declares exactly two Spark workers; EXPECTED_SPARK_WORKERS must be 2.'; exit 2; fi
if (( SCALE_REDUCERS < 1 || SCALE_REDUCERS > 4 )); then err 'SCALE_REDUCERS must be between 1 and 4.'; exit 2; fi
if (( SCALE_BLOCK_SIZE != 16384 )); then err 'The checked-in scale overlay uses a 16384-byte HDFS block size; set SCALE_BLOCK_SIZE=16384 or update the overlay and test together.'; exit 2; fi

if ! compose ps --services --filter status=running | grep -Fxq namenode; then
  err 'NameNode is not running. Start the scale profile with scripts/run-profile.sh scale.'
  exit 2
fi

records=$((SCALE_FILES * SCALE_LINES_PER_FILE))
hadoop_count=$((records * 2))
log "Generating ${SCALE_FILES} files × ${SCALE_LINES_PER_FILE} lines = ${records} records; reducers=${SCALE_REDUCERS}; HDFS block size=${SCALE_BLOCK_SIZE}; expected replication=${EXPECTED_REPLICATION}."

tmp_dir="$(mktemp -d)"
mapreduce_log="$tmp_dir/mapreduce-scale.log"
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/input"

python3 - "$tmp_dir/input" "$SCALE_FILES" "$SCALE_LINES_PER_FILE" <<'PY'
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
file_count = int(sys.argv[2])
line_count = int(sys.argv[3])
line = "hadoop spark hadoop mapreduce\n"
for index in range(file_count):
    (root / ("part-{0:05d}.txt".format(index))).write_text(line * line_count, encoding="utf-8")
PY

python3 - "$tmp_dir/input" "$tmp_dir/input.tar.gz" <<'PY'
import pathlib
import sys
import tarfile
root = pathlib.Path(sys.argv[1])
with tarfile.open(sys.argv[2], "w:gz") as archive:
    for path in sorted(root.glob("*.txt")):
        archive.add(str(path), arcname=path.name)
PY

compose exec -T namenode sh -c \
  'rm -rf /tmp/hadoop-gha-scale-input && mkdir -p /tmp/hadoop-gha-scale-input && tar -xzf - -C /tmp/hadoop-gha-scale-input' \
  < "$tmp_dir/input.tar.gz"

compose exec -T namenode hdfs dfs -rm -r -f \
  "$SCALE_INPUT_DIR" "$SCALE_MR_OUTPUT_DIR" "$SCALE_SPARK_OUTPUT_DIR" \
  "$SCALE_PROBE_HOSTS_DIR" "$SCALE_WORDCOUNT_HOSTS_DIR" >/dev/null 2>&1 || true
compose exec -T namenode hdfs dfs -mkdir -p "$SCALE_INPUT_DIR"
compose exec -T namenode bash -c \
  'hdfs dfs -put /tmp/hadoop-gha-scale-input/part-*.txt "$1"' _ "$SCALE_INPUT_DIR"

log 'Waiting for requested replication on every block in the scale input...'
compose exec -T namenode timeout "${HDFS_REPLICATION_TIMEOUT:-180}" hdfs dfs -setrep -w -R "$EXPECTED_REPLICATION" "$SCALE_INPUT_DIR"
fsck_output="$(compose exec -T namenode hdfs fsck "$SCALE_INPUT_DIR" -files -blocks -locations 2>&1)" || {
  printf '%s\n' "$fsck_output" >&2
  err 'hdfs fsck failed for the scale input.'
  exit 3
}
printf '%s\n' "$fsck_output" | python3 scripts/assert-hdfs-replication.py "$EXPECTED_REPLICATION"

hadoop_version="${HADOOP_VERSION:-3.2.1}"
examples_jar="/opt/hadoop-${hadoop_version}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${hadoop_version}.jar"
compose exec -T namenode test -r "$examples_jar" || { err "MapReduce examples JAR not found: ${examples_jar}"; exit 4; }
log 'Running the Hadoop examples WordCount on the scale dataset through YARN...'
if ! compose exec -T namenode timeout "${JOB_TIMEOUT:-300}" \
  hadoop jar "$examples_jar" wordcount -D "mapreduce.job.reduces=${SCALE_REDUCERS}" "$SCALE_INPUT_DIR" "$SCALE_MR_OUTPUT_DIR" 2>&1 | tee "$mapreduce_log"; then
  err 'Scale MapReduce job command failed.'
  exit 4
fi

splits="$(sed -nE 's/.*number of splits:[[:space:]]*([0-9]+).*/\1/p' "$mapreduce_log" | tail -n 1)"
if ! is_uint "${splits:-}" || (( splits <= SCALE_FILES )); then
  err "Expected the small HDFS block size to produce more splits than input files; files=${SCALE_FILES}, splits=${splits:-unknown}."
  exit 4
fi
map_records="$(sed -nE 's/.*Map input records=([0-9]+).*/\1/p' "$mapreduce_log" | tail -n 1)"
if [[ "$map_records" != "$records" ]]; then
  err "MapReduce consumed ${map_records:-unknown} records; expected ${records}."
  exit 4
fi
reduce_tasks="$(sed -nE 's/.*Launched reduce tasks=([0-9]+).*/\1/p' "$mapreduce_log" | tail -n 1)"
if ! is_uint "${reduce_tasks:-}" || (( reduce_tasks < SCALE_REDUCERS )); then
  err "MapReduce launched ${reduce_tasks:-unknown} reduce tasks; expected at least ${SCALE_REDUCERS}."
  exit 4
fi
log "MapReduce input split assertion passed: ${splits} splits from ${SCALE_FILES} multi-block files."
log "MapReduce record counter matches: ${map_records} records; launched reduce tasks=${reduce_tasks}."

job_id="$(grep -Eo 'job_[0-9]+_[0-9]+' "$mapreduce_log" | tail -n 1 || true)"
if [[ -z "$job_id" ]]; then err 'Could not identify the scale YARN job ID.'; exit 4; fi
app_id="application_${job_id#job_}"
app_status="$(compose exec -T namenode yarn application -status "$app_id" 2>&1)" || { printf '%s\n' "$app_status" >&2; err 'Could not retrieve scale YARN application status.'; exit 4; }
printf '%s\n' "$app_status"
grep -Eq 'Final-State[[:space:]]*:[[:space:]]*SUCCEEDED' <<< "$app_status" || { err "Scale YARN application ${app_id} did not succeed."; exit 4; }
compose exec -T namenode hdfs dfs -test -e "$SCALE_MR_OUTPUT_DIR/_SUCCESS" || { err 'Scale MapReduce _SUCCESS marker is missing.'; exit 4; }

actual_file="$tmp_dir/actual.tsv"
expected_file="$tmp_dir/expected.tsv"
compose exec -T namenode hdfs dfs -cat "${SCALE_MR_OUTPUT_DIR}/part-r-*" \
  | awk 'NF >= 2 {print $1 "\t" $2}' | LC_ALL=C sort > "$actual_file"
printf 'hadoop\t%s\nmapreduce\t%s\nspark\t%s\n' "$hadoop_count" "$records" "$records" \
  | LC_ALL=C sort > "$expected_file"
if ! diff -u "$expected_file" "$actual_file"; then err 'Scale MapReduce counts differ from the deterministic expected counts.'; exit 4; fi
log 'Scale MapReduce output matches expected counts.'

log 'Running Spark against the same scale input and comparing against MapReduce...'
SPARK_INPUT_PATH="$SCALE_INPUT_DIR" \
SPARK_OUTPUT_PATH="$SCALE_SPARK_OUTPUT_DIR" \
SPARK_WORKER_HOSTS_PATH="$SCALE_PROBE_HOSTS_DIR" \
SPARK_WORDCOUNT_WORKER_HOSTS_PATH="$SCALE_WORDCOUNT_HOSTS_DIR" \
MR_OUTPUT_PATH="$SCALE_MR_OUTPUT_DIR" \
REQUIRE_SPARK_DATA_WORKER_PARTICIPATION=1 \
SPARK_TOTAL_CORES="${SPARK_TOTAL_CORES:-$EXPECTED_SPARK_WORKERS}" \
bash scripts/run-spark-job.sh

elapsed_note="files=${SCALE_FILES},lines_per_file=${SCALE_LINES_PER_FILE},records=${records},splits=${splits},reducers=${SCALE_REDUCERS},replication=${EXPECTED_REPLICATION},spark_workers=${EXPECTED_SPARK_WORKERS}"
printf '%s\n' "$elapsed_note"
log 'Scalable HDFS/YARN/MapReduce/Spark integration test passed.'
