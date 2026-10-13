#!/usr/bin/env bash
# Fast regression tests for readiness, MapReduce launch classpath, and CI failure propagation.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/mapred-site.xml" <<'XML'
<configuration>
<property><name>mapreduce.framework.name</name><value>yarn</value></property>
<property><name>yarn.resourcemanager.address</name><value>0.0.0.0</value></property>
<property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/</value></property>
<property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/</value></property>
<property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/</value></property>
</configuration>
XML

framework="$(sed -n '/<name>mapreduce.framework.name<\/name>/s#.*<value>\([^<]*\)</value>.*#\1#p' "$tmp/mapred-site.xml")"
[[ "$framework" == "yarn" ]] || {
  printf 'FAIL: expected framework=yarn, got <%s>\n' "$framework" >&2
  exit 1
}
printf '[PASS] MapReduce framework parser returns only the requested property.\n'

# Exercise the same property/value matching logic used by the runtime preflight.
for property in yarn.app.mapreduce.am.env mapreduce.map.env mapreduce.reduce.env; do
  if ! awk -v want="$property" -v expected="HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/" '
    /<property>/ { name=""; value="" }
    /<name>/ {
      x=$0; sub(/^.*<name>/, "", x); sub(/<\/name>.*$/, "", x); name=x
    }
    /<value>/ {
      x=$0; sub(/^.*<value>/, "", x); sub(/<\/value>.*$/, "", x); value=x
    }
    /<\/property>/ && name == want && value == expected { found=1 }
    END { exit !found }
  ' "$tmp/mapred-site.xml"; then
    printf 'FAIL: MapReduce launch property %s was not parsed correctly.\n' "$property" >&2
    exit 1
  fi
done
printf '[PASS] MRAppMaster/map/reduce XML values are parsed and validated.\n'

# Regression: production readiness check must not use a login shell, whose
# profile emits `mesg: ttyname failed` into captured stdout on this image.
grep -Fq 'compose exec -T "$NAMENODE_SERVICE" sh -c' "$ROOT/scripts/run-wordcount.sh" || {
  printf 'FAIL: framework check must use a non-login shell.\n' >&2
  exit 1
}
if grep -Fq 'compose exec -T "$NAMENODE_SERVICE" bash -lc' "$ROOT/scripts/run-wordcount.sh"; then
  printf 'FAIL: login shell can contaminate framework parser output.\n' >&2
  exit 1
fi
printf '[PASS] Framework readiness command avoids login-shell stdout noise.\n'


cat > "$tmp/spark-master.json" <<'JSON'
{"workers":[
  {"id":"worker-1","state":"ALIVE","cores":1},
  {"id":"worker-2","state":"ALIVE","cores":1},
  {"id":"worker-3","state":"DEAD","cores":1}
]}
JSON
worker_count="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for w in d.get("workers", []) if w.get("alive") is True or str(w.get("state", "")).upper() == "ALIVE"))' < "$tmp/spark-master.json")"
[[ "$worker_count" == "2" ]] || {
  printf 'FAIL: expected 2 live Spark workers, got <%s>\n' "$worker_count" >&2
  exit 1
}
printf '[PASS] Spark parser counts ALIVE workers and ignores DEAD workers.\n'

# Regression: Spark output must use the same word<TAB>count format as Hadoop
# TextOutputFormat; writing a tuple RDD directly produces "('word', count)".
python3 - "$ROOT/jobs/wordcount.py" <<'SPARKTEST'
import ast
import pathlib
import sys

job_path = pathlib.Path(sys.argv[1])
source = job_path.read_text()
tree = ast.parse(source, filename=str(job_path))
formatter = next(
    (node for node in tree.body if isinstance(node, ast.FunctionDef)
     and node.name == "format_count_pair"),
    None,
)
if formatter is None:
    raise SystemExit("FAIL: Spark job has no format_count_pair serializer")
module = ast.Module(body=[formatter], type_ignores=[])
namespace = {}
exec(compile(ast.fix_missing_locations(module), str(job_path), "exec"), namespace)
actual = namespace["format_count_pair"](("Hadoop", 3))
if actual != "Hadoop\t3":
    raise SystemExit("FAIL: Spark serializer returned {!r}, expected tab-separated output".format(actual))
if "counts.map(format_count_pair).saveAsTextFile(OUTPUT_PATH)" not in source:
    raise SystemExit("FAIL: Spark save path does not apply the output serializer")
if "counts.saveAsTextFile(OUTPUT_PATH)" in source:
    raise SystemExit("FAIL: Spark still serializes raw Python tuples")
print("[PASS] Spark output uses normalized tab-separated word/count rows.")
SPARKTEST

if grep -Fq "SELECT 1 FROM VERSION" "$ROOT/docker-compose.hive.yml"; then
  printf 'FAIL: PostgreSQL healthcheck must not wait for Hive VERSION schema.\n' >&2
  exit 1
fi
grep -Fq "psql -U postgres -d metastore -tAc 'SELECT 1'" "$ROOT/docker-compose.hive.yml" || {
  printf 'FAIL: expected SQL-level PostgreSQL readiness check.\n' >&2
  exit 1
}
grep -Fq '<name>datanucleus.schema.autoCreateAll</name><value>false</value>' "$ROOT/hive/entrypoint.sh" || {
  printf 'FAIL: Hive must not try to recreate schema objects owned by postgres.\n' >&2
  exit 1
}
printf '[PASS] Hive database healthcheck and schema ownership settings are safe.\n'

# The Hive image had a malformed-XML startup attempt in CI diagnostics.
# Config files should be assembled completely before replacing their live paths.
grep -Fq 'mv -f "$core_tmp" /etc/hadoop/core-site.xml' "$ROOT/hive/entrypoint.sh" || {
  printf 'FAIL: core-site.xml must be atomically replaced.\n' >&2
  exit 1
}
grep -Fq 'mv -f "$hive_tmp" /opt/hive/conf/hive-site.xml' "$ROOT/hive/entrypoint.sh" || {
  printf 'FAIL: hive-site.xml must be atomically replaced.\n' >&2
  exit 1
}
printf '[PASS] Hive XML configuration is written atomically.\n'

# Regression: all three Hadoop 3.2.1 launch contexts must set HADOOP_MAPRED_HOME.
# Without it YARN cannot load org.apache.hadoop.mapreduce.v2.app.MRAppMaster.
for mapping in \
  'MAPRED_CONF_yarn_app_mapreduce_am_env: "HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/"' \
  'MAPRED_CONF_mapreduce_map_env: "HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/"' \
  'MAPRED_CONF_mapreduce_reduce_env: "HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/"'; do
  grep -Fq "$mapping" "$ROOT/docker-compose.yml" || {
    printf 'FAIL: missing MapReduce launch environment setting: %s\n' "$mapping" >&2
    exit 1
  }
done
grep -Fq 'MAPRED_CONF_yarn_app_mapreduce_am_env: "HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/"' "$ROOT/docker-compose.distributed.yml" || {
  printf 'FAIL: distributed NodeManager 2 is missing HADOOP_MAPRED_HOME.\n' >&2
  exit 1
}
grep -Fq 'hadoop-mapreduce-client-app-${version}.jar' "$ROOT/scripts/run-wordcount.sh" || {
  printf 'FAIL: missing MapReduce ApplicationMaster JAR preflight.\n' >&2
  exit 1
}
printf '[PASS] MapReduce ApplicationMaster and task environment is configured and preflighted.\n'

# Regression: GitHub Actions must preserve failures when tee writes logs.
python3 - "$ROOT" <<'PY'
import pathlib, re, sys
root = pathlib.Path(sys.argv[1])
workflows = [
    ".github/workflows/test-hadoop.yml",
    ".github/workflows/test-distributed.yml",
    ".github/workflows/test-full-stack.yml",
]
for rel in workflows:
    lines = (root / rel).read_text().splitlines()
    for i, line in enumerate(lines):
        if "| tee " not in line:
            continue
        start = i
        while start >= 0 and not re.match(r"^\s+- name:", lines[start]):
            start -= 1
        if start < 0 or not any("set -Eeuo pipefail" in x for x in lines[start:i]):
            raise SystemExit(f"FAIL: {rel}:{i+1} pipes test output to tee without pipefail")
print("[PASS] All workflow test pipelines preserve script failures through tee.")
PY


# Regression: distributed Spark must request enough cores and assert the
# executor host identities actually used by tasks, not just registered workers.
grep -Fq 'spark.gha.expectedWorkers' "$ROOT/scripts/run-spark-job.sh" || {
  printf 'FAIL: Spark submission must pass its expected worker count.\n' >&2
  exit 1
}
grep -Fq 'spark.cores.max=${SPARK_TOTAL_CORES:-${EXPECTED_SPARK_WORKERS:-1}}' "$ROOT/scripts/run-spark-job.sh" || {
  printf 'FAIL: Spark core cap must scale with the requested worker count.\n' >&2
  exit 1
}
grep -Fq 'WORKER_HOSTS_PATH' "$ROOT/scripts/run-spark-job.sh" || {
  printf 'FAIL: Spark harness must validate executor worker-host output.\n' >&2
  exit 1
}
grep -Fq 'host_for_partition' "$ROOT/jobs/wordcount.py" || {
  printf 'FAIL: Spark job must include an executor-host participation probe.\n' >&2
  exit 1
}
printf '[PASS] Distributed Spark requests and verifies actual worker participation.\n'

# Regression: requested HDFS replication must be checked against live replicas
# and their reported DataNode locations, not only dfs -stat %r.
grep -Fq 'hdfs fsck "$INPUT_DIR/wordcount-input.txt" -files -blocks -locations' "$ROOT/scripts/run-wordcount.sh" || {
  printf 'FAIL: replication test must inspect HDFS block locations with fsck.\n' >&2
  exit 1
}
grep -Fq 'assert-hdfs-replication.py' "$ROOT/scripts/run-wordcount.sh" || {
  printf 'FAIL: replication test must use the tested fsck parser.\n' >&2
  exit 1
}
fsck_valid_sample="$(printf '%s\n' \
  '/test/wordcount-input.txt 100 bytes, replicated: replication=2, 1 block(s): OK' \
  '0. BP-1:blk_1 len=100 Live_repl=2 [DatanodeInfoWithStorage[172.18.0.2:9866,DS-a,DISK], DatanodeInfoWithStorage[172.18.0.3:9866,DS-b,DISK]]' \
  'Status: HEALTHY')"
printf '%s\n' "$fsck_valid_sample" | python3 "$ROOT/scripts/assert-hdfs-replication.py" 2 >/dev/null || {
  printf 'FAIL: valid two-DataNode fsck sample should pass.\n' >&2
  exit 1
}
fsck_invalid_sample="$(printf '%s\n' \
  '/test/wordcount-input.txt 100 bytes, replicated: replication=2, 1 block(s): OK' \
  '0. BP-1:blk_1 len=100 Live_repl=2 [DatanodeInfoWithStorage[172.18.0.2:9866,DS-a,DISK], DatanodeInfoWithStorage[172.18.0.2:9866,DS-b,DISK]]' \
  'Status: HEALTHY')"
if printf '%s\n' "$fsck_invalid_sample" | python3 "$ROOT/scripts/assert-hdfs-replication.py" 2 >/dev/null 2>&1; then
  printf 'FAIL: two replicas on a single DataNode endpoint must fail.\n' >&2
  exit 1
fi
printf '[PASS] HDFS fsck parser verifies live replicas on distinct DataNodes.\n'

# Workflow triggers: full stack auto-runs on relevant PRs/pushes; distributed
# runs on relevant PRs/pushes and manual dispatch. Nightly schedule stays off.
python3 - "$ROOT" <<'PYWORKFLOW'
import pathlib, re, sys
root = pathlib.Path(sys.argv[1])
full = (root / ".github/workflows/test-full-stack.yml").read_text()
distributed = (root / ".github/workflows/test-distributed.yml").read_text()
for name, text in (("full-stack", full), ("distributed", distributed)):
    for event in ("pull_request", "push", "workflow_dispatch"):
        if not re.search(r"(?m)^  " + re.escape(event) + r":", text):
            raise SystemExit(f"FAIL: {name} workflow is missing {event} trigger")
if re.search(r"(?m)^  schedule:", distributed):
    raise SystemExit("FAIL: nightly schedule must remain disabled until explicitly enabled")
if not re.search(r"(?m)^  # schedule:", distributed):
    raise SystemExit("FAIL: distributed workflow should document the optional nightly schedule")
runner = (root / "scripts/run-profile.sh")
if not runner.exists():
    raise SystemExit("FAIL: local reusable profile runner is missing")
for profile in ("core", "hive", "spark", "full", "distributed"):
    if profile not in runner.read_text():
        raise SystemExit(f"FAIL: local profile runner is missing profile {profile}")
print("[PASS] Automatic CI triggers, optional nightly behavior and local profile runner are configured.")
PYWORKFLOW
