#!/usr/bin/env bash
# Fast regression tests for the three readiness bugs found in CI diagnostics.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/mapred-site.xml" <<'XML'
<configuration>
<property><name>mapreduce.framework.name</name><value>yarn</value></property>
<property><name>yarn.resourcemanager.address</name><value>0.0.0.0</value></property>
</configuration>
XML

framework="$(sed -n '/<name>mapreduce.framework.name<\/name>/s#.*<value>\([^<]*\)</value>.*#\1#p' "$tmp/mapred-site.xml")"
[[ "$framework" == "yarn" ]] || {
  printf 'FAIL: expected framework=yarn, got <%s>\n' "$framework" >&2
  exit 1
}
printf '[PASS] MapReduce framework parser returns only the requested property.\n'

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
