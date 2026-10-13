#!/usr/bin/env bash
# Minimal HiveServer2 configuration for this disposable CI-only sandbox.
set -Eeuo pipefail
log() { printf '[hive-custom] %s\n' "$*"; }

wait_for_tcp() {
  local host="$1" port="$2" limit="${3:-180}" elapsed=0
  log "Waiting for ${host}:${port}..."
  until bash -c "</dev/tcp/${host}/${port}" 2>/dev/null; do
    if (( elapsed >= limit )); then
      log "ERROR: ${host}:${port} unavailable after ${limit}s."
      return 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  log "${host}:${port} accepts connections."
}

wait_for_tcp namenode 9000 180
wait_for_tcp datanode 9864 180
wait_for_tcp resourcemanager 8088 180
wait_for_tcp hive-metastore-postgresql 5432 180

mkdir -p /etc/hadoop /opt/hive/conf
cat > /etc/hadoop/core-site.xml <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property>
</configuration>
XML

DB_PASSWORD="${HIVE_METASTORE_PASSWORD:-hive}"
cat > /opt/hive/conf/hive-site.xml <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:postgresql://hive-metastore-postgresql:5432/metastore</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>org.postgresql.Driver</value></property>
  <property><name>javax.jdo.option.ConnectionUserName</name><value>hive</value></property>
  <property><name>javax.jdo.option.ConnectionPassword</name><value>${DB_PASSWORD}</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
  <property><name>hive.metastore.schema.verification</name><value>false</value></property>
  <property><name>datanucleus.schema.autoCreateAll</name><value>true</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.server2.authentication</name><value>NOSASL</value></property>
  <property><name>hive.server2.thrift.port</name><value>10000</value></property>
  <property><name>hive.server2.thrift.bind.host</name><value>0.0.0.0</value></property>
  <property><name>hive.stats.autogather</name><value>false</value></property>
  <property><name>hive.stats.collect.rawdatasize</name><value>false</value></property>
  <property><name>hive.fetch.task.conversion</name><value>more</value></property>
</configuration>
XML

log 'Starting HiveServer2...'
exec hive --service hiveserver2
