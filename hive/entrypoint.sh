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

# Write to temporary files and atomically rename them. A process that reads
# the configuration during startup must never observe an empty/truncated XML
# file while shell redirection is still writing it.
core_tmp="$(mktemp /etc/hadoop/.core-site.xml.XXXXXX)"
cat > "$core_tmp" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property>
</configuration>
XML
chmod 0644 "$core_tmp"
mv -f "$core_tmp" /etc/hadoop/core-site.xml

# The pinned bde2020 metastore image initializes the hive role with password 'hive'.
DB_PASSWORD="hive"
hive_tmp="$(mktemp /opt/hive/conf/.hive-site.xml.XXXXXX)"
cat > "$hive_tmp" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:postgresql://hive-metastore-postgresql:5432/metastore</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>org.postgresql.Driver</value></property>
  <property><name>javax.jdo.option.ConnectionUserName</name><value>hive</value></property>
  <property><name>javax.jdo.option.ConnectionPassword</name><value>${DB_PASSWORD}</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
  <property><name>hive.metastore.schema.verification</name><value>false</value></property>
  <!-- The bde2020 PostgreSQL image initializes the Hive 2.3 schema.
       Do not let DataNucleus attempt DDL as the non-owner `hive` role. -->
  <property><name>datanucleus.schema.autoCreateAll</name><value>false</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.server2.authentication</name><value>NOSASL</value></property>
  <property><name>hive.server2.thrift.port</name><value>10000</value></property>
  <property><name>hive.server2.thrift.bind.host</name><value>0.0.0.0</value></property>
  <property><name>hive.stats.autogather</name><value>false</value></property>
  <property><name>hive.stats.collect.rawdatasize</name><value>false</value></property>
  <property><name>hive.fetch.task.conversion</name><value>more</value></property>
</configuration>
XML
chmod 0644 "$hive_tmp"
mv -f "$hive_tmp" /opt/hive/conf/hive-site.xml

log 'Starting HiveServer2...'
exec hive --service hiveserver2
