#!/bin/bash
set -euo pipefail

log() { echo "[hive-custom] $*"; }

# Wait for dependent services to be available
for svc in namenode:9870 datanode:9864 resourcemanager:8088 hive-metastore-postgresql:5432; do
    host="${svc%:*}"
    port="${svc#*:}"
    log "Waiting for ${host}:${port}..."
    for i in $(seq 1 90); do
        if bash -c "</dev/tcp/${host}/${port}" 2>/dev/null; then
            log "${host}:${port} is up."
            break
        fi
        if [ "$i" -eq 90 ]; then
            log "ERROR: ${host}:${port} never became available."
            exit 1
        fi
        sleep 2
    done
done

# Write the correct core-site.xml
log "Writing /etc/hadoop/core-site.xml with fs.defaultFS=hdfs://namenode:9000"
cat > /etc/hadoop/core-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:9000</value>
  </property>
</configuration>
EOF

# Write the correct hive-site.xml with proper PostgreSQL credentials
log "Writing /opt/hive/conf/hive-site.xml"
cat > /opt/hive/conf/hive-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://hive-metastore-postgresql:5432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>
</configuration>
EOF

log "Starting HiveServer2..."
exec hive --service hiveserver2