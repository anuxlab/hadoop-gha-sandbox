# Hadoop GHA Sandbox
This project will create a minimal but functional Hadoop YARN cluster using Docker Compose to test the pipelines and their limitations.

A minimal Hadoop YARN cluster that runs within the resource limits using Docker Compose.

## Purpose

This project demonstrates how to simulate a multi-node Hadoop ecosystem in a CI environment. It starts a core cluster (HDFS, YARN, MapReduce) and runs a WordCount job to validate the setup.

## Components

- **HDFS**: NameNode, DataNode
- **YARN**: ResourceManager, NodeManager
- **MapReduce**: Example WordCount job

## Local Usage

```bash
# Start the cluster
docker-compose up -d

# Wait for services to be ready
# Access NameNode UI at http://localhost:9870
# Access ResourceManager UI at http://localhost:8088

# Run a test job
docker exec namenode hdfs dfs -mkdir -p /test/input
echo "Hello Hadoop" > test.txt
docker exec namenode hdfs dfs -put /tmp/test.txt /test/input/
docker exec namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar wordcount /test/input /test/output
docker exec namenode hdfs dfs -cat /test/output/part-r-00000

# Stop the cluster
docker-compose down -v
