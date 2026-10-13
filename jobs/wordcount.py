# -*- coding: utf-8 -*-
"""Small deterministic PySpark WordCount for the CI Hadoop sandbox."""
import socket

from pyspark.sql import SparkSession

INPUT_PATH = "hdfs://namenode:9000/test/input"
OUTPUT_PATH = "hdfs://namenode:9000/test/spark-output"
WORKER_HOSTS_PATH = "hdfs://namenode:9000/test/spark-worker-hosts"


def format_count_pair(item):
    """Serialize an RDD (word, count) pair in Hadoop's tab-separated format."""
    return "{0}\t{1}".format(item[0], item[1])


def host_for_partition(_iterator):
    """Report the executing executor container hostname for this partition."""
    yield socket.gethostname()


def main():
    spark = SparkSession.builder.appName("HadoopGHASandboxWordCount").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try:
        counts = (
            sc.textFile(INPUT_PATH)
            .flatMap(lambda line: line.split())
            .map(lambda word: (word, 1))
            .reduceByKey(lambda left, right: left + right)
        )

        # Serialize to a stable word<TAB>count contract. Saving raw pair RDDs
        # would write Python tuple representations such as ('Hadoop', 3), which
        # do not match MapReduce TextOutputFormat or the Hive external table.
        # Avoid collect() so the driver does not need to materialize all counts.
        counts.map(format_count_pair).saveAsTextFile(OUTPUT_PATH)
        print("Spark WordCount output written to {0}".format(OUTPUT_PATH))

        # Exercise actual executor placement. A standalone cluster can have
        # several registered workers without a given application using them;
        # run enough small partitions and persist the distinct executor hosts
        # so the harness can assert worker participation in distributed mode.
        expected_workers = int(sc.getConf().get("spark.gha.expectedWorkers", "1"))
        probe_partitions = max(2, expected_workers * 2)
        worker_hosts = sorted(
            sc.parallelize(range(probe_partitions), probe_partitions)
            .mapPartitions(host_for_partition)
            .distinct()
            .collect()
        )
        if len(worker_hosts) < expected_workers:
            raise RuntimeError(
                "Expected tasks on at least {0} distinct Spark worker hosts, got {1}: {2}".format(
                    expected_workers, len(worker_hosts), worker_hosts
                )
            )
        sc.parallelize(worker_hosts, 1).saveAsTextFile(WORKER_HOSTS_PATH)
        print("Spark executor worker hosts: {0}".format(",".join(worker_hosts)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
