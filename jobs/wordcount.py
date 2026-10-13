# -*- coding: utf-8 -*-
"""Small deterministic PySpark WordCount for the CI Hadoop sandbox."""
from pyspark.sql import SparkSession

INPUT_PATH = "hdfs://namenode:9000/test/input"
OUTPUT_PATH = "hdfs://namenode:9000/test/spark-output"


def format_count_pair(item):
    """Serialize an RDD (word, count) pair in Hadoop's tab-separated format."""
    return "{0}\t{1}".format(item[0], item[1])


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
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
