"""
PySpark WordCount that reads from HDFS and writes back to HDFS.
"""
from pyspark.sql import SparkSession

INPUT_PATH = "hdfs://namenode:9000/test/input"
OUTPUT_PATH = "hdfs://namenode:9000/test/spark-output"

def main():
    spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    rdd = sc.textFile(INPUT_PATH)
    counts = (
        rdd.flatMap(lambda line: line.split())
           .map(lambda word: (word, 1))
           .reduceByKey(lambda a, b: a + b)
    )

    print("=" * 50)
    print("PySpark WordCount Results")
    print("=" * 50)
    for word, count in sorted(counts.collect(), key=lambda x: -x[1]):
        print(f"{word}\t{count}")
    print("=" * 50)

    # Save to HDFS (remove first if it exists)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path(OUTPUT_PATH)
    if fs.exists(path):
        fs.delete(path, True)
    counts.saveAsTextFile(OUTPUT_PATH)

    spark.stop()

if __name__ == "__main__":
    main()
