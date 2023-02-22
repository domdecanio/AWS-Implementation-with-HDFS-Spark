import findspark
from pyspark.sql import SparkSession
import sys

# The entry point into all functionality in Spark is the SparkSession class.
spark = (SparkSession
        .builder
        .appName("my awesome Spark SQL program")
        .master("spark://172.31.93.203:7077")
        .getOrCreate())

# You can read the data from a file into DataFrames
# df = spark.read.json("")
input_path = "hdfs://172.31.93.203:9000/" + sys.argv[1]
df2 = spark.read.option("header","true").option("delimiter", ",").csv(input_path)

# sorting as required
answer = df2.sort(["cca2", "timestamp"])

answer.write.format("csv").mode("overwrite").save("hdfs://172.31.93.203:9000/" + sys.argv[2])