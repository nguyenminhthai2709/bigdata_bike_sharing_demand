"""
Spark job mau (cho nguoi moi): tao DataFrame roi ghi xuong HDFS duoi dang CSV.

Chay tren cum standalone:
  docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/hello_hdfs.py

Kiem tra ket qua:
  docker exec hadoop-namenode hdfs dfs -cat /user/demo/spark_out/part-*
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hello-hdfs").getOrCreate()

df = spark.createDataFrame(
    [("alice", 1), ("bob", 2), ("carol", 3)],
    ["name", "value"],
)
df.show()

df.write.mode("overwrite").csv("hdfs://hadoop-namenode:9000/user/demo/spark_out")

spark.stop()
