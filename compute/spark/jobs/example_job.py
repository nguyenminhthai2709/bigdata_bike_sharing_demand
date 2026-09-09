"""
Spark job mau: doc/ghi du lieu voi MinIO (S3-compatible) qua S3A connector.

Chay tren cum:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark-jobs/example_job.py
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("example-job")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

data = [("alice", 1), ("bob", 2), ("carol", 3)]
df = spark.createDataFrame(data, ["name", "value"])
df.show()

# Ghi ket qua vao bucket "datalake" da duoc minio-init tao san
df.write.mode("overwrite").parquet("s3a://datalake/example_job_output")

spark.stop()
