"""
Milestone 3: Kiểm thử tích hợp Apache Spark (PySpark + Spark SQL) với Hadoop HDFS
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Chức năng kiểm thử (Giai đoạn E & F):
1. Kết nối tới Spark Standalone Master (spark://spark-master:7077) và HDFS (hdfs://hadoop-namenode:9000).
2. Đọc 3 tệp dữ liệu gốc từ HDFS (/bike-sharing/raw/train.csv, test.csv, sampleSubmission.csv).
3. Kiểm tra và in Schema, số dòng (Row Count) và 5 dòng mẫu đầu tiên của mỗi bảng.
4. Thực hiện phép tổng hợp (groupBy & count/avg) bằng PySpark DataFrame API.
5. Tạo Temporary View và thực thi truy vấn phân tích bằng Spark SQL.
6. Ghi thử nghiệm DataFrame (đã trích xuất đặc trưng cơ bản, loại bỏ casual & registered để chống Data Leakage)
   xuống HDFS dưới định dạng Parquet tại /bike-sharing/milestone3_test/train_sample_parquet.
7. Đọc lại dữ liệu Parquet từ HDFS để xác minh tính toàn vẹn (Schema, Row Count, Sample Rows, HDFS Block Info).
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Đặt HADOOP_USER_NAME=root để Hadoop Client trong container non-root (UID 1001 của Bitnami)
# xác thực hợp lệ với HDFS mà không bị lỗi UnixPrincipal NullPointerException.
os.environ["HADOOP_USER_NAME"] = "root"
os.environ["USER"] = "root"


def main():
    print("=" * 80)
    print("BẮT ĐẦU KIỂM THỬ MILESTONE 3: PYSPARK + SPARK SQL + HDFS PARQUET")
    print("=" * 80)

    # 1. Khởi tạo SparkSession kết nối cụm Spark Standalone và HDFS NameNode
    spark = (
        SparkSession.builder.appName("Milestone3-BikeSharing-HDFS-Test")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
        .config("spark.executorEnv.HADOOP_USER_NAME", "root")
        .config("spark.executorEnv.USER", "root")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Spark Version      : {spark.version}")
    print(f"[INFO] Spark Master       : {spark.sparkContext.master}")
    print(
        f"[INFO] Hadoop fs.defaultFS: "
        f"{spark.sparkContext._jsc.hadoopConfiguration().get('fs.defaultFS')}"
    )

    hdfs_raw_base = "hdfs://hadoop-namenode:9000/bike-sharing/raw"
    files_to_check = {
        "train": f"{hdfs_raw_base}/train.csv",
        "test": f"{hdfs_raw_base}/test.csv",
        "sampleSubmission": f"{hdfs_raw_base}/sampleSubmission.csv",
    }

    dfs = {}

    # 2. Đọc 3 tệp CSV từ HDFS và kiểm tra Schema, Row Count, 5 dòng đầu
    for name, path in files_to_check.items():
        print("\n" + "-" * 80)
        print(f"[GIAI ĐOẠN E] Đang đọc tệp '{name}' từ: {path}")
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )
        row_count = df.count()
        col_count = len(df.columns)
        dfs[name] = df

        print(f"-> Kết quả đọc '{name}': {row_count} dòng, {col_count} cột")
        print(f"-> Schema của '{name}':")
        df.printSchema()
        print(f"-> 5 dòng đầu tiên của '{name}':")
        df.show(5, truncate=False)

    # Kiểm tra đối chiếu số dòng kỳ vọng
    assert dfs["train"].count() == 10886, "Số dòng train.csv không khớp 10,886!"
    assert dfs["test"].count() == 6493, "Số dòng test.csv không khớp 6,493!"
    assert (
        dfs["sampleSubmission"].count() == 6493
    ), "Số dòng sampleSubmission.csv không khớp 6,493!"
    print("[OK] Xác nhận số dòng của cả 3 tệp CSV khớp 100% với bộ dữ liệu gốc!")

    # 3. Thực hiện xử lý cơ bản với PySpark DataFrame API (groupBy & count/avg)
    print("\n" + "-" * 80)
    print("[GIAI ĐOẠN E] Thử nghiệm PySpark DataFrame API: Thống kê theo Mùa (season)")
    train_df = dfs["train"]
    season_summary = (
        train_df.groupBy("season")
        .agg(
            F.count("*").alias("record_count"),
            F.round(F.avg("count"), 2).alias("avg_demand"),
            F.round(F.avg("temp"), 2).alias("avg_temp_celsius"),
        )
        .orderBy("season")
    )
    season_summary.show(truncate=False)

    # 4. Kiểm tra Spark SQL bằng Temporary View
    print("\n" + "-" * 80)
    print("[GIAI ĐOẠN E] Thử nghiệm Spark SQL: Thống kê theo Thời tiết & Ngày làm việc")
    train_df.createOrReplaceTempView("bike_train_view")
    sql_query = """
        SELECT
            weather,
            workingday,
            COUNT(*) AS total_hours,
            ROUND(AVG(count), 2) AS avg_bike_rentals,
            MAX(count) AS max_bike_rentals
        FROM bike_train_view
        GROUP BY weather, workingday
        ORDER BY weather ASC, workingday DESC
    """
    sql_result_df = spark.sql(sql_query)
    sql_result_df.show(truncate=False)

    # 5. Giai đoạn F: Biến đổi cơ bản (chống Data Leakage) và Ghi Parquet lên HDFS
    print("\n" + "-" * 80)
    print("[GIAI ĐOẠN F] Ghi và đọc lại dữ liệu Parquet trên HDFS")
    # Lưu ý quan trọng: Loại bỏ 'casual' và 'registered' để tránh Data Leakage khi dự đoán 'count'
    clean_sample_df = (
        train_df.drop("casual", "registered")
        .withColumn("hour", F.hour("datetime"))
        .withColumn("month", F.month("datetime"))
        .withColumn("year", F.year("datetime"))
    )

    parquet_test_path = (
        "hdfs://hadoop-namenode:9000/bike-sharing/milestone3_test/train_clean_parquet"
    )
    print(f"-> Đang ghi DataFrame ({clean_sample_df.count()} dòng) xuống: {parquet_test_path}")
    clean_sample_df.write.mode("overwrite").parquet(parquet_test_path)
    print("-> Ghi Parquet thành công!")

    # 6. Đọc lại Parquet từ HDFS và kiểm증
    parquet_back_df = spark.read.parquet(parquet_test_path)
    back_count = parquet_back_df.count()
    print(f"-> Đọc lại Parquet từ HDFS: {back_count} dòng, {len(parquet_back_df.columns)} cột")
    assert back_count == 10886, "Số dòng Parquet đọc lại không khớp 10,886!"
    assert "casual" not in parquet_back_df.columns, "Cột casual chưa được loại bỏ!"
    assert "registered" not in parquet_back_df.columns, "Cột registered chưa được loại bỏ!"

    print("-> Schema của Parquet đọc lại:")
    parquet_back_df.printSchema()
    print("-> 5 dòng mẫu từ Parquet đọc lại:")
    parquet_back_df.show(5, truncate=False)

    print("=" * 80)
    print("HOÀN TẤT KIỂM THỬ MILESTONE 3 THÀNH CÔNG 100%!")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()

