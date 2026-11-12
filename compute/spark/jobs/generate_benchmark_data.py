"""
Milestone 6 - Giai đoạn B: Tạo dữ liệu mở rộng có kiểm soát (Synthetic Scaling Benchmark Dataset)
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Mục tiêu:
- Tạo các bộ dữ liệu mở rộng theo hệ số nhân (Scale Factors):
  + 1x   :    10,886 dòng (giữ nguyên bản gốc từ /bike-sharing/features/train)
  + 10x  :   108,860 dòng
  + 50x  :   544,300 dòng
  + 100x : 1,088,600 dòng
- Giữ nguyên cấu trúc schema 20 cột của tập train features.
- Giữ nguyên phân phối thống kê của các đặc trưng và biến mục tiêu 'count'.
- Sử dụng seed cố định (SEED = 42) để tái lập kết quả 100%.
- Dịch chuyển cột 'datetime' theo chu kỳ 730 ngày (2 năm) nhân với chỉ số bản sao (replica_id)
  kết hợp nhiễu chuẩn cực nhỏ có kiểm soát (N(0, 0.05^2), seed=42) trên biến liên tục temp/atemp
  đối với các bản sao mở rộng (replica_id > 0) để tránh bản ghi trùng lặp hoàn toàn.
- Kiểm tra dung lượng đĩa và HDFS trước mỗi bước mở rộng quy mô.
- Lưu trữ tại: /bike-sharing/benchmark/data/scale_{1x,10x,50x,100x}
"""

import csv
import json
import os
import shutil
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["HADOOP_USER_NAME"] = "root"
os.environ["USER"] = "root"

SEED = 42
SCALE_FACTORS = [1, 10, 50, 100]
BASE_ROW_COUNT = 10886
MIN_FREE_DISK_GB = 5.0


def get_hdfs_dir_size_bytes(spark, hdfs_uri):
    """Đo dung lượng lưu trữ thực tế (logical bytes) của thư mục trên HDFS."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_uri)
    if not fs.exists(path):
        return 0
    content_summary = fs.getContentSummary(path)
    return int(content_summary.getLength())


def get_hdfs_free_gb(spark):
    """Kiểm tra dung lượng trống trên HDFS DataNode (GB)."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    status = fs.getStatus()
    return round(float(status.getRemaining()) / (1024**3), 2)


def get_container_free_gb():
    """Kiểm tra dung lượng trống của ổ đĩa chứa container Docker (GB)."""
    usage = shutil.disk_usage("/")
    return round(float(usage.free) / (1024**3), 2)


def main():
    print("=" * 90)
    print("MILESTONE 6 - GIAI ĐOẠN B: TẠO DỮ LIỆU MỞ RỘNG CÓ KIỂM SOÁT (1x, 10x, 50x, 100x)")
    print("=" * 90)

    spark = (
        SparkSession.builder.appName("Milestone6-StageB-GenerateBenchmarkData")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.executorEnv.HADOOP_USER_NAME", "root")
        .config("spark.executorEnv.USER", "root")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    local_out_dir = "/opt/spark-jobs/_staging_outputs/benchmark"
    os.makedirs(local_out_dir, exist_ok=True)

    source_path = "hdfs://hadoop-namenode:9000/bike-sharing/features/train"
    base_df = spark.read.parquet(source_path).cache()
    base_count = base_df.count()
    base_cols = base_df.columns

    print(f"[SOURCE] Đọc dữ liệu gốc từ {source_path}: {base_count} dòng, {len(base_cols)} cột.")
    assert base_count == BASE_ROW_COUNT, f"Số dòng gốc không khớp {BASE_ROW_COUNT}: {base_count}"

    generation_records = []

    for scale in SCALE_FACTORS:
        free_container_gb = get_container_free_gb()
        free_hdfs_gb = get_hdfs_free_gb(spark)
        print("\n" + "-" * 90)
        print(
            f"[PRE-CHECK SCALE {scale}x] Dung lượng trống Container/Host: {free_container_gb} GB | "
            f"HDFS Remaining: {free_hdfs_gb} GB"
        )
        if free_container_gb < MIN_FREE_DISK_GB:
            raise RuntimeError(
                f"CẢNH BÁO AN TOÀN: Dung lượng ổ đĩa còn dưới {MIN_FREE_DISK_GB} GB ({free_container_gb} GB). Dừng tạo dữ liệu!"
            )

        target_rows = BASE_ROW_COUNT * scale
        out_hdfs_path = f"hdfs://hadoop-namenode:9000/bike-sharing/benchmark/data/scale_{scale}x"

        print(f"[GENERATING] Đang tạo bộ dữ liệu quy mô {scale}x ({target_rows:,} dòng) -> {out_hdfs_path} ...")
        t0 = time.perf_counter()

        if scale == 1:
            scaled_df = base_df.select(*base_cols)
        else:
            # Tạo bảng chỉ số bản sao replica_id từ 0 đến scale - 1
            replica_df = spark.range(0, scale, 1, numPartitions=min(scale, 4)).withColumnRenamed(
                "id", "replica_id"
            )
            joined_df = base_df.crossJoin(replica_df)

            # Dịch chuyển datetime thêm replica_id * 730 ngày (2 năm = 63,072,000 giây)
            # Và thêm nhiễu Gaussian cực nhỏ có kiểm soát (seed=42) cho replica_id > 0
            # giữ nguyên phân phối gốc, tránh bản ghi trùng hoàn toàn
            scaled_df = (
                joined_df.withColumn(
                    "datetime",
                    F.from_unixtime(
                        F.unix_timestamp(F.col("datetime"))
                        + (F.col("replica_id") * F.lit(730 * 86400))
                    ).cast("timestamp"),
                )
                .withColumn(
                    "temp",
                    F.when(
                        F.col("replica_id") == 0,
                        F.col("temp"),
                    ).otherwise(
                        F.round(
                            F.greatest(
                                F.lit(0.5),
                                F.col("temp") + (F.randn(SEED) * F.lit(0.05)),
                            ),
                            4,
                        )
                    ),
                )
                .withColumn(
                    "atemp",
                    F.when(
                        F.col("replica_id") == 0,
                        F.col("atemp"),
                    ).otherwise(
                        F.round(
                            F.greatest(
                                F.lit(0.5),
                                F.col("atemp") + (F.randn(SEED + 1) * F.lit(0.05)),
                            ),
                            4,
                        )
                    ),
                )
                .select(*base_cols)
            )

        # Ghi dữ liệu chuẩn của từng scale dưới dạng Parquet nén Snappy lên HDFS
        scaled_df.write.mode("overwrite").option("compression", "snappy").parquet(out_hdfs_path)
        gen_time_sec = round(time.perf_counter() - t0, 4)

        # Đọc lại để xác minh số dòng và kiểm tra thống kê phân phối
        verify_df = spark.read.parquet(out_hdfs_path)
        actual_rows = verify_df.count()
        actual_cols = len(verify_df.columns)
        num_partitions = verify_df.rdd.getNumPartitions()
        size_bytes = get_hdfs_dir_size_bytes(spark, out_hdfs_path)
        size_mb = round(size_bytes / (1024**2), 4)

        stats_row = verify_df.select(
            F.round(F.mean("count"), 4).alias("mean_count"),
            F.round(F.stddev("count"), 4).alias("std_count"),
            F.round(F.mean("temp"), 4).alias("mean_temp"),
        ).collect()[0]

        assert actual_rows == target_rows, (
            f"Lỗi số dòng tại scale {scale}x: kỳ vọng {target_rows}, thực tế {actual_rows}"
        )
        assert actual_cols == 20, f"Lỗi số cột tại scale {scale}x: {actual_cols}"

        print(
            f"  [OK] Scale {scale}x hoàn tất trong {gen_time_sec} s:\n"
            f"       - Số dòng      : {actual_rows:,} dòng | Số cột: {actual_cols}\n"
            f"       - Dung lượng   : {size_bytes:,} bytes ({size_mb} MB)\n"
            f"       - Số partition : {num_partitions}\n"
            f"       - Thống kê     : mean(count)={stats_row['mean_count']}, std(count)={stats_row['std_count']}, mean(temp)={stats_row['mean_temp']}"
        )

        generation_records.append(
            {
                "scale_label": f"{scale}x",
                "scale_factor": scale,
                "num_rows": actual_rows,
                "num_columns": actual_cols,
                "hdfs_path": out_hdfs_path,
                "parquet_size_bytes": size_bytes,
                "parquet_size_mb": size_mb,
                "default_partitions": num_partitions,
                "generation_time_sec": gen_time_sec,
                "mean_count": float(stats_row["mean_count"]),
                "std_count": float(stats_row["std_count"]),
                "mean_temp": float(stats_row["mean_temp"]),
                "seed": SEED,
                "method": (
                    "Original 1x dataset"
                    if scale == 1
                    else "Controlled replication via crossJoin(range(scale)) with +730d*replica_id timestamp shift and N(0, 0.05^2) jitter (seed=42)"
                ),
            }
        )

    # Lưu metadata JSON & CSV
    meta_payload = {
        "stage": "Milestone 6 - Stage B: Synthetic Benchmark Data Generation",
        "source_dataset": source_path,
        "base_rows": BASE_ROW_COUNT,
        "num_columns": len(base_cols),
        "columns": base_cols,
        "seed": SEED,
        "methodology": (
            "Nhân bản có kiểm soát từ tập /bike-sharing/features/train (10,886 dòng, 20 cột). "
            "Bản sao gốc (replica_id=0) được giữ nguyên 100%. Các bản sao mở rộng (replica_id >= 1) "
            "được dịch chuyển thời gian (datetime) tiến thêm replica_id * 730 ngày để đảm bảo không trùng mốc thời gian, "
            "đồng thời cộng nhiễu chuẩn cực nhỏ N(0, 0.05^2) với seed=42 vào biến nhiệt độ liên tục (temp, atemp) "
            "trong khi giữ nguyên phân phối của biến mục tiêu count và các đặc trưng thời gian/thời tiết."
        ),
        "limitations": (
            "Dữ liệu mở rộng (10x, 50x, 100x) là dữ liệu tổng hợp có kiểm soát (controlled synthetic data) "
            "chỉ phục vụ mục đích đo kiểm hiệu năng lưu trữ (CSV vs Parquet) và xử lý phân tán của Apache Spark; "
            "tuyệt đối không dùng làm dữ liệu thực tế để báo cáo chất lượng dự đoán của mô hình nghiệp vụ."
        ),
        "scales": generation_records,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    local_json_path = f"{local_out_dir}/generation_metadata.json"
    with open(local_json_path, "w", encoding="utf-8") as jf:
        json.dump(meta_payload, jf, indent=2, ensure_ascii=False)
    print(f"\n[SAVED LOCAL JSON] {local_json_path}")

    local_csv_path = f"{local_out_dir}/scaling_generation_summary.csv"
    with open(local_csv_path, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=list(generation_records[0].keys()))
        writer.writeheader()
        for r in generation_records:
            writer.writerow(r)
    print(f"[SAVED LOCAL CSV]  {local_csv_path}")

    # Lưu metadata lên HDFS
    hdfs_meta_path = "hdfs://hadoop-namenode:9000/bike-sharing/benchmark/data/metadata"
    meta_rdd = spark.sparkContext.parallelize([json.dumps(meta_payload, ensure_ascii=False)], 1)
    spark.read.json(meta_rdd).coalesce(1).write.mode("overwrite").json(hdfs_meta_path)
    print(f"[SAVED HDFS META]  {hdfs_meta_path}")

    base_df.unpersist()
    spark.stop()
    print("=" * 90)
    print("HOÀN TẤT GIAI ĐOẠN B (GENERATE BENCHMARK DATA) THÀNH CÔNG!")
    print("=" * 90)


if __name__ == "__main__":
    main()

