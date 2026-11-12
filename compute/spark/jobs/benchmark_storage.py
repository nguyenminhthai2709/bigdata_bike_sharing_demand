"""
Milestone 6 - Giai đoạn C: Benchmark Lưu trữ CSV và Parquet (Snappy)
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Nội dung thực hiện:
- Với mỗi quy mô 1x (10,886), 10x (108,860), 50x (544,300), 100x (1,088,600):
  1. Đọc bộ dữ liệu chuẩn của scale vào bộ nhớ đệm (cache) trên Spark để làm nguồn ghi công bằng
     cho cả CSV và Parquet (tránh tính lẫn thời gian đọc nguồn vào thời gian ghi).
  2. Thực hiện 3 lần đo (3 runs) cho mỗi định dạng (CSV không nén với header=true, và Parquet nén Snappy):
     - Đo thời gian ghi (Write Time).
     - Đo dung lượng lưu trữ thực tế trên HDFS (Storage Size in Bytes & MB).
     - Xóa Spark SQL/Catalog cache (spark.catalog.clearCache()) trước mỗi lần đọc.
     - Đo thời gian đọc kèm Action thực tế:
       + Read + Count Action: spark.read.schema(schema).{csv|parquet}(path).count()
       + Read + Aggregation Action: tính count(*), sum(count), avg(temp), avg(humidity), max(windspeed)
     - Xác minh số dòng đọc ra khớp 100% với số dòng kỳ vọng và schema 20 cột đồng nhất.
  3. Ghi chú minh bạch về trạng thái bộ nhớ đệm (Cache State):
     - Đã gọi spark.catalog.clearCache() và không cache DataFrame đọc ra (Spark-level uncached).
     - Tuy nhiên, do DataNode chạy trên cùng Linux kernel của Docker WSL2, hệ điều hành vẫn giữ
       Linux OS Page Cache sau khi vừa ghi tệp xuống đĩa. Do đó kết quả đọc được ghi nhận chính xác là
       "Spark-Uncached / Warm OS Page Cache" (không gọi nhầm là hardware cold-cache).
"""

import csv
import json
import os
import statistics
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["HADOOP_USER_NAME"] = "root"
os.environ["USER"] = "root"

SCALE_FACTORS = [1, 10, 50, 100]
BASE_ROW_COUNT = 10886
NUM_RUNS = 3


def get_hdfs_dir_size_bytes(spark, hdfs_uri):
    """Lấy tổng dung lượng lưu trữ (bytes) của thư mục trên HDFS."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_uri)
    if not fs.exists(path):
        return 0
    return int(fs.getContentSummary(path).getLength())


def main():
    print("=" * 95)
    print("MILESTONE 6 - GIAI ĐOẠN C: BENCHMARK LƯU TRỮ CSV VÀ PARQUET (SNAPPY) - 3 RUNS/CONFIG")
    print("=" * 95)

    spark = (
        SparkSession.builder.appName("Milestone6-StageC-BenchmarkStorage")
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

    storage_summary_rows = []
    storage_detailed_runs = []

    for scale in SCALE_FACTORS:
        expected_rows = BASE_ROW_COUNT * scale
        src_path = f"hdfs://hadoop-namenode:9000/bike-sharing/benchmark/data/scale_{scale}x"

        print("\n" + "=" * 95)
        print(f"[SCALE {scale}x] Nguồn: {src_path} ({expected_rows:,} dòng)")
        print("=" * 95)

        # Nạp dữ liệu nguồn vào bộ nhớ RAM của Executor với đúng 2 partitions đồng nhất
        # để phép đo thời gian ghi (Write Time) giữa CSV và Parquet hoàn toàn công bằng
        source_df = spark.read.parquet(src_path).repartition(2).cache()
        src_cnt = source_df.count()
        canonical_schema = source_df.schema
        assert src_cnt == expected_rows, f"Lỗi số dòng nguồn: {src_cnt} != {expected_rows}"

        for fmt in ["csv", "parquet"]:
            target_path = (
                f"hdfs://hadoop-namenode:9000/bike-sharing/benchmark/storage/{fmt}/scale_{scale}x"
            )
            write_times = []
            read_count_times = []
            read_agg_times = []
            size_bytes_recorded = 0

            print(f"\n  -> Định dạng: {fmt.upper()} | Đường dẫn: {target_path} | Số lần lặp: {NUM_RUNS}")

            for run_idx in range(1, NUM_RUNS + 1):
                # 1. Đo thời gian Ghi (Write Time)
                t_w0 = time.perf_counter()
                if fmt == "csv":
                    (
                        source_df.write.mode("overwrite")
                        .option("header", "true")
                        .csv(target_path)
                    )
                else:
                    (
                        source_df.write.mode("overwrite")
                        .option("compression", "snappy")
                        .parquet(target_path)
                    )
                w_time = round(time.perf_counter() - t_w0, 4)
                write_times.append(w_time)

                # 2. Đo dung lượng lưu trữ trên HDFS
                size_bytes = get_hdfs_dir_size_bytes(spark, target_path)
                size_bytes_recorded = size_bytes

                # 3. Xóa Spark Catalog Cache trước khi đo Đọc
                spark.catalog.clearCache()

                # 4. Đo thời gian Đọc + Count Action (với cùng schema 20 cột)
                t_rc0 = time.perf_counter()
                if fmt == "csv":
                    read_df_1 = (
                        spark.read.schema(canonical_schema)
                        .option("header", "true")
                        .csv(target_path)
                    )
                else:
                    read_df_1 = spark.read.parquet(target_path)
                verified_rows = read_df_1.count()
                rc_time = round(time.perf_counter() - t_rc0, 4)
                read_count_times.append(rc_time)

                assert verified_rows == expected_rows, (
                    f"Sai số dòng khi đọc {fmt} ở scale {scale}x: {verified_rows} != {expected_rows}"
                )
                assert len(read_df_1.columns) == 20, f"Sai số cột khi đọc {fmt}: {len(read_df_1.columns)}"

                # 5. Xóa Spark Catalog Cache lần nữa và đo Đọc + Aggregation Action
                spark.catalog.clearCache()
                t_ra0 = time.perf_counter()
                if fmt == "csv":
                    read_df_2 = (
                        spark.read.schema(canonical_schema)
                        .option("header", "true")
                        .csv(target_path)
                    )
                else:
                    read_df_2 = spark.read.parquet(target_path)

                agg_res = read_df_2.select(
                    F.count("*").alias("cnt"),
                    F.sum("count").alias("sum_count"),
                    F.avg("temp").alias("avg_temp"),
                    F.avg("humidity").alias("avg_hum"),
                    F.max("windspeed").alias("max_wind"),
                ).collect()[0]
                ra_time = round(time.perf_counter() - t_ra0, 4)
                read_agg_times.append(ra_time)

                assert int(agg_res["cnt"]) == expected_rows, "Count trong aggregation không khớp!"

                print(
                    f"     [Run {run_idx}/{NUM_RUNS}] Write={w_time:.4f}s | "
                    f"Read(Count)={rc_time:.4f}s | Read(Agg)={ra_time:.4f}s | "
                    f"Size={size_bytes / (1024**2):.4f} MB"
                )

                storage_detailed_runs.append(
                    {
                        "scale_label": f"{scale}x",
                        "scale_factor": scale,
                        "num_rows": expected_rows,
                        "format": fmt.upper(),
                        "run_index": run_idx,
                        "cache_regime": "Spark-Uncached / Warm OS Page Cache",
                        "storage_size_bytes": size_bytes,
                        "storage_size_mb": round(size_bytes / (1024**2), 4),
                        "write_time_sec": w_time,
                        "read_count_time_sec": rc_time,
                        "read_agg_time_sec": ra_time,
                    }
                )

            avg_w = round(statistics.mean(write_times), 4)
            std_w = round(statistics.stdev(write_times), 4) if len(write_times) > 1 else 0.0
            avg_rc = round(statistics.mean(read_count_times), 4)
            std_rc = round(statistics.stdev(read_count_times), 4) if len(read_count_times) > 1 else 0.0
            avg_ra = round(statistics.mean(read_agg_times), 4)
            std_ra = round(statistics.stdev(read_agg_times), 4) if len(read_agg_times) > 1 else 0.0
            size_mb = round(size_bytes_recorded / (1024**2), 4)

            write_throughput = round(expected_rows / avg_w, 2) if avg_w > 0 else 0.0
            read_count_throughput = round(expected_rows / avg_rc, 2) if avg_rc > 0 else 0.0
            read_agg_throughput = round(expected_rows / avg_ra, 2) if avg_ra > 0 else 0.0

            print(
                f"  => [SUMMARY {scale}x - {fmt.upper()}] "
                f"Size={size_mb} MB | Avg Write={avg_w}s (±{std_w}s) | "
                f"Avg Read(Count)={avg_rc}s (±{std_rc}s) | Avg Read(Agg)={avg_ra}s (±{std_ra}s)"
            )

            storage_summary_rows.append(
                {
                    "scale_label": f"{scale}x",
                    "scale_factor": scale,
                    "num_rows": expected_rows,
                    "num_columns": 20,
                    "format": fmt.upper(),
                    "compression": "none" if fmt == "csv" else "snappy",
                    "num_runs": NUM_RUNS,
                    "cache_regime": "Spark-Uncached / Warm OS Page Cache",
                    "storage_size_bytes": size_bytes_recorded,
                    "storage_size_mb": size_mb,
                    "run1_write_time_sec": write_times[0],
                    "avg_write_time_sec": avg_w,
                    "std_write_time_sec": std_w,
                    "run1_read_count_time_sec": read_count_times[0],
                    "avg_read_count_time_sec": avg_rc,
                    "std_read_count_time_sec": std_rc,
                    "run1_read_agg_time_sec": read_agg_times[0],
                    "avg_read_agg_time_sec": avg_ra,
                    "std_read_agg_time_sec": std_ra,
                    "write_throughput_rows_per_sec": write_throughput,
                    "read_count_throughput_rows_per_sec": read_count_throughput,
                    "read_agg_throughput_rows_per_sec": read_agg_throughput,
                    "hdfs_path": target_path,
                }
            )

        source_df.unpersist()

    # Lưu kết quả tóm tắt ra storage_comparison.csv
    summary_csv_path = f"{local_out_dir}/storage_comparison.csv"
    with open(summary_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(storage_summary_rows[0].keys()))
        writer.writeheader()
        for r in storage_summary_rows:
            writer.writerow(r)
    print(f"\n[SAVED STORAGE COMPARISON CSV] {summary_csv_path}")

    # Lưu kết quả chi tiết từng run ra storage_runs_detail.csv
    detail_csv_path = f"{local_out_dir}/storage_runs_detail.csv"
    with open(detail_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(storage_detailed_runs[0].keys()))
        writer.writeheader()
        for r in storage_detailed_runs:
            writer.writerow(r)
    print(f"[SAVED STORAGE RUNS DETAIL CSV] {detail_csv_path}")

    # Lưu JSON
    json_path = f"{local_out_dir}/storage_benchmark.json"
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(
            {
                "stage": "Milestone 6 - Stage C: CSV vs Parquet Storage Benchmark",
                "num_runs_per_config": NUM_RUNS,
                "cache_note": (
                    "Trước mỗi lần đọc đều thực hiện spark.catalog.clearCache() và không cache DataFrame đọc ra. "
                    "Tuy nhiên, do DataNode chạy trên cùng Linux kernel (WSL2), OS Page Cache vẫn giữ các block vừa ghi. "
                    "Do đó chế độ đo được phân loại chính xác là 'Spark-Uncached / Warm OS Page Cache'."
                ),
                "summary": storage_summary_rows,
                "detailed_runs": storage_detailed_runs,
                "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            jf,
            indent=2,
            ensure_ascii=False,
        )
    print(f"[SAVED STORAGE BENCHMARK JSON] {json_path}")

    spark.stop()
    print("=" * 95)
    print("HOÀN TẤT GIAI ĐOẠN C (BENCHMARK STORAGE CSV VS PARQUET) THÀNH CÔNG!")
    print("=" * 95)


if __name__ == "__main__":
    main()

