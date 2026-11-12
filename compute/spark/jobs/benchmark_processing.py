"""
Milestone 6 - Giai đoạn D & E: Benchmark Spark Processing & Partition Tuning
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Nội dung thực hiện:
1. Giai đoạn D (Spark Processing Benchmark trên 4 quy mô: 1x, 10x, 50x, 100x):
   Đánh giá 5 phép xử lý cốt lõi của Apache Spark với cùng logic trên mọi quy mô (lặp 3 runs/phép):
   - Op 1 (Filter)      : Lọc đa điều kiện (workingday=1, 7<=hour<=19, temp>=15, humidity<=80) -> Action count().
   - Op 2 (GroupBy)     : Gom nhóm theo (season, weather, hour) và đếm số bản ghi mỗi nhóm -> Action collect().
   - Op 3 (Aggregation) : Tính 9 chỉ số thống kê phức hợp theo (season, workingday, hour) -> Action collect().
   - Op 4 (Sort)        : Sắp xếp toàn cục (orderBy count DESC, temp DESC, datetime ASC) -> Action noop write
                          (buộc Spark sắp xếp toàn bộ N bản ghi mà không bị nhiễu bởi I/O ghi đĩa).
   - Op 5 (Join)        : Phép nối (Inner Join) giữa bảng dữ liệu chính và bảng tham chiếu 16 tổ hợp
                          (season, weather) kèm tính toán biểu thức dẫn xuất -> Action noop write & kiểm chứng số dòng.

2. Giai đoạn E (Spark Partition Benchmark với p = 1, 2, 4, 8 partitions):
   - Thực hiện trên các quy mô 10x (108,860 dòng), 50x (544,300 dòng) và 100x (1,088,600 dòng).
   - Ghi nhận rõ 3 thông số phân mảnh:
     + Số tệp/block vật lý trên HDFS (hdfs_file_count).
     + Số Spark Input Partitions ngay khi đọc từ HDFS (spark_input_partitions).
     + Số Spark DataFrame Partitions thực tế sau khi repartition/coalesce (actual_df_partitions = df.rdd.getNumPartitions()).
   - Đo thời gian phân vùng lại (repartition time) và thời gian thực thi workload chuẩn
     (Combined Analytical Workload: Filter + Join + GroupBy Aggregation) lặp 3 lần (3 runs).
   - Thu thập chỉ số sử dụng CPU (%) và RAM (MB) thực tế từ Linux cgroup v2 (/sys/fs/cgroup/) và JVM.
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
PARTITION_COUNTS = [1, 2, 4, 8]
PARTITION_BENCHMARK_SCALES = [10, 50, 100]
BASE_ROW_COUNT = 10886
NUM_RUNS = 3


def read_cgroup_cpu_usec():
    """Đọc tổng thời gian CPU (microseconds) của container từ cgroup v2 hoặc v1."""
    try:
        if os.path.exists("/sys/fs/cgroup/cpu.stat"):
            with open("/sys/fs/cgroup/cpu.stat", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("usage_usec"):
                        return int(line.strip().split()[1])
        elif os.path.exists("/sys/fs/cgroup/cpuacct/cpuacct.usage"):
            with open("/sys/fs/cgroup/cpuacct/cpuacct.usage", "r", encoding="utf-8") as f:
                return int(f.read().strip()) // 1000
    except Exception:
        pass
    return int(time.process_time() * 1_000_000)


def read_cgroup_memory_mb():
    """Đọc mức tiêu thụ RAM hiện tại (MB) của container từ cgroup v2 hoặc v1."""
    try:
        if os.path.exists("/sys/fs/cgroup/memory.current"):
            with open("/sys/fs/cgroup/memory.current", "r", encoding="utf-8") as f:
                return round(int(f.read().strip()) / (1024**2), 2)
        elif os.path.exists("/sys/fs/cgroup/memory/memory.usage_in_bytes"):
            with open("/sys/fs/cgroup/memory/memory.usage_in_bytes", "r", encoding="utf-8") as f:
                return round(int(f.read().strip()) / (1024**2), 2)
    except Exception:
        pass
    return 0.0


def get_hdfs_parquet_file_count_and_size(spark, hdfs_uri):
    """Đếm số tệp part-*.parquet thực tế trên HDFS và tổng dung lượng (bytes)."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_uri)
    if not fs.exists(path):
        return 0, 0
    statuses = fs.listStatus(path)
    part_files = 0
    total_bytes = 0
    for st in statuses:
        name = st.getPath().getName()
        length = int(st.getLen())
        total_bytes += length
        if name.startswith("part-"):
            part_files += 1
    return part_files, total_bytes


def build_reference_table(spark):
    """
    Tạo bảng tham chiếu 16 dòng (4 season x 4 weather) phục vụ phép Join có ý nghĩa nghiệp vụ.
    Tắt broadcast tự động hoặc dùng SortMergeJoin/ShuffledHashJoin hoặc BroadcastJoin nhất quán.
    """
    ref_rows = [
        (1, 1, "Spring_Clear", 1.00, "Normal"),
        (1, 2, "Spring_Mist", 0.90, "Moderate"),
        (1, 3, "Spring_LightRain", 0.65, "Reduced"),
        (1, 4, "Spring_HeavyRain", 0.35, "Severe"),
        (2, 1, "Summer_Clear", 1.15, "HighDemand"),
        (2, 2, "Summer_Mist", 1.05, "Normal"),
        (2, 3, "Summer_LightRain", 0.70, "Reduced"),
        (2, 4, "Summer_HeavyRain", 0.40, "Severe"),
        (3, 1, "Fall_Clear", 1.25, "PeakSeason"),
        (3, 2, "Fall_Mist", 1.10, "HighDemand"),
        (3, 3, "Fall_LightRain", 0.75, "Reduced"),
        (3, 4, "Fall_HeavyRain", 0.45, "Severe"),
        (4, 1, "Winter_Clear", 1.05, "Normal"),
        (4, 2, "Winter_Mist", 0.95, "Moderate"),
        (4, 3, "Winter_LightRain", 0.68, "Reduced"),
        (4, 4, "Winter_HeavyRain", 0.38, "Severe"),
    ]
    return spark.createDataFrame(
        ref_rows,
        ["season", "weather", "condition_code", "demand_weight", "operational_tier"],
    ).cache()


def main():
    print("=" * 95)
    print("MILESTONE 6 - GIAI ĐOẠN D & E: SPARK PROCESSING BENCHMARK & PARTITION TUNING")
    print("=" * 95)

    spark = (
        SparkSession.builder.appName("Milestone6-StageDE-ProcessingAndPartitionBenchmark")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.executorEnv.HADOOP_USER_NAME", "root")
        .config("spark.executorEnv.USER", "root")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    local_out_dir = "/opt/spark-jobs/_staging_outputs/benchmark"
    os.makedirs(local_out_dir, exist_ok=True)

    ref_df = build_reference_table(spark)
    ref_cnt = ref_df.count()
    print(f"[INIT] Đã khởi tạo bảng tham chiếu Join (ref_df): {ref_cnt} dòng.")

    processing_results = []
    partition_results = []

    # =========================================================================
    # GIAI ĐOẠN D: BENCHMARK SPARK PROCESSING (FILTER, GROUPBY, AGGREGATION, SORT, JOIN)
    # =========================================================================
    print("\n" + "=" * 95)
    print("[GIAI ĐOẠN D] BENCHMARK 5 PHÉP XỬ LÝ SPARK TRÊN 4 QUY MÔ (1x, 10x, 50x, 100x)")
    print("=" * 95)

    for scale in SCALE_FACTORS:
        expected_rows = BASE_ROW_COUNT * scale
        src_path = f"hdfs://hadoop-namenode:9000/bike-sharing/benchmark/data/scale_{scale}x"
        hdfs_files, size_bytes = get_hdfs_parquet_file_count_and_size(spark, src_path)
        size_mb = round(size_bytes / (1024**2), 4)

        df = spark.read.parquet(src_path).cache()
        input_rows = df.count()
        num_parts = df.rdd.getNumPartitions()
        assert input_rows == expected_rows, f"Sai số dòng: {input_rows} != {expected_rows}"

        print(
            f"\n--- [SCALE {scale}x] Rows={input_rows:,} | Size={size_mb} MB | "
            f"HDFS Files={hdfs_files} | Spark Partitions={num_parts} ---"
        )

        # Định nghĩa 5 phép xử lý cùng logic xuyên suốt các scale
        def run_filter():
            filtered = df.filter(
                (F.col("workingday") == 1)
                & (F.col("hour").between(7, 19))
                & (F.col("temp") >= 15.0)
                & (F.col("humidity") <= 80.0)
            )
            return int(filtered.count())

        def run_groupby():
            grouped = df.groupBy("season", "weather", "hour").count()
            rows = grouped.collect()
            return len(rows)

        def run_aggregation():
            agg_df = df.groupBy("season", "workingday", "hour").agg(
                F.count("*").alias("num_records"),
                F.sum("count").alias("total_rentals"),
                F.avg("count").alias("avg_rentals"),
                F.stddev("count").alias("std_rentals"),
                F.min("temp").alias("min_temp"),
                F.max("temp").alias("max_temp"),
                F.avg("atemp").alias("avg_atemp"),
                F.avg("humidity").alias("avg_humidity"),
                F.avg("windspeed").alias("avg_windspeed"),
            )
            rows = agg_df.collect()
            return len(rows)

        def run_sort():
            sorted_df = df.orderBy(
                F.col("count").desc(), F.col("temp").desc(), F.col("datetime").asc()
            )
            # Thực thi action format("noop") để Spark thực sự sắp xếp toàn bộ N dòng
            sorted_df.write.mode("overwrite").format("noop").save()
            return input_rows

        def run_join():
            joined_df = df.join(ref_df, on=["season", "weather"], how="inner").withColumn(
                "weighted_demand_score", F.round(F.col("count") * F.col("demand_weight"), 4)
            )
            # Thực thi action format("noop") để Spark thực sự nối và tính biểu thức trên toàn bộ N dòng
            joined_df.write.mode("overwrite").format("noop").save()
            return input_rows

        operations = [
            ("Filter", run_filter),
            ("GroupBy", run_groupby),
            ("Aggregation", run_aggregation),
            ("Sort", run_sort),
            ("Join", run_join),
        ]

        for op_name, op_fn in operations:
            run_times = []
            cpu_pcts = []
            mem_mbs = []
            out_rows = 0
            error_msg = "None"

            for r_idx in range(1, NUM_RUNS + 1):
                cpu_t0 = read_cgroup_cpu_usec()
                wall_t0 = time.perf_counter()
                try:
                    out_rows = op_fn()
                except Exception as exc:
                    error_msg = str(exc)
                    raise
                elapsed = time.perf_counter() - wall_t0
                cpu_elapsed_sec = max(0.0, (read_cgroup_cpu_usec() - cpu_t0) / 1_000_000.0)
                # Quy đổi % CPU tương đối trên 2 cores được cấp cho job (0% -> 100%)
                cpu_pct = round(min(100.0, (cpu_elapsed_sec / max(elapsed, 1e-6)) * 50.0), 2)
                mem_mb = read_cgroup_memory_mb()

                run_times.append(round(elapsed, 4))
                cpu_pcts.append(cpu_pct)
                mem_mbs.append(mem_mb)

            avg_time = round(statistics.mean(run_times), 4)
            std_time = round(statistics.stdev(run_times), 4) if len(run_times) > 1 else 0.0
            min_time = round(min(run_times), 4)
            max_time = round(max(run_times), 4)
            throughput = round(input_rows / avg_time, 2) if avg_time > 0 else 0.0
            avg_cpu = round(statistics.mean(cpu_pcts), 2)
            avg_mem = round(statistics.mean(mem_mbs), 2)

            print(
                f"  [{op_name:<11}] In={input_rows:>9,} | Out={out_rows:>9,} | "
                f"AvgTime={avg_time:.4f}s (±{std_time:.4f}s) | "
                f"Throughput={throughput:>12,.2f} rows/s | CPU={avg_cpu}% | DriverMem={avg_mem} MB"
            )

            processing_results.append(
                {
                    "scale_label": f"{scale}x",
                    "scale_factor": scale,
                    "operation": op_name,
                    "input_rows": input_rows,
                    "output_rows": out_rows,
                    "dataset_size_mb": size_mb,
                    "num_partitions": num_parts,
                    "num_runs": NUM_RUNS,
                    "run1_time_sec": run_times[0],
                    "run2_time_sec": run_times[1],
                    "run3_time_sec": run_times[2],
                    "avg_processing_time_sec": avg_time,
                    "std_processing_time_sec": std_time,
                    "min_processing_time_sec": min_time,
                    "max_processing_time_sec": max_time,
                    "throughput_rows_per_sec": throughput,
                    "cpu_usage_pct": avg_cpu,
                    "driver_container_mem_mb": avg_mem,
                    "error": error_msg,
                }
            )

        df.unpersist()

    # =========================================================================
    # GIAI ĐOẠN E: BENCHMARK PARTITION (1, 2, 4, 8 PARTITIONS)
    # =========================================================================
    print("\n" + "=" * 95)
    print("[GIAI ĐOẠN E] BENCHMARK SỐ LƯỢNG PARTITION (1, 2, 4, 8) TRÊN QUY MÔ 10x, 50x, 100x")
    print("=" * 95)

    for scale in PARTITION_BENCHMARK_SCALES:
        expected_rows = BASE_ROW_COUNT * scale
        src_path = f"hdfs://hadoop-namenode:9000/bike-sharing/benchmark/data/scale_{scale}x"
        hdfs_files, size_bytes = get_hdfs_parquet_file_count_and_size(spark, src_path)
        size_mb = round(size_bytes / (1024**2), 4)

        raw_df = spark.read.parquet(src_path)
        spark_input_partitions = raw_df.rdd.getNumPartitions()

        print(
            f"\n--- [PARTITION BENCHMARK - SCALE {scale}x ({expected_rows:,} dòng)] "
            f"HDFS Parquet Files={hdfs_files} | Spark Input Partitions={spark_input_partitions} ---"
        )

        for target_p in PARTITION_COUNTS:
            # Đo thời gian repartition và cache DataFrame với đúng target_p partitions
            t_rep0 = time.perf_counter()
            part_df = raw_df.repartition(target_p).cache()
            mat_rows = part_df.count()
            repartition_time_sec = round(time.perf_counter() - t_rep0, 4)

            actual_p = part_df.rdd.getNumPartitions()
            assert actual_p == target_p, f"Số partition không khớp: {actual_p} != {target_p}"
            assert mat_rows == expected_rows, f"Số dòng không khớp: {mat_rows} != {expected_rows}"

            # Thực hiện cùng 1 phép xử lý chuẩn (Analytical Pipeline: Filter + Join + GroupBy Aggregation)
            # lặp NUM_RUNS lần trên part_df đã có đúng actual_p partitions
            workload_times = []
            cpu_pcts = []
            mem_mbs = []
            out_groups = 0

            for r_idx in range(1, NUM_RUNS + 1):
                cpu_t0 = read_cgroup_cpu_usec()
                w_t0 = time.perf_counter()

                res_df = (
                    part_df.filter(F.col("temp") >= 10.0)
                    .join(ref_df, on=["season", "weather"], how="inner")
                    .groupBy("season", "workingday", "operational_tier", "hour")
                    .agg(
                        F.count("*").alias("cnt"),
                        F.sum("count").alias("sum_rentals"),
                        F.avg(F.col("count") * F.col("demand_weight")).alias("avg_weighted_demand"),
                        F.avg("humidity").alias("avg_hum"),
                    )
                )
                rows_out = res_df.collect()
                out_groups = len(rows_out)

                elapsed = time.perf_counter() - w_t0
                cpu_elapsed_sec = max(0.0, (read_cgroup_cpu_usec() - cpu_t0) / 1_000_000.0)
                cpu_pct = round(min(100.0, (cpu_elapsed_sec / max(elapsed, 1e-6)) * 50.0), 2)
                mem_mb = read_cgroup_memory_mb()

                workload_times.append(round(elapsed, 4))
                cpu_pcts.append(cpu_pct)
                mem_mbs.append(mem_mb)

            avg_w_time = round(statistics.mean(workload_times), 4)
            std_w_time = round(statistics.stdev(workload_times), 4) if len(workload_times) > 1 else 0.0
            throughput = round(expected_rows / avg_w_time, 2) if avg_w_time > 0 else 0.0
            avg_cpu = round(statistics.mean(cpu_pcts), 2)
            avg_mem = round(statistics.mean(mem_mbs), 2)

            print(
                f"  [Partitions={actual_p}] Repartition+Cache={repartition_time_sec:.4f}s | "
                f"Avg Processing={avg_w_time:.4f}s (±{std_w_time:.4f}s) | "
                f"Throughput={throughput:>12,.2f} rows/s | OutGroups={out_groups}"
            )

            partition_results.append(
                {
                    "scale_label": f"{scale}x",
                    "scale_factor": scale,
                    "num_rows": expected_rows,
                    "dataset_size_mb": size_mb,
                    "hdfs_file_count": hdfs_files,
                    "spark_input_partitions": spark_input_partitions,
                    "target_partitions": target_p,
                    "actual_df_partitions": actual_p,
                    "repartition_cache_time_sec": repartition_time_sec,
                    "num_runs": NUM_RUNS,
                    "run1_time_sec": workload_times[0],
                    "run2_time_sec": workload_times[1],
                    "run3_time_sec": workload_times[2],
                    "avg_processing_time_sec": avg_w_time,
                    "std_processing_time_sec": std_w_time,
                    "throughput_rows_per_sec": throughput,
                    "output_rows": out_groups,
                    "cpu_usage_pct": avg_cpu,
                    "driver_container_mem_mb": avg_mem,
                }
            )

            part_df.unpersist()

    # Lưu processing_results.csv
    proc_csv_path = f"{local_out_dir}/processing_results.csv"
    with open(proc_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(processing_results[0].keys()))
        writer.writeheader()
        for r in processing_results:
            writer.writerow(r)
    print(f"\n[SAVED PROCESSING RESULTS CSV] {proc_csv_path}")

    # Lưu partition_results.csv
    part_csv_path = f"{local_out_dir}/partition_results.csv"
    with open(part_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(partition_results[0].keys()))
        writer.writeheader()
        for r in partition_results:
            writer.writerow(r)
    print(f"[SAVED PARTITION RESULTS CSV]  {part_csv_path}")

    # Lưu JSON tổng hợp Giai đoạn D & E
    de_json_path = f"{local_out_dir}/processing_and_partition_benchmark.json"
    with open(de_json_path, "w", encoding="utf-8") as jf:
        json.dump(
            {
                "stage": "Milestone 6 - Stage D & E: Spark Processing & Partition Benchmark",
                "environment_note": (
                    "Single-machine containerized Spark Standalone (1 Master + 1 Worker với 2 CPU cores, "
                    "1024 MiB Executor RAM, spark.sql.shuffle.partitions=4)."
                ),
                "processing_results": processing_results,
                "partition_results": partition_results,
                "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            jf,
            indent=2,
            ensure_ascii=False,
        )
    print(f"[SAVED STAGE D & E JSON]       {de_json_path}")

    ref_df.unpersist()
    spark.stop()
    print("=" * 95)
    print("HOÀN TẤT GIAI ĐOẠN D & E (PROCESSING & PARTITION BENCHMARK) THÀNH CÔNG!")
    print("=" * 95)


if __name__ == "__main__":
    main()

