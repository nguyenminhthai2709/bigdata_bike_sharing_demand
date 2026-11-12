"""
Milestone 4 - Giai đoạn C, D & F: Data Preprocessing, Feature Engineering & Lưu Parquet lên HDFS
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Nội dung thực hiện:
- C1. Xử lý kiểu dữ liệu: Chuyển 'datetime' sang TimestampType, ép kiểu chính xác cho các cột số
      (IntegerType / DoubleType) và kiểm tra giá trị không hợp lệ.
- C2. Kiểm tra dữ liệu thiếu (Null / NaN): Thống kê từng cột trên cả train.csv và test.csv;
      chỉ can thiệp nếu thực sự có dữ liệu thiếu.
- C3. Kiểm tra trùng lặp (Duplicates): Kiểm tra trùng toàn dòng và trùng mốc thời gian 'datetime'.
- C4. Kiểm tra tính hợp lệ nghiệp vụ (Validity rules):
      + count == casual + registered (trên train)
      + count >= 0, casual >= 0, registered >= 0
      + hour nằm trong [0, 23]
      + season in {1,2,3,4}, weather in {1,2,3,4}, holiday in {0,1}, workingday in {0,1}
      + humidity in [0, 100], windspeed >= 0
- D. Feature Engineering (áp dụng đồng nhất cho cả Train và Test, không dùng thông tin tương lai):
      + Đặc trưng thời gian: year, month, day, hour, dayofweek, is_weekend
      + Đặc trưng chu kỳ (Cyclical features):
          hour_sin = sin(2 * pi * hour / 24)
          hour_cos = cos(2 * pi * hour / 24)
          month_sin = sin(2 * pi * month / 12)
          month_cos = cos(2 * pi * month / 12)
      + Tách rõ 2 tập dữ liệu đầu ra:
          1) Cleaned Analytical Dataset: giữ đầy đủ các cột (bao gồm casual, registered, count trên train)
             phục vụ truy vấn phân tích hành vi người dùng bằng Spark SQL.
          2) Model Feature Dataset: LOẠI BỎ HOÀN TOÀN 'casual' và 'registered' khỏi tập đặc trưng
             để chống Data Leakage; giữ 'count' làm nhãn mục tiêu (target) riêng trên tập Train;
             tập Test tuyệt đối không tự tạo nhãn 'count'.
- F. Lưu Parquet lên HDFS với nén Snappy (coalesce(1) cho dataset nhỏ để tránh tạo nhiều small files),
      sau đó đọc lại từ HDFS để xác minh Schema và Số dòng (Train: 10,886; Test: 6,493).
"""

import json
import math
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

os.environ["HADOOP_USER_NAME"] = "root"
os.environ["USER"] = "root"


def cast_and_extract_features(df, is_train=True):
    """
    Hàm chuẩn hóa kiểu dữ liệu (C1) và trích xuất đặc trưng (D) đồng nhất cho cả Train và Test.
    Đảm bảo không sử dụng thông tin tương lai (data leakage theo thời gian).
    """
    # C1. Ép kiểu dữ liệu tường minh
    casted = (
        df.withColumn("datetime", F.to_timestamp(F.col("datetime"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
        .withColumn("season", F.col("season").cast(IntegerType()))
        .withColumn("holiday", F.col("holiday").cast(IntegerType()))
        .withColumn("workingday", F.col("workingday").cast(IntegerType()))
        .withColumn("weather", F.col("weather").cast(IntegerType()))
        .withColumn("temp", F.col("temp").cast(DoubleType()))
        .withColumn("atemp", F.col("atemp").cast(DoubleType()))
        .withColumn("humidity", F.col("humidity").cast(IntegerType()))
        .withColumn("windspeed", F.col("windspeed").cast(DoubleType()))
    )

    if is_train:
        casted = (
            casted.withColumn("casual", F.col("casual").cast(IntegerType()))
            .withColumn("registered", F.col("registered").cast(IntegerType()))
            .withColumn("count", F.col("count").cast(IntegerType()))
        )

    # D. Trích xuất đặc trưng thời gian & chu kỳ lượng giác
    two_pi = 2.0 * math.pi
    featured = (
        casted.withColumn("year", F.year("datetime").cast(IntegerType()))
        .withColumn("month", F.month("datetime").cast(IntegerType()))
        .withColumn("day", F.dayofmonth("datetime").cast(IntegerType()))
        .withColumn("hour", F.hour("datetime").cast(IntegerType()))
        # Spark dayofweek: 1 = Chủ Nhật (Sunday), 2 = Thứ Hai ... 7 = Thứ Bảy (Saturday)
        .withColumn("dayofweek", F.dayofweek("datetime").cast(IntegerType()))
        .withColumn(
            "is_weekend",
            F.when(F.col("dayofweek").isin(1, 7), F.lit(1)).otherwise(F.lit(0)).cast(IntegerType()),
        )
        .withColumn("hour_sin", F.round(F.sin(F.col("hour") * (two_pi / 24.0)), 6).cast(DoubleType()))
        .withColumn("hour_cos", F.round(F.cos(F.col("hour") * (two_pi / 24.0)), 6).cast(DoubleType()))
        .withColumn("month_sin", F.round(F.sin(F.col("month") * (two_pi / 12.0)), 6).cast(DoubleType()))
        .withColumn("month_cos", F.round(F.cos(F.col("month") * (two_pi / 12.0)), 6).cast(DoubleType()))
    )

    return featured


def check_nulls(df, dataset_name):
    """C2. Thống kê số lượng giá trị Null trên từng cột."""
    exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    row_dict = df.select(exprs).collect()[0].asDict()
    total_nulls = sum(row_dict.values())
    print(f"[C2] Kiểm tra Null trên '{dataset_name}': Tổng số Null = {total_nulls}")
    return row_dict, total_nulls


def main():
    print("=" * 85)
    print("MILESTONE 4 - GIAI ĐOẠN C, D & F: PREPROCESSING, FEATURE ENGINEERING & PARQUET")
    print("=" * 85)

    spark = (
        SparkSession.builder.appName("Milestone4-StageCDF-Preprocess-BikeSharing")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
        .config("spark.executorEnv.HADOOP_USER_NAME", "root")
        .config("spark.executorEnv.USER", "root")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_train_path = "hdfs://hadoop-namenode:9000/bike-sharing/raw/train.csv"
    raw_test_path = "hdfs://hadoop-namenode:9000/bike-sharing/raw/test.csv"

    raw_train_df = spark.read.option("header", "true").csv(raw_train_path)
    raw_test_df = spark.read.option("header", "true").csv(raw_test_path)

    # -------------------------------------------------------------------------
    # C1 & D. CHUẨN HÓA KIỂU DỮ LIỆU VÀ TẠO ĐẶC TRƯNG
    # -------------------------------------------------------------------------
    processed_train_df = cast_and_extract_features(raw_train_df, is_train=True).cache()
    processed_test_df = cast_and_extract_features(raw_test_df, is_train=False).cache()

    train_count = processed_train_df.count()
    test_count = processed_test_df.count()
    print(f"\n[C1] Đã ép kiểu và tạo đặc trưng: Train = {train_count} dòng, Test = {test_count} dòng")

    # -------------------------------------------------------------------------
    # C2. KIỂM TRA DỮ LIỆU THIẾU (NULL / MISSING VALUES)
    # -------------------------------------------------------------------------
    train_nulls, train_total_nulls = check_nulls(processed_train_df, "Train")
    test_nulls, test_total_nulls = check_nulls(processed_test_df, "Test")
    assert train_total_nulls == 0, "Phát hiện giá trị Null sau khi ép kiểu trên tập Train!"
    assert test_total_nulls == 0, "Phát hiện giá trị Null sau khi ép kiểu trên tập Test!"

    # -------------------------------------------------------------------------
    # C3. KIỂM TRA TRÙNG LẶP (DUPLICATES)
    # -------------------------------------------------------------------------
    train_distinct_rows = processed_train_df.distinct().count()
    train_distinct_dt = processed_train_df.select("datetime").distinct().count()
    test_distinct_rows = processed_test_df.distinct().count()
    test_distinct_dt = processed_test_df.select("datetime").distinct().count()

    train_dup_rows = train_count - train_distinct_rows
    train_dup_dt = train_count - train_distinct_dt
    test_dup_rows = test_count - test_distinct_rows
    test_dup_dt = test_count - test_distinct_dt

    print(
        f"[C3] Trùng lặp Train: duplicate_rows={train_dup_rows}, duplicate_datetime={train_dup_dt}"
    )
    print(
        f"[C3] Trùng lặp Test : duplicate_rows={test_dup_rows}, duplicate_datetime={test_dup_dt}"
    )

    # -------------------------------------------------------------------------
    # C4. KIỂM TRA TÍNH HỢP LỆ NGHIỆP VỤ (DATA VALIDITY CHECKS)
    # -------------------------------------------------------------------------
    validity_checks = {
        "train_count_neq_casual_plus_registered": processed_train_df.filter(
            F.col("count") != (F.col("casual") + F.col("registered"))
        ).count(),
        "train_negative_count": processed_train_df.filter(F.col("count") < 0).count(),
        "train_negative_casual_or_registered": processed_train_df.filter(
            (F.col("casual") < 0) | (F.col("registered") < 0)
        ).count(),
        "train_invalid_hour_outside_0_23": processed_train_df.filter(
            (F.col("hour") < 0) | (F.col("hour") > 23)
        ).count(),
        "test_invalid_hour_outside_0_23": processed_test_df.filter(
            (F.col("hour") < 0) | (F.col("hour") > 23)
        ).count(),
        "train_invalid_season": processed_train_df.filter(
            ~F.col("season").isin(1, 2, 3, 4)
        ).count(),
        "train_invalid_weather": processed_train_df.filter(
            ~F.col("weather").isin(1, 2, 3, 4)
        ).count(),
        "train_invalid_humidity_outside_0_100": processed_train_df.filter(
            (F.col("humidity") < 0) | (F.col("humidity") > 100)
        ).count(),
        "train_negative_windspeed": processed_train_df.filter(
            F.col("windspeed") < 0
        ).count(),
    }

    print("\n[C4] Kết quả kiểm tra tính hợp lệ dữ liệu (Validity Checks):")
    for check_name, violation_count in validity_checks.items():
        print(f"  - {check_name}: {violation_count} vi phạm")
        assert violation_count == 0, f"Vi phạm quy tắc hợp lệ: {check_name} ({violation_count})"

    # -------------------------------------------------------------------------
    # D. TẠO 2 LOẠI DATASET: ANALYTICAL DATASET & MODEL FEATURE DATASET
    # -------------------------------------------------------------------------
    # 1. Cleaned Analytical Dataset (sắp xếp theo datetime, giữ casual & registered trên train cho phân tích)
    analytical_train_cols = [
        "datetime", "year", "month", "day", "hour", "dayofweek", "is_weekend",
        "season", "holiday", "workingday", "weather",
        "temp", "atemp", "humidity", "windspeed",
        "hour_sin", "hour_cos", "month_sin", "month_cos",
        "casual", "registered", "count"
    ]
    analytical_test_cols = [
        "datetime", "year", "month", "day", "hour", "dayofweek", "is_weekend",
        "season", "holiday", "workingday", "weather",
        "temp", "atemp", "humidity", "windspeed",
        "hour_sin", "hour_cos", "month_sin", "month_cos"
    ]

    analytical_train_df = processed_train_df.select(analytical_train_cols).orderBy("datetime")
    analytical_test_df = processed_test_df.select(analytical_test_cols).orderBy("datetime")

    # 2. Model Feature Dataset (LOẠI BỎ casual & registered để chống Data Leakage!)
    feature_cols = [
        "datetime",
        "season", "holiday", "workingday", "weather",
        "temp", "atemp", "humidity", "windspeed",
        "year", "month", "day", "hour", "dayofweek", "is_weekend",
        "hour_sin", "hour_cos", "month_sin", "month_cos"
    ]
    # Tập Train cho ML: gồm các cột đặc trưng + cột mục tiêu 'count' ở cuối
    features_train_df = processed_train_df.select(feature_cols + ["count"]).orderBy("datetime")
    # Tập Test cho ML: chỉ gồm đúng các cột đặc trưng (không có casual, registered, count)
    features_test_df = processed_test_df.select(feature_cols).orderBy("datetime")

    # -------------------------------------------------------------------------
    # F. LƯU PARQUET LÊN HDFS (SNAPPY COMPRESSION, COALESCE(1) TRÁNH SMALL FILES)
    # -------------------------------------------------------------------------
    hdfs_paths = {
        "processed_train": "hdfs://hadoop-namenode:9000/bike-sharing/processed/train",
        "processed_test": "hdfs://hadoop-namenode:9000/bike-sharing/processed/test",
        "features_train": "hdfs://hadoop-namenode:9000/bike-sharing/features/train",
        "features_test": "hdfs://hadoop-namenode:9000/bike-sharing/features/test",
    }

    datasets_to_save = {
        "processed_train": (analytical_train_df, 10886, 22),
        "processed_test": (analytical_test_df, 6493, 19),
        "features_train": (features_train_df, 10886, 20),
        "features_test": (features_test_df, 6493, 19),
    }

    verification_results = {}

    for key, (df_to_write, expected_rows, expected_cols) in datasets_to_save.items():
        target_path = hdfs_paths[key]
        print(f"\n[GIAI ĐOẠN F] Đang ghi Parquet (snappy, coalesce=1) -> {target_path}")
        (
            df_to_write.coalesce(1)
            .write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(target_path)
        )

        # Đọc lại từ HDFS để xác minh
        read_back_df = spark.read.parquet(target_path)
        actual_rows = read_back_df.count()
        actual_cols = len(read_back_df.columns)
        print(f"  -> Đọc lại từ HDFS '{key}': {actual_rows} dòng, {actual_cols} cột")
        assert actual_rows == expected_rows, f"Lỗi số dòng tại {key}: {actual_rows} != {expected_rows}"
        assert actual_cols == expected_cols, f"Lỗi số cột tại {key}: {actual_cols} != {expected_cols}"

        if key.startswith("features_"):
            assert "casual" not in read_back_df.columns, f"Data Leakage: 'casual' tồn tại trong {key}!"
            assert "registered" not in read_back_df.columns, f"Data Leakage: 'registered' tồn tại trong {key}!"

        verification_results[key] = {
            "hdfs_path": target_path,
            "rows": actual_rows,
            "cols": actual_cols,
            "columns": read_back_df.columns,
        }

    print("\n[GIAI ĐOẠN F] Schema của Model Feature Dataset (features/train):")
    spark.read.parquet(hdfs_paths["features_train"]).printSchema()

    print("\n[GIAI ĐOẠN F] 5 dòng đầu của Model Feature Dataset (features/train):")
    spark.read.parquet(hdfs_paths["features_train"]).show(5, truncate=False)

    # Lưu biên bản kiểm tra chất lượng dữ liệu & preprocessing
    report_dir = "/opt/spark-jobs/_staging_outputs/preprocessing"
    os.makedirs(report_dir, exist_ok=True)
    with open(f"{report_dir}/preprocessing_summary.json", "w", encoding="utf-8") as pf:
        json.dump(
            {
                "train_nulls": train_nulls,
                "test_nulls": test_nulls,
                "duplicates": {
                    "train_dup_rows": train_dup_rows,
                    "train_dup_datetime": train_dup_dt,
                    "test_dup_rows": test_dup_rows,
                    "test_dup_datetime": test_dup_dt,
                },
                "validity_checks": validity_checks,
                "parquet_verification": verification_results,
            },
            pf,
            indent=2,
            ensure_ascii=False,
        )

    print("\n" + "=" * 85)
    print("HOÀN TẤT GIAI ĐOẠN C, D & F THÀNH CÔNG 100%!")
    print("=" * 85)
    spark.stop()


if __name__ == "__main__":
    main()

