"""
Milestone 4 - Giai đoạn B: Exploratory Data Analysis (EDA) bằng PySpark
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Nội dung phân tích:
- B1. Tổng quan dữ liệu: Số dòng, số cột, schema, kiểu dữ liệu, null/missing values,
      duplicate rows, descriptive statistics và kiểm tra giá trị bất thường (outliers/anomalies).
- B2. Phân tích biến mục tiêu (count) và các biến thành phần (casual, registered):
      Mean, Median (50th percentile), Min, Max, Stddev, Quartiles (25%, 75%), Skewness, Kurtosis,
      và phân phối theo các khoảng (bins).
- B3. Phân tích nhu cầu thuê xe theo thời gian: Giờ trong ngày (hour), Ngày trong tuần (dayofweek),
      Tháng (month), Năm (year), Mùa (season).
- B4. Phân tích nhu cầu thuê xe theo điều kiện môi trường & loại ngày:
      season, weather, workingday, holiday, temp (nhóm nhiệt độ), humidity (nhóm độ ẩm), windspeed (nhóm tốc độ gió).
- B5. Lưu toàn bộ các bảng kết quả thống kê tổng hợp dưới dạng CSV và JSON.
"""

import csv
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Đảm bảo Hadoop Client trong container sử dụng user hợp lệ
os.environ["HADOOP_USER_NAME"] = "root"
os.environ["USER"] = "root"


def save_df_to_local_csv(df, filepath):
    """Lưu Spark DataFrame nhỏ (bảng tổng hợp) ra tệp CSV chuẩn trên thư mục mount."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    rows = df.collect()
    columns = df.columns
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row[col] for col in columns])
    print(f"  [SAVED CSV] {filepath} ({len(rows)} rows)")


def main():
    print("=" * 85)
    print("MILESTONE 4 - GIAI ĐOẠN B: EXPLORATORY DATA ANALYSIS (EDA) VỚI PYSPARK")
    print("=" * 85)

    spark = (
        SparkSession.builder.appName("Milestone4-StageB-EDA-BikeSharing")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
        .config("spark.executorEnv.HADOOP_USER_NAME", "root")
        .config("spark.executorEnv.USER", "root")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    output_dir = "/opt/spark-jobs/_staging_outputs/eda"
    os.makedirs(output_dir, exist_ok=True)

    # -------------------------------------------------------------------------
    # B1. TỔNG QUAN DỮ LIỆU
    # -------------------------------------------------------------------------
    train_path = "hdfs://hadoop-namenode:9000/bike-sharing/raw/train.csv"
    test_path = "hdfs://hadoop-namenode:9000/bike-sharing/raw/test.csv"

    train_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(train_path)
    )
    test_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(test_path)
    )

    train_rows = train_df.count()
    train_cols = len(train_df.columns)
    test_rows = test_df.count()
    test_cols = len(test_df.columns)

    print(f"\n[B1] Kích thước tập Train: {train_rows} dòng, {train_cols} cột")
    print(f"[B1] Kích thước tập Test : {test_rows} dòng, {test_cols} cột")
    print("\n[B1] Schema của train.csv:")
    train_df.printSchema()

    # Kiểm tra Null / Missing values trên từng cột của train.csv
    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in train_df.columns
    ]
    null_counts_row = train_df.select(null_exprs).collect()[0].asDict()
    print("\n[B1] Số lượng giá trị Null trên từng cột (train.csv):")
    for col_name, n_null in null_counts_row.items():
        print(f"  - {col_name:<12}: {n_null}")

    # Kiểm tra Duplicate rows
    distinct_rows = train_df.distinct().count()
    duplicate_rows = train_rows - distinct_rows
    distinct_timestamps = train_df.select("datetime").distinct().count()
    duplicate_timestamps = train_rows - distinct_timestamps
    print(f"\n[B1] Kiểm tra trùng lặp (train.csv):")
    print(f"  - Tổng số bản ghi          : {train_rows}")
    print(f"  - Số bản ghi duy nhất      : {distinct_rows} (Duplicate rows = {duplicate_rows})")
    print(f"  - Số mốc datetime duy nhất : {distinct_timestamps} (Duplicate timestamps = {duplicate_timestamps})")

    # Descriptive statistics cho các cột số
    numeric_cols = [
        "season", "holiday", "workingday", "weather",
        "temp", "atemp", "humidity", "windspeed",
        "casual", "registered", "count"
    ]
    desc_rows = []
    for c in numeric_cols:
        stats = train_df.select(
            F.lit(c).alias("column"),
            F.count(F.col(c)).cast("long").alias("count"),
            F.round(F.mean(F.col(c)), 4).cast("double").alias("mean"),
            F.round(F.stddev(F.col(c)), 4).cast("double").alias("stddev"),
            F.round(F.min(F.col(c)), 4).cast("double").alias("min"),
            F.round(F.expr(f"percentile_approx({c}, 0.25)"), 4).cast("double").alias("p25"),
            F.round(F.expr(f"percentile_approx({c}, 0.50)"), 4).cast("double").alias("median"),
            F.round(F.expr(f"percentile_approx({c}, 0.75)"), 4).cast("double").alias("p75"),
            F.round(F.max(F.col(c)), 4).cast("double").alias("max"),
        ).collect()[0]
        desc_rows.append(stats)

    desc_df = spark.createDataFrame(desc_rows)
    print("\n[B1] Bảng Descriptive Statistics (train.csv):")
    desc_df.show(20, truncate=False)
    save_df_to_local_csv(desc_df, f"{output_dir}/b1_descriptive_statistics.csv")

    # Kiểm tra các giá trị bất thường (Anomalies / Potential Outliers)
    anomalies = {
        "negative_count": train_df.filter(F.col("count") < 0).count(),
        "count_mismatch_casual_plus_registered": train_df.filter(
            F.col("count") != (F.col("casual") + F.col("registered"))
        ).count(),
        "humidity_zero_records": train_df.filter(F.col("humidity") == 0).count(),
        "windspeed_zero_records": train_df.filter(F.col("windspeed") == 0).count(),
        "weather_category_4_extreme": train_df.filter(F.col("weather") == 4).count(),
        "temp_atemp_large_gap_gt_10C": train_df.filter(
            F.abs(F.col("temp") - F.col("atemp")) > 10.0
        ).count(),
        "count_iqr_upper_outliers_gt_647": train_df.filter(
            F.col("count") > (284 + 1.5 * (284 - 42))
        ).count(),
    }
    print("\n[B1] Kiểm tra giá trị đặc biệt / bất thường (Anomalies Check):")
    for k, v in anomalies.items():
        print(f"  - {k}: {v}")

    # -------------------------------------------------------------------------
    # B2. PHÂN TÍCH BIẾN MỤC TIÊU (count)
    # -------------------------------------------------------------------------
    target_stats = train_df.select(
        F.count("count").alias("total_records"),
        F.sum("count").alias("total_rentals"),
        F.round(F.mean("count"), 2).alias("mean"),
        F.round(F.stddev("count"), 2).alias("stddev"),
        F.min("count").alias("min"),
        F.expr("percentile_approx(count, 0.10)").alias("p10"),
        F.expr("percentile_approx(count, 0.25)").alias("p25"),
        F.expr("percentile_approx(count, 0.50)").alias("median"),
        F.expr("percentile_approx(count, 0.75)").alias("p75"),
        F.expr("percentile_approx(count, 0.90)").alias("p90"),
        F.expr("percentile_approx(count, 0.99)").alias("p99"),
        F.max("count").alias("max"),
        F.round(F.skewness("count"), 4).alias("skewness"),
        F.round(F.kurtosis("count"), 4).alias("kurtosis"),
    )
    print("\n[B2] Thống kê chi tiết biến mục tiêu 'count':")
    target_stats.show(truncate=False)
    save_df_to_local_csv(target_stats, f"{output_dir}/b2_target_count_stats.csv")

    # Phân phối biến count theo các khoảng (Histogram bins)
    count_dist_df = (
        train_df.withColumn(
            "demand_bin",
            F.when(F.col("count") <= 50, "01. Very Low (1-50)")
            .when(F.col("count") <= 150, "02. Low (51-150)")
            .when(F.col("count") <= 300, "03. Moderate (151-300)")
            .when(F.col("count") <= 500, "04. High (301-500)")
            .otherwise("05. Peak (>500)"),
        )
        .groupBy("demand_bin")
        .agg(
            F.count("*").alias("hours_count"),
            F.round(F.count("*") * 100.0 / train_rows, 2).alias("percentage"),
            F.round(F.mean("count"), 2).alias("avg_count"),
        )
        .orderBy("demand_bin")
    )
    print("\n[B2] Phân phối tần suất của biến mục tiêu 'count':")
    count_dist_df.show(truncate=False)
    save_df_to_local_csv(count_dist_df, f"{output_dir}/b2_target_count_distribution.csv")

    # -------------------------------------------------------------------------
    # B3. PHÂN TÍCH THEO THỜI GIAN
    # -------------------------------------------------------------------------
    time_df = (
        train_df.withColumn("year", F.year("datetime"))
        .withColumn("month", F.month("datetime"))
        .withColumn("hour", F.hour("datetime"))
        .withColumn("dayofweek", F.dayofweek("datetime"))  # 1=Sun, 2=Mon, ..., 7=Sat
    )

    # 1) Theo Giờ trong ngày (0-23)
    by_hour_df = (
        time_df.groupBy("hour")
        .agg(
            F.count("*").alias("records"),
            F.round(F.mean("count"), 2).alias("avg_count"),
            F.expr("percentile_approx(count, 0.5)").alias("median_count"),
            F.round(F.mean("casual"), 2).alias("avg_casual"),
            F.round(F.mean("registered"), 2).alias("avg_registered"),
        )
        .orderBy("hour")
    )
    print("\n[B3] Nhu cầu thuê xe theo Giờ trong ngày (hour):")
    by_hour_df.show(24, truncate=False)
    save_df_to_local_csv(by_hour_df, f"{output_dir}/b3_demand_by_hour.csv")

    # 2) Theo Ngày trong tuần (dayofweek)
    by_dow_df = (
        time_df.groupBy("dayofweek")
        .agg(
            F.count("*").alias("records"),
            F.round(F.mean("count"), 2).alias("avg_count"),
            F.round(F.mean("casual"), 2).alias("avg_casual"),
            F.round(F.mean("registered"), 2).alias("avg_registered"),
        )
        .orderBy("dayofweek")
    )
    print("\n[B3] Nhu cầu thuê xe theo Ngày trong tuần (1=CN ... 7=Thứ Bảy):")
    by_dow_df.show(truncate=False)
    save_df_to_local_csv(by_dow_df, f"{output_dir}/b3_demand_by_dayofweek.csv")

    # 3) Theo Tháng (1-12)
    by_month_df = (
        time_df.groupBy("month")
        .agg(
            F.count("*").alias("records"),
            F.round(F.mean("count"), 2).alias("avg_count"),
            F.sum("count").alias("total_count"),
            F.round(F.mean("temp"), 2).alias("avg_temp"),
        )
        .orderBy("month")
    )
    print("\n[B3] Nhu cầu thuê xe theo Tháng (month):")
    by_month_df.show(12, truncate=False)
    save_df_to_local_csv(by_month_df, f"{output_dir}/b3_demand_by_month.csv")

    # 4) Theo Năm (2011 vs 2012)
    by_year_df = (
        time_df.groupBy("year")
        .agg(
            F.count("*").alias("records"),
            F.sum("count").alias("total_count"),
            F.round(F.mean("count"), 2).alias("avg_count"),
            F.round(F.mean("casual"), 2).alias("avg_casual"),
            F.round(F.mean("registered"), 2).alias("avg_registered"),
        )
        .orderBy("year")
    )
    print("\n[B3] Nhu cầu thuê xe theo Năm (year):")
    by_year_df.show(truncate=False)
    save_df_to_local_csv(by_year_df, f"{output_dir}/b3_demand_by_year.csv")

    # -------------------------------------------------------------------------
    # B4. PHÂN TÍCH THEO ĐIỀU KIỆN THỜI TIẾT & LOẠI NGÀY (Spark DataFrame + SQL)
    # -------------------------------------------------------------------------
    time_df.createOrReplaceTempView("eda_train_view")

    # 1) Theo Mùa (season)
    by_season_df = spark.sql("""
        SELECT
            season,
            CASE season
                WHEN 1 THEN 'Spring'
                WHEN 2 THEN 'Summer'
                WHEN 3 THEN 'Fall'
                WHEN 4 THEN 'Winter'
            END AS season_name,
            COUNT(*) AS records,
            SUM(count) AS total_rentals,
            ROUND(AVG(count), 2) AS avg_count,
            ROUND(STDDEV(count), 2) AS std_count,
            ROUND(AVG(temp), 2) AS avg_temp
        FROM eda_train_view
        GROUP BY season
        ORDER BY season
    """)
    print("\n[B4] Nhu cầu thuê xe theo Mùa (season):")
    by_season_df.show(truncate=False)
    save_df_to_local_csv(by_season_df, f"{output_dir}/b4_demand_by_season.csv")

    # 2) Theo Thời tiết (weather)
    by_weather_df = spark.sql("""
        SELECT
            weather,
            CASE weather
                WHEN 1 THEN 'Clear / Partly Cloudy'
                WHEN 2 THEN 'Mist / Cloudy'
                WHEN 3 THEN 'Light Snow / Light Rain'
                WHEN 4 THEN 'Heavy Rain / Ice Pellets'
            END AS weather_desc,
            COUNT(*) AS records,
            ROUND(AVG(count), 2) AS avg_count,
            ROUND(AVG(casual), 2) AS avg_casual,
            ROUND(AVG(registered), 2) AS avg_registered
        FROM eda_train_view
        GROUP BY weather
        ORDER BY weather
    """)
    print("\n[B4] Nhu cầu thuê xe theo Điều kiện thời tiết (weather):")
    by_weather_df.show(truncate=False)
    save_df_to_local_csv(by_weather_df, f"{output_dir}/b4_demand_by_weather.csv")

    # 3) Theo Workingday & Holiday
    by_daytype_df = spark.sql("""
        SELECT
            workingday,
            holiday,
            COUNT(*) AS records,
            ROUND(AVG(count), 2) AS avg_count,
            ROUND(AVG(casual), 2) AS avg_casual,
            ROUND(AVG(registered), 2) AS avg_registered
        FROM eda_train_view
        GROUP BY workingday, holiday
        ORDER BY workingday DESC, holiday ASC
    """)
    print("\n[B4] Nhu cầu thuê xe theo Workingday & Holiday:")
    by_daytype_df.show(truncate=False)
    save_df_to_local_csv(by_daytype_df, f"{output_dir}/b4_demand_by_workingday_holiday.csv")

    # 4) Theo Nhóm Nhiệt độ (temp), Độ ẩm (humidity), Tốc độ gió (windspeed)
    by_env_bins_df = spark.sql("""
        SELECT
            CASE
                WHEN temp < 10 THEN '1. Cold (<10C)'
                WHEN temp < 20 THEN '2. Mild (10-20C)'
                WHEN temp < 30 THEN '3. Warm (20-30C)'
                ELSE '4. Hot (>=30C)'
            END AS temp_range,
            CASE
                WHEN humidity < 40 THEN 'Low (<40%)'
                WHEN humidity <= 75 THEN 'Moderate (40-75%)'
                ELSE 'High (>75%)'
            END AS humidity_level,
            COUNT(*) AS records,
            ROUND(AVG(count), 2) AS avg_count,
            ROUND(AVG(windspeed), 2) AS avg_windspeed
        FROM eda_train_view
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    print("\n[B4] Nhu cầu thuê xe theo Nhiệt độ & Độ ẩm:")
    by_env_bins_df.show(20, truncate=False)
    save_df_to_local_csv(by_env_bins_df, f"{output_dir}/b4_demand_by_temp_humidity.csv")

    # Lưu tổng hợp JSON summary cho EDA
    eda_summary_json = {
        "train_rows": train_rows,
        "train_cols": train_cols,
        "test_rows": test_rows,
        "test_cols": test_cols,
        "null_counts": null_counts_row,
        "duplicate_rows": duplicate_rows,
        "duplicate_timestamps": duplicate_timestamps,
        "anomalies": anomalies,
        "target_count_summary": target_stats.collect()[0].asDict(),
    }
    json_path = f"{output_dir}/eda_summary.json"
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(eda_summary_json, jf, indent=2, ensure_ascii=False)
    print(f"  [SAVED JSON] {json_path}")

    print("\n" + "=" * 85)
    print("HOÀN TẤT GIAI ĐOẠN B (EDA) THÀNH CÔNG!")
    print("=" * 85)
    spark.stop()


if __name__ == "__main__":
    main()
