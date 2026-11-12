"""
Milestone 4 - Giai đoạn E & F: Spark SQL Analytics
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Nội dung thực hiện:
- Đọc dữ liệu đã làm sạch (Cleaned Analytical Dataset) từ HDFS:
  hdfs://hadoop-namenode:9000/bike-sharing/processed/train
- Đăng ký Temporary View 'bike_sharing_analytics' và thực thi 8 nhóm truy vấn Spark SQL:
  1. Nhu cầu thuê xe trung bình theo giờ (hourly_demand).
  2. Nhu cầu thuê xe theo ngày trong tuần (dayofweek_demand).
  3. Nhu cầu thuê xe theo mùa (seasonal_demand).
  4. Nhu cầu thuê xe theo điều kiện thời tiết (weather_demand).
  5. So sánh workingday và non-workingday (workingday_comparison).
  6. Thống kê số lượng thuê xe theo tháng và năm (monthly_yearly_trend).
  7. Top 10 khung giờ (cụ thể theo ngày-giờ và theo khung giờ mẫu) có nhu cầu cao nhất (top10_peak_hours & top10_hourly_slots).
  8. Phân tích casual và registered phục vụ thống kê hành vi người dùng (user_behavior_casual_vs_registered).
- Lưu kết quả truy vấn dưới dạng Parquet (Snappy) lên HDFS tại /bike-sharing/analytics/<query_name>/
  và xuất các tệp CSV ra outputs/analytics/ trên máy host.
"""

import csv
import os
from pyspark.sql import SparkSession

os.environ["HADOOP_USER_NAME"] = "root"
os.environ["USER"] = "root"


def save_df_to_local_csv(df, filepath):
    """Lưu Spark DataFrame kết quả truy vấn SQL ra tệp CSV chuẩn."""
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
    print("MILESTONE 4 - GIAI ĐOẠN E & F: SPARK SQL ANALYTICS & LƯU KẾT QUẢ HDFS/CSV")
    print("=" * 85)

    spark = (
        SparkSession.builder.appName("Milestone4-StageEF-SparkSQL-Analytics")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
        .config("spark.executorEnv.HADOOP_USER_NAME", "root")
        .config("spark.executorEnv.USER", "root")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    processed_train_path = "hdfs://hadoop-namenode:9000/bike-sharing/processed/train"
    hdfs_analytics_base = "hdfs://hadoop-namenode:9000/bike-sharing/analytics"
    local_out_dir = "/opt/spark-jobs/_staging_outputs/analytics"
    os.makedirs(local_out_dir, exist_ok=True)

    train_df = spark.read.parquet(processed_train_path).cache()
    train_df.createOrReplaceTempView("bike_sharing")
    print(f"[INFO] Đã nạp bảng 'bike_sharing' từ {processed_train_path} ({train_df.count()} dòng)")

    queries = {
        "q1_hourly_demand": """
            SELECT
                hour,
                COUNT(*) AS total_records,
                SUM(count) AS total_rentals,
                ROUND(AVG(count), 2) AS avg_total_demand,
                ROUND(STDDEV(count), 2) AS std_total_demand,
                PERCENTILE_APPROX(count, 0.5) AS median_total_demand,
                ROUND(AVG(casual), 2) AS avg_casual,
                ROUND(AVG(registered), 2) AS avg_registered
            FROM bike_sharing
            GROUP BY hour
            ORDER BY hour ASC
        """,
        "q2_dayofweek_demand": """
            SELECT
                dayofweek,
                CASE dayofweek
                    WHEN 1 THEN 'Sunday'
                    WHEN 2 THEN 'Monday'
                    WHEN 3 THEN 'Tuesday'
                    WHEN 4 THEN 'Wednesday'
                    WHEN 5 THEN 'Thursday'
                    WHEN 6 THEN 'Friday'
                    WHEN 7 THEN 'Saturday'
                END AS day_name,
                is_weekend,
                COUNT(*) AS total_records,
                SUM(count) AS total_rentals,
                ROUND(AVG(count), 2) AS avg_total_demand,
                ROUND(AVG(casual), 2) AS avg_casual,
                ROUND(AVG(registered), 2) AS avg_registered
            FROM bike_sharing
            GROUP BY dayofweek, is_weekend
            ORDER BY dayofweek ASC
        """,
        "q3_seasonal_demand": """
            SELECT
                season,
                CASE season
                    WHEN 1 THEN 'Spring'
                    WHEN 2 THEN 'Summer'
                    WHEN 3 THEN 'Fall'
                    WHEN 4 THEN 'Winter'
                END AS season_name,
                COUNT(*) AS total_records,
                SUM(count) AS total_rentals,
                ROUND(AVG(count), 2) AS avg_total_demand,
                ROUND(AVG(casual), 2) AS avg_casual,
                ROUND(AVG(registered), 2) AS avg_registered,
                ROUND(AVG(temp), 2) AS avg_temp_c,
                ROUND(AVG(humidity), 2) AS avg_humidity_pct
            FROM bike_sharing
            GROUP BY season
            ORDER BY season ASC
        """,
        "q4_weather_demand": """
            SELECT
                weather,
                CASE weather
                    WHEN 1 THEN 'Clear / Partly Cloudy'
                    WHEN 2 THEN 'Mist / Cloudy'
                    WHEN 3 THEN 'Light Snow / Light Rain'
                    WHEN 4 THEN 'Heavy Rain / Ice Pellets'
                END AS weather_condition,
                COUNT(*) AS total_records,
                SUM(count) AS total_rentals,
                ROUND(AVG(count), 2) AS avg_total_demand,
                ROUND(AVG(casual), 2) AS avg_casual,
                ROUND(AVG(registered), 2) AS avg_registered,
                ROUND(AVG(temp), 2) AS avg_temp_c,
                ROUND(AVG(humidity), 2) AS avg_humidity_pct,
                ROUND(AVG(windspeed), 2) AS avg_windspeed
            FROM bike_sharing
            GROUP BY weather
            ORDER BY weather ASC
        """,
        "q5_workingday_comparison": """
            SELECT
                workingday,
                CASE workingday
                    WHEN 1 THEN 'Working Day'
                    ELSE 'Non-Working Day (Weekend/Holiday)'
                END AS day_category,
                COUNT(*) AS total_records,
                SUM(count) AS total_rentals,
                ROUND(AVG(count), 2) AS avg_total_demand,
                ROUND(AVG(casual), 2) AS avg_casual,
                ROUND(AVG(registered), 2) AS avg_registered,
                ROUND(SUM(casual) * 100.0 / SUM(count), 2) AS casual_share_pct,
                ROUND(SUM(registered) * 100.0 / SUM(count), 2) AS registered_share_pct
            FROM bike_sharing
            GROUP BY workingday
            ORDER BY workingday DESC
        """,
        "q6_monthly_yearly_trend": """
            SELECT
                year,
                month,
                CONCAT(CAST(year AS STRING), '-', LPAD(CAST(month AS STRING), 2, '0')) AS year_month,
                COUNT(*) AS total_records,
                SUM(count) AS total_rentals,
                ROUND(AVG(count), 2) AS avg_total_demand,
                ROUND(AVG(casual), 2) AS avg_casual,
                ROUND(AVG(registered), 2) AS avg_registered,
                ROUND(AVG(temp), 2) AS avg_temp_c
            FROM bike_sharing
            GROUP BY year, month
            ORDER BY year ASC, month ASC
        """,
        "q7_top10_peak_hours": """
            SELECT
                hour,
                CASE workingday
                    WHEN 1 THEN 'Working Day'
                    ELSE 'Non-Working Day'
                END AS day_category,
                COUNT(*) AS total_records,
                ROUND(AVG(count), 2) AS avg_total_demand,
                MAX(count) AS max_demand_recorded,
                ROUND(AVG(casual), 2) AS avg_casual,
                ROUND(AVG(registered), 2) AS avg_registered
            FROM bike_sharing
            GROUP BY hour, workingday
            ORDER BY avg_total_demand DESC
            LIMIT 10
        """,
        "q8_user_behavior_casual_vs_registered": """
            SELECT
                hour,
                workingday,
                ROUND(AVG(casual), 2) AS avg_casual,
                ROUND(AVG(registered), 2) AS avg_registered,
                ROUND(AVG(count), 2) AS avg_total,
                ROUND(SUM(casual) * 100.0 / SUM(count), 2) AS casual_pct,
                ROUND(SUM(registered) * 100.0 / SUM(count), 2) AS registered_pct
            FROM bike_sharing
            GROUP BY hour, workingday
            ORDER BY workingday DESC, hour ASC
        """,
    }

    for q_name, sql_text in queries.items():
        print("\n" + "-" * 85)
        print(f"[SPARK SQL] Thực thi truy vấn: {q_name}")
        res_df = spark.sql(sql_text)
        res_df.show(15, truncate=False)

        # Lưu CSV ra thư mục staging để copy về outputs/analytics/
        save_df_to_local_csv(res_df, f"{local_out_dir}/{q_name}.csv")

        # Lưu Parquet lên HDFS tại /bike-sharing/analytics/<q_name>
        hdfs_out_path = f"{hdfs_analytics_base}/{q_name}"
        (
            res_df.coalesce(1)
            .write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(hdfs_out_path)
        )
        print(f"  [SAVED HDFS PARQUET] {hdfs_out_path}")

    print("\n" + "=" * 85)
    print("HOÀN TẤT GIAI ĐOẠN E (SPARK SQL ANALYTICS) THÀNH CÔNG 100%!")
    print("=" * 85)
    spark.stop()


if __name__ == "__main__":
    main()

