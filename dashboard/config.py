"""
Cấu hình trung tâm cho ứng dụng Streamlit:
Bike Sharing Demand Analysis & Forecasting Dashboard
"""

from pathlib import Path

# Xác định đường dẫn gốc của dự án bằng pathlib (không phụ thuộc thư mục làm việc hiện tại)
DASHBOARD_DIR: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = DASHBOARD_DIR.parent
OUTPUTS_DIR: Path = PROJECT_ROOT / "outputs"
DOCS_DIR: Path = PROJECT_ROOT / "docs"
ASSETS_DIR: Path = DASHBOARD_DIR / "assets"

# Các thư mục con trong outputs/
EDA_DIR: Path = OUTPUTS_DIR / "eda"
ANALYTICS_DIR: Path = OUTPUTS_DIR / "analytics"
PREPROCESSING_DIR: Path = OUTPUTS_DIR / "preprocessing"
MODELS_DIR: Path = OUTPUTS_DIR / "models"
PREDICTIONS_DIR: Path = OUTPUTS_DIR / "predictions"
BENCHMARK_DIR: Path = OUTPUTS_DIR / "benchmark"
BENCHMARK_CHARTS_DIR: Path = BENCHMARK_DIR / "charts"

# Đường dẫn chi tiết tới các tệp kết quả (Artifacts) từ Milestone 4, 5, 6
ARTIFACT_PATHS: dict[str, Path] = {
    # Milestone 4: EDA & Preprocessing
    "eda_summary": EDA_DIR / "eda_summary.json",
    "descriptive_stats": EDA_DIR / "b1_descriptive_statistics.csv",
    "target_stats": EDA_DIR / "b2_target_count_stats.csv",
    "target_distribution": EDA_DIR / "b2_target_count_distribution.csv",
    "eda_dayofweek": EDA_DIR / "b3_demand_by_dayofweek.csv",
    "eda_hour": EDA_DIR / "b3_demand_by_hour.csv",
    "eda_month": EDA_DIR / "b3_demand_by_month.csv",
    "eda_year": EDA_DIR / "b3_demand_by_year.csv",
    "eda_season": EDA_DIR / "b4_demand_by_season.csv",
    "eda_temp_humidity": EDA_DIR / "b4_demand_by_temp_humidity.csv",
    "eda_weather": EDA_DIR / "b4_demand_by_weather.csv",
    "eda_workingday_holiday": EDA_DIR / "b4_demand_by_workingday_holiday.csv",
    "preprocessing_summary": PREPROCESSING_DIR / "preprocessing_summary.json",
    # Milestone 4: Spark SQL Analytics (q1 -> q8)
    "sql_q1_hourly": ANALYTICS_DIR / "q1_hourly_demand.csv",
    "sql_q2_dayofweek": ANALYTICS_DIR / "q2_dayofweek_demand.csv",
    "sql_q3_seasonal": ANALYTICS_DIR / "q3_seasonal_demand.csv",
    "sql_q4_weather": ANALYTICS_DIR / "q4_weather_demand.csv",
    "sql_q5_workingday": ANALYTICS_DIR / "q5_workingday_comparison.csv",
    "sql_q6_monthly_trend": ANALYTICS_DIR / "q6_monthly_yearly_trend.csv",
    "sql_q7_peak_hours": ANALYTICS_DIR / "q7_top10_peak_hours.csv",
    "sql_q8_user_behavior": ANALYTICS_DIR / "q8_user_behavior_casual_vs_registered.csv",
    # Milestone 5: Spark MLlib Models & Predictions
    "model_comparison": MODELS_DIR / "model_comparison_metrics.csv",
    "best_model_metadata": MODELS_DIR / "best_model_metadata.json",
    "feature_importances": MODELS_DIR / "feature_importances.csv",
    "validation_predictions": MODELS_DIR / "validation_predictions.csv",
    "test_predictions": MODELS_DIR / "test_predictions.csv",
    "kaggle_submission": MODELS_DIR / "kaggle_submission.csv",
    # Milestone 6: Big Data Scaling & Benchmark
    "scaling_results": BENCHMARK_DIR / "scaling_results.csv",
    "scaling_generation_summary": BENCHMARK_DIR / "scaling_generation_summary.csv",
    "generation_metadata": BENCHMARK_DIR / "generation_metadata.json",
    "storage_comparison": BENCHMARK_DIR / "storage_comparison.csv",
    "storage_runs_detail": BENCHMARK_DIR / "storage_runs_detail.csv",
    "storage_benchmark_json": BENCHMARK_DIR / "storage_benchmark.json",
    "processing_results": BENCHMARK_DIR / "processing_results.csv",
    "partition_results": BENCHMARK_DIR / "partition_results.csv",
    "processing_partition_json": BENCHMARK_DIR / "processing_and_partition_benchmark.json",
    "benchmark_summary": BENCHMARK_DIR / "benchmark_summary.json",
}

# Hằng số thông tin đề tài & cấu hình thực nghiệm đã ghi nhận
PROJECT_INFO: dict[str, str] = {
    "title": "Bike Sharing Demand Analysis & Forecasting",
    "subtitle": "Apache Hadoop, Apache Spark & Machine Learning",
    "vietnamese_title": "Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark",
    "dataset_name": "Kaggle Bike Sharing Demand",
    "target_column": "count",
}

CLUSTER_CONFIG_SPEC: dict[str, str] = {
    "deployment_mode": "Single-Machine Containerized Spark Standalone",
    "spark_version": "Apache Spark 3.5.6 (Scala 2.12, OpenJDK 17)",
    "hadoop_version": "Apache Hadoop 3.2.1 (HDFS)",
    "spark_topology": "1 Spark Master + 1 Spark Worker",
    "worker_cores": "2 CPU Cores",
    "worker_ram": "2048 MB Worker RAM (1024 MiB Executor)",
    "hdfs_replication": "HDFS replication = 1 (1 NameNode + 1 DataNode)",
}

# Bảng màu giao diện chuyên nghiệp, nhất quán và tối giản
COLORS: dict[str, str] = {
    "primary": "#1E40AF",       # Deep Cobalt Blue
    "secondary": "#2563EB",     # Royal Blue
    "teal": "#0D9488",          # Teal
    "amber": "#D97706",         # Amber
    "emerald": "#16A34A",       # Emerald Green
    "crimson": "#DC2626",       # Crimson Red
    "slate": "#475569",         # Neutral Slate
    "light_bg": "#F8FAFC",      # Card Light Background
    "border": "#E2E8F0",        # Subtle Border
}

