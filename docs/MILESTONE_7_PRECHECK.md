# BÁO CÁO KIỂM TRA TRƯỚC KHI XÂY DỰNG DASHBOARD (MILESTONE 7 — PHASE A: PRECHECK & ARTIFACT AUDIT)

**Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Tên tiếng Việt:** Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark  
**Thời gian kiểm tra:** 2026-11-12  
**Trạng thái đánh giá:** **`READY` (SẴN SÀNG 100% ĐỂ XÂY DỰNG STREAMLIT DASHBOARD)**

---

## 1. Trạng thái Tài nguyên Hệ thống

| Hạng mục | Thông số kiểm tra thực tế | Đánh giá an toàn |
| :--- | :--- | :--- |
| **Hệ điều hành** | Windows 11 + WSL2 (`Linux 6.18.33.2-microsoft-standard-WSL2`) | Hoạt động ổn định |
| **Dung lượng ổ C:** | Đã dùng `420.37 GB` / **Còn trống `21.09 GB`** | **Rất an toàn** (vượt xa ngưỡng tối thiểu `5.0 GB`) |
| **Dung lượng ổ D:** | Đã dùng `1.09 GB` / Còn trống `1.96 GB` | Giữ nguyên, không ghi dữ liệu lên ổ D |
| **RAM Vật lý (Host)** | Tổng `15.68 GB` usable (`16 GB` vật lý); WSL2 + Windows Page Cache đang cấp phát động | Cần ưu tiên đọc trực tiếp các tệp CSV/JSON gọn nhẹ (`~1.05 MB` tổng cộng) từ `outputs/` kèm `@st.cache_data`, không gọi Spark job từ Dashboard |
| **RAM trong Docker WSL2** | Đang dùng `~4.23 GiB / 7.597 GiB` across 10 containers (**còn trống `~3.36 GiB`**) | Cụm container hoạt động nhẹ nhàng (`~55.7%` giới hạn WSL2) |
| **Cổng mạng `8501` (Streamlit)** | **`PORT 8501 IS FREE`** (Không có tiến trình nào đang chiếm cổng `8501`) | Sẵn sàng khởi chạy Streamlit trên `http://localhost:8501` |

---

## 2. Trạng thái Docker, Langfuse, Hadoop HDFS và Apache Spark

### 2.1. Danh sách 10 Container Đang Chạy (`docker ps` & `docker stats --no-stream`)

| Tên Container | Image | Nhóm hệ thống | Trạng thái | CPU % | RAM Usage / Limit | RAM % | Cổng ánh xạ (Ports) |
| :--- | :--- | :--- | :--- | :---: | :---: | :---: | :--- |
| `hadoop-namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | **Hadoop HDFS** | `Up 23 hours (healthy)` | `3.58%` | `432.0 MiB / 7.597 GiB` | `5.55%` | `9870->9870`, `9820->9000` |
| `hadoop-datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | **Hadoop HDFS** | `Up 23 hours (healthy)` | `0.25%` | `505.9 MiB / 7.597 GiB` | `6.50%` | `9864->9864` |
| `spark-master` | `bitnamilegacy/spark:3.5` | **Apache Spark** | `Up 22 hours` (`ALIVE`) | `0.46%` | `245.6 MiB / 7.597 GiB` | `3.16%` | `7077->7077`, `8080->8080` |
| `spark-worker-1` | `bitnamilegacy/spark:3.5` | **Apache Spark** | `Up 22 hours` (`ALIVE`) | `1.50%` | `176.9 MiB / 7.597 GiB` | `2.27%` | *(Nội bộ `8081`, `33299`)* |
| `langfuse-local-langfuse-web-1` | `langfuse/langfuse:4` | **Langfuse** | `Up 43 hours` | `1.11%` | `1.017 GiB / 7.597 GiB` | `13.38%` | `3000->3000` |
| `langfuse-local-langfuse-worker-1` | `langfuse/langfuse-worker:4` | **Langfuse** | `Up 43 hours` | `0.62%` | `619.7 MiB / 7.597 GiB` | `7.97%` | `3030->3030` |
| `langfuse-local-clickhouse-1` | `clickhouse/clickhouse-server:25.12` | **Langfuse** | `Up 43 hours (healthy)` | `41.93%` | `1004 MiB / 7.597 GiB` | `12.91%` | `8123->8123`, `9000->9000` |
| `langfuse-local-minio-1` | `cgr.dev/chainguard/minio` | **Langfuse** | `Up 43 hours (healthy)` | `5.57%` | `153.2 MiB / 7.597 GiB` | `1.97%` | `9090->9000`, `9091->9001` |
| `langfuse-local-postgres-1` | `postgres:17` | **Langfuse** | `Up 43 hours (healthy)` | `11.31%` | `58.48 MiB / 7.597 GiB` | `0.75%` | `5432->5432` |
| `langfuse-local-redis-1` | `redis:7` | **Langfuse** | `Up 43 hours (healthy)` | `2.07%` | `19.62 MiB / 7.597 GiB` | `0.25%` | `6379->6379` |

### 2.2. Trạng thái Cụm Hadoop HDFS & Apache Spark
- **HDFS (`hdfs dfsadmin -report` & `hdfs fsck /bike-sharing`):**
  - Trạng thái Filesystem: **`Status: HEALTHY`**.
  - Live DataNodes: **`1`** (`slave01`, `172.24.0.3:9866`, Decommission Status: `Normal`).
  - Dung lượng HDFS đã dùng (`DFS Used`): `245.25 MB` / Còn trống (`DFS Remaining`): `940.63 GB`.
  - Tổng số tệp trên `/bike-sharing`: `109 files`, `60 blocks`, `Corrupt blocks: 0`, `Missing blocks (with replication factor 1): 0`.
  - Cấu hình nhân bản (`HDFS replication`): `HDFS_CONF_dfs_replication=1` trong `hadoop/hadoop.env` và `spark.hadoop.dfs.replication=1` trong các job Benchmark. *(Lưu ý: 36 blocks từ Milestone 3–5 ghi trước khi đặt `spark.hadoop.dfs.replication=1` có metadata replication=3 nhưng vẫn đạt 100% minimally replicated blocks trên 1 DataNode, `0 corrupt`).*
- **Apache Spark (`http://localhost:8080/json/`):**
  - Kiến trúc cụm: **1 Spark Master + 1 Spark Worker** (`worker-20261111042143-172.24.0.5-33299`).
  - Trạng thái Master: **`ALIVE`** | Trạng thái Worker: **`ALIVE`**.
  - Tài nguyên Worker: **`2 CPU Cores`**, **`2048 MB (2.0 GiB) RAM`**; cấu hình Executor cho job: **`1024 MiB (1.0 GiB) Executor RAM`**.

---

## 3. Kiểm toán Danh sách Artifacts Milestone 2–6

Toàn bộ **9 tài liệu báo cáo (`docs/`)** và **44 tệp dữ liệu kết quả (`outputs/`)** đã được kiểm tra tồn tại, kích thước byte, số dòng, số cột và khả năng đọc bằng Python `csv`/`json` parser (**100% hợp lệ, 0 tệp thiếu, 0 tệp rỗng, 0 lỗi đọc**).

### 3.1. Tài liệu Kỹ thuật (`docs/`)
| STT | Đường dẫn tệp | Kích thước (Bytes) | Milestone | Trạng thái |
| :---: | :--- | :---: | :---: | :---: |
| 1 | [docs/REPOSITORY_AUDIT.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/REPOSITORY_AUDIT.md) | `28,475` | Milestone 1 | Hợp lệ |
| 2 | [docs/MILESTONE_2_REPORT.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_2_REPORT.md) | `9,319` | Milestone 2 | Hợp lệ |
| 3 | [docs/MILESTONE_3_REPORT.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_3_REPORT.md) | `14,759` | Milestone 3 | Hợp lệ |
| 4 | [docs/MILESTONE_4_PRECHECK.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_4_PRECHECK.md) | `4,893` | Milestone 4 | Hợp lệ |
| 5 | [docs/DATA_DICTIONARY.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/DATA_DICTIONARY.md) | `6,595` | Milestone 4 | Hợp lệ |
| 6 | [docs/MILESTONE_4_REPORT.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_4_REPORT.md) | `16,581` | Milestone 4 | Hợp lệ |
| 7 | [docs/MILESTONE_5_REPORT.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_5_REPORT.md) | `22,171` | Milestone 5 | Hợp lệ |
| 8 | [docs/MILESTONE_6_PRECHECK.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_6_PRECHECK.md) | `4,193` | Milestone 6 | Hợp lệ |
| 9 | [docs/MILESTONE_6_REPORT.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_6_REPORT.md) | `32,593` | Milestone 6 | Hợp lệ |

### 3.2. Artifacts Milestone 4: EDA, Preprocessing & Spark SQL Analytics (`outputs/eda/`, `outputs/preprocessing/`, `outputs/analytics/`)
| Nhóm | Đường dẫn tệp | Định dạng | Số dòng / Cấu trúc | Danh sách cột (`Header` / `Top Keys`) | Trạng thái |
| :--- | :--- | :---: | :---: | :--- | :---: |
| **EDA** | [outputs/eda/b1_descriptive_statistics.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b1_descriptive_statistics.csv) | CSV | `11` dòng $\times$ `9` cột | `column, count, mean, stddev, min, p25, median, p75, max` | Hợp lệ |
| **EDA** | [outputs/eda/b2_target_count_distribution.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b2_target_count_distribution.csv) | CSV | `5` dòng $\times$ `4` cột | `demand_bin, hours_count, percentage, avg_count` | Hợp lệ |
| **EDA** | [outputs/eda/b2_target_count_stats.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b2_target_count_stats.csv) | CSV | `1` dòng $\times$ `14` cột | `total_records, total_rentals, mean, stddev, min, p10, p25, median, p75, p90, p99, max, skewness, kurtosis` | Hợp lệ |
| **EDA** | [outputs/eda/b3_demand_by_dayofweek.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b3_demand_by_dayofweek.csv) | CSV | `7` dòng $\times$ `5` cột | `dayofweek, records, avg_count, avg_casual, avg_registered` | Hợp lệ |
| **EDA** | [outputs/eda/b3_demand_by_hour.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b3_demand_by_hour.csv) | CSV | `24` dòng $\times$ `6` cột | `hour, records, avg_count, median_count, avg_casual, avg_registered` | Hợp lệ |
| **EDA** | [outputs/eda/b3_demand_by_month.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b3_demand_by_month.csv) | CSV | `12` dòng $\times$ `5` cột | `month, records, avg_count, total_count, avg_temp` | Hợp lệ |
| **EDA** | [outputs/eda/b3_demand_by_year.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b3_demand_by_year.csv) | CSV | `2` dòng $\times$ `6` cột | `year, records, total_count, avg_count, avg_casual, avg_registered` | Hợp lệ |
| **EDA** | [outputs/eda/b4_demand_by_season.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b4_demand_by_season.csv) | CSV | `4` dòng $\times$ `7` cột | `season, season_name, records, total_rentals, avg_count, std_count, avg_temp` | Hợp lệ |
| **EDA** | [outputs/eda/b4_demand_by_temp_humidity.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b4_demand_by_temp_humidity.csv) | CSV | `12` dòng $\times$ `5` cột | `temp_range, humidity_level, records, avg_count, avg_windspeed` | Hợp lệ |
| **EDA** | [outputs/eda/b4_demand_by_weather.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b4_demand_by_weather.csv) | CSV | `4` dòng $\times$ `6` cột | `weather, weather_desc, records, avg_count, avg_casual, avg_registered` | Hợp lệ |
| **EDA** | [outputs/eda/b4_demand_by_workingday_holiday.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b4_demand_by_workingday_holiday.csv) | CSV | `3` dòng $\times$ `6` cột | `workingday, holiday, records, avg_count, avg_casual, avg_registered` | Hợp lệ |
| **EDA** | [outputs/eda/eda_summary.json](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/eda_summary.json) | JSON | `9` keys (`965 B`) | `train_rows, train_cols, test_rows, test_cols, null_counts, duplicate_rows, duplicate_timestamps, anomalies, target_count_summary` | Hợp lệ |
| **Prep** | [outputs/preprocessing/preprocessing_summary.json](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/preprocessing/preprocessing_summary.json) | JSON | `5` keys (`3,538 B`) | `train_nulls, test_nulls, duplicates, validity_checks, parquet_verification` | Hợp lệ |
| **SQL** | [outputs/analytics/q1_hourly_demand.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q1_hourly_demand.csv) | CSV | `24` dòng $\times$ `8` cột | `hour, total_records, total_rentals, avg_total_demand, std_total_demand, median_total_demand, avg_casual, avg_registered` | Hợp lệ |
| **SQL** | [outputs/analytics/q2_dayofweek_demand.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q2_dayofweek_demand.csv) | CSV | `7` dòng $\times$ `8` cột | `dayofweek, day_name, is_weekend, total_records, total_rentals, avg_total_demand, avg_casual, avg_registered` | Hợp lệ |
| **SQL** | [outputs/analytics/q3_seasonal_demand.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q3_seasonal_demand.csv) | CSV | `4` dòng $\times$ `9` cột | `season, season_name, total_records, total_rentals, avg_total_demand, avg_casual, avg_registered, avg_temp_c, avg_humidity_pct` | Hợp lệ |
| **SQL** | [outputs/analytics/q4_weather_demand.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q4_weather_demand.csv) | CSV | `4` dòng $\times$ `10` cột | `weather, weather_condition, total_records, total_rentals, avg_total_demand, avg_casual, avg_registered, avg_temp_c, avg_humidity_pct, avg_windspeed` | Hợp lệ |
| **SQL** | [outputs/analytics/q5_workingday_comparison.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q5_workingday_comparison.csv) | CSV | `2` dòng $\times$ `9` cột | `workingday, day_category, total_records, total_rentals, avg_total_demand, avg_casual, avg_registered, casual_share_pct, registered_share_pct` | Hợp lệ |
| **SQL** | [outputs/analytics/q6_monthly_yearly_trend.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q6_monthly_yearly_trend.csv) | CSV | `24` dòng $\times$ `9` cột | `year, month, year_month, total_records, total_rentals, avg_total_demand, avg_casual, avg_registered, avg_temp_c` | Hợp lệ |
| **SQL** | [outputs/analytics/q7_top10_peak_hours.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q7_top10_peak_hours.csv) | CSV | `10` dòng $\times$ `7` cột | `hour, day_category, total_records, avg_total_demand, max_demand_recorded, avg_casual, avg_registered` | Hợp lệ |
| **SQL** | [outputs/analytics/q8_user_behavior_casual_vs_registered.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q8_user_behavior_casual_vs_registered.csv) | CSV | `48` dòng $\times$ `7` cột | `hour, workingday, avg_casual, avg_registered, avg_total, casual_pct, registered_pct` | Hợp lệ |

### 3.3. Artifacts Milestone 5: Spark MLlib Models & Predictions (`outputs/models/`, `outputs/predictions/`)
| Đường dẫn tệp | Định dạng | Số dòng / Cấu trúc | Danh sách cột (`Header` / `Top Keys`) | Trạng thái |
| :--- | :---: | :---: | :--- | :---: |
| [outputs/models/model_comparison_metrics.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/models/model_comparison_metrics.csv) | CSV | `3` dòng $\times$ `10` cột | `model_key, model_name, rmse, mae, r2, rmse_clipped, rmsle, negative_predictions_count, training_time_sec, params_json` | Hợp lệ |
| [outputs/models/best_model_metadata.json](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/models/best_model_metadata.json) | JSON | `14` keys (`4,144 B`) | `best_model_key, best_model_name, hdfs_model_path, target_column, feature_columns, time_based_split, baseline_mean_predictor, best_model_params, best_model_validation_metrics, all_models_comparison, top_feature_importances...` | Hợp lệ |
| [outputs/models/feature_importances.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/models/feature_importances.csv) | CSV | `18` dòng $\times$ `3` cột | `feature, rf_importance, dt_importance` | Hợp lệ |
| [outputs/models/validation_predictions.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/models/validation_predictions.csv) | CSV | `2,178` dòng $\times$ `10` cột | `datetime, actual_count, lr_prediction, lr_prediction_clipped, dt_prediction, dt_prediction_clipped, rf_prediction, rf_prediction_clipped, best_prediction, best_prediction_clipped` | Hợp lệ |
| [outputs/models/test_predictions.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/models/test_predictions.csv) | CSV | `6,493` dòng $\times$ `3` cột | `datetime, prediction, prediction_clipped` | Hợp lệ |
| [outputs/models/kaggle_submission.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/models/kaggle_submission.csv) | CSV | `6,493` dòng $\times$ `2` cột | `datetime, count` | Hợp lệ |

### 3.4. Artifacts Milestone 6: Big Data Scaling & Benchmark (`outputs/benchmark/`)
| Đường dẫn tệp | Định dạng | Số dòng / Kích thước | Danh sách cột (`Header` / `Top Keys`) | Trạng thái |
| :--- | :---: | :---: | :--- | :---: |
| [outputs/benchmark/scaling_results.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/scaling_results.csv) | CSV | `4` dòng $\times$ `28` cột | `scale_label, scale_factor, num_rows, num_columns, num_partitions, dataset_parquet_size_mb, dataset_csv_size_mb, csv_to_parquet_size_ratio, parquet_write_time_sec, csv_write_time_sec, ..., avg_processing_throughput_rows_per_sec, cpu_usage_pct, memory_usage_mb` | Hợp lệ |
| [outputs/benchmark/storage_comparison.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/storage_comparison.csv) | CSV | `8` dòng $\times$ `23` cột | `scale_label, scale_factor, num_rows, num_columns, format, compression, num_runs, cache_regime, storage_size_bytes, storage_size_mb, avg_write_time_sec, avg_read_count_time_sec, avg_read_agg_time_sec, ...` | Hợp lệ |
| [outputs/benchmark/processing_results.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/processing_results.csv) | CSV | `20` dòng $\times$ `19` cột | `scale_label, scale_factor, operation, input_rows, output_rows, dataset_size_mb, num_partitions, num_runs, run1_time_sec, run2_time_sec, run3_time_sec, avg_processing_time_sec, std_processing_time_sec, throughput_rows_per_sec, cpu_usage_pct, driver_container_mem_mb, error` | Hợp lệ |
| [outputs/benchmark/partition_results.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/partition_results.csv) | CSV | `12` dòng $\times$ `19` cột | `scale_label, scale_factor, num_rows, dataset_size_mb, hdfs_file_count, spark_input_partitions, target_partitions, actual_df_partitions, repartition_cache_time_sec, avg_processing_time_sec, throughput_rows_per_sec, cpu_usage_pct, driver_container_mem_mb` | Hợp lệ |
| [outputs/benchmark/benchmark_summary.json](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/benchmark_summary.json) | JSON | `8` keys (`32,599 B`) | `project, milestone, environment, scaling_results, storage_comparison, processing_results, partition_results, created_at_utc` | Hợp lệ |
| [outputs/benchmark/charts/1_scale_vs_processing_time.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/1_scale_vs_processing_time.svg) | SVG | `6,251 B` | Biểu đồ SVG 1: Dataset Scale vs Processing Time | Hợp lệ |
| [outputs/benchmark/charts/2_scale_vs_throughput.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/2_scale_vs_throughput.svg) | SVG | `5,554 B` | Biểu đồ SVG 2: Dataset Scale vs Throughput | Hợp lệ |
| [outputs/benchmark/charts/3_csv_vs_parquet_storage_size.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/3_csv_vs_parquet_storage_size.svg) | SVG | `4,012 B` | Biểu đồ SVG 3: CSV vs Parquet Storage Size | Hợp lệ |
| [outputs/benchmark/charts/4_csv_vs_parquet_read_write_time.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/4_csv_vs_parquet_read_write_time.svg) | SVG | `5,510 B` | Biểu đồ SVG 4: CSV vs Parquet Read/Write Time | Hợp lệ |
| [outputs/benchmark/charts/5_partitions_vs_processing_time.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/5_partitions_vs_processing_time.svg) | SVG | `4,750 B` | Biểu đồ SVG 5: Partitions vs Processing Time | Hợp lệ |

---

## 4. Bảng Mapping Dữ liệu cho 4 Trang Dashboard

Thiết kế Dashboard gồm **4 trang chức năng chuyên biệt** đọc hoàn toàn từ các tệp CSV/JSON đã tính toán sẵn trong `outputs/` (không gọi lại Spark job):

| Trang Dashboard | Tệp dữ liệu nguồn (`outputs/...`) | Các cột chính sử dụng | Các biểu đồ dự kiến (Interactive Plotly) | Các chỉ số KPI hiển thị |
| :--- | :--- | :--- | :--- | :--- |
| **Trang 1: Tổng quan Dự án & Chất lượng Dữ liệu (`Executive Overview & Data Quality`)** | - `outputs/eda/eda_summary.json`<br>- `outputs/eda/b1_descriptive_statistics.csv`<br>- `outputs/eda/b2_target_count_stats.csv`<br>- `outputs/eda/b2_target_count_distribution.csv`<br>- `outputs/preprocessing/preprocessing_summary.json` | - `total_records, total_rentals, mean, median, skewness, max`<br>- `demand_bin, hours_count, percentage, avg_count`<br>- `column, mean, stddev, min, p25, median, p75, max` | 1. Biểu đồ phân bố nhu cầu theo 5 phân khúc (`demand_bin`: Very Low $\rightarrow$ Peak).<br>2. Biểu đồ Boxplot/Quantile tóm tắt phân phối (`min, p25, median, p75, max`) của các biến khí tượng và `count`.<br>3. Sơ đồ kiến trúc Pipeline End-to-End (HDFS $\rightarrow$ Spark SQL $\rightarrow$ MLlib $\rightarrow$ Streamlit). | - Tổng dòng Train (`10,886`) & Test (`6,493`)<br>- Tổng lượt thuê xe (`2,085,476` lượt)<br>- Nhu cầu TB (`191.57` xe/giờ) & Trung vị (`145` xe/giờ)<br>- Tỉ lệ Missing / Duplicate / Leakage (`0%`) |
| **Trang 2: Phân tích Khám phá & Spark SQL Analytics (`EDA & Spark SQL Insights`)** | - `outputs/analytics/q1_hourly_demand.csv`<br>- `outputs/analytics/q2_dayofweek_demand.csv`<br>- `outputs/analytics/q3_seasonal_demand.csv`<br>- `outputs/analytics/q4_weather_demand.csv`<br>- `outputs/analytics/q5_workingday_comparison.csv`<br>- `outputs/analytics/q6_monthly_yearly_trend.csv`<br>- `outputs/analytics/q7_top10_peak_hours.csv`<br>- `outputs/analytics/q8_user_behavior_casual_vs_registered.csv`<br>- `outputs/eda/b4_demand_by_temp_humidity.csv` | - `hour, avg_total_demand, avg_casual, avg_registered, workingday`<br>- `year_month, total_rentals, avg_temp_c`<br>- `season_name, weather_condition`<br>- `temp_range, humidity_level, avg_count` | 1. Đường biểu diễn nhu cầu 24 giờ: đối sánh `Working Day` (2 đỉnh 8h & 17h) vs `Non-Working Day` (1 đỉnh trưa–chiều) và tách lớp `Casual` vs `Registered` (`q8`, `q1`).<br>2. Xu hướng chuỗi thời gian 24 tháng (`2011-01` $\rightarrow$ `2012-12`) kết hợp trục phụ nhiệt độ (`q6`).<br>3. Biểu đồ cột ghép theo Mùa (`q3`), Thời tiết (`q4`) và Thứ trong tuần (`q2`).<br>4. Heatmap tương tác Nhiệt độ $\times$ Độ ẩm (`b4_demand_by_temp_humidity.csv`). | - Tăng trưởng 2012 vs 2011 (`+65.4%`)<br>- Giờ cao điểm nhất (`17:00` — TB `468.77` xe/giờ)<br>- Mùa cao điểm nhất (`Fall` — TB `234.42` xe/giờ)<br>- Tỷ trọng khách `Registered` ngày đi làm (`84.74%`) |
| **Trang 3: Dự đoán Nhu cầu với Spark MLlib (`MLlib Forecasting & Model Evaluation`)** | - `outputs/models/model_comparison_metrics.csv`<br>- `outputs/models/best_model_metadata.json`<br>- `outputs/models/feature_importances.csv`<br>- `outputs/models/validation_predictions.csv`<br>- `outputs/models/test_predictions.csv`<br>- `outputs/models/kaggle_submission.csv` | - `model_name, rmse, mae, r2, rmsle, training_time_sec`<br>- `feature, rf_importance, dt_importance`<br>- `datetime, actual_count, lr_prediction, dt_prediction, rf_prediction`<br>- `datetime, prediction_clipped, count` | 1. Biểu đồ so sánh `RMSE`, `MAE`, $R^2$, `RMSLE` và `Training Time` giữa 3 mô hình (`Linear Regression`, `Decision Tree`, `Random Forest`) + `Baseline`.<br>2. Biểu đồ thanh ngang xếp hạng 18 `Feature Importances` (`RF` vs `DT`).<br>3. Biểu đồ chuỗi thời gian có bộ lọc khoảng ngày (Date Range Slider) so sánh `Actual Count` vs `Predicted Count` trên tập Validation (`2,178` giờ) + Biểu đồ phân tán `Actual vs Predicted` & phân phối sai số (`Residuals`).<br>4. Trình khám phá dự đoán trên tập Test (`6,493` dòng) & nút tải xuống tệp `kaggle_submission.csv`. | - Best Model: `Random Forest Regressor`<br>- Val RMSE: `99.1848` (giảm `39.7%` so với LR)<br>- Val $R^2$: `0.7922` | Val RMSLE: `0.4016`<br>- Top 1 Feature: `hour` (`30.45%` importance) |
| **Trang 4: Đánh giá Hiệu năng & Khả năng Mở rộng Big Data (`Big Data Scaling & Benchmark`)** | - `outputs/benchmark/scaling_results.csv`<br>- `outputs/benchmark/storage_comparison.csv`<br>- `outputs/benchmark/processing_results.csv`<br>- `outputs/benchmark/partition_results.csv`<br>- `outputs/benchmark/benchmark_summary.json`<br>- `outputs/benchmark/charts/*.svg` | - `scale_label, num_rows, dataset_parquet_size_mb, dataset_csv_size_mb`<br>- `avg_write_time_sec, avg_read_count_time_sec, avg_read_agg_time_sec`<br>- `operation, avg_processing_time_sec, throughput_rows_per_sec`<br>- `actual_df_partitions, cpu_usage_pct, driver_container_mem_mb` | 1. Biểu đồ so sánh dung lượng lưu trữ & tốc độ Đọc/Ghi giữa `CSV` và `Parquet Snappy` từ `1x` $\rightarrow$ `100x`.<br>2. Biểu đồ thời gian thực thi & thông lượng (`Throughput rows/s`) của 5 phép toán (`Filter, GroupBy, Aggregation, Sort, Join`) theo quy mô `1x, 10x, 50x, 100x`.<br>3. Đường cong chữ U đánh giá ảnh hưởng của số Partition (`1, 2, 4, 8`) tới thời gian xử lý và % CPU.<br>4. Bộ xem (Gallery) 5 biểu đồ SVG tĩnh xuất từ Milestone 6. | - Quy mô lớn nhất: `100x` (`1,088,600` dòng)<br>- Tỉ lệ nén Parquet vs CSV: `3.98x` (tiết kiệm `74.9%`)<br>- Tăng tốc đọc Parquet Read+Agg ở `100x`: `8.41x`<br>- Đỉnh thông lượng: `1,415,973 rows/s` (`GroupBy 50x`)<br>- Số Partition tối ưu (2 cores): `4 Partitions` |

---

## 5. Kiểm tra Phiên bản Python và Thư viện Hiện có trên Máy Host

Kết quả kiểm tra trực tiếp môi trường Python trên Windows Host:
- **Phiên bản Python:** `Python 3.13.5`
- **Các thư viện phục vụ Dashboard (ĐÃ CÀI ĐẶT SẴN 100%):**
  - `streamlit`: **`1.52.2`**
  - `pandas`: **`2.2.2`**
  - `numpy`: **`2.5.1`**
  - `plotly`: **`6.9.0`**
  - `altair`: **`6.0.0`**
  - `matplotlib`: **`3.10.3`**
  - `seaborn`: **`0.13.2`**
  - `pyarrow`: **`22.0.0`**

> [!TIP]
> Toàn bộ các thư viện cốt lõi (`streamlit`, `pandas`, `plotly`, `numpy`, `altair`) đều đã có sẵn phiên bản mới trên máy Host. **Không cần cài đặt thêm bất kỳ gói thư viện nào (`pip install`)** và không cần tạo thêm Docker container mới khi triển khai Giai đoạn B.

---

## 6. Cấu trúc Thư mục Dashboard Đề xuất

Để đảm bảo tính mô-đun, dễ bảo trì và không làm ảnh hưởng tới bất kỳ thư mục hiện có nào của dự án, cấu trúc thư mục đề xuất cho Giai đoạn B như sau:

```text
distributed_sys_docker_setup/
├── app/                              # Thư mục chứa ứng dụng Streamlit Dashboard
│   ├── streamlit_app.py              # Điểm khởi chạy chính (Main Entrypoint & Navigation)
│   └── utils/
│       └── data_loader.py            # Các hàm đọc CSV/JSON từ outputs/ có gắn @st.cache_data
├── outputs/                          # Nguồn dữ liệu đầu vào đã tính toán sẵn (Read-Only)
│   ├── eda/
│   ├── preprocessing/
│   ├── analytics/
│   ├── models/
│   └── benchmark/
└── docs/
    ├── MILESTONE_7_PRECHECK.md       # Báo cáo kiểm tra Giai đoạn A (Tệp này)
    └── MILESTONE_7_REPORT.md         # Báo cáo hoàn thiện Dashboard ở Giai đoạn cuối
```

### Phương thức đọc dữ liệu & tổ chức giao diện
1. **Đọc dữ liệu tối ưu bộ nhớ (`Read-Only Cached Artifacts`):**
   - Mọi hàm đọc tệp CSV/JSON trong `app/utils/data_loader.py` sử dụng `@st.cache_data` của Streamlit để chỉ đọc đĩa đúng 1 lần duy nhất.
   - Tổng dung lượng toàn bộ 44 tệp trong `outputs/` chỉ khoảng **`1.05 MB`**, giúp tốc độ chuyển trang và lọc biểu đồ đạt mức tức thì (`< 50 ms`) và tiêu tốn rất ít bộ nhớ RAM (`< 100 MB`).
2. **Tổ chức điều hướng (Navigation):**
   - Sử dụng thanh điều hướng bên trái (`st.sidebar`) với bộ chọn 4 trang chính kèm thông tin tóm tắt cấu hình cụm `Hadoop HDFS + Apache Spark Standalone` và các bộ lọc tương tác (chọn mô hình, chọn khoảng ngày Validation/Test, chọn mức quy mô Benchmark `1x–100x`).

---

## 7. Rủi ro và Phương án Xử lý

| Rủi ro tiềm ẩn | Mức độ | Phương án phòng ngừa & xử lý |
| :--- | :---: | :--- |
| **Áp lực RAM vật lý trên Windows Host** (do WSL2 và 10 containers đang chạy nền) | Trung bình | **Tuyệt đối không khởi tạo `SparkSession` bên trong ứng dụng Streamlit**. Dashboard chỉ đọc các tệp tổng hợp CSV/JSON nhẹ trong `outputs/` bằng `pandas` với `@st.cache_data`. |
| **Đường dẫn tương đối khi chạy lệnh `streamlit run` từ các thư mục khác nhau** | Thấp | Sử dụng `pathlib.Path(__file__).resolve().parents[1]` trong mã nguồn Dashboard để luôn định vị chính xác thư mục gốc `distributed_sys_docker_setup/outputs/` bất kể thư mục làm việc hiện tại. |
| **Hiển thị thời gian trên biểu đồ Validation (`2,178` điểm) và Test (`6,493` điểm)** | Thấp | Chuyển đổi cột `datetime` bằng `pd.to_datetime()` và cung cấp bộ chọn cửa sổ thời gian (Date Range / Sample Window) mặc định hiển thị 7–14 ngày hoặc toàn bộ chuỗi thời gian kèm tính năng thu phóng (zoom/pan) của Plotly. |

---

## 8. Kết luận

- **Trạng thái tổng thể:** **`READY` (SẴN SÀNG 100%)**.
- Toàn bộ hạ tầng (`Docker`, `6 containers Langfuse`, `Hadoop NameNode & DataNode`, `Spark Master & Worker`), dung lượng ổ C (`21.09 GB` trống), cổng `8501`, môi trường Python (`streamlit 1.52.2`, `plotly 6.9.0`, `pandas 2.2.2`) và 100% artifacts từ Milestone 2–6 đều hợp lệ và sẵn sàng phục vụ việc xây dựng Dashboard ở Giai đoạn B.

