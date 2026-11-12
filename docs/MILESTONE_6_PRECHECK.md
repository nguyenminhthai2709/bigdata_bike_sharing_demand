# BÁO CÁO KIỂM TRA TRƯỚC KHI TRIỂN KHAI MILESTONE 6 (PRECHECK)

**Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Milestone:** Milestone 6 – Big Data Scaling & Distributed Performance Benchmark  
**Thời gian kiểm tra:** 2026-11-11  
**Trạng thái:** **ĐẠT ĐIỀU KIỆN AN TOÀN (READY FOR BENCHMARK)**

---

## 1. Kiểm tra Trạng thái Git & Mã nguồn (`git status -s`)

```text
 M hadoop/hadoop.env
?? MILESTONE_2_PRECHECK.md
?? backups/
?? compute/spark/jobs/eda_bike_sharing.py
?? compute/spark/jobs/milestone3_hdfs_test.py
?? compute/spark/jobs/preprocess_bike_sharing.py
?? compute/spark/jobs/spark_sql_analytics.py
?? compute/spark/jobs/train_models.py
?? data/
?? docs/
?? outputs/
```
- Duy nhất tệp `hadoop/hadoop.env` được chỉnh sửa có chủ đích từ Milestone 2 (`HDFS_CONF_dfs_replication=1`).
- Toàn bộ kết quả của Milestone 2, 3, 4, 5 trong `docs/`, `outputs/` và `compute/spark/jobs/` được giữ nguyên vẹn.

---

## 2. Kiểm tra Dung lượng Ổ đĩa & Tài nguyên Docker (CPU / RAM)

### 2.1. Dung lượng ổ C
- **Đã sử dụng (`UsedGB`):** `427.07 GB`
- **Còn trống (`FreeGB`):** **`14.39 GB`** (Vượt xa ngưỡng an toàn tối thiểu `5.00 GB`).
- **Ước tính dung lượng dữ liệu Benchmark sẽ tạo (1x, 10x, 50x, 100x):**
  - Tổng số bản ghi tối đa ở quy mô `100x` là `1,088,600` dòng (`20` cột).
  - Tổng dung lượng lưu trữ cả định dạng Parquet (nén Snappy) và CSV trên HDFS cho cả 4 mức quy mô (`1x`, `10x`, `50x`, `100x`) ước tính khoảng **`220 MB – 280 MB`** (`< 0.3 GB`), hoàn toàn an toàn cho ổ C.

### 2.2. Trạng thái Container, CPU và RAM (`docker stats --no-stream`)
| Container | CPU % | RAM Sử dụng / Giới hạn | RAM % | Vai trò |
| :--- | :---: | :---: | :---: | :--- |
| `spark-worker-1` | `0.15%` | `286.5 MiB / 7.597 GiB` | `3.68%` | Spark Standalone Worker (`2 cores`, `2 GiB` cấp phát) |
| `spark-master` | `0.15%` | `378.1 MiB / 7.597 GiB` | `4.86%` | Spark Standalone Master |
| `hadoop-datanode` | `0.27%` | `353.2 MiB / 7.597 GiB` | `4.54%` | HDFS DataNode (`slave01`) |
| `hadoop-namenode` | `5.63%` | `431.4 MiB / 7.597 GiB` | `5.55%` | HDFS NameNode |
| `6x langfuse-local-*` | `~23.8%` | `~3.02 GiB / 7.597 GiB` | `~39.3%` | Hệ thống Langfuse chạy song song (giữ nguyên) |

- **Tổng RAM đang sử dụng trên Docker WSL2:** `~4.47 GiB / 7.597 GiB` (**còn trống `~3.13 GiB` RAM**), đảm bảo đủ bộ nhớ cho Spark Executor (`1 GiB`) thực thi tuần tự các bài đo Benchmark.

---

## 3. Kiểm tra Dung lượng HDFS Hiện tại & Thư mục Benchmark

Kết quả `hdfs dfs -du -h /bike-sharing`:
| Đường dẫn HDFS | Dung lượng Logic | Dung lượng Kèm Replica | Trạng thái |
| :--- | :---: | :---: | :--- |
| `/bike-sharing/raw` | `1.1 M` | `1.1 M` | Dữ liệu gốc Milestone 2 (Giữ nguyên) |
| `/bike-sharing/milestone3_test` | `117.7 K` | `353.1 K` | Kết quả kiểm thử Milestone 3 (Giữ nguyên) |
| `/bike-sharing/processed` | `217.4 K` | `652.3 K` | Cleaned Analytical Dataset Milestone 4 (Giữ nguyên) |
| `/bike-sharing/features` | `187.7 K` | `563.2 K` | Model Feature Dataset Milestone 4 (Giữ nguyên) |
| `/bike-sharing/analytics` | `24.5 K` | `73.5 K` | Kết quả 8 truy vấn Spark SQL Milestone 4 (Giữ nguyên) |
| `/bike-sharing/models` | `1021.4 K` | `3.0 M` | 3 PipelineModels + Best Model Milestone 5 (Giữ nguyên) |
| `/bike-sharing/predictions` | `259.1 K` | `777.3 K` | Dự đoán Test & Kaggle Submission Milestone 5 (Giữ nguyên) |
| `/bike-sharing/benchmark` | `0` | `0` | Thư mục rỗng, sẵn sàng lưu dữ liệu Benchmark Milestone 6 |

- Thư mục `outputs/benchmark/` trên máy Host chưa tồn tại (`False`), sẽ được tạo mới để lưu các tệp CSV, JSON và biểu đồ tổng hợp.
- Trong các kịch bản PySpark của Milestone 6, cấu hình `.config("spark.hadoop.dfs.replication", "1")` sẽ được thiết lập tường minh để đồng bộ hệ số nhân bản `replication = 1` với `hadoop-datanode`.

