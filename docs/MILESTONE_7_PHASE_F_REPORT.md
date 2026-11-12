# BÁO CÁO TRIỂN KHAI MILESTONE 7 — PHASE F: BIG DATA BENCHMARK DASHBOARD

- **Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark (`Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark`)
- **Giai đoạn:** Milestone 7 — Phase F (Implement Big Data Benchmark Dashboard)
- **Tệp triển khai chính:** `dashboard/pages/4_Big_Data_Benchmark.py` và `dashboard/data_loader.py` (`get_benchmark_artifacts()`)
- **Trạng thái:** **HOÀN THÀNH & ĐÃ KIỂM THỬ (10/10 HẠNG MỤC ĐẠT 100%)**

---

## 1. Tuân thủ Nguyên tắc Thiết kế & Toàn vẹn Dữ liệu

1. **100% Dữ liệu thực từ Artifacts Milestone 6 (`outputs/benchmark/`):**
   - Không khởi tạo `SparkSession`, không truy vấn HDFS trực tiếp từ giao diện Streamlit, không chạy lại benchmark và không sinh thêm dữ liệu.
   - Không tạo dữ liệu giả (bao gồm không tự tạo biểu đồ chuỗi thời gian CPU/RAM giả); toàn bộ bảng, KPI và biểu đồ đều đọc từ 5 tệp kết quả đã tạo ở Milestone 6 thông qua `@st.cache_data` trong `dashboard/data_loader.py`.
2. **Bảo toàn tuyệt đối các giai đoạn trước:**
   - Không sửa đổi bất kỳ tệp kết quả nào của Milestone 2–6.
   - Ba trang `1_Overview.py`, `2_Data_Analysis.py` và `3_Machine_Learning.py` tiếp tục hoạt động ổn định (`0 exceptions`).

---

## 2. Kiểm tra Schema Thực tế của 5 Artifacts Milestone 6

| STT | Tệp dữ liệu (`outputs/benchmark/`) | Số dòng | Số cột / Keys | Cột / Cấu trúc thực tế & Đơn vị |
| :---: | :--- | :---: | :---: | :--- |
| 1 | `scaling_results.csv` | `4` | `28` | `scale_label (1x, 10x, 50x, 100x), scale_factor (1, 10, 50, 100), num_rows (10886..1088600), num_columns (20), num_partitions (1, 1, 2, 2), dataset_parquet_size_mb, dataset_csv_size_mb, csv_to_parquet_size_ratio, parquet_write_time_sec, csv_write_time_sec, parquet_read_count_time_sec, csv_read_count_time_sec, parquet_read_agg_time_sec, csv_read_agg_time_sec, filter_time_sec, groupby_time_sec, aggregation_time_sec, sort_time_sec, join_time_sec, total_5ops_processing_time_sec, avg_op_processing_time_sec, avg_processing_throughput_rows_per_sec, filter_throughput_rows_per_sec, aggregation_throughput_rows_per_sec, sort_throughput_rows_per_sec, join_throughput_rows_per_sec, cpu_usage_pct, memory_usage_mb` |
| 2 | `storage_comparison.csv` | `8` | `23` | `scale_label, scale_factor, num_rows, num_columns, format (CSV/PARQUET), compression (none/snappy), num_runs (3), cache_regime (Spark-Uncached / Warm OS Page Cache), storage_size_bytes, storage_size_mb, run1_write_time_sec, avg_write_time_sec, std_write_time_sec, run1_read_count_time_sec, avg_read_count_time_sec, std_read_count_time_sec, run1_read_agg_time_sec, avg_read_agg_time_sec, std_read_agg_time_sec, write_throughput_rows_per_sec, read_count_throughput_rows_per_sec, read_agg_throughput_rows_per_sec, hdfs_path` |
| 3 | `processing_results.csv` | `20` | `19` | `scale_label, scale_factor, operation (Filter, GroupBy, Aggregation, Sort, Join), input_rows, output_rows, dataset_size_mb, num_partitions, num_runs (3), run1_time_sec, run2_time_sec, run3_time_sec, avg_processing_time_sec, std_processing_time_sec, min_processing_time_sec, max_processing_time_sec, throughput_rows_per_sec, cpu_usage_pct, driver_container_mem_mb, error` |
| 4 | `partition_results.csv` | `12` | `19` | `scale_label (10x, 50x, 100x), scale_factor, num_rows, dataset_size_mb, hdfs_file_count, spark_input_partitions, target_partitions (1, 2, 4, 8), actual_df_partitions, repartition_cache_time_sec, num_runs (3), run1_time_sec, run2_time_sec, run3_time_sec, avg_processing_time_sec, std_processing_time_sec, throughput_rows_per_sec, output_rows, cpu_usage_pct, driver_container_mem_mb` |
| 5 | `benchmark_summary.json` | JSON | `8` keys | `project, milestone, environment, scaling_results (4), storage_comparison (8), processing_results (20), partition_results (12), created_at_utc` |

---

## 3. Chi tiết các Hợp phần đã Triển khai trên `dashboard/pages/4_Big_Data_Benchmark.py`

### A. Section 1 — Benchmark Overview (6 KPI Cards)
- Đối chiếu chính xác với báo cáo Milestone 6 (`docs/MILESTONE_6_REPORT.md`):
  1. `Maximum Dataset Size`: **`1,088,600` rows** (`20` cột đặc trưng).
  2. `Maximum Scale`: **`100x`** (mở rộng từ `1x = 10,886` dòng).
  3. `CSV Size at 100x`: **`110.40 MB`** (`110.4020 MB`, `115,764,852` bytes).
  4. `Parquet Size at 100x`: **`27.73 MB`** (`27.7273 MB`, `29,074,221` bytes, nén Snappy).
  5. `Storage Reduction`: **`74.89%`** (Tỷ lệ nén `3.98x`).
  6. `Read + Aggregation Speedup`: **`8.41x`** (`2.5600 s` trên Parquet so với `21.5240 s` trên CSV ở `100x`; kèm thông tin `Read + Count Speedup = 7.05x`).
- Hiển thị ghi chú học thuật làm rõ rằng tập gốc có `10,886` dòng (`1x`), còn các quy mô `10x–100x` là dữ liệu benchmark tổng hợp có kiểm soát (controlled synthetic scaling).

### B. Section 2 — Data Scaling Analysis (`scaling_results.csv`)
- Bộ lọc chọn quy mô (`1x`, `10x`, `50x`, `100x`) và chế độ hiển thị thời gian xử lý (`total_5ops_processing_time_sec` vs `avg_op_processing_time_sec`).
- **3 Biểu đồ tương tác:**
  1. **Dataset Scale vs Processing Time (`s`):** Phân biệt rõ tổng thời gian 5 phép toán (`1x: 16.39s`, `10x: 3.50s`, `50x: 8.54s`, `100x: 28.74s`) và thời gian trung bình 1 phép toán (`1x: 3.28s`, `10x: 0.70s`, `50x: 1.71s`, `100x: 5.75s`).
  2. **Dataset Scale vs Throughput (`rows/sec`):** Hiển thị thông lượng trung bình của 5 phép toán (`1x: 5,355`, `10x: 176,765`, `50x: 747,565`, `100x: 440,647` dòng/giây).
  3. **Dataset Scale vs Storage Size (`MB`):** Đối chiếu dung lượng CSV và Parquet qua 4 quy mô kèm tỷ lệ nén `5.87x`, `3.90x`, `3.97x`, `3.98x`.
- Ghi chú rõ công thức tính thông lượng ($\text{Throughput}_i = \frac{\text{num\_rows}}{\text{avg\_processing\_time\_sec}_i}$) và lý giải khách quan hiện tượng khởi động JVM/Catalyst ở `1x` cũng như áp lực Shuffle/GC ở `100x`.

### C. Section 3 — CSV vs Parquet Storage (`storage_comparison.csv`)
- Bộ lọc chọn quy mô (`1x`, `10x`, `50x`, `100x`) và thao tác I/O trọng tâm; hiển thị 4 KPI động cập nhật theo quy mô được chọn.
- **4 Biểu đồ cột nhóm Plotly (trung bình 3 lần chạy):**
  1. **CSV vs Parquet Storage Size (`MB`)**.
  2. **CSV vs Parquet Write Time (`avg_write_time_sec`)**.
  3. **CSV vs Parquet Read + Count Time (`avg_read_count_time_sec`)**.
  4. **CSV vs Parquet Read + Aggregation Time (`avg_read_agg_time_sec`)**.
- Ghi chú phương pháp rõ ràng: **`Spark-Uncached / Warm OS Page Cache`** (`spark.catalog.clearCache()` trước mỗi lần đọc, `num_runs = 3`, không gọi đây là Cold Cache Benchmark).

### D. Section 4 — Spark Processing Performance (`processing_results.csv`)
- Bộ lọc theo quy mô (`1x, 10x, 50x, 100x`), phép xử lý (`Filter, GroupBy, Aggregation, Sort, Join`) và quy mô trọng tâm cho biểu đồ so sánh 5 phép toán.
- **4 Biểu đồ tương tác:**
  1. **Processing Time by Operation (`avg_processing_time_sec`)**.
  2. **Processing Time by Dataset Scale (`1x → 100x`)**.
  3. **Throughput by Dataset Scale (`throughput_rows_per_sec`)**.
  4. **So sánh 5 phép xử lý tại quy mô được chọn** (hiển thị cả `run1`, `run2`, `run3`, `CPU (%)` và `Driver RAM (MB)` trong tooltip).

### E. Section 5 — Partition Optimization (`partition_results.csv`)
- Bộ lọc theo quy mô (`10x, 50x, 100x`) và số lượng partition (`1, 2, 4, 8`).
- **3 Biểu đồ tương tác:**
  1. **Number of Partitions vs Processing Time (`avg_processing_time_sec`)**.
  2. **Number of Partitions vs Throughput (`throughput_rows_per_sec`)**.
  3. **CPU Usage by Partition (`cpu_usage_pct`)**.
- Phân tích khách quan kết quả:
  - Ở `10x` (`0.5280 s`, `206,174 rows/s`) và `50x` (`0.9043 s`, `601,902 rows/s`), **4 partitions** đạt kết quả nhanh nhất.
  - Ở `100x`, **1 partition** đạt thời gian thực thi thuần thấp nhất sau khi cache (`0.9749 s`) do tránh được shuffle liên phân vùng, trong khi **4 partitions** (`1.3309 s`, `817,943 rows/s`) dẫn đầu nhóm đa phân vùng.
  - **8 partitions** chậm hơn 4 partitions ở cả 3 quy mô (`1.8893 s` ở `10x`, `3.2569 s` ở `50x`, `1.5735 s` ở `100x`) do Worker chỉ có **2 CPU cores**, làm tăng chi phí lập lịch task và shuffle.

### F. Section 6 — Resource Utilization
- Sử dụng đúng các cột thực tế có trong artifacts (`cpu_usage_pct`, `driver_container_mem_mb`, `memory_usage_mb`, `avg_processing_time_sec`, `throughput_rows_per_sec`):
  1. **Scatter Plot:** Tương quan giữa `CPU Usage (%)` và `Average Processing Time (s)` (kích thước điểm tỷ lệ với `Throughput`).
  2. **Biểu đồ kết hợp:** Mức sử dụng bộ nhớ container (`memory_usage_mb`: `1025.86 MB` → `1130.77 MB`) và `CPU Usage (%)` trung bình theo quy mô.

### G. Section 7 — Experimental Environment & Methodology
- Hiển thị đầy đủ cấu hình thực nghiệm từ `benchmark_summary.json["environment"]`:
  - `Single-Machine Containerized Spark Standalone` (`1 Spark Master` + `1 Spark Worker`).
  - `2 CPU Cores`, `Worker RAM: 2048 MB`, `Executor RAM: 1024 MiB`, `HDFS replication = 1`, `Spark 3.5.6`, `3 lần chạy / cấu hình`.
  - Nêu rõ các giới hạn thực nghiệm học thuật (môi trường một máy chủ vật lý, dữ liệu mở rộng tổng hợp có kiểm soát, ảnh hưởng của JVM warm-up, GC pause và OS Page Cache).

---

## 4. Kết quả Kiểm thử Giai đoạn F (10/10 Hạng mục)

| STT | Hạng mục kiểm thử | Kết quả | Chi tiết xác minh |
| :---: | :--- | :---: | :--- |
| 1 | Đọc đủ 5 artifacts trong `outputs/benchmark/` | **PASS** | `get_benchmark_artifacts()` nạp và kiểm định thành công cả 5 tệp |
| 2 | Các KPI khớp báo cáo Milestone 6 | **PASS** | `1,088,600 rows`, `100x`, `110.40 MB`, `27.73 MB`, `74.89%`, `8.41x` |
| 3 | Storage comparison chính xác | **PASS** | Khớp 100% dung lượng, thời gian Write, Read+Count, Read+Aggregation ở 4 quy mô |
| 4 | Processing time và throughput đúng đơn vị | **PASS** | Thời gian tính bằng giây (`s`), thông lượng tính bằng dòng/giây (`rows/sec`) |
| 5 | Partition benchmark khớp kết quả nguồn | **PASS** | Khớp toàn bộ 12 cấu hình (`10x, 50x, 100x` × `1, 2, 4, 8 partitions`) |
| 6 | Bộ lọc tương tác hoạt động ổn định | **PASS** | Kiểm thử multiselect, radio, selectbox trên trang Benchmark qua `AppTest`: `0 exceptions` |
| 7 | Không phát sinh dữ liệu giả | **PASS** | 100% số liệu truy xuất từ CSV/JSON trong `outputs/benchmark/` |
| 8 | `AppTest` cả 4 trang không có exception | **PASS** | `1_Overview`, `2_Data_Analysis`, `3_Machine_Learning`, `4_Big_Data_Benchmark` đều đạt `0 exceptions` |
| 9 | Ba trang trước vẫn hoạt động bình thường | **PASS** | Đã kiểm chứng chuyển trang tuần tự qua `AppTest` |
| 10 | Streamlit trả `HTTP 200` và Health Check `ok` trên cổng `8501` | **PASS** | `http://127.0.0.1:8501` trả `HTTP 200`, `/_stcore/health` trả `ok` (đã dừng server test sạch sẽ) |

