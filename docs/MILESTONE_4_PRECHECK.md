# Báo cáo Kiểm tra Trước khi Triển khai Milestone 4 (MILESTONE 4 PRECHECK)

**Đề tài:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Milestone:** Milestone 4 – Data Exploration, Preprocessing & Spark SQL Analytics  
**Thời gian kiểm tra:** 2026-11-11  

---

## 1. Kết quả Kiểm tra Hệ thống & Dữ liệu (Giai đoạn A)

1. **Đối chiếu báo cáo Milestone 2 & Milestone 3:**
   - Đã đọc và xác nhận toàn bộ thông tin từ [docs/MILESTONE_2_REPORT.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_2_REPORT.md) và [docs/MILESTONE_3_REPORT.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_3_REPORT.md).
2. **Trạng thái `git status` & `compute/spark/jobs/`:**
   - Tệp đã chỉnh sửa duy nhất từ Milestone 2: `hadoop/hadoop.env` (`HDFS_CONF_dfs_replication=1`).
   - Các tệp trong `compute/spark/jobs/`:
     - `example_job.py` (`1,058 B`)
     - `hello_hdfs.py` (`679 B`)
     - `milestone3_hdfs_test.py` (`5,356 B`)
3. **Trạng thái Cụm Container (`docker compose ps` & `http://localhost:8080/json/`):**
   - `hadoop-namenode`: `Up 4 hours (healthy)`
   - `hadoop-datanode`: `Up 4 hours (healthy)`
   - `spark-master`: `Up 3 hours` (`status: ALIVE`)
   - `spark-worker-1`: `Up 3 hours` (`aliveworkers: 1`, `2 Cores`, `2048 MB RAM`)
   - 6 containers của Langfuse vẫn hoạt động bình thường.
4. **Trạng thái Dữ liệu trên HDFS (`/bike-sharing`):**
   - `/bike-sharing/raw/train.csv`: `633.2 KB` (`10,886 dòng`, `12 cột`)
   - `/bike-sharing/raw/test.csv`: `316.3 KB` (`6,493 dòng`, `9 cột`)
   - `/bike-sharing/raw/sampleSubmission.csv`: `139.5 KB` (`6,493 dòng`, `2 cột`)
   - `/bike-sharing/milestone3_test/train_clean_parquet`: Nguyên vẹn.
5. **Dung lượng ổ C hiện tại:** `FreeGB = 8.15 GB` (Không pull/build thêm image hay tạo thêm container).

---

## 2. Kế hoạch Triển khai Chi tiết (Giai đoạn B $\rightarrow$ G)

1. **Giai đoạn B – Exploratory Data Analysis (`compute/spark/jobs/eda_bike_sharing.py`):**
   - Đọc `/bike-sharing/raw/train.csv` từ HDFS bằng PySpark.
   - Kiểm tra kích thước, schema, null/missing values, duplicate rows, descriptive statistics (`min`, `max`, `mean`, `stddev`, `25%`, `50%/median`, `75%`).
   - Phân tích phân phối biến mục tiêu `count` (cùng `casual`, `registered` cho mục đích hiểu hành vi).
   - Phân tích nhu cầu theo thời gian (`hour`, `dayofweek`, `month`, `year`) và theo điều kiện (`season`, `weather`, `workingday`, `holiday`, nhóm nhiệt độ `temp`, độ ẩm `humidity`, tốc độ gió `windspeed`).
   - Xuất các bảng tổng hợp ra `/opt/spark-jobs/outputs_staging/eda/` và đồng bộ về `outputs/eda/` trên máy host.
2. **Giai đoạn C & D – Data Preprocessing & Feature Engineering (`compute/spark/jobs/preprocess_bike_sharing.py`):**
   - Ép kiểu chuẩn xác (`TimestampType` cho `datetime`, `IntegerType`/`DoubleType` cho các biến số).
   - Kiểm tra null, duplicate, và các quy tắc nghiệp vụ (`count == casual + registered`, `count >= 0`, `0 <= hour <= 23`, miền giá trị `season`, `weather`, `humidity`, `windspeed`).
   - Trích xuất đặc trưng thời gian: `year`, `month`, `day`, `hour`, `dayofweek`, `is_weekend`.
   - Trích xuất đặc trưng chu kỳ lượng giác: `hour_sin`, `hour_cos`, `month_sin`, `month_cos`.
   - Tạo 2 bộ dữ liệu chuẩn và lưu Parquet (nén `snappy`, `coalesce(1)` để tránh sinh nhiều file nhỏ) lên HDFS:
     - **Cleaned Analytical Dataset:** `/bike-sharing/processed/train/` (`10,886 dòng`, giữ `casual`, `registered`, `count` phục vụ phân tích) và `/bike-sharing/processed/test/` (`6,493 dòng`).
     - **Model Feature Dataset (Chống Data Leakage):** `/bike-sharing/features/train/` (`10,886 dòng`, **loại bỏ hoàn toàn `casual` và `registered`**, giữ `count` làm nhãn mục tiêu) và `/bike-sharing/features/test/` (`6,493 dòng`, không có `casual`, `registered`, `count`).
3. **Giai đoạn E – Spark SQL Analytics (`compute/spark/jobs/spark_sql_analytics.py`):**
   - Đọc dữ liệu đã làm sạch từ `/bike-sharing/processed/train/`, đăng ký Temporary View và chạy 8 truy vấn Spark SQL chuẩn.
   - Lưu kết quả dưới dạng Parquet tại `/bike-sharing/analytics/<ten_bang>/` trên HDFS và xuất bản CSV/JSON ra `outputs/analytics/` trên máy host để phục vụ Dashboard ở Milestone 7.
4. **Giai đoạn F & G – Kiểm chứng Parquet trên HDFS & Tạo Báo cáo:**
   - Kiểm tra lại bằng PySpark và HDFS CLI (`hdfs dfs -ls -h`, số dòng, schema, dung lượng nén).
   - Tổng hợp toàn bộ số liệu thực tế vào `docs/MILESTONE_4_REPORT.md` và `docs/DATA_DICTIONARY.md`.

