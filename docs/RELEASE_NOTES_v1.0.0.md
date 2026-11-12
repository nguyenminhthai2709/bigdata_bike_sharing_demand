# RELEASE NOTES — v1.0.0

- **Tên dự án (Tiếng Anh):** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark
- **Tên dự án (Tiếng Việt):** Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark
- **Phiên bản phát hành:** `v1.0.0` (Release Candidate — Sẵn sàng trình diễn & nghiệm thu)
- **Ngày phát hành:** `2026-11-12`
- **Repository:** `C:\Users\BOOK PRO\distributed_sys_docker_setup`

---

## 1. Mục tiêu Dự án

Xây dựng một hệ thống phân tích dữ liệu lớn và dự báo nhu cầu sử dụng xe đạp chia sẻ theo giờ đầu-cuối (End-to-End Big Data Analytics & Forecasting Pipeline) dựa trên:
- Hệ thống lưu trữ phân tán **Apache Hadoop HDFS**.
- Động cơ xử lý phân tán **Apache Spark** (**Spark SQL**, **DataFrame API**, **Spark MLlib**).
- Đánh giá khả năng mở rộng quy mô dữ liệu lên tới **1,088,600 dòng (`100x`)**.
- Ứng dụng trực quan hóa tương tác **Streamlit Dashboard** gồm 4 trang chuyên sâu.

---

## 2. Các Thành phần Đã Hoàn thiện (Milestone 2 → Milestone 7)

### 2.1. Hạ tầng Lưu trữ Phân tán — Apache Hadoop HDFS (Milestone 2)
- Triển khai `hadoop-namenode` và `hadoop-datanode` (Apache Hadoop `3.2.1`, `dfs.replication = 1`) hoạt động ổn định (`HEALTHY`).
- Lưu trữ tập dữ liệu gốc tại `/bike-sharing/raw/` (`train.csv`: `10,886` dòng; `test.csv`: `6,493` dòng; `sampleSubmission.csv`: `6,493` dòng), tập đặc trưng Parquet tại `/bike-sharing/features/`, các mô hình MLlib tại `/bike-sharing/models/` và dữ liệu benchmark tại `/bike-sharing/benchmark/`.

### 2.2. Hạ tầng Tính toán Phân tán — Apache Spark Standalone (Milestone 3)
- Triển khai `spark-master` và `spark-worker-1` (Apache Spark `3.5.6`, `2 CPU Cores`, `2048 MB Worker RAM`, `1024 MiB Executor RAM`) kết nối trực tiếp với HDFS qua RPC `hdfs://hadoop-namenode:9000`.

### 2.3. Khám phá Dữ liệu (EDA), Tiền xử lý & Spark SQL Analytics (Milestone 4)
- Kiểm định chất lượng dữ liệu: `0` giá trị khuyết (`Null`), `0` bản ghi trùng lặp dấu thời gian, tổng cộng **`2,085,476` lượt thuê** trong tập huấn luyện (`10,886` giờ quan sát), trung bình **`191.57` lượt/giờ** (`191.5741`).
- Thực thi **8 truy vấn phân tích Spark SQL** (`q1` → `q8`): xác định giờ cao điểm nhất là **`17:00` (`468.77` lượt/giờ)**, mùa cao điểm nhất là **Mùa Thu (`Fall = 234.42` lượt/giờ)**, tỷ trọng người dùng đăng ký vào ngày làm việc đạt **`84.82%`**, và mức tăng trưởng trung bình giờ từ năm 2011 sang 2012 đạt **`+65.41%`** (`144.22` → `238.56` lượt/giờ; tổng lượt thuê tăng `+66.69%`).
- Trích xuất **18 đặc trưng đầu vào** (bao gồm mã hóa chu kỳ lượng giác `hour_sin, hour_cos, month_sin, month_cos`) và loại bỏ hoàn toàn `casual`, `registered` để chống rò rỉ dữ liệu (`Data Leakage`).

### 2.4. Học máy Phân tán — Apache Spark MLlib (Milestone 5)
- Áp dụng phương pháp chia dữ liệu theo trình tự thời gian **Time-based Split (80/20)**:
  - **Train (`80%`):** `8,708` bản ghi (`2011-01-01 00:00:00` → `2012-08-05 04:00:00`).
  - **Validation (`20%`):** `2,178` bản ghi (`2012-08-05 05:00:00` → `2012-12-19 23:00:00`).
- Huấn luyện và đánh giá 3 thuật toán hồi quy (`Linear Regression`, `Decision Tree Regressor`, `Random Forest Regressor`) đối chiếu với `Baseline Mean Predictor`.
- Tạo dự báo cho **`6,493` bản ghi tập Test** (`outputs/models/test_predictions.csv`) và xuất tệp nộp bài chuẩn Kaggle (`outputs/models/kaggle_submission.csv`, `6,493` dòng, `0` giá trị null).

### 2.5. Mở rộng Dữ liệu Quy mô Lớn & Benchmark Hiệu năng (Milestone 6)
- Mở rộng dữ liệu có kiểm soát từ `1x` (`10,886` dòng) lên `10x` (`108,860` dòng), `50x` (`544,300` dòng) và `100x` (`1,088,600` dòng).
- Đo kiểm lưu trữ `CSV vs Parquet Snappy`, hiệu năng 5 phép toán xử lý (`Filter, GroupBy, Aggregation, Sort, Join`) và tối ưu hóa số lượng phân vùng (`1, 2, 4, 8 partitions`) với 3 lần chạy lặp lại cho mỗi cấu hình.

### 2.6. Ứng dụng Trực quan hóa — Streamlit Dashboard (Milestone 7)
- Xây dựng ứng dụng đa trang (`dashboard/app.py`) sử dụng `Streamlit 1.52.2`, `Plotly 6.9.0` và `Pandas 2.2.2`, đọc trực tiếp từ 37 tệp kết quả trong `outputs/` với bộ nhớ đệm `@st.cache_data`.

---

## 3. Mô hình Tốt nhất & Các Chỉ số Đánh giá (Milestone 5)

- **Mô hình chiến thắng (Best Model):** **`Random Forest Regressor`**
  - Siêu tham số: `numTrees = 30`, `maxDepth = 8`, `minInstancesPerNode = 5`, `featureSubsetStrategy = "auto"`, `subsamplingRate = 0.8`, `seed = 42`.
  - Thời gian huấn luyện trên Spark: `34.3088 giây`.

| Mô hình | Validation RMSE ↓ | Validation MAE ↓ | Validation R² ↑ | Validation RMSLE ↓ | Thời gian huấn luyện (s) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| *Baseline Mean Predictor (`174.3912`)* | *233.9322* | *176.5815* | *-0.1558* | *—* | *—* |
| **Linear Regression** | `164.4157` | `121.8134` | `0.4291` | `1.0940` | `10.5608` |
| **Decision Tree Regressor** | `100.7967` | **`65.1976`** | `0.7854` | `0.4382` | `18.7804` |
| **Random Forest Regressor (Best)** | **`99.1848`** | `66.3189` | **`0.7922`** | **`0.4016`** | `34.3088` |

> **Ghi chú quy ước tính độ đo:** `RMSE`, `MAE`, `R²` được tính trên dự đoán gốc (`prediction`); `RMSLE` được tính trên dự đoán đã cắt ngưỡng không âm (`prediction_clipped = max(prediction, 0.0)`).

- **Top 5 đặc trưng quan trọng nhất (`Random Forest Feature Importance`):**
  1. `hour` (`30.45%`)
  2. `hour_sin` (`18.02%`)
  3. `hour_cos` (`11.15%`)
  4. `atemp` (`8.96%`)
  5. `year` (`7.70%`)

---

## 4. Kết quả Benchmark Quy mô 100x (Milestone 6)

- **Quy mô tối đa (`100x`):** **`1,088,600` dòng** × `20` cột.
- **Dung lượng lưu trữ CSV (không nén):** **`110.40 MB`** (`110.4020 MB` / `115,764,852` bytes).
- **Dung lượng lưu trữ Parquet (nén Snappy):** **`27.73 MB`** (`27.7273 MB` / `29,074,221` bytes).
- **Tỷ lệ tiết kiệm dung lượng HDFS:** **`74.89%`** (Tỷ lệ nén **`3.98x`**).
- **Tăng tốc truy vấn tại `100x` (chế độ `Spark-Uncached / Warm OS Page Cache`, trung bình 3 lần chạy):**
  - `Read + Count`: Parquet đạt **`2.0197s`** so với CSV **`14.2397s`** (nhanh hơn **`7.05x`**).
  - `Read + Aggregation`: Parquet đạt **`2.5600s`** so với CSV **`21.5240s`** (nhanh hơn **`8.41x`**).
- **Tối ưu hóa Partition (`1, 2, 4, 8 partitions` trên Worker `2 CPU cores`):**
  - Cấu hình **`4 partitions`** đạt thời gian nhanh nhất ở `10x` (**`0.5280s`**, `206,174 rows/s`) và `50x` (**`0.9043s`**, `601,902 rows/s`), đồng thời dẫn đầu nhóm đa phân vùng ở `100x` (**`1.3309s`**, `817,943 rows/s`), trong khi `8 partitions` chậm hơn do chi phí lập lịch task và shuffle.

---

## 5. Bốn Trang Chức năng của Streamlit Dashboard

1. **`dashboard/pages/1_Overview.py` (Overview):** Tổng quan đề tài, Dataset Overview, 6 thẻ KPI cốt lõi, Sơ đồ kiến trúc hệ thống 8 bước và Tiến độ hoàn thành 6 Milestone.
2. **`dashboard/pages/2_Data_Analysis.py` (Data Analysis):** 5 thẻ KPI phân tích, bộ lọc tương tác (`Hour, Working Day, Season, Weather`) và 11 biểu đồ Plotly tương tác thể hiện kết quả EDA & 8 truy vấn Spark SQL (`q1` → `q8`).
3. **`dashboard/pages/3_Machine_Learning.py` (Machine Learning):** So sánh 3 mô hình hồi quy MLlib, cấu hình `Random Forest`, biểu đồ `Feature Importance` (18 đặc trưng), đối chiếu `Actual vs Predicted`, phân tích phần dư `Residual Analysis`, bộ khám phá dự báo tập Test (`6,493` dòng) và nút tải xuống tệp `bike_sharing_kaggle_submission.csv`.
4. **`dashboard/pages/4_Big_Data_Benchmark.py` (Big Data Benchmark):** 6 thẻ KPI ở `100x`, phân tích mở rộng `1x–100x`, đối chiếu `CSV vs Parquet`, đánh giá 5 phép toán Spark, tối ưu hóa `1, 2, 4, 8 Partitions`, phân tích `CPU & Memory Usage` và thuyết minh phương pháp thực nghiệm.

---

## 6. Các Giới hạn Kỹ thuật của Hệ thống

1. **Môi trường thực nghiệm một máy chủ vật lý:** Cụm Hadoop HDFS (`1 NameNode + 1 DataNode`, `replication = 1`) và Spark Standalone (`1 Master + 1 Worker`, `2 CPU cores`, `2048 MB Worker RAM`, `1024 MiB Executor`) chạy dưới dạng các container trên cùng một máy chủ Windows 11 (RAM 16 GB), không phải cụm nhiều máy chủ vật lý độc lập.
2. **Bản chất dữ liệu mở rộng 10x–100x:** Tập dữ liệu gốc có `10,886` dòng (`1x`); các bộ dữ liệu `10x` (`108,860`), `50x` (`544,300`) và `100x` (`1,088,600`) là dữ liệu benchmark tổng hợp có kiểm soát (controlled synthetic scaling) phục vụ đo kiểm kỹ thuật.
3. **Không tính độ đo sai số trên tập Test:** Tập `test.csv` (`6,493` dòng) của Kaggle không có nhãn thực tế `count`, do đó toàn bộ các độ đo `RMSE, MAE, R², RMSLE` đều được đánh giá minh bạch trên tập Validation (`2,178` dòng cuối theo Time-based Split).

---

## 7. Hướng dẫn Chuẩn bị Dữ liệu Gốc & Khởi chạy Ứng dụng Dashboard

### 7.1. Cách chuẩn bị dữ liệu gốc Kaggle (`data/raw/`)
Thư mục `data/raw/` không được đưa vào Git commit mặc định để tuân thủ quy định phân phối dữ liệu của Kaggle. Để tái tạo lại đường ống HDFS/Spark từ đầu (nếu cần), tải 3 tệp từ cuộc thi **Kaggle Bike Sharing Demand** (`https://www.kaggle.com/c/bike-sharing-demand/data`) và đặt vào thư mục `data/raw/`:
- `data/raw/train.csv` (`10,886` dòng)
- `data/raw/test.csv` (`6,493` dòng)
- `data/raw/sampleSubmission.csv` (`6,493` dòng)

*(Lưu ý: Ứng dụng Streamlit Dashboard đọc trực tiếp từ 44 tệp kết quả đã được đóng gói sẵn trong `outputs/`, nên không yêu cầu phải tải lại `data/raw/` khi chạy trình diễn Dashboard).*

### 7.2. Cách khởi chạy Streamlit Dashboard
Từ thư mục gốc của dự án (`C:\Users\BOOK PRO\distributed_sys_docker_setup`):

```powershell
streamlit run dashboard/app.py --server.port 8501
```

- **Truy cập trên trình duyệt:** `http://localhost:8501`
- **Kiểm tra trạng thái (Health Check):** `http://localhost:8501/_stcore/health`

---

## 8. Danh mục Tài liệu Bàn giao Liên quan

- `docs/MILESTONE_2_REPORT.md` — Báo cáo triển khai HDFS & tải bộ dữ liệu gốc.
- `docs/MILESTONE_3_REPORT.md` — Báo cáo triển khai Apache Spark & tích hợp HDFS.
- `docs/MILESTONE_4_REPORT.md` & `docs/DATA_DICTIONARY.md` — Báo cáo EDA, Tiền xử lý, Feature Engineering, Spark SQL & Từ điển dữ liệu.
- `docs/MILESTONE_5_REPORT.md` — Báo cáo huấn luyện & đánh giá mô hình Spark MLlib.
- `docs/MILESTONE_6_PRECHECK.md` & `docs/MILESTONE_6_REPORT.md` — Báo cáo mở rộng dữ liệu `1x–100x` & Benchmark hiệu năng phân tán.
- `docs/MILESTONE_7_PRECHECK.md` & `docs/MILESTONE_7_DESIGN.md` — Báo cáo kiểm tra artifacts & Thiết kế kiến trúc Dashboard.
- `docs/MILESTONE_7_PHASE_C_REPORT.md` → `docs/MILESTONE_7_PHASE_G_REPORT.md` — Báo cáo triển khai và kiểm thử tích hợp từng giai đoạn của Dashboard.
- `docs/MILESTONE_7_FINAL_REPORT.md` — Báo cáo tổng kết toàn diện dự án.
- `docs/DEMO_SCRIPT.md` — Kịch bản trình diễn hệ thống (8–12 phút).
- `dashboard/README.md` — Hướng dẫn cài đặt và vận hành Dashboard.

