# Báo cáo Hoàn thành Milestone 4: Data Exploration, Preprocessing & Spark SQL Analytics

**Đề tài:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Milestone:** Milestone 4 – Data Exploration, Preprocessing & Spark SQL Analytics  
**Thời gian thực hiện:** 2026-11-11  
**Trạng thái:** **HOÀN THÀNH (COMPLETED & VERIFIED)**

---

## 1. Tổng quan Dataset & Môi trường Thực thi (Giai đoạn A)

- **Nguồn dữ liệu trên HDFS (`/bike-sharing/raw/`):**
  - `train.csv`: **`10,886 dòng`**, `12 cột` (`633.2 KB`) — Dữ liệu theo giờ từ ngày 1 đến ngày 19 hàng tháng (2011–2012).
  - `test.csv`: **`6,493 dòng`**, `9 cột` (`316.3 KB`) — Dữ liệu theo giờ từ ngày 20 đến cuối tháng (2011–2012).
  - `sampleSubmission.csv`: **`6,493 dòng`**, `2 cột` (`139.5 KB`).
- **Môi trường thực thi:**
  - Cụm Spark Standalone (`spark://spark-master:7077`, Spark `3.5.6`, 1 Worker `2 Cores / 2 GiB RAM`) kết nối trực tiếp với HDFS (`hdfs://hadoop-namenode:9000`, `replication=1`).
  - Không pull hay build thêm bất kỳ image nào; không tạo thêm container; giữ nguyên 100% dữ liệu `/bike-sharing/raw/` và `/bike-sharing/milestone3_test/`.

---

## 2. Kết quả Exploratory Data Analysis – EDA (Giai đoạn B)

Script thực thi: [compute/spark/jobs/eda_bike_sharing.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/spark/jobs/eda_bike_sharing.py)  
Tệp kết quả xuất ra tại: [outputs/eda/](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/) (11 tệp CSV + 1 tệp `eda_summary.json`).

### 2.1. Thống kê Mô tả (Descriptive Statistics — `b1_descriptive_statistics.csv`)

| Cột | Count | Mean | StdDev | Min | 25% (Q1) | Median (50%) | 75% (Q3) | Max |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `season` | 10,886 | 2.5066 | 1.1162 | 1.00 | 2.00 | 3.00 | 4.00 | 4.00 |
| `holiday` | 10,886 | 0.0286 | 0.1666 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 |
| `workingday` | 10,886 | 0.6809 | 0.4662 | 0.00 | 0.00 | 1.00 | 1.00 | 1.00 |
| `weather` | 10,886 | 1.4184 | 0.6338 | 1.00 | 1.00 | 1.00 | 2.00 | 4.00 |
| `temp` (°C) | 10,886 | 20.2309 | 7.7916 | 0.82 | 13.94 | 20.50 | 26.24 | 41.00 |
| `atemp` (°C) | 10,886 | 23.6551 | 8.4746 | 0.76 | 16.665 | 24.24 | 31.06 | 45.455 |
| `humidity` (%) | 10,886 | 61.8865 | 19.2450 | 0.00 | 47.00 | 62.00 | 77.00 | 100.00 |
| `windspeed` | 10,886 | 12.7994 | 8.1645 | 0.00 | 7.0015 | 12.998 | 16.9979 | 56.9969 |
| `casual` | 10,886 | 36.0220 | 49.9605 | 0.00 | 4.00 | 17.00 | 49.00 | 367.00 |
| `registered` | 10,886 | 155.5522 | 151.0390 | 0.00 | 36.00 | 118.00 | 222.00 | 886.00 |
| **`count`** | **10,886** | **191.5741** | **181.1445** | **1.00** | **42.00** | **145.00** | **284.00** | **977.00** |

### 2.2. Phân tích Biến Mục tiêu `count` (`b2_target_count_stats.csv` & `b2_target_count_distribution.csv`)
- **Tổng số lượt thuê xe trong tập Train:** **`2,085,476 lượt`** trên `10,886 giờ`.
- **Độ lệch phân phối:** `Mean = 191.57` > `Median = 145.00`, hệ số bất đối xứng **`Skewness = 1.2419`** (phân phối lệch phải rõ rệt — right-skewed), `Kurtosis = 1.2989`, `P90 = 452`, `P99 = 774`, `Max = 977`.
- **Phân bố theo các ngưỡng nhu cầu:**
  - `Very Low (1–50 xe/giờ)`: `2,969 giờ` (**27.27%**), trung bình `18.03 xe/giờ` (chủ yếu ban đêm 0h–5h).
  - `Low (51–150 xe/giờ)`: `2,602 giờ` (**23.90%**), trung bình `98.73 xe/giờ`.
  - `Moderate (151–300 xe/giờ)`: `2,833 giờ` (**26.02%**), trung bình `218.10 xe/giờ`.
  - `High (301–500 xe/giờ)`: `1,685 giờ` (**15.48%**), trung bình `386.22 xe/giờ`.
  - `Peak (>500 xe/giờ)`: `797 giờ` (**7.32%**), trung bình `635.34 xe/giờ` (các khung giờ cao điểm đi làm/tan tầm).

---

## 3. Kết quả Kiểm tra Chất lượng Dữ liệu & Tiền xử lý (Giai đoạn C)

Script thực thi: [compute/spark/jobs/preprocess_bike_sharing.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/spark/jobs/preprocess_bike_sharing.py)  
Biên bản JSON: [outputs/preprocessing/preprocessing_summary.json](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/preprocessing/preprocessing_summary.json).

1. **Kiểm tra Dữ liệu thiếu (Missing / Null values):**
   - Toàn bộ 12 cột của `train.csv` (`10,886 dòng`) và 9 cột của `test.csv` (`6,493 dòng`) đều có **0 giá trị Null**. Do đó không cần thực hiện nội suy (imputation) làm biến dạng dữ liệu gốc.
2. **Kiểm tra Trùng lặp (Duplicates):**
   - Số dòng trùng lặp toàn bản ghi: **0** (cả Train và Test).
   - Số mốc `datetime` trùng lặp: **0** (mỗi bản ghi đại diện duy nhất cho 1 giờ).
3. **Kiểm tra Tính hợp lệ Nghiệp vụ (Validity Rules):**
   - `count != casual + registered`: **0 vi phạm** (100% các dòng đều thỏa mãn `count = casual + registered`).
   - `count < 0`, `casual < 0`, `registered < 0`: **0 vi phạm**.
   - `hour` ngoài khoảng `[0, 23]`: **0 vi phạm**.
   - `season` ngoài `{1, 2, 3, 4}` hoặc `weather` ngoài `{1, 2, 3, 4}`: **0 vi phạm**.
   - `humidity` ngoài `[0, 100]` hoặc `windspeed < 0`: **0 vi phạm**.
4. **Ghi nhận các hiện tượng đặc biệt trong dữ liệu (Không tự ý xóa):**
   - Có `22 bản ghi` có `humidity = 0` (xảy ra vào ngày 10/03/2011) và `1,313 bản ghi` có `windspeed = 0` (lặng gió hoặc ngưỡng đo tối thiểu của cảm biến khí tượng). Đây là đặc tính thực tế của bộ dữ liệu chuẩn trên cả tập Train và Test nên được giữ nguyên để mô hình học đồng nhất.
   - Có duy nhất `1 bản ghi` ở mức thời tiết khắc nghiệt `weather = 4` (`2012-01-09 18:00:00`, `count = 164`).
   - Có `300 bản ghi` có `count > 647` (vượt ngưỡng $Q3 + 1.5 \times IQR$), nhưng đây không phải nhiễu sai số mà là **nhu cầu thực tế vào giờ cao điểm (8h sáng và 17h–18h chiều) trong năm 2012**, do đó bắt buộc phải giữ lại.

---

## 4. Danh sách Đặc trưng Đã Tạo — Feature Engineering (Giai đoạn D)

Tất cả các biến đổi được áp dụng đồng nhất cho cả `train` và `test` thông qua hàm `cast_and_extract_features` (chi tiết trong [docs/DATA_DICTIONARY.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/DATA_DICTIONARY.md)):

1. **Nhóm đặc trưng thời gian rời rạc (6 features):**
   - `year` (`2011`, `2012`)
   - `month` (`1` $\rightarrow$ `12`)
   - `day` (`1` $\rightarrow$ `31`)
   - `hour` (`0` $\rightarrow$ `23`)
   - `dayofweek` (`1 = Chủ Nhật` $\rightarrow$ `7 = Thứ Bảy`)
   - `is_weekend` (`1` nếu Thứ Bảy/Chủ Nhật, `0` nếu Thứ Hai $\rightarrow$ Thứ Sáu)
2. **Nhóm đặc trưng chu kỳ lượng giác — Cyclical Encoding (4 features):**
   - `hour_sin` $= \sin(2\pi \cdot \text{hour} / 24)$ và `hour_cos` $= \cos(2\pi \cdot \text{hour} / 24)$
   - `month_sin` $= \sin(2\pi \cdot \text{month} / 12)$ và `month_cos` $= \cos(2\pi \cdot \text{month} / 12)$
3. **Kiểm soát chặt chẽ Data Leakage:**
   - Tập **Cleaned Analytical Dataset** (`/bike-sharing/processed/`): Giữ `casual`, `registered`, `count` trên Train (`22 cột`) để phục vụ thống kê hành vi người dùng.
   - Tập **Model Feature Dataset** (`/bike-sharing/features/`): **Loại bỏ hoàn toàn `casual` và `registered`**. Tập `features/train` có `20 cột` (`datetime` + `18 cột đặc trưng đầu vào` + nhãn `count`); tập `features/test` có `19 cột` (`datetime` + `18 cột đặc trưng đầu vào`).

---

## 5. Kết quả Phân tích bằng Spark SQL (Giai đoạn E)

Script thực thi: [compute/spark/jobs/spark_sql_analytics.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/spark/jobs/spark_sql_analytics.py)  
Kết quả đã được lưu song song tại HDFS `/bike-sharing/analytics/q1..q8` và thư mục [outputs/analytics/](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/).

### 5.1. Truy vấn 1 & 8: Nhu cầu theo Giờ & Hành vi `casual` vs `registered` (`q1_hourly_demand.csv`, `q8_user_behavior_casual_vs_registered.csv`)
- **Hai đỉnh cao điểm rõ rệt trong ngày làm việc (`workingday = 1`):**
  - **Đỉnh sáng (08:00):** Trung bình **`479.95 xe/giờ`** (trong đó `registered` chiếm **95.41%** với `457.90 xe`, `casual` chỉ chiếm **4.59%** với `22.05 xe`).
  - **Đỉnh chiều (17:00 & 18:00):** Đạt mức cao nhất toàn hệ thống vào lúc **17:00** với trung bình **`529.21 xe/giờ`** (`registered = 472.70`, `casual = 56.51`) và **18:00** với **`495.49 xe/giờ`**.
- **Đáy thấp điểm:** Lúc **04:00 sáng** chỉ đạt trung bình **`6.41 xe/giờ`**.
- **Sự đảo chiều vào ngày nghỉ (`workingday = 0`):** Không còn 2 đỉnh nhọn lúc 8h và 17h mà tạo thành một hình chuông rộng từ **12:00 đến 16:00** (đỉnh lúc **13:00** đạt **`387.82 xe/giờ`**, trong đó tỷ trọng khách vãng lai `casual` tăng vọt lên tới **36.7%**).

### 5.2. Truy vấn 2: Nhu cầu theo Ngày trong tuần (`q2_dayofweek_demand.csv`)
| Thứ (`dayofweek`) | Tên ngày | Cuối tuần (`is_weekend`) | Số giờ | Tổng lượt thuê | Nhu cầu TB (`avg_total`) | TB `casual` | TB `registered` |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `1` | Sunday | 1 | 1,579 | 285,546 | 180.84 | 57.05 | 123.79 |
| `2` | Monday | 0 | 1,551 | 295,296 | 190.39 | 29.84 | 160.55 |
| `3` | Tuesday | 0 | 1,539 | 291,985 | 189.72 | 22.98 | 166.74 |
| `4` | Wednesday | 0 | 1,551 | 292,226 | 188.41 | 22.52 | 165.89 |
| `5` | Thursday | 0 | 1,553 | 306,401 | 197.30 | 24.01 | 173.29 |
| `6` | Friday | 0 | 1,529 | 302,504 | **197.84** | 31.00 | 166.84 |
| `7` | Saturday | 1 | 1,584 | **311,518** | 196.67 | **63.63** | 133.04 |

### 5.3. Truy vấn 3: Nhu cầu theo Mùa (`q3_seasonal_demand.csv`)
| Mã mùa | Tên mùa | Số giờ | Tổng lượt thuê | Nhu cầu TB (`count`) | TB `casual` | TB `registered` | Nhiệt độ TB (°C) | Độ ẩm TB (%) |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `1` | Spring | 2,686 | 312,498 | **116.34** | 15.49 | 100.85 | 12.53 | 56.30 |
| `2` | Summer | 2,733 | 588,282 | **215.25** | 47.45 | 167.80 | 22.82 | 60.85 |
| `3` | Fall | 2,733 | **640,662** | **234.42** | 52.22 | 182.20 | 28.79 | 64.12 |
| `4` | Winter | 2,734 | 544,034 | **198.99** | 28.58 | 170.41 | 16.65 | 66.17 |

### 5.4. Truy vấn 4: Nhu cầu theo Điều kiện Thời tiết (`q4_weather_demand.csv`)
- **Weather 1 (Quang đãng / Ít mây):** Chiếm `7,192 giờ` (66.1%), nhu cầu cao nhất đạt trung bình **`205.24 xe/giờ`**.
- **Weather 2 (Sương mù / Nhiều mây):** Chiếm `2,834 giờ` (26.0%), nhu cầu trung bình đạt **`178.96 xe/giờ`** (-12.8% so với trời quang).
- **Weather 3 (Mưa nhẹ / Tuyết nhẹ):** Chiếm `859 giờ` (7.9%), nhu cầu giảm mạnh xuống còn **`118.85 xe/giờ`** (-42.1% so với trời quang).
- **Weather 4 (Mưa lớn / Mưa đá):** Chỉ xảy ra đúng `1 giờ` (`164.0 xe/giờ`).

### 5.5. Truy vấn 5: So sánh Ngày làm việc và Ngày nghỉ (`q5_workingday_comparison.csv`)
- **Working Day (`workingday = 1`):** `7,412 giờ`, tổng `1,430,604 lượt` (`avg = 193.01`), trong đó thành viên đăng ký (`registered`) chiếm tới **86.99%**, khách vãng lai (`casual`) chỉ chiếm **13.01%**.
- **Non-Working Day (`workingday = 0`):** `3,474 giờ`, tổng `654,872 lượt` (`avg = 188.51`), tỷ trọng khách vãng lai (`casual`) tăng gấp 2.4 lần lên **31.46%**.

### 5.6. Truy vấn 6: Xu hướng theo Tháng & Năm (`q6_monthly_yearly_trend.csv`)
- **Tăng trưởng vượt bậc theo năm:**
  - Năm **2011** (`5,422 giờ`): Tổng `781,979 lượt`, trung bình **`144.22 xe/giờ`**.
  - Năm **2012** (`5,464 giờ`): Tổng `1,303,497 lượt`, trung bình **`238.56 xe/giờ`** (tăng **+65.41%** so với năm 2011).
- **Tháng thấp nhất:** `2011-01` (`avg = 54.65 xe/giờ`, nhiệt độ TB `8.63°C`).
- **Tháng cao nhất:** `2012-09` (`avg = 292.60 xe/giờ`, tổng `133,425 lượt`).

### 5.7. Truy vấn 7: Top 10 Khung giờ Có Nhu cầu Cao nhất (`q7_top10_peak_hours.csv`)
1. **17:00 – Working Day:** Trung bình **`529.21 xe/giờ`** (Max = `970`)
2. **18:00 – Working Day:** Trung bình **`495.49 xe/giờ`** (Max = `977`)
3. **08:00 – Working Day:** Trung bình **`479.95 xe/giờ`** (Max = `839`)
4. **13:00 – Non-Working Day:** Trung bình **`387.82 xe/giờ`** (Max = `729`)
5. **12:00 – Non-Working Day:** Trung bình **`379.11 xe/giờ`** (Max = `757`)
6. **14:00 – Non-Working Day:** Trung bình **`378.73 xe/giờ`** (Max = `730`)
7. **15:00 – Non-Working Day:** Trung bình **`373.70 xe/giờ`** (Max = `724`)
8. **16:00 – Non-Working Day:** Trung bình **`367.65 xe/giờ`** (Max = `783`)
9. **19:00 – Working Day:** Trung bình **`349.28 xe/giờ`** (Max = `743`)
10. **17:00 – Non-Working Day:** Trung bình **`339.12 xe/giờ`** (Max = `729`)

---

## 6. Kiểm tra Lưu trữ Parquet trên HDFS & So sánh Dung lượng (Giai đoạn F)

Kết quả kiểm tra bằng HDFS CLI (`hdfs dfs -du -h` và `hdfs dfs -ls -h`) xác nhận toàn bộ dữ liệu Parquet (nén `Snappy`) đã được lưu trữ chuẩn xác và đọc lại thành công:

| Phân vùng dữ liệu | Đường dẫn HDFS | Số dòng | Số cột | Dung lượng CSV gốc | Dung lượng Parquet (Snappy) | Tỷ lệ giảm dung lượng |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Processed Train** | `/bike-sharing/processed/train` | `10,886` | `22` (thêm 10 cột mới) | `633.2 KB` (12 cột) | **`151.4 KB`** | **-76.1%** (dù số cột tăng gấp đôi) |
| **Processed Test** | `/bike-sharing/processed/test` | `6,493` | `19` (thêm 10 cột mới) | `316.3 KB` (9 cột) | **`66.0 KB`** | **-79.1%** |
| **Features Train (ML)** | `/bike-sharing/features/train` | `10,886` | `20` (không có `casual`, `registered`) | `633.2 KB` | **`121.7 KB`** | **-80.8%** |
| **Features Test (ML)** | `/bike-sharing/features/test` | `6,493` | `19` (không có `casual`, `registered`, `count`) | `316.3 KB` | **`66.0 KB`** | **-79.1%** |
| **Analytics Tables (8 bảng)** | `/bike-sharing/analytics/q1..q8` | `129` (tổng 8 bảng) | `5–10` | — | **`~24.6 KB`** (tổng 8 bảng) | — |

---

## 7. Các Lỗi Gặp Phải và Cách Khắc phục

- **Lỗi `PySparkTypeError: [CANNOT_MERGE_TYPE] Can not merge type LongType and DoubleType` khi gom bảng Descriptive Statistics trong `eda_bike_sharing.py`:**
  - **Nguyên nhân:** Khi tính `min`, `max` và `percentile_approx` cho từng cột rồi ghép các `Row` lại thành một DataFrame tổng hợp, các cột kiểu nguyên (`season`, `count`, `humidity`) trả về `IntegerType`/`LongType`, trong khi các cột kiểu thực (`temp`, `windspeed`) trả về `DoubleType`. Khi `spark.createDataFrame(desc_rows)` suy diễn schema giữa các dòng, PySpark 3.5 báo lỗi xung đột kiểu giữa `LongType` và `DoubleType`.
  - **Khắc phục:** Thêm `.cast("double")` tường minh cho các cột thống kê (`mean`, `stddev`, `min`, `p25`, `median`, `p75`, `max`) trong vòng lặp tạo bảng Descriptive Statistics. Job chạy lại thành công 100%.

---

## 8. Kết luận & Công việc Tiếp theo (Milestone 5)

- **Dung lượng ổ C sau khi hoàn thành Milestone 4:** Còn trống **`8.12 GB`** (ổn định tuyệt đối vì không pull/build thêm image và dữ liệu Parquet rất nhỏ gọn).
- **Sẵn sàng cho Milestone 5 (Machine Learning với Spark MLlib):**
  - Tập đặc trưng `/bike-sharing/features/train` (`10,886 dòng`, `20 cột`) và `/bike-sharing/features/test` (`6,493 dòng`, `19 cột`) đã sẵn sàng trên HDFS, sạch 100% và đảm bảo không bị Data Leakage.
  - Ở Milestone 5, chúng ta sẽ chia `/bike-sharing/features/train` thành tập **Train** và **Validation** theo mốc thời gian, xây dựng `Pipeline` (`VectorAssembler` + Regressor) cho **Linear Regression**, **Decision Tree Regressor**, **Random Forest Regressor**, đánh giá bằng `MAE`, `RMSE`, `R²`, `RMSLE`, lưu mô hình tốt nhất và sinh dự đoán cho `test.csv`.

