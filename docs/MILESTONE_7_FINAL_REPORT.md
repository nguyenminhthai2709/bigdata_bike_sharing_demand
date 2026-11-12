# BÁO CÁO TỔNG KẾT DỰ ÁN (MILESTONE 7 — FINAL PROJECT REPORT)

- **Tên đề tài (Tiếng Anh):** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark
- **Tên đề tài (Tiếng Việt):** Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark
- **Thư mục dự án:** `C:\Users\BOOK PRO\distributed_sys_docker_setup`
- **Trạng thái dự án:** **HOÀN THÀNH TOÀN BỘ (MILESTONE 1 → MILESTONE 7)**

---

## 1. Tổng quan Dự án (Project Overview)

Hệ thống xe đạp chia sẻ đô thị (Urban Bike Sharing Systems) đóng vai trò quan trọng trong giao thông xanh hiện đại. Thách thức cốt lõi của nhà vận hành là biến động nhu cầu thuê xe rất mạnh theo khung giờ trong ngày, ngày làm việc/nghỉ lễ, mùa trong năm và điều kiện thời tiết. Dự án này xây dựng một hệ thống phân tích dữ liệu lớn và dự báo nhu cầu thuê xe đạp đầu-cuối (End-to-End Big Data Analytics & Forecasting System) dựa trên hệ sinh thái **Apache Hadoop HDFS**, **Apache Spark (Spark SQL, DataFrame API, Spark MLlib)** và giao diện trực quan hóa tương tác **Streamlit + Plotly**.

---

## 2. Mục tiêu và Phạm vi (Objectives & Scope)

1. **Hạ tầng lưu trữ và tính toán phân tán (Milestone 2 & 3):**
   - Triển khai cụm lưu trữ **Apache Hadoop HDFS** (`NameNode` + `DataNode`) và cụm tính toán **Apache Spark Standalone** (`Spark Master` + `Spark Worker`) trên nền tảng Docker WSL2.
2. **Khám phá dữ liệu, Tiền xử lý & Phân tích Spark SQL (Milestone 4):**
   - Thực hiện phân tích khám phá dữ liệu (EDA), kiểm định chất lượng dữ liệu, trích xuất **18 đặc trưng đầu vào** chống rò rỉ dữ liệu (Data Leakage Control), lưu trữ định dạng cột **Parquet Snappy** trên HDFS và thực thi **8 truy vấn phân tích chuyên sâu bằng Spark SQL** (`q1` → `q8`).
3. **Huấn luyện và Đánh giá Mô hình Học máy Phân tán (Milestone 5):**
   - Huấn luyện và đánh giá 3 thuật toán hồi quy trong **Apache Spark MLlib** (`Linear Regression`, `Decision Tree Regressor`, `Random Forest Regressor`) theo phương pháp chia dữ liệu trình tự thời gian **Time-based Split (80/20)** và tạo dự báo cho `6,493` mẫu tập Test.
4. **Đánh giá Khả năng Mở rộng & Hiệu năng Phân tán (Milestone 6):**
   - Mở rộng dữ liệu có kiểm soát từ `1x` (`10,886` dòng) lên `10x` (`108,860` dòng), `50x` (`544,300` dòng) và `100x` (`1,088,600` dòng) để đo kiểm hiệu năng lưu trữ `CSV vs Parquet Snappy`, 5 phép xử lý phân tán (`Filter, GroupBy, Aggregation, Sort, Join`) và tối ưu hóa số lượng phân vùng (`1, 2, 4, 8 partitions`).
5. **Xây dựng Ứng dụng Trực quan hóa Tương tác (Milestone 7):**
   - Phát triển ứng dụng **Streamlit Dashboard** gồm 4 trang chức năng chuyên sâu, đọc trực tiếp từ 37 tệp kết quả (`outputs/`) với cơ chế bộ nhớ đệm `@st.cache_data`.

---

## 3. Dataset và Nguồn Dữ liệu (Dataset & Data Sources)

- **Nguồn dữ liệu gốc:** Cuộc thi *Kaggle Bike Sharing Demand* (`data/raw/`):
  - `train.csv`: **`10,886` bản ghi giờ** × **`12` cột** (ghi nhận từ ngày 01 đến ngày 19 hàng tháng trong hai năm `2011–2012`).
  - `test.csv`: **`6,493` bản ghi giờ** × **`9` cột** (ghi nhận từ ngày 20 đến cuối mỗi tháng trong hai năm `2011–2012`; không chứa `casual`, `registered`, `count`).
  - `sampleSubmission.csv`: **`6,493` bản ghi** × **`2` cột** (`datetime, count`).
- **Đặc điểm kiểm định chất lượng dữ liệu gốc:**
  - Số giá trị khuyết (`Null / NaN`): **`0`** trên cả `train.csv` và `test.csv`.
  - Số bản ghi trùng lặp dấu thời gian (`Duplicate datetime`): **`0`**.
  - Tổng số lượt thuê xe trong tập huấn luyện: **`2,085,476` lượt thuê** (`Casual = 392,135` lượt chiếm `18.80%`; `Registered = 1,693,341` lượt chiếm `81.20%`).

---

## 4. Kiến trúc Hadoop HDFS và Apache Spark (System Architecture)

Hệ thống được triển khai theo mô hình **Single-Machine Containerized Spark Standalone** trên máy chủ Windows 11 (RAM 16 GB, Docker Desktop WSL2):

| Thành phần | Container / Service | Phiên bản / Cấu hình | Vai trò trong Pipeline |
| :--- | :--- | :--- | :--- |
| **HDFS NameNode** | `hadoop-namenode` | Apache Hadoop `3.2.1` (RPC `9000`, Web UI `9870`) | Quản lý siêu dữ liệu không gian tên tệp và ánh xạ khối dữ liệu HDFS |
| **HDFS DataNode** | `hadoop-datanode` | Apache Hadoop `3.2.1` (`dfs.replication = 1`) | Lưu trữ vật lý các khối dữ liệu CSV, Parquet và mô hình Spark MLlib |
| **Spark Master** | `spark-master` | Apache Spark `3.5.6` (RPC `7077`, Web UI `8080`) | Điều phối tài nguyên cụm tính toán và lập lịch ứng dụng Spark |
| **Spark Worker** | `spark-worker-1` | `2 CPU Cores`, `2048 MB Worker RAM` (`1024 MiB Executor`) | Thực thi các tác vụ đọc/ghi HDFS, truy vấn Spark SQL, huấn luyện MLlib và Benchmark |
| **Presentation UI** | Host Python (`streamlit`) | `Python 3.13.5`, `Streamlit 1.52.2`, `Plotly 6.9.0` (Port `8501`) | Trực quan hóa kết quả từ `outputs/` với `@st.cache_data` (tách rời hoàn toàn khỏi Spark) |

---

## 5. Quy trình EDA, Tiền xử lý và Feature Engineering (Milestone 4)

### 5.1. Các phát hiện cốt lõi từ EDA & 8 truy vấn Spark SQL (`q1` → `q8`)
1. **Phân phối biến mục tiêu `count`:** Lệch phải dương (`Mean = 191.5741`, `StdDev = 181.1445`, `Min = 1`, `Median = 145`, `IQR = 242`, `Max = 977` lượt/giờ).
2. **Quy luật theo giờ trong ngày (`q1`, `q7`, `q8`):**
   - Khung giờ cao điểm nhất tính chung toàn tập là **17:00** (**`468.77` lượt/giờ**), tiếp theo là **18:00** (**`430.89` lượt/giờ**) và **08:00** (**`362.77` lượt/giờ**).
   - Vào **ngày làm việc (`workingday = 1`)**, nhóm `registered` chiếm tới **`84.82%`** tổng nhu cầu, tạo mô hình 2 đỉnh giao thông đi làm – tan tầm (`08:00` đạt `448.95` lượt/giờ; `17:00` đạt `529.21` lượt/giờ). Vào **ngày nghỉ (`workingday = 0`)**, tỷ trọng `casual` tăng gấp đôi lên **`31.73%`**, tạo đồ thị 1 đỉnh thoải từ `12:00` đến `16:00`.
3. **Quy luật theo thứ trong tuần (`q2`):** Khá đồng đều từ Thứ Hai đến Chủ Nhật, cao nhất vào **Thứ Bảy (`Saturday = 196.66` lượt/giờ)** và **Thứ Sáu (`Friday = 197.84` lượt/giờ)**.
4. **Quy luật theo mùa (`q3`) và thời tiết (`q4`):**
   - **Mùa Thu (`Fall`)** đạt nhu cầu trung bình cao nhất (**`234.42` lượt/giờ**), tiếp theo là **Mùa Hè (`Summer = 215.25`)**, **Mùa Đông (`Winter = 198.99`)** và thấp nhất là **Mùa Xuân (`Spring = 116.34`)**.
   - Thời tiết **Quang đãng (`Clear/Partly Cloudy`)** đạt **`205.24` lượt/giờ** (`66.07%` số bản ghi), giảm mạnh khi **Mưa nhẹ/Tuyết nhẹ (`118.85` lượt/giờ`)**.
   - Tổ hợp nhiệt độ – độ ẩm tối ưu trên ma trận $4 \times 3$ là **Trời ấm/nóng (`22–30°C`) kết hợp độ ẩm thấp (`< 55%`)** đạt **`338.12` lượt/giờ**.
5. **Tăng trưởng giữa hai năm 2011 và 2012 (`q6`, `b3_year`):**
   - Nhu cầu thuê trung bình mỗi giờ tăng từ **`144.22` lượt/giờ (năm 2011)** lên **`238.56` lượt/giờ (năm 2012)** — mức tăng trưởng **`+65.41%`** (tổng lượt thuê tăng từ `781,979` lên `1,303,497` lượt, tức `+66.69%`).

### 5.2. Feature Engineering & Kiểm soát Data Leakage
- **Loại bỏ hoàn toàn rò rỉ dữ liệu (Data Leakage):** Vì `count = casual + registered` là tổng thành phần trực tiếp của biến mục tiêu và không tồn tại trong `test.csv`, cả hai cột `casual` và `registered` đều bị loại bỏ khỏi tập đặc trưng huấn luyện.
- **Trích xuất 18 đặc trưng đầu vào (`feature_columns`):**
  - 8 đặc trưng gốc: `season, holiday, workingday, weather, temp, atemp, humidity, windspeed`.
  - 6 đặc trưng thời gian rời rạc: `year, month, day, hour, dayofweek, is_weekend`.
  - 4 đặc trưng chu kỳ lượng giác (Cyclical Encoding): `hour_sin, hour_cos, month_sin, month_cos`.

---

## 6. Ba Mô hình Machine Learning & Phương pháp Huấn luyện (Milestone 5)

- **Phương pháp chia dữ liệu theo trình tự thời gian (`Time-based Split 80/20`):**
  - **Tập Huấn luyện (`Train — 80%`):** `8,708` bản ghi (`2011-01-01 00:00:00` → `2012-08-05 04:00:00`).
  - **Tập Kiểm định (`Validation — 20%`):** `2,178` bản ghi (`2012-08-05 05:00:00` → `2012-12-19 23:00:00`).
- **Ba thuật toán được huấn luyện bằng Apache Spark MLlib:**
  1. `Linear Regression`: `StandardScaler(withMean=True, withStd=True)` + `LinearRegression(maxIter=100, regParam=0.1, elasticNetParam=0.0)`.
  2. `Decision Tree Regressor`: `DecisionTreeRegressor(maxDepth=8, minInstancesPerNode=5, seed=42)`.
  3. `Random Forest Regressor`: `RandomForestRegressor(numTrees=30, maxDepth=8, minInstancesPerNode=5, featureSubsetStrategy="auto", subsamplingRate=0.8, seed=42)`.

---

## 7. Kết quả Đánh giá Mô hình (Model Evaluation Results)

### 7.1. Bảng so sánh hiệu năng trên tập Validation (`2,178` bản ghi)

> **Quy ước tính toán:** `RMSE`, `MAE`, `R²` được tính trên giá trị dự đoán gốc (`prediction`); `RMSLE` được tính trên giá trị dự đoán không âm (`prediction_clipped = max(prediction, 0.0)`).

| Mô hình | RMSE (Raw) ↓ | MAE (Raw) ↓ | R² Score (Raw) ↑ | RMSLE (Clipped) ↓ | Số dự đoán âm | Thời gian huấn luyện (s) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| *Baseline Mean Predictor (`174.3912`)* | *233.9322* | *176.5815* | *-0.1558* | *—* | *0* | *—* |
| **Linear Regression** | `164.4157` | `121.8134` | `0.4291` | `1.0940` | `0` | `10.5608` |
| **Decision Tree Regressor** | `100.7967` | **`65.1976`** | `0.7854` | `0.4382` | `0` | `18.7804` |
| **Random Forest Regressor (Best Model)** | **`99.1848`** | `66.3189` | **`0.7922`** | **`0.4016`** | `0` | `34.3088` |

### 7.2. Mức độ quan trọng của đặc trưng (`Feature Importance` — Random Forest)
- Top 10 đặc trưng dẫn đầu:
  1. `hour`: **`30.45%`** (`0.304528`)
  2. `hour_sin`: **`18.02%`** (`0.180155`)
  3. `hour_cos`: **`11.15%`** (`0.111465`)
  4. `atemp`: **`8.96%`** (`0.089580`)
  5. `year`: **`7.70%`** (`0.077030`)
  6. `temp`: **`7.35%`** (`0.073504`)
  7. `workingday`: **`3.34%`** (`0.033424`)
  8. `humidity`: **`2.59%`** (`0.025871`)
  9. `is_weekend`: **`2.58%`** (`0.025837`)
  10. `season`: **`2.04%`** (`0.020434`)
- Ba đặc trưng khung giờ (`hour, hour_sin, hour_cos`) đóng góp **`59.61%`**, và Top 6 đặc trưng đóng góp **`83.63%`** tổng mức độ quan trọng của mô hình.

---

## 8. Kết quả Big Data Benchmark (Milestone 6)

### 8.1. Mở rộng dữ liệu & Lưu trữ CSV vs Parquet Snappy (`Spark-Uncached / Warm OS Page Cache`, `3` runs)

| Quy mô (`Scale`) | Số dòng (`num_rows`) | Dung lượng CSV (`MB`) | Dung lượng Parquet (`MB`) | Tỷ lệ nén (`CSV/Parquet`) | Giảm dung lượng (%) | Read + Count: CSV vs Parquet (`s`) | Read + Agg: CSV vs Parquet (`s`) | Tăng tốc Read + Agg |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`1x`** | `10,886` | `1.0680` | `0.1821` | `5.87x` | `82.95%` | `0.9188s` vs `0.6566s` | `1.5461s` vs `0.9930s` | `1.56x` |
| **`10x`** | `108,860` | `11.0075` | `2.8204` | `3.90x` | `74.38%` | `0.3024s` vs `0.3429s` | `0.5917s` vs `1.1456s` | `0.52x` |
| **`50x`** | `544,300` | `55.1826` | `13.9019` | `3.97x` | `74.81%` | `3.4646s` vs `1.3273s` | `4.7906s` vs `4.4700s` | `1.07x` |
| **`100x`** | **`1,088,600`** | **`110.4020`** | **`27.7273`** | **`3.98x`** | **`74.89%`** | **`14.2397s` vs `2.0197s` (`7.05x`)** | **`21.5240s` vs `2.5600s`** | **`8.41x`** |

### 8.2. Hiệu năng 5 Phép toán Spark & Tối ưu hóa Partition (`1, 2, 4, 8`)
- **Hiệu năng 5 phép toán (`Filter, GroupBy, Aggregation, Sort, Join`):**
  - Tổng thời gian 5 phép toán giảm từ `16.3879s` ở `1x` (do chi phí khởi động JVM/Codegen lần đầu) xuống `3.5003s` ở `10x`, đạt đỉnh thông lượng trung bình tại **`50x` (`747,565.49 rows/sec`, tổng thời gian `8.5353s`)**, và tăng lên `28.7421s` (`440,647.12 rows/sec`) ở `100x` khi hai phép toán trộn dữ liệu (`Sort = 11.7692s`, `Join = 8.9701s`) chịu áp lực bộ nhớ.
- **Tối ưu hóa Partition trên Worker `2 CPU cores`:**
  - Tại **`10x`** và **`50x`**, cấu hình **`4 partitions`** đạt thời gian nhanh nhất (`0.5280s` ở `10x`; `0.9043s` ở `50x`).
  - Tại **`100x`**, **`1 partition`** đạt thời gian thực thi thuần thấp nhất sau khi cache (`0.9749s`), trong khi **`4 partitions`** (`1.3309s`) dẫn đầu các cấu hình đa phân vùng.
  - Cấu hình **`8 partitions`** chậm hơn `4 partitions` ở cả 3 quy mô (`1.8893s` ở `10x`, `3.2569s` ở `50x`, `1.5735s` ở `100x`) do vượt quá khả năng thực thi song song vật lý của `2 CPU cores`, làm tăng chi phí lập lịch task và shuffle.

---

## 9. Kiến trúc Ứng dụng Streamlit Dashboard (Dashboard Architecture)

- **Kiến trúc tách biệt (Decoupled Presentation Architecture):**
  - Ứng dụng Streamlit (`dashboard/app.py`) không phụ thuộc vào kết nối mạng tới Spark hay HDFS lúc chạy, giúp loại bỏ hoàn toàn độ trễ khởi tạo JVM và tránh tranh chấp RAM với các container đang chạy.
- **Tầng nạp và kiểm định dữ liệu (`dashboard/data_loader.py`):**
  - Sử dụng `pathlib.Path(__file__).resolve()` đảm bảo chạy đúng từ mọi thư mục làm việc.
  - Áp dụng `@st.cache_data` giúp giảm thời gian đọc dữ liệu từ `31–210 ms` (lần đầu) xuống chỉ còn **`0.17–10.82 ms`** (các lần truy cập tiếp theo).

---

## 10. Mô tả 4 Trang Chức năng của Dashboard

1. **`dashboard/pages/1_Overview.py` (Overview):** Tổng quan bộ dữ liệu, 6 thẻ KPI cốt lõi, sơ đồ luồng kiến trúc 8 bước và bảng tiến độ hoàn thành toàn bộ các Milestone.
2. **`dashboard/pages/2_Data_Analysis.py` (Data Analysis):** 5 thẻ KPI phân tích, bộ lọc tương tác 4 chiều (`Hour, Working Day, Season, Weather`) và 11 biểu đồ Plotly tương tác thể hiện toàn bộ kết quả EDA & 8 truy vấn Spark SQL (`q1` → `q8`).
3. **`dashboard/pages/3_Machine_Learning.py` (Machine Learning):** So sánh 3 mô hình hồi quy Spark MLlib và Baseline, cấu hình siêu tham số `Random Forest`, biểu đồ `Feature Importance` (18 đặc trưng), đối chiếu `Actual vs Predicted`, phân tích phần dư `Residual Analysis`, bộ khám phá `6,493` dự báo tập Test và nút tải xuống tệp `bike_sharing_kaggle_submission.csv`.
4. **`dashboard/pages/4_Big_Data_Benchmark.py` (Big Data Benchmark):** 6 thẻ KPI ở quy mô `100x`, phân tích mở rộng `1x–100x`, đối chiếu `CSV vs Parquet Snappy`, đánh giá 5 phép toán Spark, tối ưu hóa `1, 2, 4, 8 Partitions`, phân tích `CPU & Memory Usage` và thuyết minh phương pháp thực nghiệm.

---

## 11. Kết quả Kiểm thử Tích hợp & Hiệu năng (Integration & Performance Testing)

1. **Kiểm thử tự động `AppTest` cả 4 trang & chuyển trang liên tục:**
   - Cả 4 trang (`Overview`, `Data Analysis`, `Machine Learning`, `Big Data Benchmark`) và toàn bộ thao tác thay đổi bộ lọc tương tác đều thực thi thành công với **`0 exceptions`**.
   - Thời gian render trang đo thực tế: `Overview = 683.9 ms` (quay lại trang `42.5 ms`), `Data Analysis = 1,866.4 ms`, `Machine Learning = 1,980.1 ms`, `Big Data Benchmark = 1,149.2 ms`.
2. **Kiểm thử độc lập thư mục làm việc (Working-Directory Independence):**
   - Chuyển thư mục làm việc sang `%TEMP%` bên ngoài repository và chạy `AppTest`: **`PASS (0 exceptions)`**.
3. **Kiểm thử máy chủ HTTP trên cổng `8501` & Mức tiêu thụ RAM:**
   - Máy chủ khởi động thành công, `/_stcore/health` trả về `ok`, trang chính trả về `HTTP 200`, dung lượng bộ nhớ RAM của tiến trình Streamlit chỉ chiếm **`77,824 KB` (`~76.0 MB`)**.
4. **Kiểm tra an toàn môi trường:**
   - Toàn bộ 6 container `langfuse-local-*`, 2 container Hadoop (`hadoop-namenode`, `hadoop-datanode`) và 2 container Spark (`spark-master`, `spark-worker-1`) đều giữ nguyên trạng thái `Up` và `healthy`. Ổ đĩa `C:` còn trống **`21.40 GB`**.

---

## 12. Hạn chế của Hệ thống (System Limitations)

1. **Quy mô cụm tính toán trên một máy chủ vật lý:** Cụm Hadoop HDFS và Spark Standalone chạy trong các container chia sẻ tài nguyên trên một máy chủ Windows 11 (`1 Worker, 2 CPU cores, 1024 MiB Executor`), do đó chưa phản ánh độ trễ truyền dữ liệu qua mạng vật lý giữa nhiều máy chủ độc lập.
2. **Bản chất dữ liệu mở rộng (10x–100x):** Được nhân bản có kiểm soát từ `10,886` dòng gốc để đánh giá hành vi kỹ thuật của Spark và Parquet.
3. **Độ lệch dự báo vào cuối năm 2012:** Do nhu cầu thuê xe tăng trưởng mạnh (`+65.41%`) giữa năm 2011 và 2012, việc chia `Time-based Split (80/20)` khiến tập Validation nằm hoàn toàn ở cuối năm 2012, dẫn tới phần dư trung bình dương (`Mean Residual = +42.37` lượt/giờ).

---

## 13. Hướng Phát triển (Future Work)

1. **Mở rộng sang cụm Multi-Node Cloud / Kubernetes:** Triển khai cụm Spark với nhiều Worker vật lý và `HDFS replication = 3` để đánh giá khả năng chịu lỗi và mở rộng ngang ở quy mô hàng chục triệu bản ghi.
2. **Tích hợp Xử lý Luồng Thời gian thực (Real-time Streaming):** Kết hợp **Apache Kafka** và **Spark Structured Streaming** để thu thập trạng thái trạm xe và cập nhật dự báo theo thời gian thực.
3. **Nâng cấp Mô hình Dự báo Chuỗi thời gian:** Thử nghiệm các mô hình Gradient Boosted Trees (`GBTRegressor`), XGBoost phân tán trên Spark hoặc mô hình kết hợp xu hướng chuỗi thời gian để giảm độ lệch trung bình khi nhu cầu tăng trưởng đột biến.

---

## 14. Kết luận (Conclusion)

Dự án **Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark** đã hoàn thành đầy đủ mọi mục tiêu đề ra qua 7 Milestone: từ triển khai hạ tầng lưu trữ phân tán **Hadoop HDFS**, xử lý và truy vấn **Spark SQL**, huấn luyện mô hình dự báo **Spark MLlib** (`Random Forest Regressor` đạt `RMSE = 99.1848`, `R² = 0.7922`, `RMSLE = 0.4016`), đánh giá khả năng mở rộng tới **1,088,600 bản ghi** (`Parquet Snappy` tiết kiệm `74.89%` dung lượng và tăng tốc đọc + tổng hợp `8.41x`), đến hoàn thiện ứng dụng **Streamlit Dashboard** 4 trang trực quan, chính xác và ổn định.

