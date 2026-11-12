# KỊCH BẢN TRÌNH DIỄN HỆ THỐNG (LIVE DEMO SCRIPT — 8 ĐẾN 12 PHÚT)

- **Đề tài:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark (`Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark`)
- **Thời lượng dự kiến:** 8 – 12 phút
- **Lệnh khởi chạy trước buổi trình diễn:**
  ```powershell
  streamlit run dashboard/app.py --server.port 8501
  ```
- **URL trình diễn:** `http://localhost:8501`

---

## Phần 1: Giới thiệu Đề tài & Mục tiêu (Phút 0:00 – 1:15)

- **Màn hình hiển thị:** Mở trang **1. Overview** (`dashboard/pages/1_Overview.py`).
- **Nội dung trình bày:**
  1. Hệ thống xe đạp chia sẻ đô thị đòi hỏi khả năng dự báo chính xác nhu cầu thuê xe theo từng giờ để điều phối xe giữa các trạm, tránh tình trạng thiếu xe giờ cao điểm hoặc dư thừa giờ thấp điểm.
  2. Mục tiêu của dự án là xây dựng một đường ống dữ liệu lớn (End-to-End Big Data Pipeline) hoàn chỉnh kết hợp:
     - Lưu trữ phân tán trên **Apache Hadoop HDFS**.
     - Tiền xử lý, phân tích chuyên sâu bằng **Apache Spark SQL** và **DataFrame API**.
     - Huấn luyện các mô hình học máy phân tán bằng **Apache Spark MLlib**.
     - Đánh giá khả năng mở rộng quy mô dữ liệu lên tới hơn **1 triệu bản ghi (`100x`)** và trình diễn trực quan trên **Streamlit Dashboard**.

---

## Phần 2: Kiến trúc Hadoop HDFS & Apache Spark (Phút 1:15 – 2:30)

- **Màn hình hiển thị:** Cuộn xuống mục **3. Kiến trúc Hệ thống Phân tán (System Architecture)** và thanh bên **Sidebar** trên trang **Overview**.
- **Nội dung trình bày:**
  1. Chỉ vào thông tin cụm trên Sidebar và khung **Ghi chú Môi trường Thực nghiệm**:
     - Hệ thống chạy dưới dạng **Single-Machine Containerized Spark Standalone** gồm `1 Hadoop NameNode`, `1 Hadoop DataNode` (`replication = 1`), `1 Spark Master` và `1 Spark Worker` (`2 CPU Cores`, `2048 MB RAM`, `1024 MiB Executor`).
  2. Trình bày luồng dữ liệu 8 bước:
     - Dữ liệu gốc từ Kaggle (`train.csv` và `test.csv`) được nạp lên HDFS tại `/bike-sharing/raw/`.
     - Spark đọc dữ liệu từ HDFS, thực hiện làm sạch, trích xuất 18 đặc trưng chu kỳ thời gian và lưu trữ xuống HDFS dưới định dạng cột **Parquet Snappy** tại `/bike-sharing/features/`.
     - Dashboard Streamlit hoạt động độc lập ở tầng trình diễn, đọc từ các tệp kết quả trong `outputs/` thông qua `@st.cache_data` với thời gian phản hồi dưới `11 ms` khi đã cache, đảm bảo không làm nghẽn tài nguyên cụm Spark.

---

## Phần 3: Giới thiệu Dataset & Phân tích Khám phá Dữ liệu — EDA (Phút 2:30 – 4:30)

- **Màn hình hiển thị:** Chuyển sang trang **2. Data Analysis** (`dashboard/pages/2_Data_Analysis.py`).
- **Nội dung trình bày:**
  1. Điểm qua 5 thẻ KPI ở đầu trang:
     - Tập huấn luyện gồm **10,886 bản ghi giờ** (từ ngày 01 đến 19 hàng tháng trong 2 năm 2011–2012), tổng cộng **2,085,476 lượt thuê**, trung bình **191.57 lượt/giờ**.
     - Giờ cao điểm nhất trong ngày là **17:00** (**468.77 lượt/giờ**); mùa có nhu cầu cao nhất là **Mùa Thu (`Fall`: 234.42 lượt/giờ)**.
  2. Thao tác trực tiếp trên **Bộ lọc Tương tác (Interactive Filters)**:
     - Kéo thanh trượt giờ hoặc chọn `Working Day (1)` để thấy sự thay đổi tức thời trên biểu đồ.
  3. Giải thích 3 phát hiện quan trọng nhất từ Spark SQL (`q1` → `q8`):
     - **Quy luật hai đỉnh (Bimodal Commuter Pattern):** Vào ngày làm việc (`Working Day = 1`), người dùng đã đăng ký (`registered` chiếm **84.82%**) tạo ra 2 đỉnh rõ rệt lúc **08:00 sáng** (`448.95` lượt/giờ) và **17:00–18:00 chiều** (`529.21` và `491.85` lượt/giờ). Ngược lại, vào ngày nghỉ/cuối tuần, đồ thị chuyển sang dạng một đỉnh thoải từ **12:00–16:00 chiều** do nhóm khách vãng lai (`casual`) tăng mạnh.
     - **Tác động của Thời tiết & Nhiệt độ – Độ ẩm:** Trên biểu đồ ma trận nhiệt $4 \times 3$ (`Temp × Humidity`), vùng nhu cầu cao nhất đạt **338.12 lượt/giờ** khi trời ấm/nóng (`22–30°C`) kết hợp với độ ẩm khô ráo (`< 55%`).
     - **Xu hướng tăng trưởng theo năm:** Nhu cầu thuê trung bình năm 2012 (**238.56 lượt/giờ**, tổng `1,303,497` lượt) tăng **+65.41%** so với năm 2011 (**144.22 lượt/giờ**, tổng `781,979` lượt).

---

## Phần 4: Huấn luyện 3 Mô hình Machine Learning & Kiểm soát Data Leakage (Phút 4:30 – 6:00)

- **Màn hình hiển thị:** Chuyển sang trang **3. Machine Learning** (`dashboard/pages/3_Machine_Learning.py`), xem Mục 1 và Mục 2.
- **Nội dung trình bày:**
  1. **Kiểm soát chặt chẽ Data Leakage:**
     - Trong tập gốc, `count = casual + registered` là đẳng thức cộng chính xác tuyệt đối. Vì vậy, hệ thống loại bỏ hoàn toàn `casual` và `registered` khỏi tập đặc trưng đầu vào và sử dụng đúng **18 đặc trưng** trong `VectorAssembler`.
  2. **Phương pháp chia dữ liệu theo trình tự thời gian (`Time-based Split 80/20`):**
     - Không chia ngẫu nhiên (Random Split) để tránh rò rỉ thông tin từ tương lai về quá khứ.
     - **80% đầu (`8,708` dòng:** `2011-01-01 00:00:00` → `2012-08-05 04:00:00`) dùng để huấn luyện; **20% cuối (`2,178` dòng:** `2012-08-05 05:00:00` → `2012-12-19 23:00:00`) dùng để kiểm định.
  3. **Đối chiếu 3 mô hình hồi quy trên Spark MLlib:**
     - `Linear Regression`: `RMSE = 164.4157`, `MAE = 121.8134`, `R² = 0.4291`, `RMSLE = 1.0940` (không nắm bắt được quan hệ phi tuyến của giờ trong ngày).
     - `Decision Tree Regressor`: `RMSE = 100.7967`, `MAE = 65.1976`, `R² = 0.7854`, `RMSLE = 0.4382`.
     - `Random Forest Regressor`: **`RMSE = 99.1848`**, **`MAE = 66.3189`**, **`R² = 0.7922`**, **`RMSLE = 0.4016`**.
     - Nhấn mạnh quy ước: `RMSE, MAE, R²` tính trên `prediction` gốc; `RMSLE` tính trên `prediction_clipped = max(prediction, 0.0)`.

---

## Phần 5: Giải thích Kết quả Random Forest, Feature Importance & Dự báo Tập Test (Phút 6:00 – 7:45)

- **Màn hình hiển thị:** Cuộn xuống Mục 3, 4, 5, 6, 7 trên trang **3. Machine Learning**.
- **Nội dung trình bày:**
  1. **Feature Importance:**
     - Chỉ vào biểu đồ ngang Top 10 đặc trưng: Ba đặc trưng giờ (`hour = 30.45%`, `hour_sin = 18.02%`, `hour_cos = 11.15%`) chiếm gần **60%** tổng mức độ quan trọng, theo sau là nhiệt độ cảm nhận `atemp` (`8.96%`), năm `year` (`7.70%`) và nhiệt độ `temp` (`7.35%`).
  2. **Actual vs Predicted & Phân tích Phần dư (`Residuals`):**
     - Trên tập Validation (`2,178` dòng), đường dự báo của Random Forest bám sát chu kỳ ngày đêm thực tế.
     - Phần dư $\text{Residual} = \text{Actual} - \text{Predicted}$ có trung bình `+42.37` lượt/giờ (`66.8%` mẫu có `Residual > 0`) do tập Validation nằm ở giai đoạn cuối năm 2012 có mức tăng trưởng nhu cầu rất cao.
  3. **Khám phá Tập Test & Tải xuống Kaggle Submission:**
     - Lọc thử tháng `2012-12` trên bộ khám phá tập Test (`6,493` dòng). Giải thích rõ tập Test không có nhãn `count` thực tế nên không tính sai số trên tập Test.
     - Nhấn nút **Tải xuống `bike_sharing_kaggle_submission.csv`** (`6,493` dòng, `2` cột `datetime, count`, `0` giá trị null).

---

## Phần 6: So sánh Lưu trữ CSV vs Parquet Snappy trên HDFS (Phút 7:45 – 9:15)

- **Màn hình hiển thị:** Chuyển sang trang **4. Big Data Benchmark** (`dashboard/pages/4_Big_Data_Benchmark.py`), xem Mục 1, 2 và 3.
- **Nội dung trình bày:**
  1. Giải thích cách mở rộng dữ liệu có kiểm soát từ `1x` (`10,886` dòng) lên `10x` (`108,860`), `50x` (`544,300`) và `100x` (`1,088,600` dòng).
  2. Chỉ vào kết quả đối chiếu **CSV vs Parquet Snappy** ở quy mô `100x` (`1,088,600` dòng):
     - **Dung lượng lưu trữ HDFS:** Giảm từ **110.40 MB (CSV)** xuống **27.73 MB (Parquet Snappy)** — tiết kiệm **74.89%** dung lượng (tỷ lệ nén **3.98x**).
     - **Tốc độ đọc & tổng hợp (`Read + Aggregation`):** Parquet chỉ mất **2.56 giây** so với **21.52 giây** của CSV — nhanh hơn **8.41 lần** nhờ cơ chế cắt tỉa cột (`Column Pruning`) và nén theo cột.
     - **Tốc độ đếm bản ghi (`Read + Count`):** Parquet nhanh hơn **7.05 lần** (`2.02s` so với `14.24s`).
     - Làm rõ phương pháp đo: Chế độ **`Spark-Uncached / Warm OS Page Cache`** (gọi `spark.catalog.clearCache()` trước mỗi lần đọc, lặp lại 3 lần chạy).

---

## Phần 7: Hiệu năng 5 Phép xử lý Spark & Tối ưu hóa Partition (Phút 9:15 – 10:45)

- **Màn hình hiển thị:** Cuộn xuống Mục 4, 5, 6 trên trang **4. Big Data Benchmark**.
- **Nội dung trình bày:**
  1. **Hiệu năng 5 phép toán (`Filter, GroupBy, Aggregation, Sort, Join`):**
     - Ở quy mô nhỏ `1x`, thời gian thực thi cao hơn `10x` do chi phí khởi động JVM và biên dịch mã Whole-Stage Codegen lần đầu.
     - Thông lượng xử lý đạt đỉnh ở quy mô **`50x` (`747,565 dòng/giây`)** và giảm nhẹ ở **`100x` (`440,647 dòng/giây`)** khi hai phép toán nặng về trộn dữ liệu (`Sort = 11.77s`, `Join = 8.97s`) gặp giới hạn bộ nhớ Executor `1024 MiB`.
  2. **Tối ưu hóa số lượng Partition (`1, 2, 4, 8 partitions`):**
     - Trên Spark Worker có **2 CPU cores**, cấu hình **4 partitions** (gấp `2x` số nhân CPU) đạt thời gian nhanh nhất ở `10x` (**0.5280 giây**, `206,174 rows/s`) và `50x` (**0.9043 giây**, `601,902 rows/s`).
     - Khi tăng lên **8 partitions**, thời gian thực thi bị chậm lại ở cả 3 quy mô (`1.8893s` ở `10x`, `3.2569s` ở `50x`) vì Worker chỉ chạy song song tối đa 2 tasks cùng lúc, làm tăng chi phí lập lịch task (task scheduling) và shuffle.

---

## Phần 8: Kết luận & Hạn chế Học thuật của Hệ thống (Phút 10:45 – 11:30)

- **Màn hình hiển thị:** Cuộn xuống Mục 7 (**Môi trường Thực nghiệm & Phương pháp Đo kiểm**) trên trang **Big Data Benchmark**.
- **Nội dung trình bày:**
  1. Dự án đã hoàn thành trọn vẹn chu trình từ triển khai hạ tầng lưu trữ phân tán **Hadoop HDFS**, xử lý và phân tích bằng **Apache Spark SQL**, huấn luyện mô hình dự báo **Spark MLlib** (`Random Forest R² = 0.7922`), đánh giá hiệu năng mở rộng tới **1.09 triệu bản ghi** và xây dựng ứng dụng **Streamlit Dashboard** 4 trang.
  2. **Các giới hạn thực nghiệm được ghi nhận minh bạch:**
     - Cụm tính toán chạy dưới dạng container trên một máy vật lý duy nhất (`1 Worker, 2 CPU cores`), không phải cụm nhiều máy chủ vật lý tách biệt.
     - Dữ liệu mở rộng `10x–100x` là dữ liệu tổng hợp có kiểm soát phục vụ đo kiểm kỹ thuật.
     - Trong tương lai, hệ thống có thể mở rộng sang cụm nhiều Worker vật lý, tích hợp luồng dữ liệu thời gian thực qua **Apache Kafka + Spark Structured Streaming** và bổ sung các đặc trưng thời tiết dự báo ngoại sinh.

