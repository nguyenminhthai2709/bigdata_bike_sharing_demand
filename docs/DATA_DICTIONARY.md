# Từ điển Dữ liệu (Data Dictionary)

**Đề tài:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Cập nhật tại:** Milestone 4 – Data Exploration, Preprocessing & Spark SQL Analytics  

---

## 1. Dữ liệu Gốc (Raw Dataset: `/bike-sharing/raw/`)

Bộ dữ liệu **Kaggle Bike Sharing Demand** ghi nhận lịch sử thuê xe đạp theo từng giờ trong 2 năm (2011 và 2012) của hệ thống Capital Bikeshare tại Washington D.C., Hoa Kỳ.
- **Quy tắc phân chia gốc của Kaggle:**
  - `train.csv` (`10,886 dòng`, `12 cột`): Gồm các ngày từ **ngày 1 đến ngày 19** của mỗi tháng.
  - `test.csv` (`6,493 dòng`, `9 cột`): Gồm các ngày từ **ngày 20 đến cuối tháng** của mỗi tháng.

| Tên cột | Kiểu dữ liệu (Spark) | Miền giá trị (Train) | Ý nghĩa & Mô tả chi tiết | Vai trò trong ML |
| :--- | :--- | :--- | :--- | :--- |
| `datetime` | `TimestampType` | `2011-01-01 00:00:00` $\rightarrow$ `2012-12-19 23:00:00` | Mốc thời gian theo từng giờ (`yyyy-MM-dd HH:mm:ss`). | Dùng để trích xuất đặc trưng thời gian & chia Train/Validation theo thời gian |
| `season` | `IntegerType` | `{1, 2, 3, 4}` | Mùa trong năm: `1` = Xuân (Spring), `2` = Hạ (Summer), `3` = Thu (Fall), `4` = Đông (Winter). | **Input Feature** |
| `holiday` | `IntegerType` | `{0, 1}` | Ngày lễ: `1` = Ngày lễ chính thức, `0` = Không phải ngày lễ. | **Input Feature** |
| `workingday` | `IntegerType` | `{0, 1}` | Ngày làm việc: `1` = Ngày làm việc (không phải cuối tuần và không phải ngày lễ), `0` = Cuối tuần hoặc ngày lễ. | **Input Feature** |
| `weather` | `IntegerType` | `{1, 2, 3, 4}` | Điều kiện thời tiết:<br>- `1`: Quang đãng, ít mây (Clear / Partly cloudy)<br>- `2`: Sương mù + Mây (Mist + Cloudy)<br>- `3`: Tuyết nhẹ, mưa nhẹ + giông (Light Snow / Light Rain)<br>- `4`: Mưa lớn + mưa đá + sương mù dày (Heavy Rain / Ice Pellets) | **Input Feature** |
| `temp` | `DoubleType` | `0.82` $\rightarrow$ `41.00` (°C) | Nhiệt độ thực tế đo được theo độ Celsius (°C). | **Input Feature** |
| `atemp` | `DoubleType` | `0.76` $\rightarrow$ `45.455` (°C) | Nhiệt độ cảm nhận ("feels-like" temperature) theo độ Celsius (°C). | **Input Feature** |
| `humidity` | `IntegerType` | `0` $\rightarrow$ `100` (%) | Độ ẩm tương đối (%). | **Input Feature** |
| `windspeed` | `DoubleType` | `0.00` $\rightarrow$ `56.9969` | Tốc độ gió. | **Input Feature** |
| `casual` | `IntegerType` | `0` $\rightarrow$ `367` | Số lượt thuê xe của người dùng vãng lai (chưa đăng ký thành viên). Chỉ có trong `train.csv`. | **LOẠI BỎ khỏi ML Features** (Gây Data Leakage vì `count = casual + registered`) |
| `registered` | `IntegerType` | `0` $\rightarrow$ `886` | Số lượt thuê xe của thành viên đã đăng ký. Chỉ có trong `train.csv`. | **LOẠI BỎ khỏi ML Features** (Gây Data Leakage vì `count = casual + registered`) |
| `count` | `IntegerType` | `1` $\rightarrow$ `977` | **Tổng số lượt thuê xe trong giờ** (`count = casual + registered`). Chỉ có trong `train.csv`. | **Target Variable (Nhãn mục tiêu)** |

---

## 2. Các Đặc trưng Mới Tạo trong Feature Engineering (Milestone 4)

Các đặc trưng sau được trích xuất đồng nhất cho cả tập Train và Test trong [compute/spark/jobs/preprocess_bike_sharing.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/spark/jobs/preprocess_bike_sharing.py), hoàn toàn dựa trên cột `datetime` tại thời điểm dự đoán (không dùng thông tin tương lai):

| Tên đặc trưng | Kiểu dữ liệu | Công thức / Quy tắc trích xuất | Miền giá trị | Ý nghĩa kỹ thuật |
| :--- | :--- | :--- | :--- | :--- |
| `year` | `IntegerType` | `F.year("datetime")` | `{2011, 2012}` | Phản ánh xu hướng tăng trưởng quy mô hệ thống giữa năm 2011 và 2012 (+65.4% nhu cầu TB). |
| `month` | `IntegerType` | `F.month("datetime")` | `1` $\rightarrow$ `12` | Nắm bắt tính mùa vụ theo tháng trong năm. |
| `day` | `IntegerType` | `F.dayofmonth("datetime")` | `1` $\rightarrow$ `19` (Train), `20` $\rightarrow$ `31` (Test) | Ngày trong tháng (phục vụ chia tập Train/Validation theo mốc thời gian và kiểm tra dữ liệu). |
| `hour` | `IntegerType` | `F.hour("datetime")` | `0` $\rightarrow$ `23` | Yếu tố quyết định mạnh nhất đến nhịp sinh hoạt trong ngày (giờ cao điểm đi làm 8h và tan tầm 17h–18h). |
| `dayofweek` | `IntegerType` | `F.dayofweek("datetime")` | `1` (Chủ Nhật) $\rightarrow$ `7` (Thứ Bảy) | Thứ trong tuần theo chuẩn Spark SQL (`1=Sun, 2=Mon, ..., 7=Sat`). |
| `is_weekend` | `IntegerType` | `1` nếu `dayofweek IN (1, 7)`, ngược lại `0` | `{0, 1}` | Tách biệt hành vi thuê xe giải trí cuối tuần với ngày thường trong tuần. |
| `hour_sin` | `DoubleType` | $\sin\left(\frac{2\pi \cdot \text{hour}}{24}\right)$ | `[-1.0, 1.0]` | Mã hóa chu kỳ 24 giờ giúp mô hình tuyến tính nhận biết giờ `23` và giờ `0` liền kề nhau. |
| `hour_cos` | `DoubleType` | $\cos\left(\frac{2\pi \cdot \text{hour}}{24}\right)$ | `[-1.0, 1.0]` | Thành phần cosin đi cặp với `hour_sin` để định vị duy nhất từng giờ trên vòng tròn lượng giác. |
| `month_sin` | `DoubleType` | $\sin\left(\frac{2\pi \cdot \text{month}}{12}\right)$ | `[-1.0, 1.0]` | Mã hóa chu kỳ 12 tháng giúp nối liền tháng 12 và tháng 1. |
| `month_cos` | `DoubleType` | $\cos\left(\frac{2\pi \cdot \text{month}}{12}\right)$ | `[-1.0, 1.0]` | Thành phần cosin đi cặp với `month_sin`. |

---

## 3. Phân biệt 2 Bộ Dữ liệu Parquet trên HDFS

1. **Cleaned Analytical Dataset (`/bike-sharing/processed/{train,test}`):**
   - `train`: `10,886 dòng`, **`22 cột`** (giữ `casual`, `registered`, `count` để phục vụ các truy vấn phân tích hành vi người dùng trong Spark SQL và Dashboard).
   - `test`: `6,493 dòng`, **`19 cột`**.
2. **Model Feature Dataset (`/bike-sharing/features/{train,test}`):**
   - `train`: `10,886 dòng`, **`20 cột`** (`datetime` + `18 cột đặc trưng đầu vào` + `count`). **Tuyệt đối không chứa `casual` và `registered`**.
   - `test`: `6,493 dòng`, **`19 cột`** (`datetime` + `18 cột đặc trưng đầu vào`). **Không chứa `casual`, `registered`, `count`**.

