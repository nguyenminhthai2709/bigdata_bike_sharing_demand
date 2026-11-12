# Bike Sharing Demand Analysis & Forecasting Dashboard

Ứng dụng **Streamlit Dashboard** trực quan hóa toàn bộ kết quả thực nghiệm của đề tài Big Data:
> **Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark**  
> *(Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark)*

---

## 1. Cấu trúc Thư mục Dashboard

```text
dashboard/
├── app.py                      # Entrypoint ứng dụng, cấu hình st.navigation (4 trang) và nạp CSS
├── config.py                   # Quản lý đường dẫn tuyệt đối (pathlib.Path), hằng số dự án & bảng màu
├── data_loader.py              # Tầng nạp dữ liệu tập trung sử dụng @st.cache_data & kiểm định toàn vẹn
├── README.md                   # Tài liệu hướng dẫn sử dụng và vận hành Dashboard
├── assets/
│   └── style.css               # Giao diện CSS tùy chỉnh cho KPI Cards, Header Banner, Info Panels
├── components/
│   ├── __init__.py
│   ├── sidebar.py              # Thanh điều hướng bên trái, tóm tắt cụm Hadoop/Spark & Dataset
│   ├── kpi_cards.py            # Thành phần tái sử dụng cho Header Banner và hàng thẻ KPI
│   └── charts.py               # Hàm chuẩn hóa layout, font, chú thích và lưới cho biểu đồ Plotly
└── pages/
    ├── 1_Overview.py           # Trang 1: Tổng quan đề tài, 6 KPI cốt lõi, Kiến trúc 8 bước & Tiến độ
    ├── 2_Data_Analysis.py      # Trang 2: Phân tích EDA & 8 truy vấn Spark SQL (11 biểu đồ Plotly)
    ├── 3_Machine_Learning.py   # Trang 3: Đánh giá 3 mô hình Spark MLlib, Feature Importance, Residuals & Test Forecast
    └── 4_Big_Data_Benchmark.py # Trang 4: Benchmark mở rộng 1x–100x, CSV vs Parquet, 5 phép xử lý & Partition 1–8
```

---

## 2. Yêu cầu Môi trường & Thư viện Sử dụng

Dashboard được thiết kế theo kiến trúc **Tách biệt tầng Tính toán và tầng Trình diễn (Decoupled Compute & Presentation Layer)**:
- Không khởi tạo `SparkSession` từ giao diện người dùng.
- Không truy vấn trực tiếp HDFS từ luồng Web UI.
- Đọc trực tiếp từ 37 tệp kết quả (`CSV`, `JSON`) đã được tính toán trước trong thư mục `outputs/` với bộ nhớ đệm `@st.cache_data`.

### Các thư viện Python sử dụng
- **Python:** `3.10+` (đã kiểm thử trên `Python 3.13.5`)
- **Streamlit:** `1.52.2` (Framework giao diện người dùng & điều hướng đa trang)
- **Plotly:** `6.9.0` (Thư viện biểu đồ tương tác `plotly.graph_objects` & `plotly.express`)
- **Pandas:** `2.2.2` (Đọc, kiểm định và cắt lát dữ liệu bảng trong bộ nhớ)
- **NumPy:** `2.5.1` (Tính toán thống kê và kiểm chứng sai số)

---

## 3. Cách Khởi chạy Dashboard

Từ thư mục gốc của dự án (`C:\Users\BOOK PRO\distributed_sys_docker_setup`):

```powershell
streamlit run dashboard/app.py --server.port 8501
```

Hoặc chạy bằng module Python từ bất kỳ thư mục làm việc nào (nhờ cơ chế định tuyến đường dẫn tuyệt đối `pathlib.Path(__file__).resolve()`):

```powershell
python -m streamlit run "C:\Users\BOOK PRO\distributed_sys_docker_setup\dashboard\app.py" --server.port 8501
```

- **Địa chỉ truy cập trên trình duyệt:** `http://localhost:8501`
- **Địa chỉ kiểm tra trạng thái (Health Check):** `http://localhost:8501/_stcore/health`

---

## 4. Nguồn Dữ liệu (Artifacts trong `outputs/`)

| Phân hệ / Milestone | Thư mục nguồn | Các tệp dữ liệu chính |
| :--- | :--- | :--- |
| **Milestone 4 — EDA & Preprocessing** | `outputs/eda/`<br>`outputs/preprocessing/` | `eda_summary.json`, `b1_descriptive_statistics.csv`, `b2_target_count_stats.csv`, `b2_target_count_distribution.csv`, `b3_demand_by_hour.csv`, `b3_demand_by_dayofweek.csv`, `b3_demand_by_month.csv`, `b3_demand_by_year.csv`, `b4_demand_by_season.csv`, `b4_demand_by_weather.csv`, `b4_demand_by_temp_humidity.csv`, `b4_demand_by_workingday_holiday.csv`, `preprocessing_summary.json` |
| **Milestone 4 — Spark SQL Analytics** | `outputs/analytics/` | `q1_hourly_demand.csv`, `q2_dayofweek_demand.csv`, `q3_seasonal_demand.csv`, `q4_weather_demand.csv`, `q5_workingday_comparison.csv`, `q6_monthly_yearly_trend.csv`, `q7_top10_peak_hours.csv`, `q8_user_behavior_casual_vs_registered.csv` |
| **Milestone 5 — Spark MLlib Models** | `outputs/models/` | `model_comparison_metrics.csv`, `best_model_metadata.json`, `feature_importances.csv`, `validation_predictions.csv` (`2,178` dòng), `test_predictions.csv` (`6,493` dòng), `kaggle_submission.csv` (`6,493` dòng) |
| **Milestone 6 — Big Data Benchmark** | `outputs/benchmark/` | `scaling_results.csv` (`1x, 10x, 50x, 100x`), `storage_comparison.csv` (`CSV vs Parquet`), `processing_results.csv` (`5 phép xử lý Spark`), `partition_results.csv` (`1, 2, 4, 8 partitions`), `benchmark_summary.json` |

---

## 5. Mô tả 4 Trang Chức năng

1. **Trang 1 — Overview (`1_Overview.py`):**
   - Giới thiệu đề tài, thông tin bộ dữ liệu Kaggle Bike Sharing Demand (`10,886` mẫu Train, `6,493` mẫu Test, `18` đặc trưng đầu vào, tổng `2,085,476` lượt thuê).
   - Hiển thị 6 thẻ KPI cốt lõi từ Milestone 5 & 6 (`Best Model = Random Forest`, `RMSE = 99.1848`, `MAE = 66.3189`, `R² = 0.7922`, `Parquet Storage Reduction = 74.89%`, `Compression Ratio = 3.98x`).
   - Sơ đồ kiến trúc 8 bước xử lý từ Kaggle CSV → Hadoop HDFS → Apache Spark → Streamlit Dashboard và lộ trình 6 Milestone.

2. **Trang 2 — Data Analysis (`2_Data_Analysis.py`):**
   - 5 thẻ KPI phân tích (`Total Rentals = 2,085,476`, `Average Rentals = 191.57/giờ`, `Peak Hour = 17:00`, `Highest Season = Fall 234.42/giờ`, `Registered Share on Working Days = 84.82%`).
   - Bộ lọc tương tác theo khoảng giờ (`0–23`), loại ngày (`Working Day`), mùa (`Season`) và điều kiện thời tiết (`Weather`).
   - 11 biểu đồ Plotly tương tác phân tích nhu cầu theo giờ, thứ tự ngày trong tuần (`Monday → Sunday`), mùa, thời tiết, ma trận nhiệt $4 \times 3$ `Temperature × Humidity`, phân hóa hành vi `Casual vs Registered`, và xu hướng tăng trưởng `+65.41%` giai đoạn `2011–2012`.

3. **Trang 3 — Machine Learning (`3_Machine_Learning.py`):**
   - So sánh 3 mô hình hồi quy Spark MLlib (`Linear Regression`, `Decision Tree Regressor`, `Random Forest Regressor`) và `Baseline Mean Predictor` theo phương pháp chia dữ liệu `Time-based Split (80/20: 8,708 Train / 2,178 Validation)`.
   - Ghi chú rõ quy ước tính `RMSE, MAE, R²` trên dự đoán gốc (`prediction`) và `RMSLE` trên dự đoán không âm (`prediction_clipped`).
   - Biểu đồ mức độ quan trọng của 18 đặc trưng (`Feature Importance`), đối chiếu `Actual vs Predicted` (Scatter $y=x$ & Time-Series), phân tích phần dư (`Residual = Actual - Predicted`), khám phá `6,493` dự báo tập Test và nút tải xuống nguyên bản `bike_sharing_kaggle_submission.csv`.

4. **Trang 4 — Big Data Benchmark (`4_Big_Data_Benchmark.py`):**
   - 6 thẻ KPI ở quy mô tối đa `100x` (`1,088,600` dòng, `CSV = 110.40 MB`, `Parquet = 27.73 MB`, giảm `74.89%` dung lượng, tăng tốc đọc và tổng hợp `8.41x`).
   - Phân tích mở rộng `1x → 100x`, đối chiếu `CSV vs Parquet Snappy` trong chế độ `Spark-Uncached / Warm OS Page Cache`, đánh giá 5 phép toán Spark (`Filter, GroupBy, Aggregation, Sort, Join`), tối ưu hóa số lượng Partition (`1, 2, 4, 8`) trên Worker `2 CPU cores`, và phân tích mức sử dụng CPU/RAM.

---

## 6. Các Giới hạn của Hệ thống

- **Môi trường cụm container trên một máy vật lý:** Hệ thống Hadoop HDFS (`1 NameNode + 1 DataNode`, `replication = 1`) và Spark Standalone (`1 Master + 1 Worker`, `2 CPU cores`, `2048 MB Worker RAM`, `1024 MiB Executor`) chạy trên một máy chủ Windows 11 (RAM 16 GB) qua Docker WSL2, không phải cụm nhiều máy chủ vật lý độc lập.
- **Dữ liệu mở rộng Benchmark (10x–100x):** Được sinh tổng hợp có kiểm soát từ `10,886` dòng gốc để phục vụ đo kiểm hiệu năng lưu trữ và xử lý phân tán.
- **Tập Test của Kaggle không có nhãn thực tế (`count`):** Do đó không tính các độ đo sai số trên tập Test mà đánh giá mô hình thông qua tập Validation chia theo trình tự thời gian (`2,178` bản ghi cuối).
