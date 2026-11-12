# BÁO CÁO KIỂM THỬ TÍCH HỢP & NGHIỆM THU CUỐI CÙNG (MILESTONE 7 — PHASE G REPORT)

- **Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark (`Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark`)
- **Giai đoạn:** Milestone 7 — Phase G (Integration Testing & Project Finalization)
- **Thời gian hoàn thành:** `2026-11-12`
- **Trạng thái nghiệm thu:** **PASS TOÀN BỘ (ĐẠT ĐIỀU KIỆN NGHIỆM THU CUỐI CÙNG)**

---

## 1. Danh sách Kiểm thử Tích hợp & Kết quả (Integration Testing Summary)

| STT | Nhóm kiểm thử | Hạng mục kiểm thử chi tiết | Kết quả | Minh chứng / Ghi chú |
| :---: | :--- | :--- | :---: | :--- |
| 1 | **Page 1 — Overview** | Header, Dataset Overview (`10,886` train / `6,493` test), 6 KPIs, Sơ đồ kiến trúc 8 bước, Tiến độ 6 Milestone, Sidebar | **PASS** | `AppTest` render thành công (`683.9 ms`, `0 exceptions`) |
| 2 | **Page 2 — Data Analysis** | 5 KPIs, 11 biểu đồ Plotly, bộ lọc `Hour (0–23)`, `Working Day`, `Season`, `Weather`, nút chuyển `Avg / Total`, thứ tự `Monday → Sunday` | **PASS** | `AppTest` render & lọc tương tác thành công (`1,866.4 ms`, `0 exceptions`) |
| 3 | **Page 3 — Machine Learning** | So sánh 3 mô hình + Baseline, 4 độ đo (`RMSE, MAE, R², RMSLE`), Cấu hình RF, Feature Importance (18 features), Actual vs Predicted, Residual Analysis, Test Explorer (`6,493` dòng), Kaggle Submission Download | **PASS** | `AppTest` render & tương tác widget thành công (`1,980.1 ms`, `0 exceptions`) |
| 4 | **Page 4 — Big Data Benchmark** | 6 KPIs ở `100x`, Scaling Analysis (`1x–100x`), CSV vs Parquet (`8` dòng), 5 phép xử lý Spark (`20` dòng), Partition Optimization (`12` dòng), Resource Utilization, Methodology | **PASS** | `AppTest` render & lọc tương tác thành công (`1,149.2 ms`, `0 exceptions`) |
| 5 | **Multi-Page Navigation** | Chuyển trang liên tục `Overview → Data Analysis → Machine Learning → Big Data Benchmark → Overview` | **PASS** | Chuyển trang mượt mà, lần quay lại `Overview` chỉ mất `42.5 ms` |
| 6 | **Kaggle Submission Download** | Kiểm tra byte nhị phân của nút `st.download_button` (`bike_sharing_kaggle_submission.csv`) | **PASS** | Khớp 100% byte-for-byte với `outputs/models/kaggle_submission.csv` (`6,493` dòng, `0` nulls) |
| 7 | **Working-Directory Independence** | Chạy `AppTest` khi thư mục làm việc hiện tại (`os.getcwd()`) là `%TEMP%` bên ngoài repository | **PASS** | Nhờ `pathlib.Path(__file__).resolve()`, ứng dụng nạp đúng toàn bộ 37 artifacts (`0 exceptions`) |
| 8 | **HTTP Server & Health Check** | Khởi chạy `streamlit run dashboard/app.py --server.port 8501` và kiểm tra HTTP | **PASS** | `http://127.0.0.1:8501` trả `HTTP 200`, `/_stcore/health` trả `ok` |

---

## 2. Kết quả Kiểm toán Toàn vẹn Dữ liệu (Data Integrity Cross-Check Audit)

Toàn bộ các con số hiển thị trên 4 trang Dashboard đã được đối chiếu tự động trực tiếp với các tệp CSV/JSON nguồn trong `outputs/`:

### 2.1. Đối chiếu Milestone 4 (EDA & Spark SQL Analytics)

| Chỉ số kiểm toán | Giá trị yêu cầu | Giá trị từ Artifacts (`outputs/eda/` & `outputs/analytics/`) | Giá trị trên Dashboard | Kết quả |
| :--- | :---: | :---: | :---: | :---: |
| **Tổng số lượt thuê (Train)** | `2,085,476` | `2,085,476` (`b2_target_count_stats.csv`) | `2,085,476` | **KHỚP 100%** |
| **Trung bình lượt thuê/giờ** | `191.57` | `191.5741` (`b2_target_count_stats.csv`) | `191.57` | **KHỚP 100%** |
| **Khung giờ cao điểm nhất (Peak Hour)** | `17:00` | `hour = 17` (`468.7653` lượt/giờ — `q1_hourly_demand.csv`) | `17:00 (468.77/giờ)` | **KHỚP 100%** |
| **Mùa có nhu cầu cao nhất (Fall)** | `234.42` | `Fall = 234.4171` lượt/giờ (`q3_seasonal_demand.csv`) | `Fall (234.42/giờ)` | **KHỚP 100%** |
| **Tăng trưởng 2011 → 2012** | `+65.41%` | Trung bình giờ tăng từ `144.22` lên `238.56` (`+65.41%`); tổng lượt thuê tăng từ `781,979` lên `1,303,497` (`+66.69%` — `b3_demand_by_year.csv`) | `+65.41%` (TB giờ) & `+66.69%` (Tổng) | **KHỚP 100%** |
| **Thứ tự ngày trong tuần** | `Monday → Sunday` | Đã ép kiểu `pd.Categorical(..., ordered=True)` trong `data_loader.py` | `Monday → Sunday` | **KHỚP 100%** |

### 2.2. Đối chiếu Milestone 5 (Machine Learning với Spark MLlib)

| Chỉ số kiểm toán | Giá trị yêu cầu | Giá trị từ Artifacts (`outputs/models/`) | Giá trị trên Dashboard | Kết quả |
| :--- | :---: | :---: | :---: | :---: |
| **Random Forest RMSE** | `99.1848` | `99.1848` (`model_comparison_metrics.csv` & `best_model_metadata.json`) | `99.1848` (`99.18`) | **KHỚP 100%** |
| **Random Forest MAE** | `66.3189` | `66.3189` | `66.3189` (`66.32`) | **KHỚP 100%** |
| **Random Forest R²** | `0.7922` | `0.7922` | `0.7922` (`79.22%`) | **KHỚP 100%** |
| **Random Forest RMSLE** | `0.4016` | `0.4016` | `0.4016` | **KHỚP 100%** |
| **Số bản ghi Validation** | `2,178` dòng | `2,178` dòng (`validation_predictions.csv`) | `2,178` dòng | **KHỚP 100%** |
| **Số bản ghi Test** | `6,493` dòng | `6,493` dòng (`test_predictions.csv`) | `6,493` dòng | **KHỚP 100%** |
| **Số bản ghi Kaggle Submission** | `6,493` dòng | `6,493` dòng, cột `datetime, count`, `0` nulls (`kaggle_submission.csv`) | `6,493` dòng (`0` nulls) | **KHỚP 100%** |

### 2.3. Đối chiếu Milestone 6 (Big Data Scaling & Benchmark)

| Chỉ số kiểm toán | Giá trị yêu cầu | Giá trị từ Artifacts (`outputs/benchmark/`) | Giá trị trên Dashboard | Kết quả |
| :--- | :---: | :---: | :---: | :---: |
| **Số dòng ở quy mô 100x** | `1,088,600` dòng | `1,088,600` (`scaling_results.csv`) | `1,088,600` rows | **KHỚP 100%** |
| **Dung lượng CSV ở 100x** | `110.40 MB` | `110.4020 MB` (`115,764,852` bytes — `storage_comparison.csv`) | `110.40 MB` | **KHỚP 100%** |
| **Dung lượng Parquet ở 100x** | `27.73 MB` | `27.7273 MB` (`29,074,221` bytes — `storage_comparison.csv`) | `27.73 MB` | **KHỚP 100%** |
| **Tỷ lệ tiết kiệm lưu trữ (Storage Reduction)** | `74.89%` | `(1 - 29074221 / 115764852) * 100 = 74.8851%` (`≈ 74.89%`) | `74.89%` (`3.98x`) | **KHỚP 100%** |
| **Tăng tốc Read + Aggregation ở 100x** | `8.41x` | `21.5240s / 2.5600s = 8.4078x` (`≈ 8.41x`) | `8.41x` | **KHỚP 100%** |

---

## 3. Kết quả Đo kiểm Hiệu năng Ứng dụng Dashboard (Performance Testing)

| Hạng mục đo kiểm | Lần chạy đầu (Cold Load) | Lần chạy có Cache (`@st.cache_data`) | Hệ số tăng tốc Cache |
| :--- | :---: | :---: | :---: |
| **`get_overview_kpis()`** | `94.06 ms` | `0.173 ms` | **`543.7x`** |
| **`get_data_analysis_artifacts()`** | `82.50 ms` | `3.073 ms` | **`26.8x`** |
| **`get_ml_artifacts()`** | `209.81 ms` | `10.819 ms` | **`19.4x`** |
| **`get_benchmark_artifacts()`** | `31.85 ms` | `1.032 ms` | **`30.9x`** |

- **Thời gian khởi động máy chủ Streamlit trên cổng `8501`:** `12.46 giây` (đạt trạng thái `/_stcore/health = ok` và trả về `HTTP 200`).
- **Bộ nhớ RAM tiêu thụ của tiến trình máy chủ Streamlit (`python.exe`):** `77,824 KB` (`~76.00 MB`) — rất nhẹ và hoàn toàn không gây áp lực lên 16 GB RAM của máy chủ Windows 11.

---

## 4. Các Vấn đề Kỹ thuật Đã Phát hiện & Xử lý trong Quá trình Phát triển

1. **Cảnh báo `use_container_width` trên Streamlit `1.52.2`:**
   - *Nguyên nhân:* Tham số `use_container_width=True` trong `st.plotly_chart()` và `st.dataframe()` đã được thay bằng `width="stretch"` trên Streamlit 1.52+.
   - *Cách xử lý:* Chuẩn hóa toàn bộ lời gọi hiển thị bảng và biểu đồ sang `width="stretch"` trong `components/charts.py` và tất cả các trang.
2. **Giá trị chuỗi `"None"` trong cột `error` của `outputs/benchmark/processing_results.csv`:**
   - *Nguyên nhân:* `pandas.read_csv()` mặc định biên dịch chuỗi `"None"` thành `NaN`, làm kích hoạt kiểm tra chống giá trị khuyết (`isna().sum()`).
   - *Cách xử lý:* Chuẩn hóa `processing_df["error"] = processing_df["error"].fillna("None")` trong `get_benchmark_artifacts()` trước khi kiểm tra `0` nulls trên toàn bộ các cột số liệu.
3. **Cập nhật trạng thái hoàn thành Milestone 7 trên trang `1_Overview.py`:**
   - *Cách xử lý:* Cập nhật trạng thái của dòng `Milestone 7: Dashboard & Project Finalization` từ `"Đang triển khai"` sang `"Hoàn thành"` (`✅`) sau khi cả 4 trang đã hoàn tất.

---

## 5. Kiểm tra An toàn Môi trường (Environment Safety Verification)

- **Trạng thái 10 Docker Containers đang hoạt động:**
  - **6 Container Langfuse (giữ nguyên 100%, `Up 44 hours`):**
    - `langfuse-local-langfuse-worker-1` (`Up`)
    - `langfuse-local-langfuse-web-1` (`Up`)
    - `langfuse-local-postgres-1` (`Up, healthy`)
    - `langfuse-local-redis-1` (`Up, healthy`)
    - `langfuse-local-clickhouse-1` (`Up, healthy`)
    - `langfuse-local-minio-1` (`Up, healthy`)
  - **2 Container Hadoop HDFS (giữ nguyên 100%, `Up 25 hours`):**
    - `hadoop-namenode` (`Up, healthy`)
    - `hadoop-datanode` (`Up, healthy`)
  - **2 Container Apache Spark (giữ nguyên 100%, `Up 24 hours`):**
    - `spark-master` (`Up`)
    - `spark-worker-1` (`Up`)
- **Dung lượng ổ đĩa `C:`:** Còn trống **`21.40 GB`** (`22,976,966,656` bytes).
- **Bảo toàn dữ liệu:** Không có bất kỳ tệp nào trong `data/raw/` hoặc `outputs/` (Milestone 2–6) bị sửa đổi hay ghi đè.

---

## 6. Trạng thái Hoàn thành 4 Trang & Danh mục Tài liệu Bàn giao

### 6.1. Trạng thái 4 trang ứng dụng Streamlit
- [x] `dashboard/pages/1_Overview.py` — **HOÀN THÀNH & ĐÃ KIỂM THỬ**
- [x] `dashboard/pages/2_Data_Analysis.py` — **HOÀN THÀNH & ĐÃ KIỂM THỬ**
- [x] `dashboard/pages/3_Machine_Learning.py` — **HOÀN THÀNH & ĐÃ KIỂM THỬ**
- [x] `dashboard/pages/4_Big_Data_Benchmark.py` — **HOÀN THÀNH & ĐÃ KIỂM THỬ**

### 6.2. Danh mục báo cáo và tài liệu dự án trong `docs/` và `dashboard/`
1. `docs/MILESTONE_2_REPORT.md` — Báo cáo triển khai Hadoop HDFS & tải dữ liệu gốc.
2. `docs/MILESTONE_3_REPORT.md` — Báo cáo triển khai Apache Spark Standalone & tích hợp HDFS.
3. `docs/MILESTONE_4_REPORT.md` & `docs/DATA_DICTIONARY.md` — Báo cáo EDA, Feature Engineering, Spark SQL & Từ điển dữ liệu.
4. `docs/MILESTONE_5_REPORT.md` — Báo cáo huấn luyện & đánh giá mô hình Spark MLlib.
5. `docs/MILESTONE_6_PRECHECK.md` & `docs/MILESTONE_6_REPORT.md` — Báo cáo mở rộng dữ liệu `1x–100x` & Benchmark hiệu năng phân tán.
6. `docs/MILESTONE_7_PRECHECK.md` — Báo cáo kiểm tra môi trường & 37 artifacts (Phase A).
7. `docs/MILESTONE_7_DESIGN.md` — Tài liệu thiết kế kiến trúc & giao diện Dashboard (Phase B).
8. `docs/MILESTONE_7_PHASE_C_REPORT.md` — Báo cáo triển khai Dashboard Core & trang Overview (Phase C).
9. `docs/MILESTONE_7_PHASE_D_REPORT.md` — Báo cáo triển khai trang Data Analysis (Phase D).
10. `docs/MILESTONE_7_PHASE_E_REPORT.md` — Báo cáo triển khai trang Machine Learning (Phase E).
11. `docs/MILESTONE_7_PHASE_F_REPORT.md` — Báo cáo triển khai trang Big Data Benchmark (Phase F).
12. `docs/MILESTONE_7_PHASE_G_REPORT.md` — Báo cáo kiểm thử tích hợp & nghiệm thu cuối cùng (Phase G).
13. `docs/MILESTONE_7_FINAL_REPORT.md` — Báo cáo tổng kết toàn diện dự án (14 mục).
14. `docs/DEMO_SCRIPT.md` — Kịch bản trình diễn trực tiếp hệ thống (8–12 phút).
15. `dashboard/README.md` — Tài liệu hướng dẫn cài đặt, cấu trúc và vận hành Dashboard.

