# BÁO CÁO KẾT QUẢ TRIỂN KHAI MILESTONE 7 — GIAI ĐOẠN C: DASHBOARD CORE & OVERVIEW PAGE

**Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Tên ứng dụng:** Bike Sharing Demand Analysis & Forecasting Dashboard  
**Repository:** `C:\Users\BOOK PRO\distributed_sys_docker_setup`  
**Ngày thực hiện:** 2026-11-12  
**Trạng thái:** **HOÀN THÀNH 100% GIAI ĐOẠN C (VERIFIED & READY FOR PHASE D)**

---

## 1. Danh sách các Tệp Mã nguồn Đã Tạo

Toàn bộ cấu trúc thư mục `dashboard/` đã được khởi tạo theo đúng thiết kế tại [docs/MILESTONE_7_DESIGN.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/MILESTONE_7_DESIGN.md), giữ nguyên 100% dữ liệu và artifacts của Milestone 2–6:

| STT | Đường dẫn tệp | Vai trò & Chức năng |
| :---: | :--- | :--- |
| 1 | [dashboard/app.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/app.py) | Entrypoint chính của ứng dụng: cấu hình `st.set_page_config`, nạp CSS, định nghĩa điều hướng 4 trang bằng `st.navigation` & `st.Page` và hiển thị Sidebar. |
| 2 | [dashboard/config.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/config.py) | Định nghĩa `PROJECT_ROOT`, `OUTPUTS_DIR`, `EDA_DIR`, `ANALYTICS_DIR`, `PREPROCESSING_DIR`, `MODELS_DIR`, `BENCHMARK_DIR`, từ điển `ARTIFACT_PATHS` (37 đường dẫn) bằng `pathlib.Path(__file__).resolve()`, thông số cụm thực nghiệm và bảng màu chuẩn. |
| 3 | [dashboard/data_loader.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/data_loader.py) | Cung cấp `load_csv()`, `load_json()`, `get_overview_kpis()` có gắn `@st.cache_data`, kiểm tra sự tồn tại và toàn vẹn tệp, đối chiếu chéo số liệu và phát sinh ngoại lệ `ArtifactLoadError` rõ ràng nếu tệp thiếu/lỗi. |
| 4 | [dashboard/components/__init__.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/components/__init__.py) | Khởi tạo package `components`. |
| 5 | [dashboard/components/sidebar.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/components/sidebar.py) | Hiển thị thông tin Dataset (`Train: 10,886`, `Test: 6,493`, `Target: count`) và cấu hình thực nghiệm đã ghi nhận (`1 Spark Master + 1 Spark Worker`, `2 CPU cores`, `2048 MB Worker RAM`, `HDFS replication = 1`). |
| 6 | [dashboard/components/kpi_cards.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/components/kpi_cards.py) | Cung cấp các hàm tái sử dụng `render_page_header()`, `render_kpi_card()`, và `render_kpi_row()` sử dụng `st.columns`. |
| 7 | [dashboard/components/charts.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/components/charts.py) | Cung cấp hàm chuẩn hóa layout Plotly `apply_standard_chart_layout()` và `render_plotly_chart()`. |
| 8 | [dashboard/assets/style.css](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/assets/style.css) | Định kiểu CSS hiện đại cho Header Banner, KPI Cards, Architecture Pipeline Steps, Environment Notice và Milestone Timeline. |
| 9 | [dashboard/pages/1_Overview.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/pages/1_Overview.py) | **Trang 1 (Hoàn thiện 100%):** Hiển thị Header, Dataset Overview, 6 KPI Cards đối chiếu trực tiếp từ nguồn, Sơ đồ Kiến trúc 8 bước và Tiến độ Milestones 2–7. |
| 10 | [dashboard/pages/2_Data_Analysis.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/pages/2_Data_Analysis.py) | **Trang 2 (Trang khung):** Tiêu đề, mô tả và trạng thái `"Đang triển khai ở Milestone 7"`. |
| 11 | [dashboard/pages/3_Machine_Learning.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/pages/3_Machine_Learning.py) | **Trang 3 (Trang khung):** Tiêu đề, mô tả và trạng thái `"Đang triển khai ở Milestone 7"`. |
| 12 | [dashboard/pages/4_Big_Data_Benchmark.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/pages/4_Big_Data_Benchmark.py) | **Trang 4 (Trang khung):** Tiêu đề, mô tả và trạng thái `"Đang triển khai ở Milestone 7"`. |
| 13 | [dashboard/README.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/README.md) | Hướng dẫn chạy nhanh ứng dụng Streamlit trên cổng `8501`. |

---

## 2. Kiến trúc Dashboard Đã Triển khai

1. **Định vị đường dẫn tuyệt đối bằng `pathlib`:** Toàn bộ 37 tệp kết quả trong `outputs/` được ánh xạ qua `PROJECT_ROOT = Path(__file__).resolve().parent.parent`, hoàn toàn độc lập với thư mục làm việc hiện tại khi gọi lệnh `streamlit run`.
2. **Cơ chế nạp dữ liệu an toàn & tối ưu (`@st.cache_data`):**
   - Không khởi tạo `SparkSession`, không gọi HDFS RPC trực tiếp từ giao diện web.
   - Kiểm tra `file_path.exists()` và `file_path.stat().st_size > 0` trước khi đọc.
   - Tự động đối chiếu chéo số liệu giữa `best_model_metadata.json` và `model_comparison_metrics.csv`.
3. **Điều hướng đa trang chuẩn Streamlit 1.52.2 (`st.navigation` & `st.Page`):**
   - Quản lý tập trung 4 trang (`1. Overview`, `2. Data Analysis (EDA & SQL)`, `3. Machine Learning (MLlib)`, `4. Big Data Benchmark`) kèm Sidebar cố định hiển thị cấu hình thực nghiệm.

---

## 3. Bảng Đối chiếu Các Chỉ số KPI & Nguồn Dữ liệu trên Trang Overview

Tất cả các chỉ số hiển thị trên [dashboard/pages/1_Overview.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/pages/1_Overview.py) đều được trích xuất tự động qua hàm `get_overview_kpis()` trong [dashboard/data_loader.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/data_loader.py):

| Hạng mục / Thẻ KPI | Giá trị hiển thị | Tệp dữ liệu nguồn (`outputs/...`) | Trường / Công thức trích xuất |
| :--- | :---: | :--- | :--- |
| **Dataset Name** | `Kaggle Bike Sharing Demand` | `dashboard/config.py` | `PROJECT_INFO["dataset_name"]` |
| **Training Records** | `10,886` | `outputs/eda/eda_summary.json` | `train_rows` (`10886`, `12` cột gốc $\rightarrow$ `18` features) |
| **Test Records** | `6,493` | `outputs/eda/eda_summary.json` | `test_rows` (`6493`, `9` cột gốc $\rightarrow$ `18` features) |
| **Target Variable** | `count` | `outputs/models/best_model_metadata.json` | `target_column` (tổng `2,085,476` lượt thuê trong Train) |
| **KPI 1: Best Model** | **`Random Forest`** | `outputs/models/best_model_metadata.json` | `best_model_name` (`Random Forest Regressor`) |
| **KPI 2: Validation RMSE** | **`99.1848`** | `outputs/models/best_model_metadata.json` & `model_comparison_metrics.csv` | `best_model_validation_metrics["rmse"]` (đối chiếu khớp 100% với CSV) |
| **KPI 3: Validation MAE** | **`66.3189`** | `outputs/models/best_model_metadata.json` & `model_comparison_metrics.csv` | `best_model_validation_metrics["mae"]` (đối chiếu khớp 100% với CSV) |
| **KPI 4: Validation R²** | **`0.7922`** *(RMSLE: `0.4016`)* | `outputs/models/best_model_metadata.json` & `model_comparison_metrics.csv` | `best_model_validation_metrics["r2"]` & `["rmsle"]` |
| **KPI 5: Parquet Storage Reduction** | **`74.89%`** | `outputs/benchmark/storage_comparison.csv` | $(1 - 29,074,221 / 115,764,852) \times 100 = 74.89\%$ tại `100x` (`27.73 MB` vs `110.40 MB`) |
| **KPI 6: Parquet Compression Ratio** | **`3.98x`** | `outputs/benchmark/scaling_results.csv` | `csv_to_parquet_size_ratio` tại `scale_factor == 100` (`3.98x`) |

---

## 4. Kết quả Kiểm thử Giai đoạn C

Đã thực hiện đầy đủ 7 bước kiểm thử tự động và kiểm thử máy chủ thực tế:

1. **Kiểm tra import toàn bộ module (`config`, `data_loader`, `components.sidebar`, `components.kpi_cards`, `components.charts`):**
   - Kết quả: **`PASS`** (`0` lỗi cú pháp hay phụ thuộc).
2. **Kiểm tra 37 đường dẫn nguồn dữ liệu trong `ARTIFACT_PATHS`:**
   - Kết quả: **`PASS`** (`37/37` tệp CSV/JSON tồn tại và hợp lệ).
3. **Kiểm tra đọc CSV/JSON và đối chiếu 6 KPI (`get_overview_kpis()`):**
   - Kết quả: **`PASS`** — Khớp chính xác tuyệt đối với báo cáo Milestone 5 & 6 (`train_rows=10886`, `test_rows=6493`, `best_model_name='Random Forest'`, `val_rmse=99.1848`, `val_mae=66.3189`, `val_r2=0.7922`, `parquet_reduction_pct=74.89`, `compression_ratio=3.98`).
4. **Kiểm tra render Trang Overview và điều hướng 4 trang bằng `streamlit.testing.v1.AppTest`:**
   - Render mặc định `pages/1_Overview.py`: **`PASS (0 exceptions)`** — Hiển thị đầy đủ 4 `st.metric` Dataset Overview, 6 thẻ KPI, sơ đồ kiến trúc 8 bước và danh sách 6 Milestones (Milestone 2–6: `Hoàn thành`, Milestone 7: `Đang triển khai`).
   - Chuyển trang bằng `at.switch_page()` lần lượt sang `pages/2_Data_Analysis.py`, `pages/3_Machine_Learning.py`, và `pages/4_Big_Data_Benchmark.py`: **`PASS (0 exceptions)`**.
5. **Kiểm thử khởi chạy Streamlit Server thực tế trên cổng `8501`:**
   - Khởi chạy `python -m streamlit run dashboard/app.py --server.port 8501 --server.headless true`.
   - Kiểm tra endpoint `http://localhost:8501/_stcore/health` trả về `ok` và `http://localhost:8501` trả về **`HTTP 200 OK`**.
   - Đã dừng tiến trình Streamlit kiểm thử sau khi xác minh để giải phóng cổng `8501`.

---

## 5. Các Lỗi Phát hiện và Cách Xử lý

- **Không phát sinh lỗi (`0 errors`)** trong suốt quá trình triển khai và kiểm thử Giai đoạn C nhờ đã kiểm증 trước API của `Streamlit 1.52.2` và cấu trúc của toàn bộ 37 tệp artifacts ở Giai đoạn A & B.

---

## 6. Trạng thái Hoàn thành

- **Dashboard Core (`app.py`, `config.py`, `data_loader.py`, `components/*`, `assets/style.css`):** **Hoàn thành 100%**.
- **Trang 1 (`pages/1_Overview.py`):** **Hoàn thành 100%**.
- **Điều hướng 4 trang (`pages/1..4`):** **Hoàn thành 100%**.
- **An toàn hệ thống:** Cụm Hadoop HDFS, Apache Spark và 6 containers Langfuse giữ nguyên trạng thái hoạt động ổn định.

