# DANH MỤC KIỂM TRA NGHIỆM THU DỰ ÁN (ACCEPTANCE CHECKLIST — v1.0.0)

- **Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark
- **Phiên bản:** `v1.0.0`
- **Ngày lập:** `2026-11-12`

---

## 1. Kiến trúc và Hạ tầng (Architecture & Infrastructure)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **INF-01** | Hadoop NameNode và DataNode hoạt động ổn định (`healthy`), `dfs.replication = 1` | **PASS** | `docker ps` xác nhận `hadoop-namenode` và `hadoop-datanode` ở trạng thái `Up (healthy)`; `docs/MILESTONE_2_REPORT.md` |
| **INF-02** | Spark Master và Spark Worker hoạt động ổn định (`2 CPU cores`, `2048 MB RAM`) | **PASS** | `docker ps` xác nhận `spark-master` và `spark-worker-1` ở trạng thái `Up`; `docs/MILESTONE_3_REPORT.md` |
| **INF-03** | PySpark kết nối đọc/ghi thành công CSV và Parquet trên HDFS (`hdfs://hadoop-namenode:9000`) | **PASS** | `compute/spark/jobs/milestone3_hdfs_test.py` và `outputs/preprocessing/preprocessing_summary.json` |
| **INF-04** | Kiến trúc tách biệt tầng Tính toán (Spark/HDFS) và tầng Trình diễn (Streamlit Dashboard) | **PASS** | `dashboard/data_loader.py` chỉ đọc từ `outputs/` qua `@st.cache_data`, không khởi tạo `SparkSession` |

---

## 2. Dataset và Chất lượng Dữ liệu (Dataset & Data Quality)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **DAT-01** | Tập dữ liệu gốc tải lên `/bike-sharing/raw/` đầy đủ (`train.csv: 10,886`, `test.csv: 6,493`, `sampleSubmission.csv: 6,493`) | **PASS** | `outputs/eda/eda_summary.json` (`train_rows=10886`, `test_rows=6493`) & `docs/MILESTONE_2_REPORT.md` |
| **DAT-02** | Kiểm tra giá trị khuyết (`0 Nulls`) và trùng lặp thời gian (`0 Duplicates`) | **PASS** | `outputs/preprocessing/preprocessing_summary.json` (`null_counts = 0`, `duplicate_timestamps = 0`) |
| **DAT-03** | Kiểm soát hoàn toàn Data Leakage (loại bỏ `casual` và `registered` khỏi tập đặc trưng) | **PASS** | `outputs/models/best_model_metadata.json` (`excluded_leakage_columns: ["casual", "registered"]`) |
| **DAT-04** | Trích xuất đủ 18 đặc trưng đầu vào và lưu trữ Parquet trên HDFS (`/bike-sharing/features/`) | **PASS** | `outputs/preprocessing/preprocessing_summary.json` (`18` feature columns, `parquet_verification`) |

---

## 3. Phân tích Khám phá (EDA) và Spark SQL (Milestone 4)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **EDA-01** | Thống kê mô tả và phân phối biến mục tiêu `count` (`Total = 2,085,476`, `Mean = 191.57`, `Median = 145`) | **PASS** | `outputs/eda/b1_descriptive_statistics.csv`, `outputs/eda/b2_target_count_stats.csv` |
| **EDA-02** | Hoàn thành 8 truy vấn phân tích chuyên sâu bằng Spark SQL (`q1` → `q8`) | **PASS** | 8 tệp CSV trong `outputs/analytics/q1_hourly_demand.csv` → `q8_user_behavior_casual_vs_registered.csv` |
| **EDA-03** | Xác định đúng giờ cao điểm (`17:00` — `468.77` lượt/giờ), mùa cao nhất (`Fall` — `234.42` lượt/giờ) và tăng trưởng `2011–2012` (`+65.41%` TB giờ) | **PASS** | `outputs/analytics/q1_hourly_demand.csv`, `q3_seasonal_demand.csv`, `outputs/eda/b3_demand_by_year.csv` |

---

## 4. Học máy Phân tán — Spark MLlib (Milestone 5)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **ML-01** | Chia dữ liệu theo trình tự thời gian `Time-based Split 80/20` (`8,708` Train / `2,178` Validation) | **PASS** | `outputs/models/best_model_metadata.json` (`time_based_split`) |
| **ML-02** | Huấn luyện và đánh giá 3 mô hình (`Linear Regression`, `Decision Tree`, `Random Forest`) kèm `Baseline` | **PASS** | `outputs/models/model_comparison_metrics.csv` (`3` dòng) & `best_model_metadata.json` |
| **ML-03** | `Random Forest Regressor` đạt kết quả tốt nhất (`RMSE = 99.1848`, `MAE = 66.3189`, `R² = 0.7922`, `RMSLE = 0.4016`) | **PASS** | `outputs/models/model_comparison_metrics.csv` & `docs/MILESTONE_5_REPORT.md` |
| **ML-04** | Trích xuất mức độ quan trọng của 18 đặc trưng (`hour = 30.45%`, `hour_sin = 18.02%`, `hour_cos = 11.15%`, ...) | **PASS** | `outputs/models/feature_importances.csv` (`18` dòng) |
| **ML-05** | Dự báo tập Test (`6,493` dòng) và tạo tệp `kaggle_submission.csv` hợp lệ (`6,493` dòng, `2` cột `datetime, count`, `0` nulls) | **PASS** | `outputs/models/test_predictions.csv` & `outputs/models/kaggle_submission.csv` |

---

## 5. Mở rộng Dữ liệu & Benchmark Hiệu năng (Milestone 6)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **BM-01** | Mở rộng dữ liệu có kiểm soát ở 4 quy mô `1x` (`10,886`), `10x` (`108,860`), `50x` (`544,300`), `100x` (`1,088,600` dòng) | **PASS** | `outputs/benchmark/scaling_results.csv` (`4` dòng) & `scaling_generation_summary.csv` |
| **BM-02** | Đo kiểm lưu trữ `CSV vs Parquet Snappy` (tại `100x`: `110.40 MB` vs `27.73 MB`, giảm `74.89%`, nén `3.98x`, `Read+Agg` nhanh hơn `8.41x`) | **PASS** | `outputs/benchmark/storage_comparison.csv` (`8` dòng, trung bình 3 lần chạy) |
| **BM-03** | Đo kiểm hiệu năng 5 phép toán Spark (`Filter, GroupBy, Aggregation, Sort, Join`) qua 4 quy mô (`3` lần chạy/cấu hình) | **PASS** | `outputs/benchmark/processing_results.csv` (`20` dòng) |
| **BM-04** | Đo kiểm tối ưu hóa Partition (`1, 2, 4, 8 partitions`) ở quy mô `10x, 50x, 100x` trên Worker `2 CPU cores` | **PASS** | `outputs/benchmark/partition_results.csv` (`12` dòng) |

---

## 6. Ứng dụng Streamlit Dashboard (Milestone 7)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **UI-01** | Trang 1 — `Overview` (`1_Overview.py`) hiển thị đúng 6 KPI, Dataset Overview, Sơ đồ kiến trúc và Tiến độ | **PASS** | Đã kiểm chứng qua `AppTest` và đối chiếu `get_overview_kpis()` |
| **UI-02** | Trang 2 — `Data Analysis` (`2_Data_Analysis.py`) hiển thị đúng 5 KPI, 11 biểu đồ Plotly, bộ lọc 4 chiều và thứ tự `Monday → Sunday` | **PASS** | Đã kiểm chứng qua `AppTest` và `docs/MILESTONE_7_PHASE_D_REPORT.md` |
| **UI-03** | Trang 3 — `Machine Learning` (`3_Machine_Learning.py`) hiển thị đủ 7 mục (Model Comparison, Best Config, Feature Importance, Actual vs Predicted, Residuals, Test Explorer, Kaggle Download) | **PASS** | Đã kiểm chứng qua `AppTest` và `docs/MILESTONE_7_PHASE_E_REPORT.md` |
| **UI-04** | Trang 4 — `Big Data Benchmark` (`4_Big_Data_Benchmark.py`) hiển thị đủ 7 mục (6 KPIs, Scaling, CSV vs Parquet, 5 Ops, Partitions, Resource Usage, Methodology) | **PASS** | Đã kiểm chứng qua `AppTest` và `docs/MILESTONE_7_PHASE_F_REPORT.md` |

---

## 7. Kiểm thử Tích hợp & Hiệu năng (Integration & Performance Testing)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **INT-01** | `AppTest` chạy qua cả 4 trang và tương tác với các bộ lọc không phát sinh ngoại lệ (`0 exceptions`) | **PASS** | Kết quả kiểm thử Phase G (`docs/MILESTONE_7_PHASE_G_REPORT.md`) |
| **INT-02** | Chạy ứng dụng từ thư mục làm việc bên ngoài (`%TEMP%`) không gây lỗi đường dẫn | **PASS** | Kiểm thử độc lập thư mục làm việc đạt `PASS (0 exceptions)` |
| **INT-03** | Máy chủ HTTP trên cổng `8501` trả về `HTTP 200` và `/_stcore/health = ok` | **PASS** | Khởi động trong `12.46s`, RAM tiêu thụ `~76.0 MB` (`docs/MILESTONE_7_PHASE_G_REPORT.md`) |

---

## 8. An toàn Môi trường & Quản lý Phiên bản (Environment Safety & Release Control)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **ENV-01** | 6 container `langfuse-local-*` giữ nguyên trạng thái hoạt động (`Up` & `healthy`) | **PASS** | `docker ps` xác nhận cả 6 container `Up 44 hours` |
| **ENV-02** | 2 container Hadoop và 2 container Spark giữ nguyên trạng thái hoạt động (`Up` & `healthy`) | **PASS** | `docker ps` xác nhận `hadoop-namenode`, `hadoop-datanode`, `spark-master`, `spark-worker-1` `Up` |
| **ENV-03** | Không có tệp dữ liệu gốc (`data/raw/`) hoặc kết quả (`outputs/`) bị sửa đổi ngoài ý muốn | **PASS** | Kiểm toán toàn vẹn 37 artifacts trong Phase G khớp 100% |
| **GIT-01** | Đóng băng phiên bản Git (`git commit` & `git tag v1.0.0`) | **NOT VERIFIED (CHỜ XÁC NHẬN)** | `git status` ban đầu có các tệp của Milestone 2–7 chưa được commit vào nhánh `main`; theo quy tắc an toàn Giai đoạn A & E, đã tạm dừng trước khi tạo commit/tag để người dùng xác nhận |

---

## 9. Tài liệu Bàn giao (Project Documentation Deliverables)

| Mã | Tiêu chí nghiệm thu | Trạng thái | Bằng chứng xác minh (Evidence) |
| :---: | :--- | :---: | :--- |
| **DOC-01** | Báo cáo tổng kết dự án `docs/MILESTONE_7_FINAL_REPORT.md` đầy đủ 14 mục | **PASS** | Tệp tồn tại (`19,593` bytes), đầy đủ nội dung từ Milestone 1–7 |
| **DOC-02** | Báo cáo kiểm thử tích hợp `docs/MILESTONE_7_PHASE_G_REPORT.md` | **PASS** | Tệp tồn tại (`12,114` bytes), ghi nhận đầy đủ kết quả kiểm thử & kiểm toán số liệu |
| **DOC-03** | Kịch bản trình diễn `docs/DEMO_SCRIPT.md` và hướng dẫn `dashboard/README.md` | **PASS** | Tệp tồn tại (`11,973` bytes & `8,830` bytes), có lệnh `streamlit run dashboard/app.py --server.port 8501` |
| **DOC-04** | Tài liệu phát hành `docs/RELEASE_NOTES_v1.0.0.md` và `docs/RELEASE_FREEZE_REPORT.md` | **PASS** | Đã tạo đầy đủ trong thư mục `docs/` |

