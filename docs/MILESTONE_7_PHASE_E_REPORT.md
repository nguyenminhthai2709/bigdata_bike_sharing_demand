# BÁO CÁO TRIỂN KHAI MILESTONE 7 — PHASE E: MACHINE LEARNING DASHBOARD

- **Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark (`Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark`)
- **Giai đoạn:** Milestone 7 — Phase E (Implement Machine Learning Dashboard)
- **Tệp triển khai chính:** `dashboard/pages/3_Machine_Learning.py` và `dashboard/data_loader.py` (`get_ml_artifacts()`, `load_raw_bytes()`)
- **Trạng thái:** **HOÀN THÀNH & ĐÃ KIỂM THỬ (12/12 HẠNG MỤC ĐẠT 100%)**

---

## 1. Tuân thủ Nguyên tắc Thiết kế & Toàn vẹn Dữ liệu

1. **100% Dữ liệu thực từ Artifacts Milestone 5 (`outputs/models/`):**
   - Không khởi tạo `SparkSession`, không truy vấn HDFS trực tiếp từ giao diện Streamlit, không huấn luyện lại mô hình.
   - Không tạo dữ liệu giả; toàn bộ bảng, KPI và biểu đồ đều đọc từ 6 tệp kết quả đã tạo ở Milestone 5 thông qua `@st.cache_data` trong `dashboard/data_loader.py`.
2. **Bảo toàn tuyệt đối các giai đoạn trước:**
   - Không sửa đổi bất kỳ tệp kết quả nào của Milestone 2–6.
   - Trang `1_Overview.py` và `2_Data_Analysis.py` tiếp tục hoạt động ổn định, không phát sinh lỗi.

---

## 2. Kiểm tra Schema Thực tế của 6 Artifacts Milestone 5

| STT | Tệp dữ liệu (`outputs/models/`) | Số dòng | Số cột / Keys | Cột / Cấu trúc thực tế |
| :--- | :--- | :---: | :---: | :--- |
| 1 | `model_comparison_metrics.csv` | `3` | `10` | `model_key, model_name, rmse, mae, r2, rmse_clipped, rmsle, negative_predictions_count, training_time_sec, params_json` |
| 2 | `best_model_metadata.json` | JSON | `14` keys | `best_model_key, best_model_name, hdfs_model_path, target_column, feature_columns (18), excluded_leakage_columns (casual, registered), excluded_non_feature_columns (datetime), time_based_split, baseline_mean_predictor, best_model_params, best_model_validation_metrics, all_models_comparison, top_feature_importances, created_at_utc` |
| 3 | `feature_importances.csv` | `18` | `3` | `feature, rf_importance, dt_importance` |
| 4 | `validation_predictions.csv` | `2,178` | `10` | `datetime, actual_count, lr_prediction, lr_prediction_clipped, dt_prediction, dt_prediction_clipped, rf_prediction, rf_prediction_clipped, best_prediction, best_prediction_clipped` |
| 5 | `test_predictions.csv` | `6,493` | `3` | `datetime, prediction, prediction_clipped` |
| 6 | `kaggle_submission.csv` | `6,493` | `2` | `datetime, count` (0 giá trị null) |

---

## 3. Chi tiết các Hợp phần đã Triển khai trên `dashboard/pages/3_Machine_Learning.py`

### A. Section 1 — Model Performance Comparison
- **5 Thẻ KPI tổng hợp mô hình tốt nhất (`Random Forest Regressor`):**
  - `Best Model`: `Random Forest Regressor`
  - `Validation RMSE`: `99.18` (chính xác `99.1848`, giảm `57.6%` so với Baseline Mean Predictor `233.9322`)
  - `Validation MAE`: `66.32` (chính xác `66.3189`)
  - `Validation R²`: `0.7922` (`79.22%`)
  - `Validation RMSLE`: `0.4016`
- **Ghi chú quy ước tính toán rõ ràng trên giao diện:**
  - `RMSE`, `MAE`, `R²` được tính trên giá trị dự đoán gốc (`prediction`).
  - `RMSLE` được tính trên giá trị dự đoán đã cắt âm (`prediction_clipped = max(prediction, 0.0)`).
- **Bảng tổng hợp & 2 Biểu đồ cột nhóm Plotly:**
  - So sánh trực quan `Linear Regression` (`RMSE=164.4157, MAE=121.8134, R²=0.4291, RMSLE=1.0940`), `Decision Tree Regressor` (`RMSE=100.7967, MAE=65.1976, R²=0.7854, RMSLE=0.4382`) và `Random Forest Regressor` (`RMSE=99.1848, MAE=66.3189, R²=0.7922, RMSLE=0.4016`).
  - Kèm khung giải thích công thức toán học và ý nghĩa của `RMSE`, `MAE`, `R²`, `RMSLE`.

### B. Section 2 — Best Model & Configuration
- Trích xuất trực tiếp từ `outputs/models/best_model_metadata.json`:
  - **Hyperparameters của `Random Forest Regressor`:** `numTrees = 30`, `maxDepth = 8`, `minInstancesPerNode = 5`, `featureSubsetStrategy = "auto"`, `subsamplingRate = 0.8`, `seed = 42`.
  - **Phương pháp Time-based Split (80/20):**
    - Tập Train (`80%`): `8,708` bản ghi (`2011-01-01 00:00:00` → `2012-08-05 04:00:00`).
    - Tập Validation (`20%`): `2,178` bản ghi (`2012-08-05 05:00:00` → `2012-12-19 23:00:00`).
  - **Kiểm soát Data Leakage:** Loại bỏ hoàn toàn `casual`, `registered` và `datetime` khỏi `VectorAssembler` (sử dụng đúng `18` đặc trưng).
  - **Biểu đồ ngang so sánh Thời gian Huấn luyện (`training_time_sec`):** `Linear Regression = 10.5608 s`, `Decision Tree = 18.7804 s`, `Random Forest = 34.3088 s`.

### C. Section 3 — Feature Importance
- Đọc từ `outputs/models/feature_importances.csv` (`18` dòng).
- Hỗ trợ 3 chế độ xem tương tác:
  1. **Top 10 đặc trưng quan trọng nhất (Random Forest)** — sắp xếp giảm dần từ trên xuống dưới: `hour` (`30.45%`), `hour_sin` (`18.02%`), `hour_cos` (`11.15%`), `atemp` (`8.96%`), `year` (`7.70%`), `temp` (`7.35%`), `workingday` (`3.34%`), `humidity` (`2.59%`), `is_weekend` (`2.58%`), `season` (`2.04%`).
  2. **So sánh Top 10: Random Forest vs Decision Tree**.
  3. **Toàn bộ 18 đặc trưng (Random Forest)** kèm bảng chi tiết đầy đủ 18 đặc trưng trong `st.expander`.

### D. Section 4 — Actual vs Predicted (Validation Set — `2,178` bản ghi)
- Đọc từ `outputs/models/validation_predictions.csv` (`2,178` dòng).
- Bộ lọc tương tác: chọn mô hình (`Random Forest Regressor`, `Decision Tree Regressor`, `Linear Regression`), chọn khoảng ngày trên tập Validation (`2012-08-05` → `2012-12-19`), và tùy chọn chồng đồng thời cả 3 mô hình trên biểu đồ chuỗi thời gian.
- **Biểu đồ 1:** Scatter Plot `Actual Rentals` (trục X) vs `Predicted Rentals` (trục Y) kèm đường tham chiếu lý tưởng $y = x$.
- **Biểu đồ 2:** Time-Series Line Chart đối chiếu đường `Actual Count` và `Predicted Count` theo từng giờ.

### E. Section 5 — Residual Analysis (Validation Set — `2,178` bản ghi)
- Tính trực tiếp trong bộ nhớ: $\text{Residual} = \text{Actual} - \text{Predicted}$ trên giá trị dự đoán gốc (`rf_prediction`, `dt_prediction`, `lr_prediction`).
- **4 KPI Cards cho phần dư của mô hình đang chọn (với Random Forest):**
  - `Mean Residual (Bias)`: `+42.37` lượt/giờ (phản ánh xu hướng tăng trưởng mạnh của nhu cầu thuê xe vào cuối năm 2012).
  - `Validation MAE (|Residual|)`: `66.32` lượt/giờ (`66.3187`, khớp với bảng đánh giá).
  - `Residual StdDev`: `89.70` lượt/giờ (`Median Residual = +19.42` lượt/giờ).
  - `Tỷ lệ Residual > 0`: `66.8%` (`1,455 / 2,178` giờ).
- **Biểu đồ 1:** Histogram phân phối phần dư kèm đường dọc tham chiếu $\text{Residual} = 0$.
- **Biểu đồ 2:** Scatter Plot $\text{Residual}$ theo $\text{Predicted}$ kèm đường ngang tham chiếu $\text{Residual} = 0$.
- Giải thích rõ ý nghĩa $\text{Residual} > 0$ (dự báo thấp hơn thực tế), $\text{Residual} < 0$ (dự báo cao hơn thực tế) và $\text{Residual} \approx 0$.

### F. Section 6 — Test Prediction Explorer (`6,493` bản ghi)
- Đọc từ `outputs/models/test_predictions.csv` (`6,493` dòng: `datetime, prediction, prediction_clipped`).
- Hiển thị 4 KPI Cards (`6,493` bản ghi, dự báo trung bình `183.40` lượt/giờ, trung vị `172.69` lượt/giờ, khoảng `[4.08, 734.98]` lượt/giờ, `0` dự đoán âm).
- Bộ lọc theo tháng (`2011-01` → `2012-12`) và ô tìm kiếm theo chuỗi `datetime`.
- Biểu đồ chuỗi thời gian dự báo trên tập Test, biểu đồ Histogram phân phối dự báo trên toàn bộ `6,493` mẫu, và bảng xem trước dữ liệu.

### G. Section 7 — Kaggle Submission (`6,493` bản ghi)
- Đọc và kiểm định tự động `outputs/models/kaggle_submission.csv`:
  - Có header hợp lệ, đúng `6,493` dòng dữ liệu, đúng 2 cột `datetime, count`, `0` giá trị null, giá trị `count` nguyên không âm từ `4` đến `735`.
- Hiển thị bảng xem trước (10 dòng đầu & 5 dòng cuối) và nút `st.download_button` tải xuống tệp `bike_sharing_kaggle_submission.csv` giữ nguyên 100% nội dung byte gốc của `outputs/models/kaggle_submission.csv`.

---

## 4. Kết quả Kiểm thử Giai đoạn E (12/12 Hạng mục)

| STT | Hạng mục kiểm thử | Kết quả | Chi tiết xác minh |
| :---: | :--- | :---: | :--- |
| 1 | Nạp đầy đủ 6 tệp trong `outputs/models/` | **PASS** | `get_ml_artifacts()` nạp thành công cả 6 tệp qua `@st.cache_data` |
| 2 | Độ đo 3 mô hình khớp Milestone 5 | **PASS** | `RF (RMSE=99.1848, R²=0.7922, RMSLE=0.4016)`, `DT (RMSE=100.7967)`, `LR (RMSE=164.4157)` |
| 3 | Feature Importance khớp CSV | **PASS** | Đúng `18` đặc trưng, sắp xếp giảm dần (`hour=30.45%`, `hour_sin=18.02%`, `hour_cos=11.15%`, ...) |
| 4 | Validation predictions có `2,178` dòng | **PASS** | `len(val_df) == 2178` (`2012-08-05 05:00:00` → `2012-12-19 23:00:00`) |
| 5 | Test predictions có `6,493` dòng | **PASS** | `len(test_df) == 6493` (`2011-01-20 00:00:00` → `2012-12-31 23:00:00`) |
| 6 | Kaggle submission hợp lệ (`6,493` dòng) | **PASS** | Đúng header `datetime, count`, `6,493` dòng, `0` nulls |
| 7 | Tính toán Residual chính xác | **PASS** | `Residual = actual_count - prediction`, trung bình `|rf_residual| = 66.3187` khớp `MAE` |
| 8 | Biểu đồ hiển thị không lỗi | **PASS** | Toàn bộ 8 biểu đồ Plotly trên trang Machine Learning render thành công |
| 9 | Bộ lọc tương tác hoạt động ổn định | **PASS** | Kiểm thử qua `AppTest` với radio, selectbox, date_input, checkbox, text_input: `0 exceptions` |
| 10 | Nút tải xuống Kaggle Submission hoạt động | **PASS** | `st.download_button` trả đúng byte nguyên bản của `outputs/models/kaggle_submission.csv` |
| 11 | Trang `Overview` và `Data Analysis` không bị ảnh hưởng | **PASS** | Chạy `AppTest` qua cả 3 trang `Overview`, `Data Analysis`, `Machine Learning`: `0 exceptions` |
| 12 | Kiểm thử HTTP Server thực tế trên cổng `8501` | **PASS** | `/_stcore/health = ok`, trang chính trả về `HTTP 200` (đã dừng tiến trình test sạch sẽ) |

