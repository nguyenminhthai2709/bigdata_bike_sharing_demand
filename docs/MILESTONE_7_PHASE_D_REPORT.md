# BÁO CÁO KẾT QUẢ TRIỂN KHAI MILESTONE 7 — GIAI ĐOẠN D: DATA ANALYSIS DASHBOARD

**Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Tên ứng dụng:** Bike Sharing Demand Analysis & Forecasting Dashboard  
**Tệp triển khai chính:** [dashboard/pages/2_Data_Analysis.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/pages/2_Data_Analysis.py) & [dashboard/data_loader.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/data_loader.py)  
**Ngày thực hiện:** 2026-11-12  
**Trạng thái:** **HOÀN THÀNH 100% GIAI ĐOẠN D (VERIFIED & READY FOR PHASE E)**

---

## 1. Danh sách các Artifacts Đã Sử dụng

Toàn bộ dữ liệu hiển thị trên Trang 2 (`Data Analysis`) được đọc trực tiếp qua hàm `get_data_analysis_artifacts()` trong [dashboard/data_loader.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/dashboard/data_loader.py) (gắn `@st.cache_data`) từ các kết quả thực tế của Milestone 4:

| STT | Nhóm | Đường dẫn tệp nguồn | Số dòng $\times$ Số cột | Vai trò trên trang Data Analysis |
| :---: | :--- | :--- | :---: | :--- |
| 1 | **EDA** | [outputs/eda/b2_target_count_stats.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b2_target_count_stats.csv) | `1` $\times$ `14` | Trích xuất KPI `Total Rentals` (`2,085,476`) và `Average Rentals` (`191.57`). |
| 2 | **EDA** | [outputs/eda/b4_demand_by_temp_humidity.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b4_demand_by_temp_humidity.csv) | `12` $\times$ `5` | Xây dựng biểu đồ Heatmap 2 chiều `temp_range` (4 nhóm) $\times$ `humidity_level` (3 nhóm). |
| 3 | **EDA** | [outputs/eda/b3_demand_by_year.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b3_demand_by_year.csv) | `2` $\times$ `6` | Biểu đồ tăng trưởng nhu cầu trung bình giữa năm `2011` (`144.22`) và `2012` (`238.56` — `+65.41%`). |
| 4 | **EDA** | [outputs/eda/b2_target_count_distribution.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b2_target_count_distribution.csv) | `5` $\times$ `4` | Bảng phân bố 5 phân khúc nhu cầu (`Very Low` $\rightarrow$ `Peak`). |
| 5 | **EDA** | [outputs/eda/b1_descriptive_statistics.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/eda/b1_descriptive_statistics.csv) | `11` $\times$ `9` | Bảng thống kê mô tả (`mean, stddev, min, p25, median, p75, max`) của 11 biến số. |
| 6 | **SQL** | [outputs/analytics/q1_hourly_demand.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q1_hourly_demand.csv) | `24` $\times$ `8` | KPI `Peak Hour` (`17:00` — `468.77`) & Biểu đồ nhu cầu theo 24 giờ (`Hourly Average` tách biệt với `Total Rentals`). |
| 7 | **SQL** | [outputs/analytics/q2_dayofweek_demand.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q2_dayofweek_demand.csv) | `7` $\times$ `8` | Biểu đồ nhu cầu trung bình theo thứ (`Friday: 197.84`) và tổng lượt thuê theo thứ (`Saturday: 311,518`), sắp xếp chuẩn `Monday → Sunday`. |
| 8 | **SQL** | [outputs/analytics/q3_seasonal_demand.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q3_seasonal_demand.csv) | `4` $\times$ `9` | KPI `Highest Seasonal Demand` (`Fall: 234.42`) & Biểu đồ nhu cầu trung bình theo 4 mùa. |
| 9 | **SQL** | [outputs/analytics/q4_weather_demand.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q4_weather_demand.csv) | `4` $\times$ `10` | Biểu đồ ảnh hưởng của 4 nhóm thời tiết đối chiếu với [docs/DATA_DICTIONARY.md](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docs/DATA_DICTIONARY.md). |
| 10 | **SQL** | [outputs/analytics/q5_workingday_comparison.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q5_workingday_comparison.csv) | `2` $\times$ `9` | KPI `Registered Share on Working Days` (`86.99%`) & Biểu đồ cột chồng tỷ trọng `Casual` vs `Registered`. |
| 11 | **SQL** | [outputs/analytics/q6_monthly_yearly_trend.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q6_monthly_yearly_trend.csv) | `24` $\times$ `9` | Biểu đồ xu hướng nhu cầu trung bình qua 24 tháng (`2011-01` $\rightarrow$ `2012-12`) kèm đường nhiệt độ `avg_temp_c`. |
| 12 | **SQL** | [outputs/analytics/q7_top10_peak_hours.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q7_top10_peak_hours.csv) | `10` $\times$ `7` | Bảng Top 10 khung giờ cao điểm nhất theo loại ngày. |
| 13 | **SQL** | [outputs/analytics/q8_user_behavior_casual_vs_registered.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/analytics/q8_user_behavior_casual_vs_registered.csv) | `48` $\times$ `7` | Biểu đồ so sánh 24 giờ giữa `Working Day` vs `Non-working Day` và phân tách `avg_casual` vs `avg_registered`. |

---

## 2. Bảng Mapping CSV, Tên Cột và Các Biểu đồ Đã Triển khai

| Phân khu | Tên Biểu đồ / Thành phần | Tệp nguồn | Các cột sử dụng chính xác | Đơn vị hiển thị |
| :--- | :--- | :--- | :--- | :--- |
| **A. KPIs** | 5 Thẻ KPI Tổng quan Phân tích | `b2_target_count_stats.csv`, `q1`, `q3`, `q5` | `total_rentals`, `mean`, `hour`, `avg_total_demand`, `season_name`, `registered_share_pct` | `lượt thuê`, `lượt/giờ`, `%` |
| **B. Hourly** | Biểu đồ 1: Nhu cầu 24 Giờ (Chuyển đổi `Hourly Average` / `Total Rentals`) | `q1_hourly_demand.csv` | `hour`, `avg_total_demand`, `median_total_demand`, `total_rentals`, `total_records` | `lượt/giờ` hoặc `lượt` (tách biệt rõ) |
| **B. Hourly** | Biểu đồ 2: So sánh Working Day vs Non-working Day theo 24 Giờ | `q8_user_behavior_casual_vs_registered.csv` | `hour`, `workingday`, `avg_total` | `lượt/giờ` |
| **B. Hourly** | Bảng Top 10 Khung giờ Cao điểm | `q7_top10_peak_hours.csv` | `hour`, `day_category`, `total_records`, `avg_total_demand`, `max_demand_recorded`, `avg_casual`, `avg_registered` | `lượt/giờ`, `lượt` |
| **C. Weekly & Seasonal** | Biểu đồ 3: Nhu cầu Trung bình theo Thứ (`Monday → Sunday`) | `q2_dayofweek_demand.csv` | `day_name`, `avg_total_demand`, `is_weekend`, `total_records` | `lượt/giờ` |
| **C. Weekly & Seasonal** | Biểu đồ 4: Tổng Lượt thuê theo Thứ (`Monday → Sunday`) | `q2_dayofweek_demand.csv` | `day_name`, `total_rentals`, `is_weekend`, `total_records` | `lượt` |
| **C. Weekly & Seasonal** | Biểu đồ 5: Nhu cầu Trung bình theo Mùa | `q3_seasonal_demand.csv` | `season_name`, `avg_total_demand`, `total_rentals`, `avg_temp_c`, `avg_humidity_pct` | `lượt/giờ` |
| **D. Weather** | Biểu đồ 6: Nhu cầu Trung bình theo Điều kiện Thời tiết | `q4_weather_demand.csv` | `weather`, `weather_condition`, `avg_total_demand`, `total_records`, `total_rentals`, `avg_temp_c`, `avg_humidity_pct` | `lượt/giờ` |
| **E. Temp & Humidity** | Biểu đồ 7: Heatmap 2D Nhiệt độ $\times$ Độ ẩm (`4 × 3`) | `b4_demand_by_temp_humidity.csv` | `temp_range`, `humidity_level`, `avg_count`, `records` | `lượt/giờ` |
| **F. User Segmentation** | Biểu đồ 8: Tỷ trọng Lượt thuê Casual vs Registered | `q5_workingday_comparison.csv` | `day_category`, `registered_share_pct`, `casual_share_pct`, `avg_registered`, `avg_casual` | `%` (Tỷ trọng lượt thuê) |
| **F. User Segmentation** | Biểu đồ 9: Đường cong 24 Giờ của Registered & Casual | `q8_user_behavior_casual_vs_registered.csv` | `hour`, `workingday`, `avg_registered`, `avg_casual` | `lượt/giờ` |
| **G. Yearly Growth** | Biểu đồ 10: So sánh Nhu cầu Trung bình Năm 2011 vs 2012 | `b3_demand_by_year.csv` | `year`, `avg_count`, `total_count`, `records`, `avg_casual`, `avg_registered` | `lượt/giờ` (`+65.41%`) |
| **G. Yearly Growth** | Biểu đồ 11: Xu hướng 24 Tháng (`2011-01 → 2012-12`) & Nhiệt độ | `q6_monthly_yearly_trend.csv` | `year_month`, `year`, `avg_total_demand`, `total_rentals`, `avg_temp_c` | `lượt/giờ`, `°C` |

---

## 3. Đối chiếu Số liệu Hiển thị với Báo cáo Milestone 4

Tất cả các con số tham chiếu đều khớp 100% với artifacts thực tế từ Milestone 4:
1. **KPIs đầu trang:**
   - `Total Rentals`: **`2,085,476`** lượt thuê (`b2_target_count_stats.csv`).
   - `Average Rentals`: **`191.57`** lượt/giờ (`b2_target_count_stats.csv`).
   - `Peak Hour`: **`17:00`** với **`468.77`** lượt/giờ (`q1_hourly_demand.csv`).
   - `Highest Seasonal Demand`: **`Fall`** với **`234.42`** lượt/giờ (`q3_seasonal_demand.csv`).
   - `Registered Share on Working Days`: **`86.99%`** (`q5_workingday_comparison.csv`).
2. **Phân tích theo Giờ (`q1`, `q8`):**
   - Toàn tập Train (`q1`): `17:00` (`468.77`), `18:00` (`430.86`), `08:00` (`362.77`), thấp nhất `04:00` (`6.41`).
   - Trên `Working Day` (`q8`): 3 đỉnh cao nhất là `17:00` (`529.21`), `18:00` (`495.49`) và `08:00` (`479.95`).
3. **Phân tích theo Tuần & Mùa (`q2`, `q3`):**
   - Sắp xếp chuẩn `Monday → Sunday` bằng `pd.Categorical(..., ordered=True)`.
   - `Friday` có nhu cầu trung bình cao nhất: **`197.84`** lượt/giờ.
   - `Saturday` có tổng lượt thuê cao nhất: **`311,518`** lượt.
   - Theo mùa: `Fall` (**`234.42`**), `Summer` (**`215.25`**), `Winter` (**`198.99`**), `Spring` (**`116.34`**).
4. **Phân tích Thời tiết & Heatmap Nhiệt độ–Độ ẩm (`q4`, `b4`):**
   - `Clear / Partly Cloudy`: **`205.24`** (`n = 7,192`); `Mist / Cloudy`: **`178.96`** (`n = 2,834`); `Light Snow / Light Rain`: **`118.85`** (`n = 859`); `Heavy Rain / Ice Pellets`: **`164.00`** (`n = 1` bản ghi duy nhất, đã ghi chú rõ trên giao diện).
   - Heatmap `b4_demand_by_temp_humidity.csv`: Đầy đủ 12 ô (`4 temp_range` $\times$ `3 humidity_level`), cao nhất tại `3. Warm (20-30C)` + `Low (<40%)` đạt **`390.03`** lượt/giờ (`n = 518`).
5. **Phân khúc Người dùng & Tăng trưởng Năm (`q5`, `b3_demand_by_year`):**
   - `Working Day`: `Registered = 86.99%`, `Casual = 13.01%`.
   - `Non-Working Day`: `Registered = 68.54%`, `Casual = 31.46%` (đã chú thích rõ là tỷ trọng số lượt thuê).
   - Tăng trưởng năm: `2011` (**`144.22`**) $\rightarrow$ `2012` (**`238.56`**), tăng **`+65.41%`**.

---

## 4. Bộ lọc Tương tác & Những Nội dung Giới hạn theo Cấu trúc Artifacts

- **Các bộ lọc đã triển khai:**
  1. `Khoảng giờ trong ngày (Hour 0–23)`: Lọc trực tiếp các bảng `q1_hourly_demand.csv` và `q8_user_behavior_casual_vs_registered.csv`.
  2. `Loại ngày (Working Day / Non-working Day)`: Lọc trực tiếp bảng `q8` và bảng `q7_top10_peak_hours.csv`.
  3. `Mùa trong năm (Season)`: Lọc trực tiếp biểu đồ mùa `q3_seasonal_demand.csv`.
  4. `Điều kiện thời tiết (Weather)`: Lọc trực tiếp biểu đồ thời tiết `q4_weather_demand.csv`.
  5. `Nút chuyển đổi đại lượng (Radio Toggle)`: Cho phép chuyển đổi rõ ràng giữa `Hourly Average (avg_total_demand)` và `Total Rentals (total_rentals)` trên biểu đồ `q1`.
- **Giới hạn kỹ thuật minh bạch:** Vì các tệp trong `outputs/analytics/` và `outputs/eda/` là các bảng đã tổng hợp sẵn theo từng chiều độc lập (ví dụ `q3` tổng hợp theo `season`, `q1` tổng hợp theo `hour`), bộ lọc của chiều nào chỉ áp dụng cho các bảng có chứa cột của chiều đó — tuyệt đối không tự suy diễn phép lọc chéo đa chiều (như lọc `Hour` trên bảng `Season`) để đảm bảo tính chính xác thống kê 100%.

---

## 5. Kết quả Kiểm thử Giai đoạn D

1. **Kiểm tra đọc toàn bộ 14 tệp CSV/JSON của trang Data Analysis:** `PASS`.
2. **Kiểm tra thứ tự `Monday → Sunday` trên `q2_dayofweek`:** `PASS` (`['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']`).
3. **Kiểm tra đối chiếu 5 KPIs với báo cáo Milestone 4:** `PASS` (`2,085,476`, `191.57`, `17:00`, `Fall`, `86.99%`).
4. **Kiểm thử `streamlit.testing.v1.AppTest` trên cả Trang 1 (`Overview`) và Trang 2 (`Data Analysis`) kèm thao tác tương tác widget (`radio` toggle):** `PASS (0 exceptions)`.
5. **Kiểm thử khởi chạy Streamlit Server trên cổng `8501`:** `PASS` (`HTTP 200 OK`, `/_stcore/health = ok`).

