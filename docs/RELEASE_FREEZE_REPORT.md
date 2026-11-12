# BÁO CÁO ĐÓNG BĂNG PHIÊN BẢN & CHUẨN BỊ NGHIỆM THU (RELEASE FREEZE REPORT — v1.0.0)

- **Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark (`Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark`)
- **Phiên bản mục tiêu:** `v1.0.0`
- **Repository:** `C:\Users\BOOK PRO\distributed_sys_docker_setup`
- **Nhánh Git hiện tại (`branch`):** `main`
- **Commit `HEAD` hiện tại:** `693eccb` (`fix hostname for hadoop`)
- **Remote:** `origin https://github.com/trung10kg/distributed_sys_docker_setup.git`
- **Trạng thái Tag `v1.0.0`:** **TẠM DỪNG CHỜ XÁC NHẬN (`PAUSED BY SAFETY RULE`)** do `git status` ban đầu chứa các tệp từ Milestone 2–7 chưa được commit.

---

## 1. Kết quả Kiểm tra Trạng thái Git (Giai đoạn A & E)

### 1.1. Lịch sử 5 commit gần nhất trên nhánh `main`
```text
693eccb fix hostname for hadoop
d00c3db fix spark on jupyter
6522113 test spark with jupyter
7d0cdc0 add nifi , jupyter, kafka ui
26fa3c0 add service for system
```

### 1.2. Phân loại chi tiết các thay đổi chưa commit trong Working Tree (`git status`)

Khi kiểm tra `git status` ở Giai đoạn A, working tree chứa toàn bộ các tệp đã được xây dựng xuyên suốt từ **Milestone 2 đến Milestone 7** nhưng chưa được tạo commit trong Git:

| Phân nhóm | Trạng thái Git | Đường dẫn tệp / thư mục | Nguồn gốc & Mục đích |
| :--- | :---: | :--- | :--- |
| **1. Cấu hình Hadoop HDFS** | `modified` | `hadoop/hadoop.env` | Thay đổi duy nhất 1 dòng ở Milestone 2: `HDFS_CONF_dfs_replication=2` → `HDFS_CONF_dfs_replication=1` để phù hợp với 1 DataNode |
| **2. Sao lưu & Precheck M2** | `untracked` | `backups/`, `MILESTONE_2_PRECHECK.md` | Bản sao lưu cấu hình có timestamp và báo cáo kiểm tra trước Milestone 2 |
| **3. Dữ liệu gốc (Raw CSV)** | `untracked` | `data/` (`data/raw/train.csv`, `test.csv`, `sampleSubmission.csv`) | Bộ dữ liệu gốc Kaggle Bike Sharing Demand chuẩn bị ở Milestone 1–2 |
| **4. Mã nguồn Spark Jobs** | `untracked` | `compute/spark/jobs/milestone3_hdfs_test.py`<br>`compute/spark/jobs/eda_bike_sharing.py`<br>`compute/spark/jobs/preprocess_bike_sharing.py`<br>`compute/spark/jobs/spark_sql_analytics.py`<br>`compute/spark/jobs/train_models.py`<br>`compute/spark/jobs/generate_benchmark_data.py`<br>`compute/spark/jobs/benchmark_storage.py`<br>`compute/spark/jobs/benchmark_processing.py`<br>`compute/spark/jobs/summarize_benchmark.py` | 9 kịch bản PySpark thực thi ở Milestone 3, 4, 5 và 6 |
| **5. Kết quả Thực nghiệm** | `untracked` | `outputs/` (`eda/`, `preprocessing/`, `analytics/`, `models/`, `benchmark/`) | 37 tệp kết quả CSV/JSON và 5 biểu đồ SVG từ Milestone 4, 5, 6 |
| **6. Ứng dụng Dashboard** | `untracked` | `dashboard/` (`app.py`, `config.py`, `data_loader.py`, `README.md`, `assets/`, `components/`, `pages/`) | Mã nguồn ứng dụng Streamlit Dashboard 4 trang hoàn thiện ở Milestone 7 |
| **7. Tài liệu & Báo cáo** | `untracked` | `docs/` (Toàn bộ báo cáo Milestone 2–7, `DATA_DICTIONARY.md`, `DEMO_SCRIPT.md`, `RELEASE_NOTES_v1.0.0.md`, `ACCEPTANCE_CHECKLIST.md`, `RELEASE_FREEZE_REPORT.md`) | Hệ thống báo cáo kỹ thuật, kịch bản demo và tài liệu phát hành `v1.0.0` |

### 1.3. Lý do Tạm dừng Giai đoạn E (`git commit` & `git tag v1.0.0`)
Theo đúng điều kiện an toàn bắt buộc của nhiệm vụ:
> *"Chỉ thực hiện [Giai đoạn E — Tạo phiên bản Git v1.0.0] khi: `git status` ban đầu sạch... Không tự động xóa, ghi đè hoặc commit các thay đổi chưa được xác nhận. Nếu phát hiện thay đổi mã nguồn hoặc dữ liệu chưa rõ mục đích, dừng trước khi tạo tag và báo cáo để người dùng quyết định."*

Do `git status` ban đầu chứa các tệp chưa commit của Milestone 2–7 nêu trên, tôi **chưa thực hiện `git commit` và chưa gắn tag `v1.0.0`** để chờ bạn xác nhận phương án đóng gói Git (xem Mục 5 bên dưới).

---

## 2. Xác nhận Tài liệu Bàn giao (Giai đoạn B, C, D)

Tất cả các tài liệu bàn giao đã được xác minh tồn tại, đầy đủ nội dung và nhất quán số liệu 100%:

| STT | Đường dẫn tài liệu | Dung lượng | Trạng thái |
| :---: | :--- | :---: | :---: |
| 1 | `docs/MILESTONE_7_FINAL_REPORT.md` | `19,593 bytes` | **PASS** (14 mục tổng kết toàn dự án) |
| 2 | `docs/MILESTONE_7_PHASE_G_REPORT.md` | `12,114 bytes` | **PASS** (Báo cáo kiểm thử tích hợp & kiểm toán dữ liệu) |
| 3 | `docs/DEMO_SCRIPT.md` | `11,973 bytes` | **PASS** (Kịch bản trình diễn 8–12 phút) |
| 4 | `dashboard/README.md` | `8,830 bytes` | **PASS** (Hướng dẫn chạy `streamlit run dashboard/app.py --server.port 8501`) |
| 5 | `docs/RELEASE_NOTES_v1.0.0.md` | Đã tạo mới | **PASS** (Ghi chú phát hành `v1.0.0` đầy đủ 10 mục) |
| 6 | `docs/ACCEPTANCE_CHECKLIST.md` | Đã tạo mới | **PASS** (31 tiêu chí nghiệm thu chia theo 9 nhóm) |
| 7 | `docs/RELEASE_FREEZE_REPORT.md` | Đã tạo mới | **PASS** (Báo cáo đóng băng phiên bản & bộ Q&A bảo vệ đồ án) |

---

## 3. Tổng hợp Trạng thái Nghiệm thu (`PASS` / `NOT VERIFIED`)

- **30 / 31 tiêu chí trong `docs/ACCEPTANCE_CHECKLIST.md` đạt `PASS` (có đầy đủ bằng chứng thực nghiệm):**
  - Kiến trúc & Hạ tầng (`INF-01` → `INF-04`): **PASS**
  - Dataset & Chất lượng dữ liệu (`DAT-01` → `DAT-04`): **PASS**
  - EDA & Spark SQL (`EDA-01` → `EDA-03`): **PASS**
  - Machine Learning (`ML-01` → `ML-05`): **PASS**
  - Big Data Benchmark (`BM-01` → `BM-04`): **PASS**
  - Streamlit Dashboard 4 trang (`UI-01` → `UI-04`): **PASS**
  - Integration & Performance Testing (`INT-01` → `INT-03`): **PASS**
  - An toàn môi trường Docker/Langfuse/Hadoop/Spark (`ENV-01` → `ENV-03`): **PASS**
  - Tài liệu bàn giao (`DOC-01` → `DOC-04`): **PASS**
- **1 / 31 tiêu chí ở trạng thái `NOT VERIFIED (CHỜ XÁC NHẬN)`:**
  - `GIT-01` (Tạo Git commit & tag `v1.0.0`): Đang chờ người dùng xác nhận phạm vi các tệp cần đưa vào commit phát hành.

---

## 4. Chuẩn bị Trình diễn & Bộ Câu hỏi Phản biện (Giai đoạn F — 10 đến 12 Phút)

### 4.1. Thứ tự trình diễn 10–12 phút (Đối chiếu với `docs/DEMO_SCRIPT.md`)

1. **Bước 1 — Giới thiệu bài toán và mục tiêu (1.5 phút):** Mở trang **Overview**, nêu bài toán điều phối xe đạp đô thị theo giờ và mục tiêu xây dựng pipeline Big Data trên Hadoop HDFS + Apache Spark.
2. **Bước 2 — Trình bày kiến trúc HDFS và Spark (1.5 phút):** Chỉ vào sơ đồ kiến trúc 8 bước và Sidebar (`1 NameNode + 1 DataNode, rep=1`, `1 Spark Master + 1 Worker, 2 cores, 2048 MB RAM`) cùng cơ chế tách biệt tầng trình diễn Streamlit (`@st.cache_data`).
3. **Bước 3 — Giới thiệu Dataset và kết quả EDA (2.0 phút):** Chuyển sang trang **Data Analysis**, trình bày `10,886` bản ghi giờ (`2,085,476` lượt thuê, trung bình `191.57/giờ`), quy luật 2 đỉnh giờ đi làm – tan tầm (`08:00` và `17:00`), mùa Thu cao điểm nhất (`234.42/giờ`), ma trận nhiệt độ – độ ẩm và mức tăng trưởng `+65.41%` giữa năm 2011 và 2012.
4. **Bước 4 — Trình bày mô hình ML và kết quả dự báo (2.5 phút):** Chuyển sang trang **Machine Learning**, giải thích cách chống Data Leakage (loại bỏ `casual, registered`), chia dữ liệu `Time-based Split (80/20)`, đối chiếu 3 mô hình và lý giải vì sao `Random Forest Regressor` chiến thắng (`RMSE = 99.1848, MAE = 66.3189, R² = 0.7922, RMSLE = 0.4016`), xem Feature Importance (`hour` dẫn đầu `30.45%`), phân tích phần dư và tải xuống tệp `bike_sharing_kaggle_submission.csv`.
5. **Bước 5 — Trình bày Benchmark CSV/Parquet và hiệu năng (2.0 phút):** Chuyển sang trang **Big Data Benchmark**, chỉ ra ở quy mô `100x` (`1,088,600` dòng), Parquet Snappy giảm dung lượng từ `110.40 MB` xuống `27.73 MB` (`-74.89%`) và tăng tốc `Read + Aggregation` gấp `8.41x`; phân tích hiệu năng 5 phép toán Spark và kết quả tối ưu hóa `4 partitions` trên Worker `2 CPU cores`.
6. **Bước 6 — Tương tác trực tiếp trên 4 trang Dashboard (1.0 phút):** Thao tác nhanh trên các bộ lọc giờ, chọn mô hình Validation, lọc tháng trên tập Test và lọc quy mô Benchmark.
7. **Bước 7 — Kết luận và hướng phát triển (1.0 phút):** Tóm tắt kết quả đạt được, các giới hạn thực nghiệm học thuật và hướng mở rộng sang cụm Multi-Node / Streaming.

---

### 4.2. Bộ 10 Câu hỏi & Trả lời Phản biện Nghiệm thu (Q&A Reference)

#### Câu 1: Vì sao chọn Apache Spark cho bài toán này?
- **Trả lời:** Apache Spark cung cấp một nền tảng hợp nhất (Unified Engine) cho phép thực hiện xuyên suốt cả 4 khâu trên cùng một kiến trúc phân tán: (1) Đọc/ghi trực tiếp trên Hadoop HDFS, (2) Tiền xử lý và truy vấn phân tích bằng **Spark SQL / DataFrame API** với bộ tối ưu hóa Catalyst & Whole-Stage Codegen, (3) Huấn luyện mô hình học máy bằng **Spark MLlib Pipeline**, và (4) Khả năng mở rộng quy mô dữ liệu từ chục nghìn dòng (`1x`) lên hàng triệu dòng (`100x = 1,088,600` dòng) mà không cần viết lại mã nguồn xử lý.

#### Câu 2: Vì sao dùng định dạng Parquet thay vì CSV cho các bước xử lý và huấn luyện?
- **Trả lời:** CSV là định dạng văn bản lưu theo dòng (row-oriented), không lưu kèm kiểu dữ liệu và phải quét toàn bộ tệp mỗi khi truy vấn. Ngược lại, **Apache Parquet (nén Snappy)** là định dạng nhị phân lưu theo cột (columnar storage), bảo toàn schema chính xác, hỗ trợ cắt tỉa cột (`Column Pruning`) và đẩy bộ lọc xuống tầng đọc (`Predicate Pushdown`). Thực nghiệm ở quy mô `100x` (`1,088,600` dòng) chứng minh Parquet giảm dung lượng lưu trữ từ **`110.40 MB` xuống `27.73 MB` (tiết kiệm `74.89%`)** và giúp truy vấn `Read + Aggregation` nhanh hơn **`8.41 lần`** (`2.56s` so với `21.52s`).

#### Câu 3: Vì sao chọn Random Forest Regressor làm mô hình tốt nhất?
- **Trả lời:** Nhu cầu thuê xe đạp phụ thuộc phi tuyến mạnh và có sự tương tác phức tạp giữa khung giờ (`hour`) với loại ngày (`workingday`: ngày đi làm có 2 đỉnh `08:00` và `17:00`, ngày nghỉ có 1 đỉnh trưa–chiều) cũng như nhiệt độ và độ ẩm. `Linear Regression` không nắm bắt được cấu trúc tương tác phi tuyến này (`R² = 0.4291`, `RMSE = 164.4157`). `Decision Tree` đơn lẻ nắm bắt được phi tuyến (`R² = 0.7854`) nhưng dễ nhạy cảm với phương sai cục bộ. **`Random Forest Regressor`** kết hợp 30 cây quyết định (`numTrees=30, maxDepth=8, subsamplingRate=0.8`) giúp giảm phương sai, đạt **`RMSE = 99.1848`**, **`R² = 0.7922`** và **`RMSLE = 0.4016`** tốt nhất trên tập Validation.

#### Câu 4: Các chỉ số RMSE, MAE, R² và RMSLE có ý nghĩa gì?
- **Trả lời:**
  - **RMSE (`99.1848`):** Căn bậc hai trung bình bình phương sai số (đơn vị: lượt thuê/giờ), phạt nặng các sai số lớn tại các giờ cao điểm.
  - **MAE (`66.3189`):** Trung bình trị tuyệt đối sai số, cho biết trung bình mỗi giờ dự báo lệch khoảng ~66 lượt thuê so với thực tế.
  - **R² (`0.7922`):** Hệ số xác định, cho biết mô hình giải thích được **79.22%** biến thiên của nhu cầu thuê xe thực tế.
  - **RMSLE (`0.4016`):** Sai số logarit trung bình căn bậc hai — độ đo chính thức của cuộc thi Kaggle Bike Sharing Demand, đánh giá sai số tương đối theo tỷ lệ phần trăm giữa dự báo và thực tế (được tính trên `prediction_clipped = max(prediction, 0.0)`).

#### Câu 5: Vì sao chia tập dữ liệu theo trình tự thời gian (Time-based Split 80/20) thay vì chia ngẫu nhiên (Random Split)?
- **Trả lời:** Dữ liệu thuê xe đạp là chuỗi thời gian có xu hướng tăng trưởng mạnh theo năm (`+65.41%` từ năm 2011 sang 2012) và có tính tự tương quan theo mùa/ngày. Nếu chia ngẫu nhiên (`Random Split`), mô hình sẽ được học các giờ của cuối năm 2012 rồi quay lại "dự đoán" các giờ liền kề trong cùng giai đoạn đó, gây ra rò rỉ thông tin tương lai (Temporal Leakage) và làm sai lệch độ chính xác thực tế. Chia **Time-based Split (80% đầu từ `01/2011 → 08/2012` làm Train, 20% cuối từ `08/2012 → 12/2012` làm Validation)** phản ánh trung thực bài toán dự báo tương lai.

#### Câu 6: Vì sao không dùng `casual` và `registered` làm đặc trưng đầu vào cho mô hình?
- **Trả lời:** Trong bộ dữ liệu gốc, `count = casual + registered` là đẳng thức cộng chính xác 100% trên mọi dòng dữ liệu. Hơn nữa, trong thực tế dự báo tương lai (và trong tệp `test.csv` của Kaggle), hai cột `casual` và `registered` hoàn toàn không tồn tại trước khi sự kiện thuê xe diễn ra. Nếu đưa `casual` hoặc `registered` vào mô hình sẽ gây ra lỗi **Data Leakage nghiêm trọng**, khiến mô hình chỉ học phép cộng tầm thường và không thể sử dụng được trên tập Test.

#### Câu 7: Dữ liệu mở rộng 10x, 50x và 100x trong Milestone 6 được tạo như thế nào?
- **Trả lời:** Từ tập đặc trưng chuẩn hóa `1x` (`10,886` dòng × `20` cột), kịch bản `generate_benchmark_data.py` thực hiện nhân bản có kiểm soát (controlled synthetic scaling) lên `10x` (`108,860` dòng), `50x` (`544,300` dòng) và `100x` (`1,088,600` dòng) với hạt giống cố định (`seed=42`), kết hợp dịch chuyển nhãn thời gian theo từng bản sao và cộng nhiễu chuẩn cực nhỏ có kiểm soát lên các biến liên tục (`temp, atemp, humidity, windspeed`) trong khi vẫn giữ nguyên phân phối thống kê và tính toàn vẹn của các cột phân loại.

#### Câu 8: Cụm Spark với 1 Master và 1 Worker có được xem là hệ thống phân tán không?
- **Trả lời:** Về mặt **kiến trúc phần mềm và giao thức thực thi**, đây là một cụm **Spark Standalone + HDFS phân tán đầy đủ**: có sự tách biệt rõ ràng giữa `NameNode` và `DataNode` (giao tiếp qua RPC port `9000`), giữa `Driver`, `Spark Master` (port `7077`) và `Spark Worker / Executor`, có cơ chế chia khối dữ liệu (HDFS blocks), phân vùng (`RDD/DataFrame partitions`), lập lịch task song song trên `2 CPU cores` và trao đổi dữ liệu qua `Shuffle`. Tuy nhiên, về mặt **hạ tầng vật lý**, toàn bộ các container chạy trên cùng một máy chủ (Single-Host Containerized Cluster), nên báo cáo luôn ghi chú minh bạch rằng kết quả chưa bao gồm độ trễ mạng vật lý liên máy chủ.

#### Câu 9: Kết quả thực nghiệm Benchmark có những hạn chế nào cần lưu ý?
- **Trả lời:** Có 4 giới hạn học thuật đã được ghi nhận rõ trong báo cáo và trên Dashboard:
  1. Tài nguyên giới hạn của Worker (`2 CPU cores`, `2048 MB Worker RAM`, `1024 MiB Executor`) khiến ở quy mô `100x` xuất hiện áp lực bộ nhớ và GC pause đối với các phép toán nặng về shuffle như `Sort` và `Join`.
  2. Chế độ đo I/O là **`Spark-Uncached / Warm OS Page Cache`** (xóa cache của Spark bằng `spark.catalog.clearCache()`, nhưng các tệp trên đĩa vẫn nằm trong Page Cache của nhân hệ điều hành Linux/WSL2).
  3. Lượt chạy đầu tiên ở quy mô `1x` chịu chi phí khởi tạo JVM, Catalyst Optimizer và biên dịch bytecode Whole-Stage Codegen nên chậm hơn `10x`.
  4. Dữ liệu `10x–100x` là dữ liệu tổng hợp có kiểm soát phục vụ đo kiểm kỹ thuật.

#### Câu 10: Vì sao không tính các độ đo RMSE, MAE, R², RMSLE trên tập Test (`6,493` dòng)?
- **Trả lời:** Theo thiết kế chuẩn của cuộc thi Kaggle Bike Sharing Demand, tệp `test.csv` (`6,493` dòng, tương ứng với các ngày từ 20 đến cuối mỗi tháng) **không cung cấp cột nhãn thực tế `count`**. Khi không có nhãn thực tế $y_i$, không thể tính toán sai số $y_i - \hat{y}_i$ trên tập Test. Do đó, mọi chỉ số đánh giá mô hình phải được tính trên tập **Validation (`2,178` dòng)** có nhãn thực tế, còn tập Test dùng để phân tích phân phối dự báo và xuất tệp `kaggle_submission.csv`.

---

## 5. Hướng dẫn Đóng băng Git `v1.0.0` (Chờ Người dùng Xác nhận)

Khi bạn đã kiểm tra và đồng ý đưa các tệp của dự án vào phiên bản phát hành `v1.0.0`, bạn có thể xác nhận để tôi thực thi (hoặc bạn trực tiếp chạy) một trong hai phương án sau:

- **Phương án 1 (Khuyến nghị — Đóng gói toàn bộ dự án Milestone 2–7 vào tag `v1.0.0`):**
  ```powershell
  git add hadoop/hadoop.env compute/ dashboard/ docs/ outputs/ data/ MILESTONE_2_PRECHECK.md
  git commit -m "release: finalize Bike Sharing Spark project v1.0.0"
  git tag -a v1.0.0 -m "Bike Sharing Demand Analysis and Forecasting v1.0.0"
  ```
- **Phương án 2 (Chỉ commit tài liệu phát hành nếu bạn muốn tự quản lý các thư mục khác riêng):**
  ```powershell
  git add docs/RELEASE_NOTES_v1.0.0.md docs/ACCEPTANCE_CHECKLIST.md docs/RELEASE_FREEZE_REPORT.md
  git commit -m "release: finalize Bike Sharing Spark project v1.0.0"
  git tag -a v1.0.0 -m "Bike Sharing Demand Analysis and Forecasting v1.0.0"
  ```

---

## 6. Kết luận về Trạng thái Sẵn sàng Trình diễn (Demo Readiness Conclusion)

- **Trạng thái kỹ thuật:** **SẴN SÀNG 100% (`READY FOR DEMO & ACCEPTANCE`)**.
- Toàn bộ 4 trang của **Streamlit Dashboard**, 37 tệp kết quả trong `outputs/`, các dịch vụ **Hadoop HDFS**, **Apache Spark**, **Langfuse** và bộ tài liệu bàn giao (`FINAL REPORT`, `PHASE G REPORT`, `DEMO SCRIPT`, `RELEASE NOTES`, `ACCEPTANCE CHECKLIST`) đều đã hoàn tất, nhất quán tuyệt đối và sẵn sàng phục vụ trình diễn nghiệm thu.

