# BÁO CÁO KỸ THUẬT MILESTONE 6: BIG DATA SCALING & DISTRIBUTED PERFORMANCE BENCHMARK

**Dự án:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Tên tiếng Việt:** Phân tích dữ liệu quy mô lớn và dự đoán nhu cầu sử dụng xe đạp chia sẻ bằng Apache Spark  
**Ngày thực hiện:** 2026-11-11  
**Trạng thái:** Hoàn thành 100% (Giai đoạn A $\rightarrow$ G)

> [!IMPORTANT]
> **Tuyên bố phạm vi môi trường thực nghiệm:** Toàn bộ kết quả đo kiểm trong báo cáo này được thực hiện trên môi trường **Single-machine / Containerized Apache Spark Standalone** chạy trên một máy vật lý duy nhất (`Windows 11 + Docker Desktop WSL2`) với **1 Spark Master**, **1 Spark Worker** (`2 CPU cores`, `2 GiB RAM`, `1 GiB Executor Heap`) và **1 HDFS DataNode** (`dfs.replication = 1`). Kết quả phản ánh chính xác hành vi mở rộng quy mô dữ liệu, định dạng lưu trữ và cơ chế phân vùng của Apache Spark trong môi trường container hóa một nút, **không đại diện cho hiệu năng tuyệt đối của cụm nhiều máy chủ vật lý (multi-node physical cluster)**.

---

## 1. Mục tiêu Benchmark

Milestone 6 đánh giá thực nghiệm khả năng mở rộng và hiệu năng xử lý của Apache Spark + Hadoop HDFS thông qua 6 mục tiêu cụ thể:
1. **Mở rộng dữ liệu có kiểm soát (Controlled Data Scaling):** Nhân bản có kiểm soát từ tập dữ liệu chuẩn `1x` (`10,886` dòng) lên các mức quy mô `10x` (`108,860` dòng), `50x` (`544,300` dòng) và `100x` (`1,088,600` dòng).
2. **Đánh giá định dạng lưu trữ (CSV vs Parquet Snappy):** Đo lường dung lượng lưu trữ trên HDFS, thời gian ghi (`Write Time`), thời gian đọc kèm đếm bản ghi (`Read + Count`) và thời gian đọc kèm tổng hợp cột (`Read + Aggregation`) qua 3 lần lặp (`3 runs`) cho mỗi cấu hình.
3. **Đánh giá thời gian xử lý 5 phép toán Spark cốt lõi (`Spark Processing Benchmark`):** Đo thời gian thực thi và độ lệch chuẩn của `Filter`, `GroupBy`, `Aggregation`, `Sort`, và `Join` trên cả 4 mức quy mô.
4. **Đánh giá ảnh hưởng của số lượng Partition (`Partition Tuning Benchmark`):** Thử nghiệm `1`, `2`, `4`, và `8` partitions trên các quy mô `10x`, `50x`, `100x`; phân biệt rõ giữa *HDFS file/block*, *Spark input partition* và *Spark DataFrame partition*.
5. **Đo thông lượng (`Throughput`):** Tính toán thông lượng xử lý thực tế theo công thức chuẩn $\text{Throughput} = \text{Number of Processed Rows} / \text{Processing Time (seconds)}$.
6. **Theo dõi tài nguyên hệ thống (`CPU & Memory Usage`) và trực quan hóa:** Ghi nhận mức tiêu thụ CPU (%), RAM (MB) và xuất bộ 5 biểu đồ chuẩn SVG cùng bảng dữ liệu tổng hợp tại `outputs/benchmark/`.

---

## 2. Kiến trúc môi trường thực nghiệm

| Thành phần | Thông số cấu hình thực tế | Ghi chú kỹ thuật |
| :--- | :--- | :--- |
| **Máy chủ vật lý (Host)** | Windows 11, CPU Intel Core i7-1165G7, RAM vật lý 16 GB | Docker Desktop chạy trên nền WSL2 (`Linux 6.18.33.2-microsoft-standard-WSL2`). |
| **Giới hạn tài nguyên Docker** | `7.597 GiB` tổng RAM cấp cho WSL2 Docker | Chia sẻ cùng 6 container `langfuse-local-*` đang hoạt động (~3.02 GiB RAM). |
| **Dung lượng ổ đĩa C:** | Còn trống `14.39 GB` (trước Benchmark) $\rightarrow$ `13.87 GB` (sau Benchmark) | Luôn cao hơn gấp ~2.8 lần ngưỡng an toàn tối thiểu `5.0 GB`. |
| **HDFS Cluster** | `hadoop-namenode` + `hadoop-datanode` (`Hadoop 3.2.1`, Java 8) | Cấu hình `dfs.replication = 1`, giao tiếp qua `hdfs://hadoop-namenode:9000`. |
| **Apache Spark Cluster** | `spark-master` + `spark-worker-1` (`Spark 3.5.6`, OpenJDK `17.0.15`, Python `3.12.10`) | Chế độ Standalone (`spark://spark-master:7077`). |
| **Cấu hình Spark Job** | `spark.cores.max = 2`, `spark.executor.memory = 1g`, `spark.sql.shuffle.partitions = 4` | Mỗi kịch bản chạy độc lập tuần tự, không chạy chồng chéo. |

---

## 3. Phương pháp tạo dữ liệu tổng hợp (Synthetic Data Generation)

Kịch bản [generate_benchmark_data.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/spark/jobs/generate_benchmark_data.py) tạo các bộ dữ liệu mở rộng từ tập nguồn `/bike-sharing/features/train` (`10,886` dòng, `20` cột) theo quy trình chặt chẽ:
1. **Giữ nguyên 100% bản gốc cho `replica_id = 0`:** Ở quy mô `1x` (và phần tử gốc `replica_id = 0` trong các quy mô `10x`, `50x`, `100x`), toàn bộ giá trị của `10,886` bản ghi gốc được bảo toàn tuyệt đối.
2. **Dịch chuyển thời gian có quy luật (`Timestamp Shifting`) cho `replica_id >= 1`:**
   - Thực hiện phép tích Đề-các có kiểm soát `base_df.crossJoin(spark.range(0, scale))` với cột chỉ số bản sao `replica_id` $\in \{0, 1, \dots, \text{scale}-1\}$.
   - Dịch chuyển cột `datetime` tiến thêm một khoảng cố định bằng chu kỳ 2 năm (`730 ngày = 63,072,000 giây`) nhân với `replica_id`:
     $$\text{datetime}_{\text{new}} = \text{datetime}_{\text{orig}} + \text{replica\_id} \times 730 \text{ ngày}$$
   - Nhờ đó, toàn bộ `1,088,600` bản ghi ở quy mô `100x` đều có mốc `datetime` duy nhất, không xảy ra hiện tượng trùng khóa thời gian.
3. **Nhiễu chuẩn cực nhỏ có kiểm soát (`Controlled Gaussian Jitter`, `SEED = 42`):**
   - Với `replica_id > 0`, cộng thêm nhiễu Gaussian $\mathcal{N}(0, 0.05^2)$ cố định hạt giống `SEED = 42` vào hai biến khí tượng liên tục `temp` và `atemp` (giới hạn dưới tại `0.5`), trong khi giữ nguyên các cột phân loại/rời rạc và biến mục tiêu `count`.
   - Cách làm này đảm bảo các bản ghi nhân bản không trùng lặp y hệt từng byte (tránh việc bộ nén Parquet gian lận tỉ lệ nén bất thường), đồng thời giữ nguyên phân phối thống kê của dữ liệu gốc.
4. **Ranh giới sử dụng:** Dữ liệu tổng hợp tại `/bike-sharing/benchmark/` chỉ phục vụ mục đích đo kiểm hiệu năng lưu trữ và tính toán phân tán của Spark, tuyệt đối không dùng để huấn luyện hay báo cáo độ chính xác nghiệp vụ của mô hình Machine Learning ở Milestone 5.

---

## 4. Các quy mô dữ liệu đã tạo (Giai đoạn B)

Trích xuất từ [scaling_generation_summary.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/scaling_generation_summary.csv) và [generation_metadata.json](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/generation_metadata.json):

| Quy mô (`Scale`) | Hệ số nhân | Số dòng (`Rows`) | Số cột (`Cols`) | Dung lượng Parquet gốc (MB) | Số Partition mặc định | Thời gian sinh dữ liệu (s) | `mean(count)` | `std(count)` | `mean(temp)` | Đường dẫn HDFS |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **`1x`** | 1 | `10,886` | 20 | `0.1188 MB` (`124,620 B`) | 1 | `12.1846 s` | `191.5741` | `181.1445` | `20.2309` | `/bike-sharing/benchmark/data/scale_1x` |
| **`10x`** | 10 | `108,860` | 20 | `1.7003 MB` (`1,782,847 B`) | 1 | `24.6025 s` | `191.5741` | `181.1370` | `20.2305` | `/bike-sharing/benchmark/data/scale_10x` |
| **`50x`** | 50 | `544,300` | 20 | `5.6926 MB` (`5,969,100 B`) | 2 | `21.5147 s` | `191.5741` | `181.1363` | `20.2307` | `/bike-sharing/benchmark/data/scale_50x` |
| **`100x`** | 100 | `1,088,600` | 20 | `10.3520 MB` (`10,854,832 B`) | 2 | `22.7906 s` | `191.5741` | `181.1362` | `20.2308` | `/bike-sharing/benchmark/data/scale_100x` |

---

## 5. So sánh định dạng lưu trữ CSV và Parquet (Giai đoạn C)

Kịch bản [benchmark_storage.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/spark/jobs/benchmark_storage.py) thực hiện **3 lần chạy lặp lại (`3 runs`)** cho mỗi định dạng ở từng quy mô.
- **Trạng thái bộ nhớ đệm (Cache Regime):** Trước mỗi phép đọc đều gọi `spark.catalog.clearCache()` và không `.cache()` DataFrame đọc ra (*Spark-Uncached*). Tuy nhiên, vì container `hadoop-datanode` chạy trên cùng nhân Linux WSL2, các block vừa ghi vẫn nằm trong bộ đệm trang của hệ điều hành (*Warm OS Page Cache*). Do đó, chế độ đo được định danh minh bạch là **`Spark-Uncached / Warm OS Page Cache`**.

### Bảng tổng hợp kết quả thực nghiệm CSV vs Parquet ([storage_comparison.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/storage_comparison.csv))

| Quy mô | Định dạng | Dung lượng HDFS (MB) | Tỉ lệ tiết kiệm dung lượng | Avg Write Time (s) | Avg Read + Count Time (s) | Avg Read + Agg Time (s) | Write Throughput (rows/s) | Read + Count Throughput (rows/s) | Read + Agg Throughput (rows/s) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`1x`** (`10,886`) | **CSV** | `1.0680 MB` | — | `2.0240 ± 0.3795` | `0.9188 ± 0.4074` | `1.5461 ± 0.9545` | `5,378.46` | `11,848.06` | `7,040.94` |
| **`1x`** (`10,886`) | **Parquet** | **`0.1821 MB`** | **5.87x nhỏ hơn** (-82.95%) | `2.1244 ± 1.3306` | **`0.6566 ± 0.4068`** | **`0.9930 ± 0.7788`** | `5,124.27` | **`16,579.35`** | **`10,962.74`** |
| **`10x`** (`108,860`) | **CSV** | `11.0075 MB` | — | `2.1247 ± 0.7717` | **`0.3024 ± 0.0259`** | **`0.5917 ± 0.0786`** | `51,235.47` | **`359,986.77`** | **`183,978.37`** |
| **`10x`** (`108,860`) | **Parquet** | **`2.8204 MB`** | **3.90x nhỏ hơn** (-74.38%) | **`1.6711 ± 0.4565`** | `0.3429 ± 0.0234` | `1.1456 ± 0.9128` | **`65,142.72`** | `317,468.65` | `95,024.44` |
| **`50x`** (`544,300`) | **CSV** | `55.1826 MB` | — | **`5.8692 ± 1.8123`** | `3.4646 ± 4.5883` | `4.7906 ± 4.6775` | **`92,738.36`** | `157,103.27` | `113,618.34` |
| **`50x`** (`544,300`) | **Parquet** | **`13.9019 MB`** | **3.97x nhỏ hơn** (-74.81%) | `13.5761 ± 3.9891` | **`1.3273 ± 0.4567`** | **`4.4700 ± 4.2922`** | `40,092.52` | **`410,080.61`** | **`121,767.34`** |
| **`100x`** (`1,088,600`) | **CSV** | `110.4020 MB` | — | **`17.8713 ± 11.0388`** | `14.2397 ± 10.8659` | `21.5240 ± 14.3733` | **`60,913.31`** | `76,448.24` | `50,576.10` |
| **`100x`** (`1,088,600`) | **Parquet** | **`27.7273 MB`** | **3.98x nhỏ hơn** (-74.89%) | `41.0146 ± 11.1661` | **`2.0197 ± 0.4926`** | **`2.5600 ± 1.1042`** | `26,541.77` | **`538,990.94`** | **`425,234.38`** |

### Nhận xét kỹ thuật về CSV vs Parquet
1. **Hiệu quả nén dung lượng vượt trội:** Ở mọi quy mô từ `10x` đến `100x`, định dạng cột **Parquet nén Snappy** chỉ chiếm đúng **~25.1% dung lượng** so với CSV thuần túy (`27.73 MB` so với `110.40 MB` ở `100x`, tiết kiệm **3.98 lần**).
2. **Ưu thế đọc và tổng hợp ở quy mô lớn (`50x` & `100x`):**
   - Ở quy mô nhỏ `10x` (`11 MB`), CSV đọc khá nhanh nhờ nằm gọn trong bộ đệm trang và chi phí giải mã metadata footer của Parquet chiếm tỷ trọng đáng kể.
   - Tuy nhiên, khi lên tới quy mô **`100x` (`1,088,600` dòng)**, CSV bộc lộ điểm nghẽn nghiêm trọng do phải quét và phân tích cú pháp văn bản (`text parsing`) trên toàn bộ `110.40 MB` gồm 20 cột: thời gian `Read + Count` của CSV mất `14.2397s` trong khi Parquet chỉ mất **`2.0197s` (nhanh gấp 7.05 lần)**; thời gian `Read + Aggregation` của CSV mất `21.5240s` trong khi Parquet chỉ mất **`2.5600s` (nhanh gấp 8.41 lần)** nhờ cơ chế **Columnar Projection** (chỉ đọc đúng 4 cột số cần tính toán) và **Vectorized Parquet Reader** của Spark.
3. **Sự đánh đổi khi Ghi (`Write-Once, Read-Many`):** Khi ghi ở quy mô `50x` và `100x`, Parquet tốn nhiều thời gian CPU hơn CSV (`41.0146s` so với `17.8713s` ở `100x`) do phải xây dựng từ điển mã hóa cột (Dictionary/RLE encoding), tính toán thống kê `min/max` cho từng Row Group và nén Snappy. Đây là sự đánh đổi kinh điển và hoàn toàn xứng đáng trong các hệ thống Data Lakehouse đọc nhiều lần.

---

## 6. Kết quả Spark Processing Benchmark (Giai đoạn D)

Kịch bản [benchmark_processing.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/spark/jobs/benchmark_processing.py) đo kiểm 5 phép toán Spark với cùng logic trên cả 4 quy mô (lặp `3 runs` mỗi phép, trích xuất từ [processing_results.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/processing_results.csv)):

| Quy mô | Phép xử lý (`Operation`) | Số dòng vào (`Input`) | Số dòng ra (`Output`) | Số Partition | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg Time (s) $\downarrow$ | Std (s) | Throughput (rows/s) $\uparrow$ | CPU (%) | RAM (MB) |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`1x`** | `Filter` | 10,886 | 2,556 | 1 | 3.1859 | 0.8821 | 1.7823 | **1.9501** | 1.1610 | `5,582.28` | 77.35% | 885.12 |
| **`1x`** | `GroupBy` | 10,886 | 289 | 1 | 4.9118 | 1.4721 | 10.8664 | **5.7501** | 4.7529 | `1,893.18` | 44.68% | 950.69 |
| **`1x`** | `Aggregation` | 10,886 | 192 | 1 | 4.0356 | 13.4051 | 1.0396 | **6.1601** | 6.4507 | `1,767.18` | 73.38% | 1,054.60 |
| **`1x`** | `Sort` | 10,886 | 10,886 | 1 | 2.8731 | 0.9329 | 0.4865 | **1.4308** | 1.2688 | `7,608.33` | 81.60% | 1,113.52 |
| **`1x`** | `Join` | 10,886 | 10,886 | 1 | 1.5146 | 1.1471 | 0.6288 | **1.0968** | 0.4450 | `9,925.24` | 64.33% | 1,125.35 |
| **`10x`** | `Filter` | 108,860 | 25,560 | 1 | 0.5343 | 0.4747 | 0.3174 | **0.4421** | 0.1121 | `246,233.88` | 55.48% | 1,113.14 |
| **`10x`** | `GroupBy` | 108,860 | 289 | 1 | 0.4840 | 0.5882 | 0.5604 | **0.5442** | 0.0540 | `200,036.75` | 67.76% | 1,113.69 |
| **`10x`** | `Aggregation` | 108,860 | 192 | 1 | 0.5609 | 0.5522 | 0.8270 | **0.6467** | 0.1562 | `168,331.53` | 49.39% | 1,114.42 |
| **`10x`** | `Sort` | 108,860 | 108,860 | 1 | 2.0346 | 1.0091 | 0.7818 | **1.2752** | 0.6674 | `85,367.00` | 38.68% | 1,120.93 |
| **`10x`** | `Join` | 108,860 | 108,860 | 1 | 0.7197 | 0.5831 | 0.4734 | **0.5921** | 0.1234 | `183,854.08` | 74.86% | 1,122.50 |
| **`50x`** | `Filter` | 544,300 | 127,800 | 2 | 1.1936 | 0.2260 | 0.2614 | **0.5603** | 0.5487 | `971,443.87` | 73.99% | 1,126.04 |
| **`50x`** | `GroupBy` | 544,300 | 289 | 2 | 0.5038 | 0.2353 | 0.4142 | **0.3844** | 0.1367 | **`1,415,972.94`** | 63.08% | 1,128.29 |
| **`50x`** | `Aggregation` | 544,300 | 192 | 2 | 0.8404 | 0.5579 | 0.6502 | **0.6828** | 0.1440 | `797,158.76` | 54.34% | 1,132.28 |
| **`50x`** | `Sort` | 544,300 | 544,300 | 2 | 9.5014 | 3.7578 | 3.8995 | **5.7196** | 3.2759 | `95,164.00` | 19.35% | 1,133.21 |
| **`50x`** | `Join` | 544,300 | 544,300 | 2 | 1.4579 | 1.2079 | 0.8989 | **1.1882** | 0.2800 | `458,087.86` | 34.74% | 1,134.05 |
| **`100x`** | `Filter` | 1,088,600 | 255,600 | 2 | 2.4392 | 1.0186 | 1.5258 | **1.6612** | 0.7199 | `655,309.41` | 67.18% | 1,132.25 |
| **`100x`** | `GroupBy` | 1,088,600 | 289 | 2 | 0.7696 | 0.8472 | 1.2688 | **0.9619** | 0.2686 | `1,131,718.47` | 52.31% | 1,133.34 |
| **`100x`** | `Aggregation` | 1,088,600 | 192 | 2 | 10.9984 | 3.1486 | 1.9920 | **5.3797** | 4.9002 | `202,353.29` | 45.63% | 1,132.11 |
| **`100x`** | `Sort` | 1,088,600 | 1,088,600 | 2 | 22.6818 | 7.1678 | 5.4581 | **11.7692** | 9.4891 | `92,495.67` | 29.46% | 1,129.38 |
| **`100x`** | `Join` | 1,088,600 | 1,088,600 | 2 | 20.4268 | 3.7084 | 2.7750 | **8.9701** | 9.9328 | `121,358.74` | 29.43% | 1,108.27 |

*(Không có bất kỳ lỗi thực thi nào xảy ra ở cả 60 lượt chạy; cột `error = None` trên toàn bộ bảng).*

---

## 7. Kết quả Partition Benchmark (Giai đoạn E)

### 7.1. Phân biệt 3 khái niệm phân mảnh dữ liệu trong Hadoop & Spark
1. **HDFS File / Block (`hdfs_file_count`):** Đơn vị lưu trữ vật lý trên DataNode (kích thước block mặc định của HDFS là `128 MB`). Do dữ liệu Parquet ở các mức `10x` (`1.70 MB`), `50x` (`5.69 MB`), `100x` (`10.35 MB`) đều nhỏ hơn `128 MB`, mỗi tệp `part-*.parquet` nằm gọn trong 1 HDFS block.
2. **Spark Input Partition (`spark_input_partitions`):** Số phân vùng được Spark tạo ra ngay khi đọc dữ liệu từ HDFS (`spark.read.parquet(path)`), phụ thuộc vào số tệp vật lý và cấu hình `spark.sql.files.maxPartitionBytes`. Trong thí nghiệm này, `spark_input_partitions = 1` ở `10x` và `= 2` ở `50x, 100x`.
3. **Spark DataFrame Partition (`actual_df_partitions`):** Số phân vùng thực tế trong bộ nhớ của RDD/DataFrame sau khi người dùng chủ động gọi `.repartition(p)` và kiểm chứng bằng `df.rdd.getNumPartitions()`. Đây là số lượng task song song mà Spark Scheduler tạo ra trong stage xử lý.

### 7.2. Bảng kết quả thực nghiệm Partition (`1, 2, 4, 8` Partitions — [partition_results.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/partition_results.csv))

Workload chuẩn áp dụng đồng nhất: `Filter(temp >= 10.0)` $\rightarrow$ `Inner Join(ref_df)` $\rightarrow$ `GroupBy(season, workingday, operational_tier, hour).agg(count, sum, avg_weighted_demand, avg_humidity)` $\rightarrow$ `.collect()`.

| Quy mô | Số dòng | HDFS Files | Spark Input Partitions | Target / Actual DF Partitions (`getNumPartitions()`) | Thời gian `repartition + cache` (s) | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg Processing Time (s) $\downarrow$ | Std (s) | Throughput (rows/s) $\uparrow$ | CPU (%) | RAM (MB) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`10x`** | 108,860 | 1 | 1 | **1** | 19.0894 | 2.5932 | 0.9483 | 0.9339 | `1.4918` | 0.9539 | `72,972.25` | 47.92% | 1,120.90 |
| **`10x`** | 108,860 | 1 | 1 | **2** | 4.3856 | 2.0510 | 1.6400 | 0.6867 | `1.4592` | 0.6999 | `74,602.52` | 77.89% | 1,114.56 |
| **`10x`** | 108,860 | 1 | 1 | **4** | 13.1627 | 0.6104 | 0.3555 | 0.6181 | **`0.5280`** | 0.1494 | **`206,174.24`** | 83.99% | 1,114.78 |
| **`10x`** | 108,860 | 1 | 1 | **8** | 2.1347 | 1.3812 | 2.3572 | 1.9295 | `1.8893` | 0.4892 | `57,619.22` | 59.44% | 1,116.17 |
| **`50x`** | 544,300 | 1 | 2 | **1** | 36.7076 | 1.4583 | 1.8406 | 1.6837 | `1.6609` | 0.1922 | `327,713.89` | 78.30% | 1,106.81 |
| **`50x`** | 544,300 | 1 | 2 | **2** | 24.3247 | 0.6767 | 1.5205 | 1.4340 | `1.2104` | 0.4642 | `449,686.05` | 61.80% | 1,096.16 |
| **`50x`** | 544,300 | 1 | 2 | **4** | 22.1119 | 0.7511 | 0.8413 | 1.1206 | **`0.9043`** | 0.1926 | **`601,902.02`** | 75.77% | 1,088.06 |
| **`50x`** | 544,300 | 1 | 2 | **8** | 24.2949 | 5.0393 | 3.2145 | 1.5169 | `3.2569` | 1.7616 | `167,122.11` | 67.96% | 1,080.72 |
| **`100x`** | 1,088,600 | 1 | 2 | **1** | 33.0215 | 0.9333 | 1.0406 | 0.9508 | **`0.9749`** | 0.0576 | **`1,116,627.35`** | 54.98% | 1,063.15 |
| **`100x`** | 1,088,600 | 1 | 2 | **2** | 46.7238 | 20.5834 | 2.5360 | 6.7648 | `9.9614` | 9.4388 | `109,281.83` | 50.68% | 1,038.66 |
| **`100x`** | 1,088,600 | 1 | 2 | **4** | 70.9827 | 1.7839 | 0.8976 | 1.3111 | **`1.3309`** | 0.4435 | **`817,942.75`** | 83.51% | 1,019.45 |
| **`100x`** | 1,088,600 | 1 | 2 | **8** | 26.7965 | 1.8070 | 0.6687 | 2.2449 | `1.5735` | 0.8136 | `691,833.49` | 67.44% | 1,025.70 |

---

## 8. Phân tích Thông lượng (Throughput) Tổng thể

Bảng tổng hợp thông lượng theo từng quy mô dữ liệu ([scaling_results.csv](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/scaling_results.csv)):

| Quy mô (`Scale`) | Số dòng (`Rows`) | Tổng thời gian 5 phép toán (s) | Thời gian TB / phép toán (s) | Thông lượng TB 5 phép toán (rows/s) | Thông lượng `Filter` (rows/s) | Thông lượng `Aggregation` (rows/s) | Thông lượng `Sort` (rows/s) | Thông lượng `Join` (rows/s) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`1x`** | `10,886` | `16.3879 s` | `3.2776 s` | `5,355.24` | `5,582.28` | `1,767.18` | `7,608.33` | `9,925.24` |
| **`10x`** | `108,860` | **`3.5003 s`** | **`0.7001 s`** | `176,764.65` | `246,233.88` | `168,331.53` | `85,367.00` | `183,854.08` |
| **`50x`** | `544,300` | `8.5353 s` | `1.7071 s` | **`747,565.49`** | **`971,443.87`** | **`797,158.76`** | **`95,164.00`** | **`458,087.86`** |
| **`100x`** | `1,088,600` | `28.7421 s` | `5.7484 s` | `440,647.12` | `655,309.41` | `202,353.29` | `92,495.67` | `121,358.74` |

- **Đỉnh thông lượng (Peak Throughput)** đạt được ở quy mô **`50x` (`544,300` dòng)** với thông lượng trung bình **`747,565.49 rows/s`** (riêng phép `GroupBy` đạt **`1,415,972.94 rows/s`** và `Filter` đạt **`971,443.87 rows/s`**).
- Tại quy mô `1x` (`10,886` dòng), kích thước dữ liệu quá nhỏ nên chi phí cố định (Spark job scheduling, Catalyst query planning, Whole-Stage Codegen JIT compilation trong lần chạy đầu tiên) chiếm phần lớn thời gian thực thi, khiến thông lượng chỉ đạt `5,355.24 rows/s`.

---

## 9. Theo dõi Tài nguyên CPU và RAM

1. **Mức sử dụng CPU (`cpu_usage_pct`):**
   - Trong Giai đoạn D (Processing Benchmark), mức sử dụng CPU trung bình trên 2 cores được cấp phát dao động từ **`44.80%` đến `68.27%`**. Các phép toán thiên về quét bộ nhớ như `Filter` tận dụng CPU rất cao (`67.18% – 77.35%`), trong khi phép `Sort` ở `50x` và `100x` có mức CPU thấp hơn (`19.35% – 29.46%`) do luồng thực thi phải chờ trao đổi dữ liệu qua mạng nội bộ (shuffle I/O) và thu gom rác bộ nhớ (JVM Garbage Collection).
   - Trong Giai đoạn E (Partition Benchmark), cấu hình **`4 partitions`** đạt hiệu suất sử dụng CPU cao nhất (**`75.77% – 83.99%`**), chứng tỏ 2 nhân Worker được lấp đầy ống dẫn xử lý (pipeline utilization) tốt nhất khi có 4 task gối đầu nhau.
2. **Mức sử dụng RAM (`memory_usage_mb`):**
   - Mức tiêu thụ RAM của container Spark dao động ổn định trong khoảng **`1,019.45 MB – 1,134.05 MB`** xuyên suốt cả bài đo từ `1x` đến `100x`, nằm hoàn toàn trong giới hạn an toàn của Docker WSL2 và không gây hiện tượng `OutOfMemoryError` hay ảnh hưởng tới 6 container Langfuse.

---

## 10. Hệ thống Biểu đồ Trực quan

Năm biểu đồ chuẩn SVG độ phân giải cao đã được xuất tự động tại thư mục `outputs/benchmark/charts/`:
1. [1_scale_vs_processing_time.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/1_scale_vs_processing_time.svg) — Quy mô dữ liệu vs Thời gian xử lý Spark.
2. [2_scale_vs_throughput.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/2_scale_vs_throughput.svg) — Quy mô dữ liệu vs Thông lượng (`rows/s`).
3. [3_csv_vs_parquet_storage_size.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/3_csv_vs_parquet_storage_size.svg) — So sánh dung lượng lưu trữ CSV vs Parquet.
4. [4_csv_vs_parquet_read_write_time.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/4_csv_vs_parquet_read_write_time.svg) — So sánh thời gian Ghi & Đọc CSV vs Parquet.
5. [5_partitions_vs_processing_time.svg](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/outputs/benchmark/charts/5_partitions_vs_processing_time.svg) — Số lượng Partition vs Thời gian xử lý.

Dưới đây là các biểu đồ trực quan hóa trực tiếp các số liệu thực nghiệm:

### 10.1. Dataset Scale vs Processing Time (Tổng thời gian 5 phép toán & Thời gian Sort/Join)
```mermaid
xychart-beta
    title "Dataset Scale vs Total Processing Time of 5 Spark Operations (Seconds)"
    x-axis ["1x (10.8K)", "10x (108.8K)", "50x (544.3K)", "100x (1.09M)"]
    y-axis "Time (Seconds)" 0 --> 32
    bar [16.39, 3.50, 8.54, 28.74]
    line [1.43, 1.28, 5.72, 11.77]
```

### 10.2. Dataset Scale vs Average Throughput (`Rows / Second`)
```mermaid
xychart-beta
    title "Dataset Scale vs Average Processing Throughput (Rows / Second)"
    x-axis ["1x (10.8K)", "10x (108.8K)", "50x (544.3K)", "100x (1.09M)"]
    y-axis "Throughput (Rows/sec)" 0 --> 800000
    bar [5355, 176765, 747565, 440647]
```

### 10.3. CSV vs Parquet Storage Size on HDFS (`MB`)
```mermaid
xychart-beta
    title "Storage Size Comparison: CSV vs Parquet Snappy (MB) at 1x, 10x, 50x, 100x"
    x-axis ["1x CSV", "1x Parquet", "10x CSV", "10x Parquet", "50x CSV", "50x Parquet", "100x CSV", "100x Parquet"]
    y-axis "Storage Size (MB)" 0 --> 120
    bar [1.07, 0.18, 11.01, 2.82, 55.18, 13.90, 110.40, 27.73]
```

### 10.4. CSV vs Parquet Read + Aggregation Time (`Seconds`)
```mermaid
xychart-beta
    title "Read + Aggregation Time: CSV vs Parquet (Seconds) at 1x, 10x, 50x, 100x"
    x-axis ["1x CSV", "1x Parquet", "10x CSV", "10x Parquet", "50x CSV", "50x Parquet", "100x CSV", "100x Parquet"]
    y-axis "Time (Seconds)" 0 --> 24
    bar [1.55, 0.99, 0.59, 1.15, 4.79, 4.47, 21.52, 2.56]
```

### 10.5. Number of Partitions (`1, 2, 4, 8`) vs Processing Time (`Seconds`) on Scale `50x` (`544,300` rows)
```mermaid
xychart-beta
    title "Number of Partitions vs Processing Time on Scale 50x (Seconds)"
    x-axis ["1 Partition", "2 Partitions", "4 Partitions", "8 Partitions"]
    y-axis "Avg Processing Time (Seconds)" 0 --> 4
    bar [1.6609, 1.2104, 0.9043, 3.2569]
```

---

## 11. Phân tích Chuyên sâu Kết quả Thực nghiệm

1. **Hiệu ứng Khởi động JVM / Catalyst Codegen và Điểm ngọt Quy mô (`Sweet Spot Scaling`):**
   - Khi chuyển từ `1x` (`10,886` dòng) lên `10x` (`108,860` dòng), tổng thời gian thực thi 5 phép toán giảm từ `16.3879s` xuống `3.5003s` dù dữ liệu gấp 10 lần. Nguyên nhân là ở `1x` (tập dữ liệu đầu tiên chạy ngay sau khi khởi tạo `SparkSession`), Spark phải biên dịch mã bytecode Java (`Whole-Stage Code Generation`), khởi tạo kết nối BlockManager và cấp phát bộ đệm Shuffle.
   - Từ `10x` (`108,860` dòng) lên `50x` (`544,300` dòng — tăng gấp 5 lần), tổng thời gian 5 phép toán chỉ tăng từ `3.5003s` lên `8.5353s` (tăng **2.44 lần** — mở rộng dưới tuyến tính *sub-linear time scaling*), giúp thông lượng trung bình tăng vọt từ `176,764.65 rows/s` lên **`747,565.49 rows/s`**.
   - Từ `50x` (`544,300` dòng) lên `100x` (`1,088,600` dòng — tăng gấp 2 lần), các phép toán ít dịch chuyển dữ liệu như `GroupBy` (`0.3844s` $\rightarrow$ `0.9619s`) và `Filter` (`0.5603s` $\rightarrow$ `1.6612s`) vẫn giữ tốc độ gần tuyến tính. Tuy nhiên, các phép toán đòi hỏi sắp xếp và trộn toàn cục (`Sort`: `5.7196s` $\rightarrow$ `11.7692s`, `Join`: `1.1882s` $\rightarrow$ `8.9701s`, `Aggregation` 9 chỉ số: `0.6828s` $\rightarrow$ `5.3797s`) bắt đầu chịu áp lực bộ nhớ trên Executor `1024 MiB RAM`, làm phát sinh thu gom rác (GC pause ở `Run 1`) và tràn bộ đệm shuffle (shuffle spill).
2. **Quy luật Chọn Số lượng Partition (`Tại sao nhiều Partition hơn không phải lúc nào cũng tốt hơn?`):**
   - Trên hệ thống có **2 CPU cores** cấp cho Executor (`spark.cores.max = 2`), kết quả ở cả `10x` và `50x` đều chỉ ra quy luật hình chữ U (U-shaped curve) kinh điển:
     - **`1 partition`:** Chỉ sử dụng được 1 luồng CPU duy nhất (`1 core` bị bỏ phí), thời gian xử lý ở `50x` là `1.6609s` (`327,713.89 rows/s`).
     - **`2 partitions`:** Tận dụng đủ 2 cores song song, giảm thời gian ở `50x` xuống `1.2104s` (`449,686.05 rows/s`).
     - **`4 partitions` (Tối ưu = `2x số CPU cores`):** Đạt thời gian nhanh nhất ở cả `10x` (`0.5280s`, `206,174.24 rows/s`) và `50x` (`0.9043s`, `601,902.02 rows/s`). Việc chia thành 4 partitions vừa giúp cân bằng tải tốt hơn giữa 2 cores, vừa giảm kích thước bảng băm trung gian của mỗi task trong bộ nhớ.
     - **`8 partitions` (Quá phân mảnh — Over-partitioning):** Thời gian xử lý bị suy giảm mạnh lên `1.8893s` ở `10x` (chậm hơn 3.58 lần so với 4 partitions) và `3.2569s` ở `50x` (chậm hơn 3.60 lần so với 4 partitions). Nguyên nhân là trên 2 CPU cores, 8 partitions phải xếp hàng qua 4 đợt lập lịch (4 waves of tasks), làm tăng gấp đôi chi phí tuần tự hóa task (task serialization/deserialization), chi phí mở tệp shuffle trung gian ($8 \times 4 = 32$ shuffle blocks) và chi phí hợp nhất kết quả về Driver.
   - Riêng ở `100x` (`1,088,600` dòng đã cache sẵn trong RAM), `1 partition` đạt `0.9749s` vì toàn bộ phép `Filter + Broadcast/Local Join + HashAggregate` diễn ra cục bộ trong 1 luồng mà không cần trộn dữ liệu qua mạng (zero shuffle exchange), nhưng đổi lại bước chuẩn bị `repartition(1) + cache` trước đó tốn tới `33.0215s`. Trong số các cấu hình đa phân vùng (`2, 4, 8`), **`4 partitions` (`1.3309s`, `817,942.75 rows/s`)** tiếp tục là cấu hình song song ổn định và hiệu quả nhất.

---

## 12. Giới hạn của thí nghiệm (Experimental Limitations)

1. **Môi trường Single-Node Containerized:** Cụm Spark Standalone (`1 Master + 1 Worker`) và HDFS (`1 NameNode + 1 DataNode`) cùng chạy trên một máy chủ vật lý Windows 11 qua WSL2 và chia sẻ tài nguyên với 6 container Langfuse. Do đó, tốc độ I/O mạng và đĩa chịu ảnh hưởng của lớp ảo hóa WSL2 và Linux OS Page Cache.
2. **Giới hạn bộ nhớ Executor (`1 GiB`):** Để bảo vệ sự ổn định của toàn hệ thống 16 GB RAM, Executor chỉ được cấp `1024 MiB` RAM và `2 CPU cores`. Ở quy mô `100x` (`1.09 triệu` dòng), các phép toán nặng như `Sort` toàn cục bắt đầu gặp hiện tượng GC pause trong lần chạy đầu tiên (`Run 1 = 22.68s`, sau đó giảm xuống `7.17s` và `5.46s` ở `Run 2` và `Run 3`).
3. **Bản chất của dữ liệu tổng hợp:** Dữ liệu `10x, 50x, 100x` được sinh bằng phương pháp nhân bản có dịch chuyển thời gian và cộng nhiễu chuẩn nhẹ từ `10,886` dòng gốc, do đó độ đa dạng (cardinality) của các khóa gom nhóm như `(season, weather, hour)` vẫn giữ nguyên (tối đa `289` nhóm cho `GroupBy` và `192` nhóm cho `Aggregation`).

---

## 13. Kết luận

Milestone 6 đã hoàn thành toàn diện việc chứng minh và định lượng hiệu năng xử lý dữ liệu lớn của hệ thống **Apache Spark + Hadoop HDFS** trên 4 cấp độ quy mô từ `10,886` dòng (`1x`) đến `1,088,600` dòng (`100x`):
1. **Về lưu trữ phân tán:** Định dạng **Parquet (nén Snappy)** vượt trội toàn diện so với CSV khi tiết kiệm **~75% dung lượng đĩa** (`27.73 MB` so với `110.40 MB` ở `100x`), tăng tốc độ đọc đếm dòng lên **7.05 lần** và tăng tốc độ đọc tổng hợp dữ liệu lên **8.41 lần** (`425,234 rows/s` so với `50,576 rows/s`).
2. **Về khả năng mở rộng tính toán:** Hệ thống Spark đạt thông lượng đỉnh trung bình **`747,565 rows/s`** ở quy mô `50x` (tối đa **`1.42 triệu rows/s`** cho phép `GroupBy`) và duy trì thông lượng **`440,647 rows/s`** ở quy mô `100x` (`1.09 triệu` dòng) chỉ với 2 CPU cores và 1 GiB Executor RAM.
3. **Về tối ưu hóa Partition:** Thực nghiệm chứng minh việc đặt số lượng partition bằng **2 lần số nhân CPU (`4 partitions` cho `2 cores`)** mang lại hiệu năng xử lý song song tối ưu nhất, trong khi việc tăng lên `8 partitions` gây suy giảm hiệu năng tới **3.6 lần** do chi phí quản lý task và trộn dữ liệu (shuffle overhead).

