# Báo cáo Hoàn thành Milestone 3: Triển khai Apache Spark và Tích hợp HDFS

**Đề tài:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Milestone:** Milestone 3 – Triển khai Apache Spark và Tích hợp HDFS  
**Thời gian thực hiện:** 2026-11-11  
**Trạng thái:** **HOÀN THÀNH (COMPLETED & VERIFIED)**

---

## 1. Cấu hình Spark Đã Sử dụng (Giai đoạn A)

Qua kiểm tra [docker-compose.yml](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docker-compose.yml) và container thực tế:
- **Docker Image:** `bitnamilegacy/spark:3.5`
- **Phiên bản thành phần trong Image:**
  - **Apache Spark:** `3.5.6` (Scala `2.12.18`, OpenJDK `17.0.15` 64-Bit Server VM)
  - **Python:** `3.12.10`
  - **Hadoop Client Libraries tích hợp sẵn:** `hadoop-client-api-3.3.4.jar`, `hadoop-client-runtime-3.3.4.jar`, `hadoop-shaded-guava-1.1.1.jar` (tương thích hoàn toàn giao thức HDFS RPC với Hadoop `3.2.1` trên NameNode/DataNode).
- **Mạng Docker (Network):** `bigdata-cluster_bigdata-network` (cùng mạng bridge nội bộ với `hadoop-namenode` và `hadoop-datanode`).
- **Biến môi trường của `spark-master`:**
  - `SPARK_MODE=master`
  - `SPARK_RPC_AUTHENTICATION_ENABLED=no`, `SPARK_RPC_ENCRYPTION_ENABLED=no`, `SPARK_SSL_ENABLED=no`
- **Biến môi trường của `spark-worker-1`:**
  - `SPARK_MODE=worker`
  - `SPARK_MASTER_URL=spark://spark-master:7077`
  - `SPARK_WORKER_MEMORY=2G`
  - `SPARK_WORKER_CORES=2`
- **Bind Mounts:** `./compute/spark/jobs` trên máy host được ánh xạ vào `/opt/spark-jobs` trên cả `spark-master` và `spark-worker-1`.

---

## 2. Các Service Được Khởi chạy & Trạng thái Master/Worker (Giai đoạn B & C)

Chỉ khởi chạy đúng 2 services của Spark (`spark-master` và `spark-worker-1`), giữ nguyên 2 container HDFS và 6 container Langfuse (không bật `spark-worker-2`, `jupyter`, hay YARN):

```powershell
docker compose up -d spark-master
docker compose up -d spark-worker-1
```

### 2.1. Trạng thái các Container (`docker compose ps`)
| Tên Container | Image | Service | Trạng thái | Cổng ánh xạ (Ports) |
| :--- | :--- | :--- | :--- | :--- |
| `hadoop-namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | `hadoop-namenode` | `Up (healthy)` | `9870->9870`, `9820->9000` |
| `hadoop-datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | `hadoop-datanode` | `Up (healthy)` | `9864->9864` |
| `spark-master` | `bitnamilegacy/spark:3.5` | `spark-master` | `Up` (`ALIVE`) | `7077->7077`, `8080->8080` |
| `spark-worker-1` | `bitnamilegacy/spark:3.5` | `spark-worker-1` | `Up` (`ALIVE`) | *(Nội bộ `8081`, `33299`)* |

### 2.2. Trạng thái Spark Master UI (`http://localhost:8080/json/`)
Truy vấn trực tiếp JSON API của Spark Master Web UI xác nhận `spark-worker-1` đã đăng ký thành công và ứng dụng kiểm thử đã hoàn tất (`FINISHED`):

```json
{
  "url" : "spark://782ea1746901:7077",
  "workers" : [ {
    "id" : "worker-20261111042143-172.24.0.5-33299",
    "host" : "172.24.0.5",
    "port" : 33299,
    "webuiaddress" : "http://172.24.0.5:8081",
    "cores" : 2,
    "coresused" : 0,
    "coresfree" : 2,
    "memory" : 2048,
    "memoryused" : 0,
    "memoryfree" : 2048,
    "state" : "ALIVE"
  } ],
  "aliveworkers" : 1,
  "cores" : 2,
  "memory" : 2048,
  "completedapps" : [ {
    "id" : "app-20261111044717-0002",
    "name" : "Milestone3-BikeSharing-HDFS-Test",
    "cores" : 2,
    "user" : "root",
    "memoryperexecutor" : 1024,
    "state" : "FINISHED"
  } ],
  "status" : "ALIVE"
}
```

---

## 3. Cấu hình CPU / RAM & Mức Tiêu thụ Thực tế

- **Cấu hình cấp phát cho Spark Worker 1:** `2 CPU Cores`, `2048 MB (2.0 GiB) RAM`.
- **Cấu hình cấp phát cho Spark Job (`milestone3_hdfs_test.py`):** `spark.executor.memory=1g`, `spark.cores.max=2`.
- **Mức tiêu thụ RAM thực tế của các Container (`docker stats --no-stream`):**
  - `spark-master`: `~191.5 MiB`
  - `spark-worker-1`: `~203.5 MiB` (khi idle) / `~950 MiB` (khi chạy Executor)
  - `hadoop-namenode`: `~431.7 MiB`
  - `hadoop-datanode`: `~291.6 MiB`
  - **Tổng RAM cụm Big Data (HDFS + Spark):** **~1.12 GiB** khi rảnh và **~1.9 GiB** khi chạy tải — hoàn toàn nằm gọn trong phần RAM trống của Docker WSL2 mà không ảnh hưởng tới 6 container Langfuse.

---

## 4. Kết quả Kiểm tra Kết nối HDFS (Giai đoạn D)

Kiểm tra phân giải DNS (`getent hosts`) và kết nối TCP từ bên trong container `spark-master` tới `hadoop-namenode` và `slave01` (`hadoop-datanode`):

```text
172.24.0.2      hadoop-namenode
172.24.0.3      slave01
TCP connection to hadoop-namenode:9000 OK
```

- Không xảy ra lỗi `UnknownHostException`, `No FileSystem for scheme: hdfs`, hay `Connection refused`.
- Spark nhận diện chính xác `fs.defaultFS = hdfs://hadoop-namenode:9000`.

---

## 5. Kết quả Đọc 3 File CSV từ HDFS bằng PySpark (Giai đoạn E)

Tệp kịch bản kiểm thử: [compute/spark/jobs/milestone3_hdfs_test.py](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/spark/jobs/milestone3_hdfs_test.py).

Lệnh thực thi chuẩn từ container `spark-master`:
```powershell
docker exec -u 0 spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-jobs/milestone3_hdfs_test.py
```

### 5.1. Tổng hợp Số dòng và Schema của 3 DataFrames

| DataFrame | Đường dẫn HDFS | Số dòng đọc được | Số cột | Khớp với dữ liệu gốc |
| :--- | :--- | :--- | :--- | :--- |
| **`train`** | `hdfs://hadoop-namenode:9000/bike-sharing/raw/train.csv` | **`10,886`** | `12` | **100% (10,886 dòng)** |
| **`test`** | `hdfs://hadoop-namenode:9000/bike-sharing/raw/test.csv` | **`6,493`** | `9` | **100% (6,493 dòng)** |
| **`sampleSubmission`** | `hdfs://hadoop-namenode:9000/bike-sharing/raw/sampleSubmission.csv` | **`6,493`** | `2` | **100% (6,493 dòng)** |

### 5.2. Schema & 5 Dòng đầu tiên của `train.csv`
```text
root
 |-- datetime: timestamp (nullable = true)
 |-- season: integer (nullable = true)
 |-- holiday: integer (nullable = true)
 |-- workingday: integer (nullable = true)
 |-- weather: integer (nullable = true)
 |-- temp: double (nullable = true)
 |-- atemp: double (nullable = true)
 |-- humidity: integer (nullable = true)
 |-- windspeed: double (nullable = true)
 |-- casual: integer (nullable = true)
 |-- registered: integer (nullable = true)
 |-- count: integer (nullable = true)

+-------------------+------+-------+----------+-------+----+------+--------+---------+------+----------+-----+
|datetime           |season|holiday|workingday|weather|temp|atemp |humidity|windspeed|casual|registered|count|
+-------------------+------+-------+----------+-------+----+------+--------+---------+------+----------+-----+
|2011-01-01 00:00:00|1     |0      |0         |1      |9.84|14.395|81      |0.0      |3     |13        |16   |
|2011-01-01 01:00:00|1     |0      |0         |1      |9.02|13.635|80      |0.0      |8     |32        |40   |
|2011-01-01 02:00:00|1     |0      |0         |1      |9.02|13.635|80      |0.0      |5     |27        |32   |
|2011-01-01 03:00:00|1     |0      |0         |1      |9.84|14.395|75      |0.0      |3     |10        |13   |
|2011-01-01 04:00:00|1     |0      |0         |1      |9.84|14.395|75      |0.0      |0     |1         |1    |
+-------------------+------+-------+----------+-------+----+------+--------+---------+------+----------+-----+
```

### 5.3. Schema & 5 Dòng đầu tiên của `test.csv` và `sampleSubmission.csv`
```text
# test.csv (6493 dòng, 9 cột)
+-------------------+------+-------+----------+-------+-----+------+--------+---------+
|datetime           |season|holiday|workingday|weather|temp |atemp |humidity|windspeed|
+-------------------+------+-------+----------+-------+-----+------+--------+---------+
|2011-01-20 00:00:00|1     |0      |1         |1      |10.66|11.365|56      |26.0027  |
|2011-01-20 01:00:00|1     |0      |1         |1      |10.66|13.635|56      |0.0      |
|2011-01-20 02:00:00|1     |0      |1         |1      |10.66|13.635|56      |0.0      |
|2011-01-20 03:00:00|1     |0      |1         |1      |10.66|12.88 |56      |11.0014  |
|2011-01-20 04:00:00|1     |0      |1         |1      |10.66|12.88 |56      |11.0014  |
+-------------------+------+-------+----------+-------+-----+------+--------+---------+

# sampleSubmission.csv (6493 dòng, 2 cột)
+-------------------+-----+
|datetime           |count|
+-------------------+-----+
|2011-01-20 00:00:00|0    |
|2011-01-20 01:00:00|0    |
|2011-01-20 02:00:00|0    |
|2011-01-20 03:00:00|0    |
|2011-01-20 04:00:00|0    |
+-------------------+-----+
```

---

## 6. Kết quả PySpark DataFrame API & Spark SQL (Giai đoạn E)

### 6.1. Thống kê theo Mùa (`season`) bằng DataFrame API (`groupBy` & `agg`)
```text
+------+------------+----------+----------------+
|season|record_count|avg_demand|avg_temp_celsius|
+------+------------+----------+----------------+
|1     |2686        |116.34    |12.53           |
|2     |2733        |215.25    |22.82           |
|3     |2733        |234.42    |28.79           |
|4     |2734        |198.99    |16.65           |
+------+------------+----------+----------------+
```

### 6.2. Truy vấn Thống kê theo Thời tiết & Ngày làm việc bằng Spark SQL (`bike_train_view`)
```text
+-------+----------+-----------+----------------+----------------+
|weather|workingday|total_hours|avg_bike_rentals|max_bike_rentals|
+-------+----------+-----------+----------------+----------------+
|1      |1         |4839       |206.16          |977             |
|1      |0         |2353       |203.35          |757             |
|2      |1         |1937       |184.71          |890             |
|2      |0         |897        |166.53          |783             |
|3      |1         |635        |118.22          |891             |
|3      |0         |224        |120.63          |545             |
|4      |1         |1          |164.0           |164             |
+-------+----------+-----------+----------------+----------------+
```

---

## 7. Kết quả Ghi và Đọc lại Parquet trên HDFS (Giai đoạn F)

1. **Biến đổi thử nghiệm (Tuân thủ nguyên tắc chống Data Leakage):**
   - Đã loại bỏ 2 cột `casual` và `registered` (`train_df.drop("casual", "registered")`).
   - Trích xuất thêm 3 cột thời gian: `hour`, `month`, `year`.
2. **Đường dẫn Parquet trên HDFS:**
   `hdfs://hadoop-namenode:9000/bike-sharing/milestone3_test/train_clean_parquet`
3. **Danh sách file trên HDFS (`hdfs dfs -ls -h`):**
   ```text
   Found 2 items
   -rw-r--r--   3 root  supergroup          0 2026-11-11 04:51 /bike-sharing/milestone3_test/train_clean_parquet/_SUCCESS
   -rw-r--r--   3 spark supergroup    117.7 K 2026-11-11 04:51 /bike-sharing/milestone3_test/train_clean_parquet/part-00000-1c703bb3-f15f-450a-a849-ac4f53d42617-c000.snappy.parquet
   ```
   *(Nhận xét: Dung lượng file giảm từ **633.2 KB** ở định dạng CSV xuống chỉ còn **117.7 KB** ở định dạng Snappy Parquet — tiết kiệm hơn 81% dung lượng lưu trữ).*
4. **Kết quả đọc lại Parquet từ HDFS:**
   - Số dòng: **`10,886 dòng`**, **`13 cột`** (đã loại bỏ `casual`, `registered` và thêm `hour`, `month`, `year`).
   - 5 dòng mẫu đọc lại từ Parquet:
     ```text
     +-------------------+------+-------+----------+-------+----+------+--------+---------+-----+----+-----+----+
     |datetime           |season|holiday|workingday|weather|temp|atemp |humidity|windspeed|count|hour|month|year|
     +-------------------+------+-------+----------+-------+----+------+--------+---------+-----+----+-----+----+
     |2011-01-01 00:00:00|1     |0      |0         |1      |9.84|14.395|81      |0.0      |16   |0   |1    |2011|
     |2011-01-01 01:00:00|1     |0      |0         |1      |9.02|13.635|80      |0.0      |40   |1   |1    |2011|
     |2011-01-01 02:00:00|1     |0      |0         |1      |9.02|13.635|80      |0.0      |32   |2   |1    |2011|
     |2011-01-01 03:00:00|1     |0      |0         |1      |9.84|14.395|75      |0.0      |13   |3   |1    |2011|
     |2011-01-01 04:00:00|1     |0      |0         |1      |9.84|14.395|75      |0.0      |1    |4   |1    |2011|
     +-------------------+------+-------+----------+-------+----+------+--------+---------+-----+----+-----+----+
     ```
5. **Xác nhận an toàn dữ liệu gốc:** Thư mục `/bike-sharing/raw/` vẫn giữ nguyên vẹn 3 tệp `train.csv` (633.2 K), `test.csv` (316.3 K) và `sampleSubmission.csv` (139.5 K).

---

## 8. Các Lỗi Gặp Phải và Cách Khắc phục

1. **Vấn đề: Lỗi `basedir must be absolute: ?/.ivy2/local` và `UnixLoginModule: NullPointerException: invalid null input: name` khi chạy `spark-submit` với user mặc định của image Bitnami:**
   - **Nguyên nhân gốc rễ:** Image `bitnamilegacy/spark:3.5` chạy container dưới quyền non-root UID `1001`, nhưng UID `1001` không có dòng định danh tương ứng trong tệp `/etc/passwd` của container. Khi JVM (OpenJDK 17) khởi chạy `spark-submit` và `Hadoop UserGroupInformation`, hàm native `UnixSystem.getUsername()` tra cứu UID `1001` trong `/etc/passwd` trả về `null` (và `user.home` trở thành `?`).
   - **Cách khắc phục sạch và không cần sửa image:**
     1. Truyền cờ `-u 0` (`root` — đã có sẵn trong `/etc/passwd`) khi gọi `docker exec -u 0 spark-master /opt/bitnami/spark/bin/spark-submit ...`.
     2. Đồng thời đặt `os.environ["HADOOP_USER_NAME"] = "root"` và `.config("spark.executorEnv.HADOOP_USER_NAME", "root")` trong script PySpark để cả Driver và Executor trên Worker xác thực danh tính với HDFS NameNode một cách nhất quán.

---

## 9. Dung lượng Ổ C Sau Triển khai

Kiểm tra bằng lệnh PowerShell `Get-PSDrive C`:
- **Đã sử dụng (`UsedGB`):** `433.41 GB`
- **Còn trống (`FreeGB`):** **`8.05 GB`**
- **Lưu ý quan trọng cho các Milestone tiếp theo:** Do toàn bộ image của cả Hadoop và Spark hiện đã được tải đầy đủ về máy, ở các Milestone 4, 5, 6, 7 chúng ta **không cần pull thêm bất kỳ Docker image nặng nào nữa** (tránh build thêm image Jupyter nếu không cần thiết vì mọi job PySpark đều chạy trực tiếp trên `spark-master` và `spark-worker-1`, còn Streamlit Dashboard có thể chạy nhẹ nhàng). Nhờ đó dung lượng ổ C sẽ được bảo toàn ổn định ở mức > 8 GB.

