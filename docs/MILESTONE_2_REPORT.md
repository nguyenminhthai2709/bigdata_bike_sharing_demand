# Báo cáo Hoàn thành Milestone 2: Triển khai HDFS và Upload Dataset

**Đề tài:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Milestone:** Milestone 2 – Hadoop HDFS & Data Ingestion  
**Thời gian thực hiện:** 2026-11-11  
**Trạng thái:** **HOÀN THÀNH (COMPLETED & VERIFIED)**

---

## 1. Các Thay đổi Cấu hình & Sao lưu (Giai đoạn A)

1. **Sao lưu an toàn có timestamp:**
   - Thư mục sao lưu: `backups/milestone2_20261111_093655/`
   - Các tệp đã sao lưu:
     - `docker-compose.yml.bak` (`20,769 bytes`)
     - `hadoop.env.bak` (`4,201 bytes`)
2. **Thay đổi trong [hadoop/hadoop.env](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/hadoop/hadoop.env):**
   - Chỉ thay đổi duy nhất dòng 28 từ `HDFS_CONF_dfs_replication=2` thành `HDFS_CONF_dfs_replication=1` để tương thích với cấu hình 1 DataNode (`hadoop-datanode`).
   - Kết quả `git diff hadoop/hadoop.env`:
     ```diff
     -HDFS_CONF_dfs_replication=2
     +HDFS_CONF_dfs_replication=1
     ```
3. **Tạo tệp `.env` từ `.env.example`:**
   - Do tệp [docker-compose.yml](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docker-compose.yml) có khai báo biến bắt buộc `${NIFI_PASSWORD:?...}`, lệnh `docker compose config -q` yêu cầu sự tồn tại của tệp `.env`.
   - Đã sao chép nguyên bản `.env.example` sang `.env` (không chỉnh sửa nội dung, không hiển thị thông tin nhạy cảm).
   - Giữ nguyên 100% tệp [docker-compose.yml](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docker-compose.yml), [compute/jupyter/Dockerfile](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/compute/jupyter/Dockerfile) và `spark-defaults.conf`.
4. **Kiểm tra cấu hình Docker Compose:**
   - Lệnh `docker compose config -q` trả về `ExitCode: 0` (hợp lệ hoàn toàn, không có bất kỳ cảnh báo hay lỗi cú pháp nào).

---

## 2. Các Image Được Tải & Kiểm tra Dung lượng (Giai đoạn B)

Đã thực hiện tải có chọn lọc **chỉ 2 image cần thiết** cho HDFS NameNode và DataNode bằng lệnh `docker compose pull hadoop-namenode hadoop-datanode`:

| Repository | Tag | Kích thước hiển thị (Virtual Size) | Ghi chú |
| :--- | :--- | :--- | :--- |
| `bde2020/hadoop-namenode` | `2.0.0-hadoop3.2.1-java8` | `2.05 GB` | Chia sẻ toàn bộ base layers Debian/Java8/Hadoop với DataNode |
| `bde2020/hadoop-datanode` | `2.0.0-hadoop3.2.1-java8` | `2.05 GB` | Chỉ tốn thêm các layer mỏng riêng của daemon DataNode |

**Thống kê `docker system df` sau khi tải image:**
```text
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          18        12        9.735GB   1.051GB (10%)
Containers      14        6         290.8kB   94.21kB (32%)
Local Volumes   11        11        969.4MB   0B (0%)
Build Cache     59        0         1.648GB   268.3MB
```
*(Tổng dung lượng Images trong Docker chỉ tăng từ `7.684 GB` lên `9.735 GB`, tức thực tế chỉ chiếm thêm **~2.05 GB** nhờ cơ chế chia sẻ layer).*

---

## 3. Các Service Đã Khởi chạy & Trạng thái Hệ thống (Giai đoạn C & D)

Chỉ khởi chạy đúng 2 services bằng lệnh:
```powershell
docker compose up -d hadoop-namenode hadoop-datanode
```

### 3.1. Trạng thái Container (`docker compose ps`)
| Tên Container | Image | Service | Trạng thái (Status) | Cổng ánh xạ (Ports) |
| :--- | :--- | :--- | :--- | :--- |
| `hadoop-namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | `hadoop-namenode` | **`Up (healthy)`** | `0.0.0.0:9870->9870/tcp`, `0.0.0.0:9820->9000/tcp` |
| `hadoop-datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | `hadoop-datanode` | **`Up (healthy)`** | `0.0.0.0:9864->9864/tcp` |

- Toàn bộ **6 containers của Langfuse** (`langfuse-web-1`, `langfuse-worker-1`, `postgres-1`, `redis-1`, `clickhouse-1`, `minio-1`) vẫn giữ nguyên trạng thái `Up (20 hours)`, không bị ảnh hưởng.
- Kiểm tra truy cập **HDFS NameNode Web UI** tại `http://localhost:9870`: Trả về **`HTTP 200 OK`**.

### 3.2. Kết quả `hdfs dfsadmin -report`
```text
Configured Capacity: 1081101176832 (1006.85 GB)
Present Capacity: 1012946878464 (943.38 GB)
DFS Remaining: 1012946853888 (943.38 GB)
DFS Used: 24576 (24 KB)
DFS Used%: 0.00%
Replicated Blocks:
	Under replicated blocks: 0
	Blocks with corrupt replicas: 0
	Missing blocks: 0
	Missing blocks (with replication factor 1): 0

-------------------------------------------------
Live datanodes (1):

Name: 172.24.0.3:9866 (hadoop-datanode.bigdata-cluster_bigdata-network)
Hostname: slave01
Decommission Status : Normal
Configured Capacity: 1081101176832 (1006.85 GB)
DFS Used: 24576 (24 KB)
Non DFS Used: 13161943040 (12.26 GB)
DFS Remaining: 1012946853888 (943.38 GB)
DFS Used%: 0.00%
DFS Remaining%: 93.70%
Xceivers: 1
```
- DataNode `slave01` (`172.24.0.3:9866`) đã đăng ký thành công với NameNode (`hadoop-namenode/172.24.0.2:9000`).

---

## 4. Kết quả Upload & Xác minh Dataset trên HDFS (Giai đoạn E)

Đã khởi tạo cấu trúc thư mục chuẩn trên HDFS (`/bike-sharing/raw`, `/bike-sharing/processed`, `/bike-sharing/features`, `/bike-sharing/predictions`, `/bike-sharing/benchmark`) và upload 3 tệp CSV từ `data/raw/` lên `/bike-sharing/raw/`.

### 4.1. Đường dẫn HDFS, Dung lượng và Số dòng Đối chiếu

| Tệp dữ liệu | Đường dẫn trên HDFS | Dung lượng (Bytes / Human) | Tổng dòng trên HDFS (`wc -l`) | Số dòng dữ liệu (trừ Header) | Số dòng gốc (`data/raw/`) | Kết quả đối chiếu |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`train.csv`** | `/bike-sharing/raw/train.csv` | `648,353 B` (`633.2 K`) | `10,887` | **`10,886`** | `10,886` | **Khớp 100%** |
| **`test.csv`** | `/bike-sharing/raw/test.csv` | `323,856 B` (`316.3 K`) | `6,494` | **`6,493`** | `6,493` | **Khớp 100%** |
| **`sampleSubmission.csv`** | `/bike-sharing/raw/sampleSubmission.csv` | `142,861 B` (`139.5 K`) | `6,494` | **`6,493`** | `6,493` | **Khớp 100%** |

- **Kết quả `hdfs dfs -ls -h /bike-sharing/raw`:**
  ```text
  Found 3 items
  -rw-r--r--   1 root supergroup    139.5 K 2026-11-11 03:08 /bike-sharing/raw/sampleSubmission.csv
  -rw-r--r--   1 root supergroup    316.3 K 2026-11-11 03:08 /bike-sharing/raw/test.csv
  -rw-r--r--   1 root supergroup    633.2 K 2026-11-11 03:08 /bike-sharing/raw/train.csv
  ```
- **Kết quả `hdfs dfs -count /bike-sharing/raw`:**
  - Số thư mục (`DIR_COUNT`): `1`
  - Số tệp (`FILE_COUNT`): `3`
  - Tổng dung lượng (`CONTENT_SIZE`): `1,115,070 bytes` (~1.06 MB)

### 4.2. Kiểm tra Đọc mẫu từ HDFS (`hdfs dfs -cat ... | head -n 3`)

- **`/bike-sharing/raw/train.csv`:**
  ```csv
  datetime,season,holiday,workingday,weather,temp,atemp,humidity,windspeed,casual,registered,count
  2011-01-01 00:00:00,1,0,0,1,9.84,14.395,81,0,3,13,16
  2011-01-01 01:00:00,1,0,0,1,9.02,13.635,80,0,8,32,40
  ```
- **`/bike-sharing/raw/test.csv`:**
  ```csv
  datetime,season,holiday,workingday,weather,temp,atemp,humidity,windspeed
  2011-01-20 00:00:00,1,0,1,1,10.66,11.365,56,26.0027
  2011-01-20 01:00:00,1,0,1,1,10.66,13.635,56,0
  ```
- **`/bike-sharing/raw/sampleSubmission.csv`:**
  ```csv
  datetime,count
  2011-01-20 00:00:00,0
  2011-01-20 01:00:00,0
  ```

### 4.3. Kiểm tra Sức khỏe Block (`hdfs fsck /bike-sharing/raw -files -blocks -locations`)
- **Trạng thái hệ thống file:** `The filesystem under path '/bike-sharing/raw' is HEALTHY`
- **Tổng số blocks đã xác thực:** `3 blocks` (mỗi file 1 block, `replication=1`, `Live_repl=1` nằm trên DataNode `172.24.0.3:9866`).
- **Under-replicated / Corrupt / Missing blocks:** `0 (0.0 %)`.

---

## 5. Dung lượng Ổ C Sau Triển khai

Kiểm tra bằng lệnh PowerShell `Get-PSDrive C`:
- **Đã sử dụng (`UsedGB`):** `425.63 GB`
- **Còn trống (`FreeGB`):** **`15.83 GB`** (Cao hơn rất nhiều so với ngưỡng cảnh báo an toàn 8.00 GB).

---

## 6. Ghi nhận Tình huống Phát sinh & Cách Xử lý

1. **Tình huống 1 — Biến bắt buộc trong `docker-compose.yml` khi chưa có `.env`:**
   - **Nguyên nhân:** Khi chạy `docker compose config -q` mà chưa tạo tệp `.env`, Docker Compose báo lỗi `required variable NIFI_PASSWORD is missing a value` do cú pháp `${NIFI_PASSWORD:?...}` ở dòng 183 của `docker-compose.yml`.
   - **Xử lý:** Sao chép `.env.example` sang `.env` theo đúng chuẩn của repository (không in nội dung nhạy cảm ra màn hình). Sau đó `docker compose config -q` trả về `ExitCode: 0`.
2. **Tình huống 2 — Thông báo `cat: Unable to write to output stream` khi kết hợp `hdfs dfs -cat | head -n 3`:**
   - **Nguyên nhân:** Đây là hành vi chuẩn của Unix pipe (`SIGPIPE`): khi lệnh `head -n 3` đọc đủ 3 dòng đầu tiên và đóng luồng đọc ngay lập tức, tiến trình Java của `hdfs dfs -cat` phát hiện pipe đã đóng nên in cảnh báo rồi dừng đọc phần còn lại của file.
   - **Đánh giá:** Hoàn toàn bình thường và giúp tiết kiệm thời gian không phải truyền hết file qua stdout.

