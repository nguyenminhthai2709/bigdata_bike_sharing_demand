# Báo cáo Kiểm tra Trước khi Triển khai Milestone 2 (MILESTONE 2 PRECHECK)

**Đề tài:** Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark  
**Repository:** `distributed_sys_docker_setup`  
**Thư mục làm việc:** `c:\Users\BOOK PRO\distributed_sys_docker_setup`  
**Trạng thái:** Chỉ kiểm tra (Read-only Precheck) — Chưa thay đổi bất kỳ cấu hình hay khởi chạy container nào.

---

## 1. Kiểm tra `git status` và Các Thay đổi Hiện có

Kết quả chạy `git status` và `git diff --stat`:
- **Nhánh hiện tại:** `main` (đồng bộ 100% với `origin/main`).
- **Tệp đã theo dõi bị chỉnh sửa (Modified tracked files):** **0 tệp** (`git diff` hoàn toàn trống — toàn bộ file gốc của repository gồm `docker-compose.yml`, `hadoop/hadoop.env`, `.env.example`, `compute/*` vẫn nguyên vẹn 100%).
- **Thư mục chưa theo dõi (Untracked directories):**
  - `data/` (chứa dữ liệu gốc bạn vừa thêm tại `data/raw/`).
  - `docs/` (chứa tài liệu `docs/REPOSITORY_AUDIT.md` từ Milestone 1).
- **Tệp `.env`:** Hiện **chưa tồn tại** (`Test-Path .env` trả về `False`).

---

## 2. Kiểm tra `docker-compose.yml` & Tên Chính xác các Service

Repository sử dụng tệp cấu hình duy nhất là [docker-compose.yml](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docker-compose.yml) (project name: `bigdata-cluster`, mạng nội bộ: `bigdata-network`).

Dưới đây là tên chính xác (`service name` và `container_name`) của các thành phần thuộc nhóm Hadoop, Spark và Jupyter:

| Thành phần | Tên Service trong `docker-compose.yml` | `container_name` | Image sử dụng |
| :--- | :--- | :--- | :--- |
| **Hadoop NameNode** | `hadoop-namenode` | `hadoop-namenode` | `bde2020/hadoop-namenode:${HADOOP_IMAGE_TAG:-2.0.0-hadoop3.2.1-java8}` |
| **Hadoop DataNode 1** | `hadoop-datanode` | `hadoop-datanode` (hostname: `slave01`) | `bde2020/hadoop-datanode:${HADOOP_IMAGE_TAG:-2.0.0-hadoop3.2.1-java8}` |
| **Hadoop DataNode 2** | `hadoop-datanode-2` | `hadoop-datanode-2` (hostname: `slave02`) | `bde2020/hadoop-datanode:${HADOOP_IMAGE_TAG:-2.0.0-hadoop3.2.1-java8}` |
| **YARN ResourceManager** | `hadoop-resourcemanager` | `hadoop-resourcemanager` | `bde2020/hadoop-resourcemanager:${HADOOP_IMAGE_TAG:-2.0.0-hadoop3.2.1-java8}` |
| **YARN NodeManager** | `hadoop-nodemanager` | `hadoop-nodemanager` | `bde2020/hadoop-nodemanager:${HADOOP_IMAGE_TAG:-2.0.0-hadoop3.2.1-java8}` |
| **YARN HistoryServer** | `hadoop-historyserver` | `hadoop-historyserver` | `bde2020/hadoop-historyserver:${HADOOP_IMAGE_TAG:-2.0.0-hadoop3.2.1-java8}` |
| **Spark Master** | `spark-master` | `spark-master` | `${SPARK_IMAGE:-bitnamilegacy/spark:3.5}` |
| **Spark Worker 1** | `spark-worker-1` | `spark-worker-1` | `${SPARK_IMAGE:-bitnamilegacy/spark:3.5}` |
| **Spark Worker 2** | `spark-worker-2` | `spark-worker-2` | `${SPARK_IMAGE:-bitnamilegacy/spark:3.5}` |
| **JupyterLab** | `jupyter` | `jupyter` | `bigdata-cluster/jupyter-pyspark:3.5` (build từ `./compute/jupyter`) |

*(Các service khác trong file nhưng không thuộc nhóm trên: `postgres`, `minio`, `minio-init`, `kafka`, `kafka-ui`, `nifi`, `flink-jobmanager`, `flink-taskmanager`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`).*

---

## 3. Kiểm tra Quan hệ Phụ thuộc (Dependencies) giữa các Service

Trong [docker-compose.yml](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docker-compose.yml), quan hệ phụ thuộc giữa các service liên quan được định nghĩa qua cả `depends_on` của Docker Compose và biến môi trường `SERVICE_PRECONDITION` (cơ chế chờ cổng của image `bde2020`):

1. **Nhóm HDFS:**
   - `hadoop-namenode`: Không phụ thuộc vào service nào (khởi chạy đầu tiên). Có `healthcheck` kiểm tra cổng `9870`.
   - `hadoop-datanode`:
     - `depends_on`: `hadoop-namenode`
     - `SERVICE_PRECONDITION`: `"hadoop-namenode:9870"` (chờ NameNode mở cổng HTTP 9870 mới bắt đầu chạy daemon DataNode).
   - `hadoop-datanode-2`:
     - `depends_on`: `hadoop-namenode`
     - `SERVICE_PRECONDITION`: `"hadoop-namenode:9870"`
2. **Nhóm YARN (Không bắt buộc khi chạy Spark Standalone):**
   - `hadoop-resourcemanager`: `depends_on` $\rightarrow$ `hadoop-namenode`, `hadoop-datanode`; `SERVICE_PRECONDITION: "hadoop-namenode:9000 hadoop-namenode:9870 hadoop-datanode:9864"`.
   - `hadoop-nodemanager` & `hadoop-historyserver`: `depends_on` $\rightarrow$ `hadoop-resourcemanager`.
3. **Nhóm Spark & Jupyter:**
   - `spark-master`: Không có `depends_on` cứng trong Compose, tự khởi chạy độc lập.
   - `spark-worker-1` & `spark-worker-2`: `depends_on` $\rightarrow$ `spark-master` (kết nối tới `spark://spark-master:7077`).
   - `jupyter`: `depends_on` $\rightarrow$ `spark-master` (cấu hình sẵn `spark.hadoop.fs.defaultFS=hdfs://hadoop-namenode:9000` trong `compute/jupyter/spark-defaults.conf`).

---

## 4. Kiểm tra Cấu hình Hadoop Replication Hiện tại

Kiểm tra trong tệp [hadoop/hadoop.env](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/hadoop/hadoop.env) (dòng 15–31):

```ini
CORE_CONF_fs_defaultFS=hdfs://hadoop-namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
HDFS_CONF_dfs_webhdfs_enabled=true
HDFS_CONF_dfs_permissions_enabled=false
HDFS_CONF_dfs_replication=2
HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false
```

- **Trạng thái hiện tại:** `HDFS_CONF_dfs_replication=2`.
- **Đánh giá kỹ thuật:**
  - Cấu hình gốc đặt `dfs.replication=2` vì tác giả repo giả định bật cả 2 DataNodes (`hadoop-datanode` và `hadoop-datanode-2`).
  - Với giới hạn RAM hiện tại (chỉ bật **1 DataNode** duy nhất là `hadoop-datanode`), nếu giữ `HDFS_CONF_dfs_replication=2`, mọi block ghi xuống HDFS chỉ có 1 bản sao thực tế so với yêu cầu 2 bản sao, dẫn đến cảnh báo `Under-replicated blocks` liên tục trên NameNode và gây lỗi khi thực hiện một số thao tác ghi nối (append).
  - **Cần điều chỉnh ở bước triển khai:** Đổi thành `HDFS_CONF_dfs_replication=1` trước khi khởi chạy HDFS.

---

## 5. Kiểm tra Các Volume và Bind Mount Hiện có

### 5.1. Named Volumes (Lưu trong Docker WSL2 Virtual Disk trên ổ C)
Đối với nhóm Hadoop & Spark, [docker-compose.yml](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docker-compose.yml) định nghĩa:
- `hadoop_namenode` $\rightarrow$ gắn vào `/hadoop/dfs/name` của `hadoop-namenode` (lưu FsImage và EditLogs).
- `hadoop_datanode` $\rightarrow$ gắn vào `/hadoop/dfs/data` của `hadoop-datanode` (lưu các HDFS data blocks).
- `hadoop_datanode_2` $\rightarrow$ gắn vào `/hadoop/dfs/data` của `hadoop-datanode-2`.
- `hadoop_historyserver` $\rightarrow$ gắn vào `/hadoop/yarn/timeline` của `hadoop-historyserver`.

*(Hiện tại các volume này chưa được tạo trên máy vì chưa chạy `docker compose up` lần nào).*

### 5.2. Bind Mounts (Ánh xạ thư mục từ máy Host vào Container)
Hiện tại trong [docker-compose.yml](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docker-compose.yml):
- **`hadoop-namenode` và `hadoop-datanode`:**
  - Chưa có bind mount thư mục dữ liệu từ máy host (chỉ nạp biến môi trường từ `./hadoop/hadoop.env`).
  - **Hệ quả:** Container `hadoop-namenode` hiện chưa nhìn thấy thư mục `./data/raw/` trên máy host nếu không dùng `docker cp` hoặc thêm bind mount.
- **`spark-master`, `spark-worker-1`, `spark-worker-2`:**
  - Chỉ mount `./compute/spark/jobs:/opt/spark-jobs`.
  - Chưa mount `./src`, `./data`, `./outputs`, `./models`.
- **`jupyter`:**
  - Mount `./compute/jupyter/notebooks:/home/jovyan/work` và `./compute/spark/jobs:/opt/spark-jobs`.

---

## 6. Xác nhận Cổng (Ports) và Nguy cơ Xung đột

Đối chiếu các cổng đang mở bởi 6 containers của cụm `langfuse-local` với toàn bộ cổng trong [docker-compose.yml](file:///c:/Users/BOOK%20PRO/distributed_sys_docker_setup/docker-compose.yml):

| Cổng Host | Container đang chiếm (Langfuse) | Service trong `docker-compose.yml` | Trạng thái cho dự án Bike Sharing |
| :--- | :--- | :--- | :--- |
| `3000`, `3030` | `langfuse-web-1`, `langfuse-worker-1` | *(Không có)* | **An toàn** |
| `5432` | `langfuse-local-postgres-1` | `postgres` (`5432:5432`) | **XUNG ĐỘT** $\rightarrow$ Tuyệt đối không bật `postgres` |
| `6379` | `langfuse-local-redis-1` | *(Không có)* | **An toàn** |
| `8123` | `langfuse-local-clickhouse-1` | *(Không có)* | **An toàn** |
| `9000` | `langfuse-local-clickhouse-1` | `minio` (`9000:9000`) | **XUNG ĐỘT** $\rightarrow$ Tuyệt đối không bật `minio` |
| `9090`, `9091` | `langfuse-local-minio-1` | *(Không có)* | **An toàn** |
| `9870` | *(Trống)* | `hadoop-namenode` (Web UI) | **An toàn (Sử dụng)** |
| `9820` | *(Trống)* | `hadoop-namenode` (`9820:9000` RPC) | **An toàn (Sử dụng)** — Cổng 9000 nội bộ đã được ánh xạ ra 9820 trên host |
| `9864` | *(Trống)* | `hadoop-datanode` (Web UI & WebHDFS) | **An toàn (Sử dụng)** |
| `7077`, `8080` | *(Trống)* | `spark-master` (RPC & Web UI) | **An toàn (Sử dụng)** |
| `8888`, `4040` | *(Trống)* | `jupyter` (JupyterLab & Spark App UI) | **An toàn (Sử dụng)** |

---

## 7. Đánh giá những Service Thực sự Cần thiết & Rủi ro Tài nguyên

### 7.1. Danh sách Service Tối ưu Cần Bật
Để đảm bảo máy chạy mượt mà trong giới hạn **~4.89 GiB RAM trống của Docker** và **14.49 GB ổ C còn trống**, dự án chỉ sử dụng đúng **4–5 services**:
1. **`hadoop-namenode`** (Bắt buộc — Quản lý HDFS)
2. **`hadoop-datanode`** (Bắt buộc — Lưu trữ HDFS blocks)
3. **`spark-master`** (Bắt buộc ở Milestone 3 — Quản lý cụm Spark)
4. **`spark-worker-1`** (Bắt buộc ở Milestone 3 — Thực thi tính toán PySpark/MLlib)
5. **`jupyter`** (Tùy chọn bật khi cần chạy Notebook tương tác; tắt khi không dùng để tiết kiệm ~600MB RAM).

*(Riêng trong **Milestone 2**, chúng ta thậm chí chỉ cần bật trước 2 services: `hadoop-namenode` và `hadoop-datanode` để kiểm thử HDFS và nạp dữ liệu).*

### 7.2. Phân tích Rủi ro Tài nguyên (Ổ đĩa C & RAM)
- **Dung lượng ổ C (còn trống 14.49 GB):**
  - Ở Milestone 2, khi khởi chạy `hadoop-namenode` và `hadoop-datanode`, Docker chỉ tải 2 images của `bde2020` (dùng chung base layer Debian + Java 8 + Hadoop 3.2.1), tổng dung lượng chiếm dụng trên ổ C chỉ khoảng **~1.1 – 1.3 GB** (ổ C sẽ còn trống > 13 GB).
  - Sang Milestone 3, khi tải thêm `bitnamilegacy/spark:3.5`, sẽ tốn thêm khoảng **~1.4 GB** (ổ C vẫn còn trống > 11.5 GB — hoàn toàn trong ngưỡng an toàn).
- **Dung lượng RAM:**
  - Khi chỉ chạy `hadoop-namenode` + `hadoop-datanode` ở Milestone 2, mức RAM tiêu thụ thêm chỉ khoảng **~800 MB – 1 GB**.

---

## 8. Kiểm tra Vị trí Dataset và Xác nhận Tính Toàn vẹn của 3 File CSV

Đã kiểm tra trực tiếp thư mục `data/raw/` bằng PowerShell (`Get-ChildItem`, `Get-Content`, `Get-FileHash`). Cả 3 tệp dữ liệu gốc đều tồn tại đầy đủ, nguyên vẹn và khớp chính xác với yêu cầu:

| Tên tệp | Đường dẫn | Dung lượng (Bytes) | Tổng số dòng (gồm Header) | Số dòng dữ liệu (Data Rows) | Số cột | Mã băm SHA-256 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`train.csv`** | `data/raw/train.csv` | `648,353` (~633 KB) | `10,887` | **`10,886`** | `12` | `0CF70C35EEB6405A40CC9E124854E2DA901FA88FE7374A75D141E7401BF4C857` |
| **`test.csv`** | `data/raw/test.csv` | `323,856` (~316 KB) | `6,494` | **`6,493`** | `9` | `3F1D0EBB197BA9B9A79280B30912093CE18959FAC6E1931691092CDA449EB63E` |
| **`sampleSubmission.csv`** | `data/raw/sampleSubmission.csv` | `142,861` (~140 KB) | `6,494` | **`6,493`** | `2` | `58C27951183B10437D6752CF5BAC708AE9F80490D991C24FF76DE296F71CB2F2` |

**Kiểm tra cấu trúc cột (Header) thực tế:**
- `train.csv`: `datetime,season,holiday,workingday,weather,temp,atemp,humidity,windspeed,casual,registered,count`
- `test.csv`: `datetime,season,holiday,workingday,weather,temp,atemp,humidity,windspeed`
- `sampleSubmission.csv`: `datetime,count`

---

## 9. Kế hoạch Triển khai HDFS An toàn trong Milestone 2

Khi bạn xác nhận cho phép triển khai Milestone 2, chúng ta sẽ thực hiện tuần tự 6 bước an toàn sau:

### Bước 1: Sao lưu cấu hình gốc & Tạo `.env`
1. Sao lưu `docker-compose.yml` $\rightarrow$ `docker-compose.bak.yml`.
2. Sao lưu `hadoop/hadoop.env` $\rightarrow$ `hadoop/hadoop.env.bak`.
3. Tạo file `.env` từ `.env.example` (`Copy-Item .env.example .env`).

### Bước 2: Tinh chỉnh cấu hình tối thiểu cho HDFS 1-DataNode
1. Trong `hadoop/hadoop.env`: Đổi `HDFS_CONF_dfs_replication=2` thành `HDFS_CONF_dfs_replication=1`.
2. Trong `docker-compose.yml`:
   - Thêm bind mount `./data:/opt/data:ro` và `./src:/opt/src` vào `hadoop-namenode` (và `spark-master`, `spark-worker-1`, `jupyter` để chuẩn bị cho Milestone 3) nhằm giúp container truy cập trực tiếp dataset và script mà không tốn thêm dung lượng copy file.
   - Kiểm tra cú pháp bằng lệnh: `docker compose config -q`.

### Bước 3: Chỉ khởi chạy 2 dịch vụ HDFS (`hadoop-namenode` và `hadoop-datanode`)
Chạy lệnh PowerShell chỉ định rõ tên 2 services (tuyệt đối không bật toàn bộ cụm):
```powershell
docker compose up -d hadoop-namenode hadoop-datanode
```

### Bước 4: Kiểm tra sức khỏe cụm HDFS & Dung lượng ổ C
1. Kiểm tra trạng thái container:
   ```powershell
   docker compose ps
   ```
   *(Kỳ vọng: `hadoop-namenode` ở trạng thái `healthy`, `hadoop-datanode` ở trạng thái `Up`).*
2. Kiểm tra báo cáo HDFS DataNode:
   ```powershell
   docker exec hadoop-namenode hdfs dfsadmin -report
   ```
   *(Kỳ vọng: `Live datanodes (1)`).*
3. Kiểm tra lại dung lượng ổ C (`Get-PSDrive C`).

### Bước 5: Xây dựng Module 1 (`src/ingestion/upload_to_hdfs.py`) & Đẩy dữ liệu lên HDFS
Viết script Python tiêu chuẩn kiểm tra tính toàn vẹn của `train.csv`, `test.csv`, `sampleSubmission.csv` (số dòng, số cột, tên cột) và thực hiện:
1. Tạo cây thư mục chuẩn trên HDFS:
   - `/bike-sharing/raw`
   - `/bike-sharing/processed`
   - `/bike-sharing/features`
   - `/bike-sharing/predictions`
   - `/bike-sharing/benchmark`
2. Upload `train.csv`, `test.csv`, `sampleSubmission.csv` lên `/bike-sharing/raw/`.

### Bước 6: Xác minh dữ liệu trên HDFS
Kiểm tra danh sách file, kích thước block, hệ số replication (=1) và đọc thử 5 dòng đầu của `train.csv` và `test.csv` trực tiếp từ HDFS:
```powershell
docker exec hadoop-namenode hdfs dfs -ls -h /bike-sharing/raw
docker exec hadoop-namenode hdfs dfs -cat /bike-sharing/raw/train.csv | Select-Object -First 5
```

