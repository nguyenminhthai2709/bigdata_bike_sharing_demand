# Hướng dẫn sử dụng cụm Big Data (cho người mới)

Tài liệu này hướng dẫn **từng bước** cách bật, kiểm tra và dùng thử từng thành
phần của cụm. Đọc [README.md](README.md) để hiểu tổng quan kiến trúc trước.

> Mọi lệnh chạy trong thư mục gốc dự án (`d:\Big_data`), bằng **PowerShell** hoặc
> **Git Bash** trên Windows.

---

## 1. Cụm này gồm những gì?

| Nhóm | Service | Dùng để làm gì |
|------|---------|----------------|
| **Lưu trữ** | HDFS (`hadoop-namenode` + 2×`hadoop-datanode`) | Hệ thống file phân tán — lưu file lớn, chia block, nhân bản 2 bản |
| **Tài nguyên** | YARN (`hadoop-resourcemanager` + `hadoop-nodemanager`) | Cấp phát CPU/RAM cho job (MapReduce, Spark-on-YARN...) |
| **Tính toán** | Spark (`spark-master` + 2×`spark-worker`) | Xử lý dữ liệu lớn theo batch/micro-batch, nhanh hơn MapReduce |
| **Streaming** | Kafka (`kafka`) + Kafka UI (`kafka-ui`) | Hàng đợi message real-time. Kafka không có UI sẵn nên có thêm `kafka-ui` |
| **Ingestion** | NiFi (`nifi`) | Kéo–thả pipeline: hút / định tuyến / biến đổi dữ liệu giữa các hệ thống |

> Còn Flink, Airflow, MinIO, Postgres đã định nghĩa sẵn trong `docker-compose.yml`
> nhưng **không bật** ở hướng dẫn này (xem README nếu cần).

**Sơ đồ mental model:**

```
   NiFi  ──►  Kafka  ──►  Spark  ──►  HDFS
 (thu thập)  (luồng)   (xử lý)    (lưu kết quả)
                          │
                          └── xin CPU/RAM từ ──►  YARN
```

---

## 2. Chuẩn bị

1. Cài **Docker Desktop** (Windows/Mac), mở lên, đợi biểu tượng cá voi hết quay.
2. Docker Desktop → **Settings → Resources**: để RAM ít nhất **6 GB** (cụm này
   idle tốn ~4 GB, chạy job sẽ hơn).
3. Kiểm tra Docker sẵn sàng:
   ```bash
   docker version        # in ra Client + Server => OK
   ```

---

## 3. Bật cụm

```bash
# Bật đúng 12 service: Hadoop + Kafka (+ Kafka UI) + Spark + NiFi
docker compose up -d $(docker compose config --services | grep -E 'hadoop|kafka|spark|nifi')

# HOẶC gõ đầy đủ:
docker compose up -d hadoop-namenode hadoop-datanode hadoop-datanode-2 \
  hadoop-resourcemanager hadoop-nodemanager hadoop-historyserver \
  kafka kafka-ui spark-master spark-worker-1 spark-worker-2 nifi
```

Lần đầu sẽ tải image (~15 GB) nên lâu. Nếu mạng nghẽn giữa chừng, **chạy lại lệnh
trên** — image đã tải xong được giữ cache.

**Đợi ~2–3 phút** rồi kiểm tra:

```bash
docker compose ps
```

Cột `STATUS` của Hadoop/Kafka/NiFi phải là **`(healthy)`**. `spark-master` /
`spark-worker` không có healthcheck nên chỉ hiện `Up` là bình thường.

> `hadoop-resourcemanager` có thể `Restarting` vài lần lúc đầu (đợi namenode) —
> bình thường, sẽ tự ổn định.

---

## 4. Kiểm tra cụm sống

```bash
# HDFS: phải thấy "Live datanodes (2)"
docker exec hadoop-namenode hdfs dfsadmin -report | grep "Live datanodes"

# YARN: phải thấy 1 node RUNNING
docker exec hadoop-resourcemanager yarn node -list

# Spark: 2 worker đăng ký
docker logs spark-master 2>&1 | grep "Registering worker"

# Kafka: broker trả lời
docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -1

# Kafka UI: HTTP 200
curl -s -o /dev/null -w "Kafka UI %{http_code}\n" http://localhost:8090

# NiFi: HTTP 200
curl -sk -o /dev/null -w "NiFi %{http_code}\n" https://localhost:8443/nifi/
```

---

## 5. Địa chỉ các giao diện web

Mở bằng trình duyệt trên **chính máy chạy Docker**. Từ máy khác thì thay
`localhost` bằng IP LAN của máy đó.

| Giao diện | Địa chỉ | Ghi chú |
|-----------|---------|---------|
| **HDFS NameNode** | http://localhost:9870 | Tab *Datanodes*, *Utilities → Browse the file system* |
| HDFS DataNode 1 / 2 | http://localhost:9864 / http://localhost:9865 | |
| **YARN ResourceManager** | http://localhost:8088 | Danh sách application, hàng đợi |
| YARN NodeManager | http://localhost:8042 | |
| YARN History Server | http://localhost:8188 | Log job đã chạy xong |
| **Spark Master** | http://localhost:8080 | Worker, application, executor |
| Spark Application UI | http://localhost:4040 | Chỉ sống khi có job chạy (cần map cổng 4040) |
| **Kafka UI** | http://localhost:8090 | Kafka không có UI sẵn — tool ngoài. Xem topic, message, consumer group |
| **NiFi** | https://localhost:8443/nifi | **https**, login `admin` / `nifiadmin12345` |

> NiFi báo lỗi chứng chỉ ("Your connection is not private") vì dùng cert tự ký →
> bấm **Advanced → Proceed**, hoặc trên Chrome/Edge bấm vào trang rồi gõ
> `thisisunsafe`.

---

## 6. Bài tập làm quen từng service

### 6.1. HDFS — đọc/ghi file phân tán

```bash
# Tạo thư mục trên HDFS
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/demo

# Đưa 1 file từ trong container lên HDFS
docker exec hadoop-namenode bash -c "echo 'hello big data' > /tmp/hello.txt && hdfs dfs -put -f /tmp/hello.txt /user/demo/"

# Liệt kê + đọc lại
docker exec hadoop-namenode hdfs dfs -ls /user/demo
docker exec hadoop-namenode hdfs dfs -cat /user/demo/hello.txt

# Xem file này được chia block & nằm trên datanode nào (replication = 2)
docker exec hadoop-namenode hdfs fsck /user/demo/hello.txt -files -blocks -locations
```

Sau đó vào http://localhost:9870 → *Utilities → Browse the file system* →
`/user/demo` để thấy file bằng giao diện.

### 6.2. YARN — chạy job MapReduce mẫu (tính số Pi)

```bash
docker exec hadoop-resourcemanager bash -c \
  "hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 2 10"
```

Trong lúc chạy, mở http://localhost:8088 sẽ thấy 1 application `RUNNING`. Xong thì
`FINISHED` và in ra `Estimated value of Pi is ...`.

### 6.3. Spark — chạy job trên cụm standalone

**a) Job bundled (SparkPi) — không cần chuẩn bị gì:**

```bash
docker exec spark-master bash -c \
  "/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 \
   --class org.apache.spark.examples.SparkPi \
   \$(ls /opt/bitnami/spark/examples/jars/spark-examples_*.jar) 10"
```

Kết quả: `Pi is roughly 3.14...`. Mở http://localhost:8080 xem app vừa chạy.

**b) Job PySpark của bạn — ghi kết quả xuống HDFS:**

Tạo file `compute/spark/jobs/hello_hdfs.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hello-hdfs").getOrCreate()
df = spark.createDataFrame([("alice", 1), ("bob", 2), ("carol", 3)], ["name", "value"])
df.show()
df.write.mode("overwrite").csv("hdfs://hadoop-namenode:9000/user/demo/spark_out")
spark.stop()
```

Thư mục `compute/spark/jobs/` đã được mount sẵn vào container tại `/opt/spark-jobs`:

```bash
docker exec spark-master bash -c \
  "/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 \
   /opt/spark-jobs/hello_hdfs.py"

# Kiểm tra kết quả trên HDFS
docker exec hadoop-namenode hdfs dfs -ls /user/demo/spark_out
docker exec hadoop-namenode hdfs dfs -cat /user/demo/spark_out/part-*
```

> File mẫu [compute/spark/jobs/example_job.py](compute/spark/jobs/example_job.py)
> ghi ra MinIO (S3) — **chỉ chạy được khi bật thêm service `minio`**.

### 6.4. Kafka — gửi/nhận message

Mở **2 cửa sổ terminal**.

Terminal 1 — tạo topic & mở consumer (chờ nhận):
```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic demo --partitions 3 --replication-factor 1

docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic demo --from-beginning
```

Terminal 2 — gõ message (mỗi dòng Enter là 1 message):
```bash
docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo
> xin chao
> message thu 2
```

Message gõ ở Terminal 2 sẽ hiện ngay ở Terminal 1. `Ctrl+C` để thoát.

Từ code **ngoài Docker** (trên máy host) thì bootstrap server là `localhost:9094`.

**Xem bằng giao diện:** mở http://localhost:8090 (Kafka UI) → cluster
`bigdata-kafka` → *Topics* → `demo` → tab *Messages* để đọc message, hoặc
*Produce Message* để gửi message mà không cần CLI.

### 6.5. NiFi — pipeline kéo–thả đầu tiên

1. Mở https://localhost:8443/nifi → qua cảnh báo cert → login `admin` /
   `nifiadmin12345`.
2. Kéo icon **Processor** (góc trên trái) vào canvas → chọn **GenerateFlowFile** →
   Add. Double-click → tab *Scheduling* đặt *Run Schedule* = `10 sec`.
3. Kéo thêm 1 Processor → **PutFile** → Add. Double-click → tab *Properties* đặt
   *Directory* = `/data`.
4. Tab *Settings* của PutFile: tick **success** và **failure** ở mục
   *Automatically Terminate Relationships*.
5. Rê từ **GenerateFlowFile** sang **PutFile** để nối (relationship `success`).
6. Bôi đen cả 2 → bấm nút **Start** (▶). Sau ~10s, xem file xuất hiện ở host:
   ```bash
   ls streaming/nifi/data/
   ```

Thư mục `streaming/nifi/data/` (host) = `/data` (trong container NiFi).

---

## 7. Các service gọi nhau bằng tên gì?

Bên trong mạng Docker (`bigdata-network`), dùng **tên service** làm hostname:

| Muốn kết nối tới | Địa chỉ dùng trong container | Địa chỉ từ máy host |
|------------------|------------------------------|---------------------|
| HDFS | `hdfs://hadoop-namenode:9000` | `hdfs://localhost:9820` |
| Spark master | `spark://spark-master:7077` | `spark://localhost:7077` |
| Kafka | `kafka:9092` | `localhost:9094` |
| YARN RM | `hadoop-resourcemanager:8032` | — |

Ví dụ: processor trong NiFi muốn đẩy dữ liệu vào Kafka thì điền broker là
`kafka:9092` (không phải `localhost`).

---

## 8. Tắt / bật lại / dọn dẹp

```bash
# Dừng tạm (giữ container + dữ liệu) — bật lại nhanh
docker compose stop

# Bật lại sau khi stop (không tải lại image)
docker compose start

# Xoá container nhưng GIỮ dữ liệu (volume)
docker compose down

# Xoá SẠCH cả dữ liệu (HDFS, flow NiFi, Kafka... về trắng)
docker compose down -v
```

Xem cụm đang ăn bao nhiêu RAM:
```bash
docker stats --no-stream
```

---

## 9. Lỗi thường gặp

| Triệu chứng | Cách xử lý |
|-------------|------------|
| `docker compose up` báo `image ... not found` | Bitnami đã gỡ vài image — `.env` đã trỏ sang bản `bitnamilegacy` / `custom`. Chạy lại lệnh up. |
| Container cứ `Restarting` | Xem log: `docker logs <tên-container>`. RM/NM restart lúc đầu là bình thường. |
| NiFi mở không được | Phải là `https://localhost:8443/nifi`; qua cảnh báo cert (`thisisunsafe`). |
| Spark UI (8080) trắng trang | Đang gõ `https://` → đổi thành `http://`. |
| "Live datanodes (0)" | Đợi thêm 1–2 phút; hoặc datanode chết — `docker compose restart hadoop-datanode hadoop-datanode-2`. |
| Máy lag, hết RAM | Tăng RAM cho Docker Desktop, hoặc tắt bớt: `docker compose stop nifi` (NiFi nặng nhất ~1.2 GB). |
| Job Spark/YARN treo ở `ACCEPTED` | YARN hết RAM (chỉ có 4 GB cho job). Đợi job khác xong, hoặc giảm `--executor-memory`. |

---

## 10. Đọc tiếp

- [README.md](README.md) — tổng quan, cấu hình, mở rộng
- [hadoop/hadoop.env](hadoop/hadoop.env) — chỉnh RAM/CPU YARN, replication HDFS
- [.env](.env) — user/password, phiên bản image
- [docker-compose.yml](docker-compose.yml) — toàn bộ định nghĩa service
