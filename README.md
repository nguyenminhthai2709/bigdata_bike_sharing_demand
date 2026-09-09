# Big Data Cluster (Docker Compose)

Cụm xử lý dữ liệu lớn phân tán chạy bằng Docker Compose, gồm:

| Lớp            | Công cụ                                   |
|----------------|--------------------------------------------|
| Compute        | Apache Spark (1 master + 2 worker)          |
| Compute        | Apache Flink (1 jobmanager + 1 taskmanager) |
| Hadoop         | HDFS (1 namenode + 2 datanode) + YARN (1 RM + 1 NM) |
| Orchestration  | Apache Airflow (LocalExecutor)              |
| Storage        | MinIO (S3-compatible)                       |
| Database       | PostgreSQL                                  |
| Streaming      | Apache Kafka (KRaft mode, không cần ZK)     |

## Cấu trúc thư mục

```
Big_data/
├── docker-compose.yml       # Toàn bộ cấu hình cụm (1 file duy nhất)
├── .env                     # Biến môi trường (user/pass, port, bucket...)
├── compute/                 # Lớp compute
│   ├── spark/
│   │   └── jobs/            # Script PySpark/Spark job để ở đây
│   └── flink/
│       └── jobs/            # Job Flink (JAR/Python) để ở đây
├── hadoop/                  # Lớp Hadoop (HDFS + YARN)
│   └── hadoop.env           # Toàn bộ cấu hình Hadoop (core/hdfs/yarn/mapred-site)
│                            # Dữ liệu HDFS nằm ở named volume hadoop_namenode / hadoop_datanode
├── orchestration/           # Lớp orchestration
│   └── airflow/
│       ├── dags/            # DAG của bạn để ở đây
│       ├── logs/
│       └── plugins/
├── storage/                 # Lớp storage
│   └── minio/                # Dữ liệu MinIO nằm ở named volume `minio_data`;
│                              # thư mục này để chứa config/policy nếu cần sau này
├── database/                 # Lớp database
│   └── postgres/
│       └── init/             # Script SQL/Shell chạy khi Postgres khởi tạo lần đầu
└── streaming/                # Lớp streaming
    └── kafka/                 # Dữ liệu Kafka nằm ở named volume `kafka_data`;
                                # thư mục này để chứa config (server.properties...) nếu cần sau này
```

> `minio/` và `kafka/` chưa bind-mount vào container — dữ liệu được Docker quản
> lý qua named volume (`minio_data`, `kafka_data`) để tránh lỗi permission khi
> bind-mount trên Windows. Hai thư mục này sẵn sàng để bạn bỏ file cấu hình vào
> khi cần tuỳ biến sâu hơn.

## Yêu cầu

- Docker + Docker Compose v2
- Tối thiểu ~8–10 GB RAM rảnh cho Docker (Spark + Flink + Airflow + Hadoop khá nặng).
  Nếu máy yếu, có thể bỏ bớt cụm Hadoop:
  `docker compose stop $(docker compose config --services | grep hadoop)`
  (hoặc chạy bớt 1 datanode: bỏ `hadoop-datanode-2` khi `up`).

## Khởi chạy

```bash
# Lên toàn bộ cụm
docker compose up -d

# Xem trạng thái / log
docker compose ps
docker compose logs -f airflow-webserver

# Tắt cụm (giữ lại dữ liệu)
docker compose down

# Tắt và xoá luôn volume dữ liệu (reset sạch)
docker compose down -v
```

Lần đầu chạy sẽ hơi lâu vì Airflow cần `db migrate` + cài thêm provider
(`_PIP_ADDITIONAL_REQUIREMENTS`), Postgres cần chạy init script để tạo thêm
database `airflow`, và MinIO cần tạo bucket mặc định.

### Chỉ chạy cụm Hadoop (HDFS + YARN)

Nếu chỉ cần Hadoop (không cần Spark/Flink/Airflow/Kafka...):

```bash
# 0) Bật Docker Desktop trước (Windows/Mac) rồi đợi engine sẵn sàng:
docker version        # chạy được -> engine đã lên

# 1) Lên 6 service Hadoop (HDFS: 1 namenode + 2 datanode;
#    YARN: 1 resourcemanager + 1 nodemanager; + historyserver)
docker compose up -d hadoop-namenode hadoop-datanode hadoop-datanode-2 \
  hadoop-resourcemanager hadoop-nodemanager hadoop-historyserver

# Cách ngắn gọn tương đương (lấy mọi service có tên chứa "hadoop"):
docker compose up -d $(docker compose config --services | grep hadoop)

# 2) Xem trạng thái — đợi tất cả chuyển sang (healthy)
docker compose ps

# 3) Kiểm tra HDFS — phải thấy "Live datanodes (2)"
docker exec -it hadoop-namenode hdfs dfsadmin -report

# 4) Kiểm tra YARN — phải thấy 1 node RUNNING
docker exec -it hadoop-resourcemanager yarn node -list
```

```bash
# Tắt riêng cụm Hadoop (giữ dữ liệu trong volume)
docker compose stop $(docker compose config --services | grep hadoop)

# Tắt và xoá container (vẫn giữ dữ liệu HDFS trong named volume)
docker compose rm -sf $(docker compose config --services | grep hadoop)

# Reset sạch dữ liệu HDFS
docker compose down
docker volume rm bigdata-cluster_hadoop_namenode \
  bigdata-cluster_hadoop_datanode bigdata-cluster_hadoop_datanode_2
```

> **Lưu ý khởi động**: `hadoop-resourcemanager` / `hadoop-nodemanager` /
> `hadoop-historyserver` có `SERVICE_PRECONDITION` nên container sẽ **tự
> restart vài lần** trong lúc chờ namenode + datanode mở cổng — đây là hành vi
> bình thường của image bde2020, `RestartCount` sẽ ngừng tăng khi cụm sẵn sàng.
> Namenode lên trước, khoảng 1–2 phút sau cả cụm mới `healthy`.

## Truy cập các UI

| Dịch vụ          | URL                          | Tài khoản mặc định           |
|-------------------|------------------------------|-------------------------------|
| Airflow           | http://localhost:8085        | admin / admin123              |
| Spark Master UI   | http://localhost:8080        | -                              |
| Flink UI          | http://localhost:8081        | -                              |
| HDFS NameNode UI  | http://localhost:9870        | -                              |
| HDFS DataNode #1  | http://localhost:9864        | -                              |
| HDFS DataNode #2  | http://localhost:9865        | -                              |
| YARN ResourceMgr  | http://localhost:8088        | -                              |
| YARN NodeManager  | http://localhost:8042        | -                              |
| YARN History UI   | http://localhost:8188        | -                              |
| HDFS RPC (từ host)| hdfs://localhost:9820        | - (trong docker: hadoop-namenode:9000) |
| MinIO Console     | http://localhost:9001        | minioadmin / minioadmin123    |
| MinIO API (S3)    | http://localhost:9000        | -                              |
| Kafka (broker)    | localhost:9094 (từ host)     | -                              |
| Postgres          | localhost:5432               | admin / admin123 / db `datawarehouse` |

> Đổi toàn bộ user/password mặc định trong file `.env` trước khi dùng ngoài
> mục đích học tập/demo local.

## Ghi chú vận hành

- **Kafka** chạy KRaft mode (không cần Zookeeper). Tạo topic thử:
  ```bash
  docker exec -it kafka kafka-topics.sh --create --topic demo \
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
  ```
- **Spark**: submit job từ container `spark-master`, xem ví dụ trong
  [compute/spark/jobs/example_job.py](compute/spark/jobs/example_job.py).
- **Flink**: copy JAR/script vào `compute/flink/jobs/`, submit qua UI (http://localhost:8081)
  hoặc `docker exec flink-jobmanager flink run /opt/flink/jobs/<job>.jar`.
- **Airflow**: đặt DAG vào `orchestration/airflow/dags/`, scheduler sẽ tự nhận sau vài giây.
  Xem DAG mẫu [orchestration/airflow/dags/example_pipeline.py](orchestration/airflow/dags/example_pipeline.py).
- **Hadoop** — HDFS: `hadoop-namenode` + `hadoop-datanode` + `hadoop-datanode-2`;
  YARN: `hadoop-resourcemanager` + `hadoop-nodemanager`; log job: `hadoop-historyserver`.
  ```bash
  # Báo cáo HDFS — phải thấy "Live datanodes (2)"
  docker exec -it hadoop-namenode hdfs dfsadmin -report

  # Ghi 1 file rồi xem block nằm trên datanode nào (replication = 2)
  docker exec -it hadoop-namenode bash -c "echo hello hdfs > /tmp/t.txt && hdfs dfs -mkdir -p /demo && hdfs dfs -put -f /tmp/t.txt /demo/ && hdfs fsck /demo/t.txt -files -blocks -locations"

  # Chạy thử job MapReduce (Pi) trên YARN
  docker exec -it hadoop-resourcemanager bash -c \
    "hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 2 5"
  ```
  Toàn bộ cấu hình Hadoop ở [hadoop/hadoop.env](hadoop/hadoop.env) (`dfs.replication`, RAM/CPU YARN...).
  Container khác trong cụm truy cập HDFS qua `hdfs://hadoop-namenode:9000`.
  File tạo trước khi thêm datanode #2 vẫn ở replication cũ — nâng lên bằng:
  `docker exec hadoop-namenode hdfs dfs -setrep -R -w 2 /`
- **MinIO**: 2 bucket `datalake` và `spark-warehouse` được tạo sẵn bởi service
  `minio-init` (chỉnh danh sách qua biến `MINIO_DEFAULT_BUCKETS` trong `.env`).
- Muốn scale thêm Flink taskmanager:
  ```bash
  docker compose up -d --scale flink-taskmanager=3
  ```

## Mở rộng thêm

- Đổi Airflow từ `LocalExecutor` sang `CeleryExecutor`/`KubernetesExecutor` nếu
  cần chạy nhiều task song song ở quy mô lớn hơn.
- Thêm Schema Registry / Kafka Connect nếu cần quản lý schema hoặc kết nối
  Kafka <-> hệ thống khác.
- Với môi trường production, nên build image Airflow riêng (Dockerfile) thay
  vì dùng `_PIP_ADDITIONAL_REQUIREMENTS` (chỉ khuyến nghị cho dev).
