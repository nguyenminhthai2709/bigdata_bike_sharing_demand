# Big Data Cluster (Docker Compose)

> 🚀 **Người mới bắt đầu?** Đọc [GUIDE.md](GUIDE.md) — hướng dẫn từng bước bật cụm
> và làm bài tập làm quen với HDFS / YARN / Spark / Kafka / NiFi.

Cụm xử lý dữ liệu lớn phân tán chạy bằng Docker Compose, gồm:

| Lớp            | Công cụ                                   |
|----------------|--------------------------------------------|
| Compute        | Apache Spark (1 master + 2 worker)          |
| Compute        | JupyterLab + PySpark (notebook tương tác)   |
| Compute        | Apache Flink (1 jobmanager + 1 taskmanager) |
| Hadoop         | HDFS (1 namenode + 2 datanode) + YARN (1 RM + 1 NM) |
| Orchestration  | Apache Airflow (LocalExecutor)              |
| Storage        | MinIO (S3-compatible)                       |
| Database       | PostgreSQL                                  |
| Streaming      | Apache Kafka (KRaft mode, không cần ZK)     |
| Ingestion      | Apache NiFi (dataflow kéo-thả, single-node) |

## Cấu trúc thư mục

```
Big_data/
├── docker-compose.yml       # Toàn bộ cấu hình cụm (1 file duy nhất)
├── .env                     # Biến môi trường (user/pass, port, bucket...)
├── compute/                 # Lớp compute
│   ├── spark/
│   │   └── jobs/            # Script PySpark/Spark job để ở đây
│   ├── jupyter/
│   │   ├── Dockerfile      # JupyterLab build trên image Spark của cụm
│   │   ├── spark-defaults.conf
│   │   └── notebooks/     # Notebook của bạn (bind-mount, được commit)
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
└── streaming/                # Lớp streaming + ingestion
    ├── kafka/                 # Dữ liệu Kafka nằm ở named volume `kafka_data`;
    │                          # thư mục này để chứa config (server.properties...) nếu cần sau này
    └── nifi/
        └── data/             # Bind-mount vào /data trong container NiFi
                                # (dùng cho processor GetFile / PutFile khi tập).
                                # Flow/repo của NiFi lưu ở named volume nifi_*
```

> `minio/` và `kafka/` chưa bind-mount vào container — dữ liệu được Docker quản
> lý qua named volume (`minio_data`, `kafka_data`) để tránh lỗi permission khi
> bind-mount trên Windows. Hai thư mục này sẵn sàng để bạn bỏ file cấu hình vào
> khi cần tuỳ biến sâu hơn.

## Yêu cầu

- Docker + Docker Compose v2
- Tối thiểu ~8–10 GB RAM rảnh cho Docker (Spark + Flink + Airflow + Hadoop + NiFi khá nặng;
  riêng NiFi thêm ~1–1.5 GB). Nếu máy yếu, có thể bỏ bớt cụm Hadoop:
  `docker compose stop $(docker compose config --services | grep hadoop)`
  (hoặc chạy bớt 1 datanode: bỏ `hadoop-datanode-2` khi `up`; tắt NiFi: `docker compose stop nifi`).

## Khởi chạy

### Chạy toàn bộ cụm phân tán

```bash
# 0) Bật Docker Desktop (Windows/Mac) rồi đợi engine sẵn sàng
docker version                       # chạy được -> engine đã lên

# 1) Lên TẤT CẢ service và CHỜ tới khi mọi thứ (healthy)
docker compose up -d --wait

#    (nếu bản compose không hỗ trợ --wait thì dùng 2 lệnh sau)
docker compose up -d
docker compose ps                    # lặp lại tới khi cột STATUS toàn (healthy)

# 2) Xem log 1 service khi cần
docker compose logs -f airflow-webserver     # hoặc nifi / hadoop-namenode / kafka ...

# 3) Tắt cụm
docker compose down                  # giữ lại dữ liệu (named volume)
docker compose down -v               # xoá sạch cả dữ liệu (reset về trắng)
```

**18 service sẽ lên** (Compose tự lo thứ tự phụ thuộc):
`postgres` → `minio` (+ `minio-init`), `kafka`, `nifi`,
`hadoop-namenode` + `hadoop-datanode` ×2 + `hadoop-resourcemanager` +
`hadoop-nodemanager` + `hadoop-historyserver`,
`spark-master` + `spark-worker` ×2, `flink-jobmanager` + `flink-taskmanager`,
`airflow-webserver` + `airflow-scheduler` (+ `airflow-init` chạy 1 lần rồi thoát).

**Kiểm tra nhanh sau khi lên:**

```bash
docker compose ps                                            # tất cả (healthy)
docker exec hadoop-namenode hdfs dfsadmin -report | grep "Live datanodes"   # -> (2)
docker exec hadoop-resourcemanager yarn node -list 2>/dev/null | grep RUNNING # -> 1 node
curl -s http://localhost:8080/json/ | grep -o '"aliveworkers":[0-9]*'        # -> 2
curl -sk -o /dev/null -w 'NiFi HTTP %{http_code}\n' https://localhost:8443/nifi/  # -> 200
```

### Khởi động lại sau khi tắt máy / Docker restart

```bash
docker compose start        # bật lại container đã tạo (nhanh, KHÔNG pull lại)
docker compose stop         # dừng tạm (không xoá container)
```

> **Lần đầu chạy khá lâu**: phải **pull image (~20 GB)**, Airflow `db migrate` +
> cài provider (`_PIP_ADDITIONAL_REQUIREMENTS`), Postgres chạy init script tạo
> database `airflow`, MinIO tạo bucket mặc định. Nếu mạng pull chậm/nghẽn, cứ
> chạy lại `docker compose up -d` — image nào tải xong đã được giữ cache.

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
| JupyterLab        | http://localhost:8888        | token: `bigdata` (`.env` `JUPYTER_TOKEN`) |
| Spark App UI      | http://localhost:4040        | - (chỉ khi có job Spark chạy)   |
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
| Kafka UI          | http://localhost:8090        | - (Kafka không có UI sẵn)       |
| NiFi UI           | https://localhost:8443/nifi  | admin / nifiadmin12345 (`.env`) |
| Postgres          | localhost:5432               | admin / admin123 / db `datawarehouse` |

> Đổi toàn bộ user/password mặc định trong file `.env` trước khi dùng ngoài
> mục đích học tập/demo local.
>
> NiFi dùng **HTTPS với cert tự ký** → trình duyệt sẽ báo "Not secure", bấm
> *Advanced → Proceed* để vào. Mật khẩu NiFi (`NIFI_PASSWORD`) bắt buộc ≥ 12 ký tự.

## Ghi chú vận hành

- **Kafka** chạy KRaft mode (không cần Zookeeper). Tạo topic thử:
  ```bash
  docker exec -it kafka kafka-topics.sh --create --topic demo \
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
  ```
  Kafka không có UI tích hợp → dùng **Kafka UI** ở http://localhost:8090
  (service `kafka-ui`, image đổi qua `KAFKA_UI_IMAGE` trong `.env`): xem topic,
  browse message, consumer group, produce message ngay trên trình duyệt.
- **NiFi** (ingestion, single-node) — UI https://localhost:8443/nifi, login single-user
  (`NIFI_USERNAME` / `NIFI_PASSWORD` trong `.env`). Chạy riêng:
  ```bash
  docker compose up -d nifi
  docker logs -f nifi          # chờ dòng "NiFi has started"
  ```
  - Trong docker network, NiFi gọi các service khác bằng hostname: `kafka:9092`,
    `hdfs://hadoop-namenode:9000`, `http://minio:9000`, `postgres:5432`.
  - Thư mục `streaming/nifi/data/` (host) = `/data` (container) — dùng cho
    processor `GetFile` / `PutFile` khi tập.
  - Flow + toàn bộ repo lưu ở named volume `nifi_conf` / `nifi_flowfile` /
    `nifi_content` / `nifi_provenance` / `nifi_db` / `nifi_state` → giữ nguyên
    qua `docker compose down`, chỉ mất khi `down -v`.
  - Image `apache/nifi:1.28.1` ~1.8 GB, lần pull đầu khá lâu.
- **Spark**: submit job từ container `spark-master`, xem ví dụ trong
  [compute/spark/jobs/example_job.py](compute/spark/jobs/example_job.py).
- **JupyterLab + PySpark**: notebook để code PySpark tương tác, build trên chính
  image Spark của cụm (Python/Spark/Scala khớp → nối thẳng vào `spark://spark-master:7077`).
  ```bash
  docker compose up -d --build jupyter     # lần đầu cần --build
  ```
  Mở http://localhost:8888 (token `bigdata`) → notebook mẫu
  [compute/jupyter/notebooks/00-pyspark-quickstart.ipynb](compute/jupyter/notebooks/00-pyspark-quickstart.ipynb).
  Notebook lưu ở `compute/jupyter/notebooks/`. `spark-defaults.conf` đã set sẵn
  `spark.driver.host=jupyter` để executor kết nối ngược lại được.
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
