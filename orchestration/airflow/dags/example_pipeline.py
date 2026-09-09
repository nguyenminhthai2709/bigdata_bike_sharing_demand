"""
DAG mẫu để kiểm tra cụm đã lên đủ và các thành phần liên lạc được với nhau:
Postgres -> Spark -> MinIO.

Dùng làm điểm khởi đầu, hãy thay các PythonOperator bằng SparkSubmitOperator,
S3Hook, PostgresHook... tuỳ nhu cầu pipeline thực tế.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def check_postgres_connection():
    import psycopg2
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        user="admin",
        password="admin123",
        dbname="datawarehouse",
    )
    conn.close()
    print("Ket noi Postgres OK")


with DAG(
    dag_id="example_pipeline",
    description="DAG kiem tra cum bigdata: Postgres, Spark, MinIO",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "healthcheck"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_db = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection,
    )

    end = EmptyOperator(task_id="end")

    start >> check_db >> end
