#!/bin/bash
# Tạo thêm database riêng cho Airflow metadata bên trong cùng instance Postgres.
# Script trong /docker-entrypoint-initdb.d chỉ chạy MỘT LẦN lúc volume postgres_data
# được khởi tạo lần đầu.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE "${AIRFLOW_DB}";
    GRANT ALL PRIVILEGES ON DATABASE "${AIRFLOW_DB}" TO "$POSTGRES_USER";
EOSQL
