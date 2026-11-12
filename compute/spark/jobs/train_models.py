"""
Milestone 5: Huấn luyện, Đánh giá, Lưu trữ và Dự đoán với Apache Spark MLlib
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Nội dung thực hiện tuần tự (Giai đoạn A -> F):
- Giai đoạn A: Kiểm tra schema, số dòng, danh sách 18 cột đặc trưng đầu vào;
  xác nhận không có 'casual', 'registered' (chống Data Leakage); xác nhận 'train' có 'count' (20 cột)
  và 'test' không có 'count' (19 cột).
- Giai đoạn B: Chia tập Train và Validation theo thời gian (Time-based Split 80% / 20%):
  + Sắp xếp theo 'datetime' tăng dần.
  + 80% bản ghi đầu (8,708 dòng) làm Training set.
  + 20% bản ghi cuối (2,178 dòng) làm Validation set.
  + Xác nhận max(datetime) của Training < min(datetime) của Validation.
- Giai đoạn C: Xây dựng Pipeline MLlib riêng biệt cho từng mô hình (huấn luyện tuần tự):
  + Baseline Mean Predictor (dự đoán bằng trung bình count của tập Training) để đối chiếu tham khảo.
  + Mô hình 1: Linear Regression (VectorAssembler -> StandardScaler -> LinearRegression).
  + Mô hình 2: Decision Tree Regressor (VectorAssembler -> DecisionTreeRegressor, maxDepth=8, minInstancesPerNode=5).
  + Mô hình 3: Random Forest Regressor (VectorAssembler -> RandomForestRegressor, numTrees=30, maxDepth=8, seed=42).
- Giai đoạn D: Đánh giá mô hình trên tập Validation bằng RegressionEvaluator:
  + Tính RMSE, MAE, R2 trên cột 'prediction' gốc (đánh giá khách quan) và kiểm tra số lượng dự đoán âm.
  + Tạo thêm cột 'prediction_clipped = greatest(prediction, 0.0)' và tính thêm RMSLE.
  + Lập bảng so sánh thực nghiệm, đo thời gian huấn luyện từng mô hình và chọn Best Model dựa trên Validation RMSE.
  + Lưu metrics (CSV, JSON) và Validation predictions vào outputs/models/.
- Giai đoạn E: Lưu cả 3 PipelineModel lên HDFS (/bike-sharing/models/{linear_regression,decision_tree,random_forest})
  và lưu mô hình tốt nhất cùng metadata vào /bike-sharing/models/best_model/.
- Giai đoạn F: Sử dụng mô hình tốt nhất để dự đoán trên tập Test (/bike-sharing/features/test, 6,493 dòng):
  + Lưu kết quả đầy đủ (datetime, prediction, prediction_clipped) lên HDFS (/bike-sharing/predictions/test_predictions)
    và tệp CSV local.
  + Xuất tệp Kaggle submission chuẩn (datetime, count) gồm đúng 6,493 dòng tại outputs/models/kaggle_submission.csv.
"""

import csv
import json
import os
import time
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ["HADOOP_USER_NAME"] = "root"
os.environ["USER"] = "root"

FEATURE_COLS = [
    "season",
    "holiday",
    "workingday",
    "weather",
    "temp",
    "atemp",
    "humidity",
    "windspeed",
    "year",
    "month",
    "day",
    "hour",
    "dayofweek",
    "is_weekend",
    "hour_sin",
    "hour_cos",
    "month_sin",
    "month_cos",
]


def save_df_to_local_csv(df, filepath):
    """Lưu Spark DataFrame ra tệp CSV chuẩn trên thư mục mount."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    rows = df.collect()
    columns = df.columns
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row[col] for col in columns])
    print(f"  [SAVED CSV] {filepath} ({len(rows)} rows)")


def hdfs_path_exists(spark, hdfs_uri):
    """Kiểm tra đường dẫn HDFS đã tồn tại hay chưa."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_uri)
    return fs.exists(path)


def main():
    print("=" * 90)
    print("MILESTONE 5: MACHINE LEARNING VỚI APACHE SPARK MLlib")
    print("=" * 90)

    spark = (
        SparkSession.builder.appName("Milestone5-SparkMLlib-BikeSharing")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
        .config("spark.executorEnv.HADOOP_USER_NAME", "root")
        .config("spark.executorEnv.USER", "root")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    local_out_dir = "/opt/spark-jobs/_staging_outputs/models"
    os.makedirs(local_out_dir, exist_ok=True)

    # =========================================================================
    # GIAI ĐOẠN A: KIỂM TRA DỮ LIỆU VÀ SCHEMA TRƯỚC KHI HUẤN LUYỆN
    # =========================================================================
    train_feat_path = "hdfs://hadoop-namenode:9000/bike-sharing/features/train"
    test_feat_path = "hdfs://hadoop-namenode:9000/bike-sharing/features/test"

    full_train_df = spark.read.parquet(train_feat_path)
    test_df = spark.read.parquet(test_feat_path)

    full_train_count = full_train_df.count()
    test_count = test_df.count()

    print(f"\n[GIAI ĐOẠN A] Kiểm tra dữ liệu đầu vào từ HDFS:")
    print(f"  - Train features ({train_feat_path}): {full_train_count} dòng, {len(full_train_df.columns)} cột")
    print(f"  - Test features  ({test_feat_path}) : {test_count} dòng, {len(test_df.columns)} cột")

    assert full_train_count == 10886, f"Số dòng Train không khớp 10886: {full_train_count}"
    assert test_count == 6493, f"Số dòng Test không khớp 6493: {test_count}"
    assert "casual" not in full_train_df.columns and "registered" not in full_train_df.columns, (
        "Phát hiện Data Leakage: casual/registered có trong Train features!"
    )
    assert "casual" not in test_df.columns and "registered" not in test_df.columns, (
        "Phát hiện Data Leakage: casual/registered có trong Test features!"
    )
    assert "count" in full_train_df.columns, "Thiếu cột mục tiêu 'count' trong Train features!"
    assert "count" not in test_df.columns, "Cột 'count' không được phép tồn tại trong Test features!"

    for f_col in FEATURE_COLS:
        assert f_col in full_train_df.columns, f"Thiếu feature '{f_col}' trong Train!"
        assert f_col in test_df.columns, f"Thiếu feature '{f_col}' trong Test!"

    print(f"  - Đã xác nhận đủ {len(FEATURE_COLS)} cột đặc trưng hợp lệ: {FEATURE_COLS}")
    print("  - Xác nhận KHÔNG có 'casual', 'registered' và KHÔNG đưa 'datetime' hay 'count' vào VectorAssembler.")

    # =========================================================================
    # GIAI ĐOẠN B: CHIA TẬP TRAIN VÀ VALIDATION THEO THỜI GIAN (TIME-BASED SPLIT 80/20)
    # =========================================================================
    print("\n" + "-" * 90)
    print("[GIAI ĐOẠN B] Chia tập Train (80%) và Validation (20%) theo thứ tự thời gian (Time-based Split)")

    # Đánh số thứ tự dòng chính xác theo datetime tăng dần
    window_spec = Window.orderBy(F.col("datetime").asc())
    indexed_df = full_train_df.withColumn("row_idx", F.row_number().over(window_spec))

    split_cutoff = int(full_train_count * 0.8)  # 8,708 dòng đầu (80%)
    train_split_df = (
        indexed_df.filter(F.col("row_idx") <= split_cutoff)
        .drop("row_idx")
        .withColumn("label", F.col("count").cast("double"))
        .cache()
    )
    val_split_df = (
        indexed_df.filter(F.col("row_idx") > split_cutoff)
        .drop("row_idx")
        .withColumn("label", F.col("count").cast("double"))
        .cache()
    )

    train_n = train_split_df.count()
    val_n = val_split_df.count()

    train_time_bounds = train_split_df.select(
        F.min("datetime").alias("min_dt"), F.max("datetime").alias("max_dt")
    ).collect()[0]
    val_time_bounds = val_split_df.select(
        F.min("datetime").alias("min_dt"), F.max("datetime").alias("max_dt")
    ).collect()[0]

    train_min_dt = str(train_time_bounds["min_dt"])
    train_max_dt = str(train_time_bounds["max_dt"])
    val_min_dt = str(val_time_bounds["min_dt"])
    val_max_dt = str(val_time_bounds["max_dt"])

    print(f"  - Training Set (80%)  : {train_n} dòng | Từ {train_min_dt} đến {train_max_dt}")
    print(f"  - Validation Set (20%): {val_n} dòng | Từ {val_min_dt} đến {val_max_dt}")

    assert train_n + val_n == full_train_count, "Tổng số dòng Train + Validation không khớp 10,886!"
    assert train_time_bounds["max_dt"] < val_time_bounds["min_dt"], (
        "Lỗi Time-based Split: Thời gian Training chồng lấn với Validation!"
    )
    print("  - [OK] Xác nhận không có bất kỳ bản ghi Validation nào nằm trước hoặc trùng thời gian Training!")

    # =========================================================================
    # GIAI ĐOẠN C & D: XÂY DỰNG PIPELINE MLlib, HUẤN LUYỆN TUẦN TỰ & ĐÁNH GIÁ
    # =========================================================================
    rmse_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    rmse_clipped_eval = RegressionEvaluator(
        labelCol="label", predictionCol="prediction_clipped", metricName="rmse"
    )

    # Tính Baseline Mean Predictor (dự đoán mọi điểm Validation bằng trung bình count của tập Training)
    train_mean_target = float(train_split_df.select(F.mean("label")).collect()[0][0])
    baseline_val_df = val_split_df.withColumn("prediction", F.lit(train_mean_target))
    baseline_rmse = float(rmse_eval.evaluate(baseline_val_df))
    baseline_mae = float(mae_eval.evaluate(baseline_val_df))
    baseline_r2 = float(r2_eval.evaluate(baseline_val_df))
    print(f"\n[BASELINE] Mean Predictor (train_mean = {train_mean_target:.2f}):")
    print(f"  -> Val RMSE = {baseline_rmse:.4f} | Val MAE = {baseline_mae:.4f} | Val R2 = {baseline_r2:.4f}")

    # Định nghĩa 3 Pipeline riêng biệt
    # 1) Linear Regression Pipeline (VectorAssembler -> StandardScaler -> LinearRegression)
    lr_assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    lr_scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withMean=True, withStd=True
    )
    lr_estimator = LinearRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.0,  # L2 Ridge regularization ổn định trên các biến tương quan temp/atemp
    )
    lr_pipeline = Pipeline(stages=[lr_assembler, lr_scaler, lr_estimator])

    # 2) Decision Tree Regressor Pipeline (VectorAssembler -> DecisionTreeRegressor)
    dt_assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    dt_estimator = DecisionTreeRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        maxDepth=8,
        minInstancesPerNode=5,
        seed=42,
    )
    dt_pipeline = Pipeline(stages=[dt_assembler, dt_estimator])

    # 3) Random Forest Regressor Pipeline (VectorAssembler -> RandomForestRegressor)
    rf_assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    rf_estimator = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        numTrees=30,
        maxDepth=8,
        minInstancesPerNode=5,
        featureSubsetStrategy="auto",
        subsamplingRate=0.8,
        seed=42,
    )
    rf_pipeline = Pipeline(stages=[rf_assembler, rf_estimator])

    models_spec = [
        {
            "key": "linear_regression",
            "name": "Linear Regression",
            "pipeline": lr_pipeline,
            "hdfs_path": "hdfs://hadoop-namenode:9000/bike-sharing/models/linear_regression",
            "params": {
                "scaler": "StandardScaler(withMean=True, withStd=True)",
                "maxIter": 100,
                "regParam": 0.1,
                "elasticNetParam": 0.0,
            },
        },
        {
            "key": "decision_tree",
            "name": "Decision Tree Regressor",
            "pipeline": dt_pipeline,
            "hdfs_path": "hdfs://hadoop-namenode:9000/bike-sharing/models/decision_tree",
            "params": {
                "maxDepth": 8,
                "minInstancesPerNode": 5,
                "seed": 42,
            },
        },
        {
            "key": "random_forest",
            "name": "Random Forest Regressor",
            "pipeline": rf_pipeline,
            "hdfs_path": "hdfs://hadoop-namenode:9000/bike-sharing/models/random_forest",
            "params": {
                "numTrees": 30,
                "maxDepth": 8,
                "minInstancesPerNode": 5,
                "featureSubsetStrategy": "auto",
                "subsamplingRate": 0.8,
                "seed": 42,
            },
        },
    ]

    metrics_list = []
    trained_models = {}
    val_predictions_map = {}

    for spec in models_spec:
        m_key = spec["key"]
        m_name = spec["name"]
        print("\n" + "-" * 90)
        print(f"[GIAI ĐOẠN C] Đang huấn luyện mô hình: {m_name} ...")

        t0 = time.perf_counter()
        fitted_model = spec["pipeline"].fit(train_split_df)
        train_time_sec = round(time.perf_counter() - t0, 4)

        print(f"  -> Hoàn tất huấn luyện {m_name} trong {train_time_sec} giây.")

        # Dự đoán trên tập Validation
        val_pred_df = fitted_model.transform(val_split_df)
        val_pred_df = val_pred_df.withColumn(
            "prediction_clipped",
            F.greatest(F.col("prediction"), F.lit(0.0)),
        ).withColumn(
            "sq_log_error",
            F.pow(
                F.log1p(F.col("prediction_clipped")) - F.log1p(F.col("label")),
                2.0,
            ),
        )

        # Tính các chỉ số đánh giá khách quan trên Validation
        rmse_val = round(float(rmse_eval.evaluate(val_pred_df)), 4)
        mae_val = round(float(mae_eval.evaluate(val_pred_df)), 4)
        r2_val = round(float(r2_eval.evaluate(val_pred_df)), 4)
        rmse_clipped_val = round(float(rmse_clipped_eval.evaluate(val_pred_df)), 4)
        rmsle_val = round(
            float(val_pred_df.select(F.sqrt(F.mean("sq_log_error"))).collect()[0][0]),
            4,
        )
        neg_pred_count = int(val_pred_df.filter(F.col("prediction") < 0).count())

        print(
            f"[GIAI ĐOẠN D] Kết quả Validation của {m_name}:\n"
            f"  - RMSE (raw)     : {rmse_val}\n"
            f"  - MAE  (raw)     : {mae_val}\n"
            f"  - R2   (raw)     : {r2_val}\n"
            f"  - RMSE (clipped) : {rmse_clipped_val}\n"
            f"  - RMSLE (clipped): {rmsle_val}\n"
            f"  - Dự đoán âm (<0): {neg_pred_count} / {val_n} dòng\n"
            f"  - Training Time  : {train_time_sec} s"
        )

        metrics_entry = {
            "model_key": m_key,
            "model_name": m_name,
            "rmse": rmse_val,
            "mae": mae_val,
            "r2": r2_val,
            "rmse_clipped": rmse_clipped_val,
            "rmsle": rmsle_val,
            "negative_predictions_count": neg_pred_count,
            "training_time_sec": train_time_sec,
            "params_json": json.dumps(spec["params"]),
        }
        metrics_list.append(metrics_entry)
        trained_models[m_key] = (spec, fitted_model)
        val_predictions_map[m_key] = val_pred_df

    # Chọn mô hình tốt nhất dựa trên Validation RMSE thấp nhất
    best_entry = min(metrics_list, key=lambda x: x["rmse"])
    best_key = best_entry["model_key"]
    best_name = best_entry["model_name"]
    best_spec, best_fitted_model = trained_models[best_key]

    print("\n" + "=" * 90)
    print(
        f"[KẾT LUẬN LỰA CHỌN MÔ HÌNH] Mô hình tốt nhất trên Validation: {best_name} "
        f"(RMSE = {best_entry['rmse']}, MAE = {best_entry['mae']}, R2 = {best_entry['r2']})"
    )
    print("=" * 90)

    # Lưu bảng so sánh metrics ra CSV tại outputs/models/model_comparison_metrics.csv
    metrics_csv_path = f"{local_out_dir}/model_comparison_metrics.csv"
    with open(metrics_csv_path, mode="w", newline="", encoding="utf-8") as mf:
        fieldnames = [
            "model_key",
            "model_name",
            "rmse",
            "mae",
            "r2",
            "rmse_clipped",
            "rmsle",
            "negative_predictions_count",
            "training_time_sec",
            "params_json",
        ]
        writer = csv.DictWriter(mf, fieldnames=fieldnames)
        writer.writeheader()
        for row in metrics_list:
            writer.writerow(row)
    print(f"  [SAVED METRICS CSV] {metrics_csv_path}")

    # Lưu Validation predictions (kèm dự đoán của cả 3 mô hình để dễ đối chiếu trên Dashboard)
    val_lr = val_predictions_map["linear_regression"].select(
        F.col("datetime"),
        F.col("count").alias("actual_count"),
        F.round(F.col("prediction"), 2).alias("lr_prediction"),
        F.round(F.col("prediction_clipped"), 2).alias("lr_prediction_clipped"),
    )
    val_dt = val_predictions_map["decision_tree"].select(
        F.col("datetime"),
        F.round(F.col("prediction"), 2).alias("dt_prediction"),
        F.round(F.col("prediction_clipped"), 2).alias("dt_prediction_clipped"),
    )
    val_rf = val_predictions_map["random_forest"].select(
        F.col("datetime"),
        F.round(F.col("prediction"), 2).alias("rf_prediction"),
        F.round(F.col("prediction_clipped"), 2).alias("rf_prediction_clipped"),
    )

    combined_val_pred_df = (
        val_lr.join(val_dt, on="datetime", how="inner")
        .join(val_rf, on="datetime", how="inner")
        .withColumn(
            "best_prediction",
            F.col(f"{'rf' if best_key == 'random_forest' else ('dt' if best_key == 'decision_tree' else 'lr')}_prediction"),
        )
        .withColumn(
            "best_prediction_clipped",
            F.col(f"{'rf' if best_key == 'random_forest' else ('dt' if best_key == 'decision_tree' else 'lr')}_prediction_clipped"),
        )
        .orderBy("datetime")
    )
    save_df_to_local_csv(
        combined_val_pred_df, f"{local_out_dir}/validation_predictions.csv"
    )

    # Trích xuất Feature Importances của Decision Tree và Random Forest
    dt_stage = trained_models["decision_tree"][1].stages[-1]
    rf_stage = trained_models["random_forest"][1].stages[-1]
    feature_importances = []
    for idx, f_name in enumerate(FEATURE_COLS):
        feature_importances.append(
            {
                "feature": f_name,
                "dt_importance": round(float(dt_stage.featureImportances[idx]), 6),
                "rf_importance": round(float(rf_stage.featureImportances[idx]), 6),
            }
        )
    feature_importances.sort(key=lambda x: x["rf_importance"], reverse=True)

    fi_csv_path = f"{local_out_dir}/feature_importances.csv"
    with open(fi_csv_path, mode="w", newline="", encoding="utf-8") as fif:
        writer = csv.DictWriter(
            fif, fieldnames=["feature", "rf_importance", "dt_importance"]
        )
        writer.writeheader()
        for row in feature_importances:
            writer.writerow(row)
    print(f"  [SAVED FEATURE IMPORTANCE CSV] {fi_csv_path}")

    # =========================================================================
    # GIAI ĐOẠN E: LƯU MÔ HÌNH VÀ METADATA LÊN HDFS
    # =========================================================================
    print("\n" + "-" * 90)
    print("[GIAI ĐOẠN E] Kiểm tra và lưu các PipelineModel lên HDFS:")
    for m_key, (spec, fitted_model) in trained_models.items():
        m_hdfs_path = spec["hdfs_path"]
        exists_before = hdfs_path_exists(spark, m_hdfs_path)
        print(f"  - Đường dẫn {m_hdfs_path} (Tồn tại trước đó: {exists_before}) -> Đang lưu...")
        fitted_model.write().overwrite().save(m_hdfs_path)
        print(f"    [SAVED HDFS MODEL] {m_hdfs_path}")

    best_model_hdfs_dir = "hdfs://hadoop-namenode:9000/bike-sharing/models/best_model"
    best_pipeline_hdfs_path = f"{best_model_hdfs_dir}/pipeline_model"
    best_metadata_hdfs_path = f"{best_model_hdfs_dir}/metadata"

    print(f"  - Đang lưu Best Model ({best_name}) -> {best_pipeline_hdfs_path}")
    best_fitted_model.write().overwrite().save(best_pipeline_hdfs_path)

    created_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata_payload = {
        "best_model_key": best_key,
        "best_model_name": best_name,
        "hdfs_model_path": best_pipeline_hdfs_path,
        "target_column": "count",
        "feature_columns": FEATURE_COLS,
        "excluded_leakage_columns": ["casual", "registered"],
        "excluded_non_feature_columns": ["datetime"],
        "time_based_split": {
            "train_ratio": 0.8,
            "validation_ratio": 0.2,
            "train_rows": train_n,
            "validation_rows": val_n,
            "train_start": train_min_dt,
            "train_end": train_max_dt,
            "validation_start": val_min_dt,
            "validation_end": val_max_dt,
        },
        "baseline_mean_predictor": {
            "train_mean_count": round(train_mean_target, 4),
            "rmse": round(baseline_rmse, 4),
            "mae": round(baseline_mae, 4),
            "r2": round(baseline_r2, 4),
        },
        "best_model_params": best_spec["params"],
        "best_model_validation_metrics": best_entry,
        "all_models_comparison": metrics_list,
        "top_feature_importances": feature_importances[:10],
        "created_at_utc": created_at_utc,
    }

    # Lưu metadata JSON trên local (outputs/models/) và trên HDFS (/bike-sharing/models/best_model/metadata)
    local_meta_path = f"{local_out_dir}/best_model_metadata.json"
    with open(local_meta_path, "w", encoding="utf-8") as mf:
        json.dump(metadata_payload, mf, indent=2, ensure_ascii=False)
    print(f"  [SAVED METADATA JSON] {local_meta_path}")

    meta_rdd = spark.sparkContext.parallelize([json.dumps(metadata_payload, ensure_ascii=False)], 1)
    spark.read.json(meta_rdd).coalesce(1).write.mode("overwrite").json(best_metadata_hdfs_path)
    print(f"  [SAVED HDFS METADATA] {best_metadata_hdfs_path}")

    # =========================================================================
    # GIAI ĐOẠN F: DỰ ĐOÁN TRÊN TẬP TEST (6,493 DÒNG) & XUẤT KAGGLE SUBMISSION
    # =========================================================================
    print("\n" + "-" * 90)
    print(f"[GIAI ĐOẠN F] Dự đoán trên tập Test (6,493 dòng) bằng mô hình tốt nhất: {best_name}")

    test_ordered_df = test_df.orderBy(F.col("datetime").asc())
    test_pred_raw_df = best_fitted_model.transform(test_ordered_df)

    test_predictions_df = (
        test_pred_raw_df.select(
            F.col("datetime"),
            F.round(F.col("prediction"), 4).alias("prediction"),
            F.round(F.greatest(F.col("prediction"), F.lit(0.0)), 4).alias("prediction_clipped"),
        )
        .orderBy(F.col("datetime").asc())
    )

    test_pred_count = test_predictions_df.count()
    test_neg_count = test_predictions_df.filter(F.col("prediction") < 0).count()
    assert test_pred_count == 6493, f"Số dòng dự đoán Test không khớp 6,493: {test_pred_count}"

    print(f"  -> Số dòng dự đoán trên Test: {test_pred_count} dòng (Dự đoán âm trước khi clip: {test_neg_count})")
    print("  -> 10 dòng dự đoán đầu tiên trên tập Test:")
    test_predictions_df.show(10, truncate=False)

    # Lưu predictions dưới dạng Parquet lên HDFS: /bike-sharing/predictions/
    hdfs_pred_parquet_path = "hdfs://hadoop-namenode:9000/bike-sharing/predictions/test_predictions_parquet"
    (
        test_predictions_df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(hdfs_pred_parquet_path)
    )
    print(f"  [SAVED HDFS PREDICTIONS PARQUET] {hdfs_pred_parquet_path}")

    # Lưu file Kaggle Submission chuẩn (datetime, count)
    submission_df = test_predictions_df.select(
        F.date_format(F.col("datetime"), "yyyy-MM-dd HH:mm:ss").alias("datetime"),
        F.round(F.col("prediction_clipped"), 0).cast("integer").alias("count"),
    ).orderBy("datetime")

    hdfs_submission_csv_path = "hdfs://hadoop-namenode:9000/bike-sharing/predictions/kaggle_submission_csv"
    (
        submission_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(hdfs_submission_csv_path)
    )
    print(f"  [SAVED HDFS SUBMISSION CSV] {hdfs_submission_csv_path}")

    # Lưu CSV ra thư mục local staging để copy về outputs/models/ và outputs/predictions/
    save_df_to_local_csv(test_predictions_df, f"{local_out_dir}/test_predictions.csv")
    save_df_to_local_csv(submission_df, f"{local_out_dir}/kaggle_submission.csv")

    print("\n" + "=" * 90)
    print("HOÀN TẤT MILESTONE 5 (SPARK MLlib TRAINING, EVALUATION & PREDICTION) THÀNH CÔNG 100%!")
    print("=" * 90)
    spark.stop()


if __name__ == "__main__":
    main()

