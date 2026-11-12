"""
Bộ nạp dữ liệu (Data Loader) cho ứng dụng Streamlit Dashboard.
Đọc trực tiếp từ các tệp CSV/JSON kết quả đã lưu trong outputs/ với @st.cache_data.
Tuyệt đối không khởi tạo SparkSession và không tự tạo dữ liệu giả thay thế.
"""

from pathlib import Path
import json
from typing import Any
import pandas as pd
import streamlit as st

from config import ARTIFACT_PATHS


class ArtifactLoadError(RuntimeError):
    """Ngoại lệ phát sinh khi tệp artifact không tồn tại, rỗng hoặc sai cấu trúc."""


def _resolve_path(path_or_key: str | Path) -> Path:
    """Chuyển đổi khóa artifact hoặc đường dẫn thành đối tượng Path tuyệt đối."""
    if isinstance(path_or_key, Path):
        return path_or_key.resolve()
    if path_or_key in ARTIFACT_PATHS:
        return ARTIFACT_PATHS[path_or_key].resolve()
    candidate = Path(path_or_key)
    if candidate.is_absolute():
        return candidate
    raise ArtifactLoadError(f"Khóa hoặc đường dẫn artifact không hợp lệ: '{path_or_key}'")


@st.cache_data(show_spinner=False)
def load_csv(path_or_key: str | Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    """
    Đọc tệp CSV từ đường dẫn hoặc tên khóa trong ARTIFACT_PATHS.
    Kiểm tra sự tồn tại và tính hợp lệ trước khi trả về DataFrame.
    """
    file_path = _resolve_path(path_or_key)
    if not file_path.exists():
        raise ArtifactLoadError(f"Không tìm thấy tệp dữ liệu CSV nguồn: {file_path}")
    if file_path.stat().st_size == 0:
        raise ArtifactLoadError(f"Tệp dữ liệu CSV bị rỗng (0 bytes): {file_path}")

    try:
        df = pd.read_csv(file_path, parse_dates=parse_dates)
    except Exception as exc:
        raise ArtifactLoadError(f"Lỗi khi đọc tệp CSV '{file_path}': {exc}") from exc

    if df.empty:
        raise ArtifactLoadError(f"DataFrame đọc từ '{file_path}' không có bản ghi nào.")
    return df


@st.cache_data(show_spinner=False)
def load_json(path_or_key: str | Path) -> dict[str, Any]:
    """
    Đọc tệp JSON từ đường dẫn hoặc tên khóa trong ARTIFACT_PATHS.
    Kiểm tra sự tồn tại và cấu trúc JSON hợp lệ.
    """
    file_path = _resolve_path(path_or_key)
    if not file_path.exists():
        raise ArtifactLoadError(f"Không tìm thấy tệp dữ liệu JSON nguồn: {file_path}")
    if file_path.stat().st_size == 0:
        raise ArtifactLoadError(f"Tệp dữ liệu JSON bị rỗng (0 bytes): {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        raise ArtifactLoadError(f"Lỗi phân tích cú pháp JSON tại '{file_path}': {exc}") from exc

    if not isinstance(data, dict) or not data:
        raise ArtifactLoadError(f"Nội dung JSON tại '{file_path}' không phải là dictionary hợp lệ.")
    return data


@st.cache_data(show_spinner=False)
def load_raw_bytes(path_or_key: str | Path) -> bytes:
    """
    Đọc nguyên vẹn byte nhị phân của tệp (phục vụ tải xuống tệp gốc như kaggle_submission.csv).
    """
    file_path = _resolve_path(path_or_key)
    if not file_path.exists():
        raise ArtifactLoadError(f"Không tìm thấy tệp dữ liệu nguồn: {file_path}")
    if file_path.stat().st_size == 0:
        raise ArtifactLoadError(f"Tệp dữ liệu bị rỗng (0 bytes): {file_path}")
    try:
        return file_path.read_bytes()
    except Exception as exc:
        raise ArtifactLoadError(f"Lỗi khi đọc byte từ tệp '{file_path}': {exc}") from exc


@st.cache_data(show_spinner=False)
def get_ml_artifacts() -> dict[str, Any]:
    """
    Nạp và kiểm tra nghiêm ngặt toàn bộ 6 tệp kết quả của Milestone 5 (Spark MLlib)
    từ outputs/models/ phục vụ Trang 3: Machine Learning.
    """
    model_comparison_df = load_csv("model_comparison")
    best_model_meta = load_json("best_model_metadata")
    feature_importances_df = load_csv("feature_importances")
    val_preds_df = load_csv("validation_predictions")
    test_preds_df = load_csv("test_predictions")
    kaggle_sub_df = load_csv("kaggle_submission")
    kaggle_raw_bytes = load_raw_bytes("kaggle_submission")

    # 1. Kiểm tra số dòng và cột của 6 tệp
    if len(model_comparison_df) != 3:
        raise ArtifactLoadError(
            f"model_comparison_metrics.csv phải có 3 mô hình, thực tế: {len(model_comparison_df)}"
        )
    if len(feature_importances_df) != 18:
        raise ArtifactLoadError(
            f"feature_importances.csv phải có 18 đặc trưng, thực tế: {len(feature_importances_df)}"
        )
    if len(val_preds_df) != 2178:
        raise ArtifactLoadError(
            f"validation_predictions.csv phải có 2,178 bản ghi, thực tế: {len(val_preds_df)}"
        )
    if len(test_preds_df) != 6493:
        raise ArtifactLoadError(
            f"test_predictions.csv phải có 6,493 bản ghi, thực tế: {len(test_preds_df)}"
        )
    if len(kaggle_sub_df) != 6493 or list(kaggle_sub_df.columns) != ["datetime", "count"]:
        raise ArtifactLoadError(
            f"kaggle_submission.csv không hợp lệ (số dòng={len(kaggle_sub_df)}, cột={list(kaggle_sub_df.columns)})"
        )
    if int(kaggle_sub_df.isna().sum().sum()) != 0:
        raise ArtifactLoadError("kaggle_submission.csv chứa giá trị null!")

    # 2. Chuẩn bị cột thời gian và tính Residual trên tập Validation (Residual = Actual - Predicted)
    val_df = val_preds_df.copy()
    val_df["datetime_dt"] = pd.to_datetime(val_df["datetime"])
    val_df["date"] = val_df["datetime_dt"].dt.date
    val_df["hour"] = val_df["datetime_dt"].dt.hour
    val_df["rf_residual"] = val_df["actual_count"] - val_df["rf_prediction"]
    val_df["dt_residual"] = val_df["actual_count"] - val_df["dt_prediction"]
    val_df["lr_residual"] = val_df["actual_count"] - val_df["lr_prediction"]
    val_df["best_residual"] = val_df["actual_count"] - val_df["best_prediction"]

    # 3. Chuẩn bị cột thời gian trên tập Test
    test_df = test_preds_df.copy()
    test_df["datetime_dt"] = pd.to_datetime(test_df["datetime"])
    test_df["date"] = test_df["datetime_dt"].dt.date
    test_df["year_month"] = test_df["datetime_dt"].dt.strftime("%Y-%m")
    test_df["hour"] = test_df["datetime_dt"].dt.hour

    # 4. Sắp xếp Feature Importances giảm dần theo rf_importance
    fi_df = feature_importances_df.sort_values("rf_importance", ascending=False).reset_index(drop=True)
    fi_df["rf_importance_pct"] = fi_df["rf_importance"] * 100.0
    fi_df["dt_importance_pct"] = fi_df["dt_importance"] * 100.0
    fi_df["rank"] = fi_df.index + 1

    return {
        "model_comparison": model_comparison_df,
        "best_model_meta": best_model_meta,
        "feature_importances": fi_df,
        "validation_predictions": val_df,
        "test_predictions": test_df,
        "kaggle_submission": kaggle_sub_df,
        "kaggle_raw_bytes": kaggle_raw_bytes,
    }


@st.cache_data(show_spinner=False)
def get_overview_kpis() -> dict[str, Any]:
    """
    Trích xuất và đối chiếu chéo các chỉ số KPI cho trang Overview trực tiếp từ artifacts
    Milestone 4 (eda_summary.json, preprocessing_summary.json),
    Milestone 5 (best_model_metadata.json, model_comparison_metrics.csv),
    và Milestone 6 (storage_comparison.csv, scaling_results.csv).
    """
    eda_meta = load_json("eda_summary")
    prep_meta = load_json("preprocessing_summary")
    best_meta = load_json("best_model_metadata")
    model_df = load_csv("model_comparison")
    storage_df = load_csv("storage_comparison")
    scaling_df = load_csv("scaling_results")

    # 1. Thông tin bản ghi Train / Test
    train_rows = int(eda_meta["train_rows"])
    test_rows = int(eda_meta["test_rows"])
    train_cols = int(eda_meta["train_cols"])
    test_cols = int(eda_meta["test_cols"])
    target_col = str(best_meta["target_column"])
    num_features = len(best_meta["feature_columns"])
    total_rentals = int(eda_meta["target_count_summary"]["total_rentals"])

    # 2. Thông tin Best Model từ best_model_metadata.json & đối chiếu với model_comparison_metrics.csv
    best_key = str(best_meta["best_model_key"])
    best_full_name = str(best_meta["best_model_name"])
    # Hiển thị ngắn gọn "Random Forest" theo chuẩn thiết kế KPI
    best_short_name = best_full_name.replace(" Regressor", "")

    val_metrics = best_meta["best_model_validation_metrics"]
    val_rmse = float(val_metrics["rmse"])
    val_mae = float(val_metrics["mae"])
    val_r2 = float(val_metrics["r2"])
    val_rmsle = float(val_metrics["rmsle"])

    # Đối chiếu chéo với model_comparison_metrics.csv
    matched_row = model_df.loc[model_df["model_key"] == best_key]
    if matched_row.empty:
        raise ArtifactLoadError(f"Không tìm thấy '{best_key}' trong model_comparison_metrics.csv")
    csv_rmse = float(matched_row["rmse"].iloc[0])
    csv_mae = float(matched_row["mae"].iloc[0])
    csv_r2 = float(matched_row["r2"].iloc[0])
    if abs(val_rmse - csv_rmse) > 1e-4 or abs(val_mae - csv_mae) > 1e-4 or abs(val_r2 - csv_r2) > 1e-4:
        raise ArtifactLoadError("Số liệu giữa best_model_metadata.json và model_comparison_metrics.csv không khớp!")

    # 3. Thông tin Benchmark lưu trữ ở quy mô 100x từ storage_comparison.csv & scaling_results.csv
    st_100_csv = storage_df.loc[(storage_df["scale_factor"] == 100) & (storage_df["format"] == "CSV")]
    st_100_pq = storage_df.loc[(storage_df["scale_factor"] == 100) & (storage_df["format"] == "PARQUET")]
    sc_100 = scaling_df.loc[scaling_df["scale_factor"] == 100]

    if st_100_csv.empty or st_100_pq.empty or sc_100.empty:
        raise ArtifactLoadError("Thiếu dữ liệu quy mô 100x trong storage_comparison.csv hoặc scaling_results.csv")

    csv_bytes_100x = float(st_100_csv["storage_size_bytes"].iloc[0])
    pq_bytes_100x = float(st_100_pq["storage_size_bytes"].iloc[0])
    csv_mb_100x = float(st_100_csv["storage_size_mb"].iloc[0])
    pq_mb_100x = float(st_100_pq["storage_size_mb"].iloc[0])

    parquet_reduction_pct = round((1.0 - (pq_bytes_100x / csv_bytes_100x)) * 100.0, 2)
    compression_ratio = round(float(sc_100["csv_to_parquet_size_ratio"].iloc[0]), 2)

    return {
        "train_rows": train_rows,
        "test_rows": test_rows,
        "total_rows": train_rows + test_rows,
        "train_cols": train_cols,
        "test_cols": test_cols,
        "num_features": num_features,
        "target_column": target_col,
        "total_rentals": total_rentals,
        "best_model_name": best_short_name,
        "best_model_full_name": best_full_name,
        "val_rmse": val_rmse,
        "val_mae": val_mae,
        "val_r2": val_r2,
        "val_rmsle": val_rmsle,
        "parquet_reduction_pct": parquet_reduction_pct,
        "compression_ratio": compression_ratio,
        "csv_mb_100x": csv_mb_100x,
        "pq_mb_100x": pq_mb_100x,
        "parquet_verification": prep_meta.get("parquet_verification", {}),
    }


@st.cache_data(show_spinner=False)
def get_data_analysis_artifacts() -> dict[str, Any]:
    """
    Nạp và kiểm tra toàn bộ các tệp CSV/JSON phục vụ Trang 2: Data Analysis
    từ outputs/eda/, outputs/analytics/ và outputs/preprocessing/.
    """
    eda_summary = load_json("eda_summary")
    prep_summary = load_json("preprocessing_summary")

    q1_hourly = load_csv("sql_q1_hourly")
    q2_dayofweek = load_csv("sql_q2_dayofweek")
    q3_seasonal = load_csv("sql_q3_seasonal")
    q4_weather = load_csv("sql_q4_weather")
    q5_workingday = load_csv("sql_q5_workingday")
    q6_monthly = load_csv("sql_q6_monthly_trend")
    q7_peak_hours = load_csv("sql_q7_peak_hours")
    q8_user_behavior = load_csv("sql_q8_user_behavior")

    b1_desc = load_csv("descriptive_stats")
    b2_dist = load_csv("target_distribution")
    b2_stats = load_csv("target_stats")
    b3_year = load_csv("eda_year")
    b4_temp_hum = load_csv("eda_temp_humidity")
    b4_weather = load_csv("eda_weather")

    # Sắp xếp đúng thứ tự ngày trong tuần từ Monday -> Sunday (không sắp theo alphabet)
    monday_to_sunday = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    q2_sorted = q2_dayofweek.copy()
    q2_sorted["day_name"] = pd.Categorical(
        q2_sorted["day_name"], categories=monday_to_sunday, ordered=True
    )
    q2_sorted = q2_sorted.sort_values("day_name").reset_index(drop=True)

    # Trích xuất các chỉ số KPI cho đầu trang Data Analysis
    total_rentals = int(b2_stats["total_rentals"].iloc[0])
    avg_rentals = float(b2_stats["mean"].iloc[0])

    peak_hour_row = q1_hourly.sort_values("avg_total_demand", ascending=False).iloc[0]
    peak_hour_int = int(peak_hour_row["hour"])
    peak_hour_str = f"{peak_hour_int:02d}:00"
    peak_hour_avg = float(peak_hour_row["avg_total_demand"])

    top_season_row = q3_seasonal.sort_values("avg_total_demand", ascending=False).iloc[0]
    highest_season_name = str(top_season_row["season_name"])
    highest_season_avg = float(top_season_row["avg_total_demand"])

    wd_row = q5_workingday.loc[q5_workingday["workingday"] == 1].iloc[0]
    reg_share_workingday = float(wd_row["registered_share_pct"])

    return {
        "eda_summary": eda_summary,
        "prep_summary": prep_summary,
        "q1_hourly": q1_hourly,
        "q2_dayofweek": q2_sorted,
        "q3_seasonal": q3_seasonal,
        "q4_weather": q4_weather,
        "q5_workingday": q5_workingday,
        "q6_monthly": q6_monthly,
        "q7_peak_hours": q7_peak_hours,
        "q8_user_behavior": q8_user_behavior,
        "b1_desc": b1_desc,
        "b2_dist": b2_dist,
        "b2_stats": b2_stats,
        "b3_year": b3_year,
        "b4_temp_hum": b4_temp_hum,
        "b4_weather": b4_weather,
        "kpis": {
            "total_rentals": total_rentals,
            "avg_rentals": avg_rentals,
            "peak_hour_str": peak_hour_str,
            "peak_hour_avg": peak_hour_avg,
            "highest_season_name": highest_season_name,
            "highest_season_avg": highest_season_avg,
            "reg_share_workingday": reg_share_workingday,
        },
    }


@st.cache_data(show_spinner=False)
def get_benchmark_artifacts() -> dict[str, Any]:
    """
    Nạp và kiểm định nghiêm ngặt toàn bộ 5 tệp kết quả của Milestone 6 (Big Data Benchmark)
    từ outputs/benchmark/ phục vụ Trang 4: Big Data Benchmark.
    """
    scaling_df = load_csv("scaling_results")
    storage_df = load_csv("storage_comparison")
    processing_df = load_csv("processing_results")
    partition_df = load_csv("partition_results")
    benchmark_summary = load_json("benchmark_summary")

    # 1. Kiểm tra số dòng và tính toàn vẹn dữ liệu
    if len(scaling_df) != 4:
        raise ArtifactLoadError(f"scaling_results.csv phải có 4 dòng (1x, 10x, 50x, 100x), thực tế: {len(scaling_df)}")
    if len(storage_df) != 8:
        raise ArtifactLoadError(f"storage_comparison.csv phải có 8 dòng (4 scales x 2 formats), thực tế: {len(storage_df)}")
    if len(processing_df) != 20:
        raise ArtifactLoadError(f"processing_results.csv phải có 20 dòng (4 scales x 5 ops), thực tế: {len(processing_df)}")
    if len(partition_df) != 12:
        raise ArtifactLoadError(f"partition_results.csv phải có 12 dòng (3 scales x 4 partitions), thực tế: {len(partition_df)}")

    # Cột 'error' trong processing_results.csv lưu chuỗi 'None' (không có lỗi) nên pandas mặc định hiểu là NaN
    if "error" in processing_df.columns:
        processing_df = processing_df.copy()
        processing_df["error"] = processing_df["error"].fillna("None")

    for name, df in [
        ("scaling_results.csv", scaling_df),
        ("storage_comparison.csv", storage_df),
        ("processing_results.csv", processing_df),
        ("partition_results.csv", partition_df),
    ]:
        if int(df.isna().sum().sum()) != 0:
            raise ArtifactLoadError(f"Phát hiện giá trị Null trong tệp '{name}'")

    # 2. Sắp xếp theo thứ tự scale_factor tăng dần
    scaling_sorted = scaling_df.sort_values("scale_factor").reset_index(drop=True)
    storage_sorted = storage_df.sort_values(["scale_factor", "format"]).reset_index(drop=True)
    processing_sorted = processing_df.sort_values(["scale_factor"]).reset_index(drop=True)
    partition_sorted = partition_df.sort_values(["scale_factor", "target_partitions"]).reset_index(drop=True)

    # 3. Trích xuất KPI ở quy mô tối đa (100x) và đối chiếu chéo giữa scaling_results.csv & storage_comparison.csv
    row_100x = scaling_sorted.loc[scaling_sorted["scale_factor"] == 100].iloc[0]
    st_csv_100x = storage_sorted.loc[
        (storage_sorted["scale_factor"] == 100) & (storage_sorted["format"] == "CSV")
    ].iloc[0]
    st_pq_100x = storage_sorted.loc[
        (storage_sorted["scale_factor"] == 100) & (storage_sorted["format"] == "PARQUET")
    ].iloc[0]

    max_rows = int(row_100x["num_rows"])
    max_scale_label = str(row_100x["scale_label"])
    csv_mb_100x = float(st_csv_100x["storage_size_mb"])
    pq_mb_100x = float(st_pq_100x["storage_size_mb"])
    csv_bytes_100x = float(st_csv_100x["storage_size_bytes"])
    pq_bytes_100x = float(st_pq_100x["storage_size_bytes"])

    storage_reduction_pct = round((1.0 - (pq_bytes_100x / csv_bytes_100x)) * 100.0, 2)
    compression_ratio_100x = float(row_100x["csv_to_parquet_size_ratio"])

    csv_read_agg_100x = float(st_csv_100x["avg_read_agg_time_sec"])
    pq_read_agg_100x = float(st_pq_100x["avg_read_agg_time_sec"])
    read_agg_speedup_100x = round(csv_read_agg_100x / pq_read_agg_100x, 2)

    csv_read_cnt_100x = float(st_csv_100x["avg_read_count_time_sec"])
    pq_read_cnt_100x = float(st_pq_100x["avg_read_count_time_sec"])
    read_cnt_speedup_100x = round(csv_read_cnt_100x / pq_read_cnt_100x, 2)

    # Đối chiếu chéo với benchmark_summary.json
    if len(benchmark_summary.get("scaling_results", [])) != 4:
        raise ArtifactLoadError("benchmark_summary.json không khớp số lượng bản ghi scaling_results")

    return {
        "scaling": scaling_sorted,
        "storage": storage_sorted,
        "processing": processing_sorted,
        "partition": partition_sorted,
        "summary_json": benchmark_summary,
        "kpis": {
            "max_rows": max_rows,
            "max_scale_label": max_scale_label,
            "csv_mb_100x": csv_mb_100x,
            "pq_mb_100x": pq_mb_100x,
            "storage_reduction_pct": storage_reduction_pct,
            "compression_ratio_100x": compression_ratio_100x,
            "read_agg_speedup_100x": read_agg_speedup_100x,
            "read_cnt_speedup_100x": read_cnt_speedup_100x,
            "csv_read_agg_100x": csv_read_agg_100x,
            "pq_read_agg_100x": pq_read_agg_100x,
        },
    }



