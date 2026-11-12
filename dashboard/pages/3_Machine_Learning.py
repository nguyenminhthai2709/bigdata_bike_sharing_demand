"""
Trang 3: Machine Learning (Spark MLlib Forecasting & Evaluation)
Trực quan hóa kết quả huấn luyện, đánh giá 3 mô hình hồi quy (Linear Regression, Decision Tree, Random Forest),
mức độ quan trọng đặc trưng (Feature Importance), phân tích phần dư (Residuals), khám phá dự báo tập Test
và tải xuống tệp Kaggle Submission từ thư mục outputs/models/.
"""

from pathlib import Path
import sys
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parents[1]
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from config import COLORS
from data_loader import ArtifactLoadError, get_ml_artifacts
from components.charts import apply_standard_chart_layout, render_plotly_chart
from components.kpi_cards import render_kpi_row, render_page_header

# =============================================================================
# HEADER & NẠP ARTIFACTS MILESTONE 5
# =============================================================================
render_page_header(
    badge_text="Milestone 5 — Distributed Machine Learning with Apache Spark MLlib",
    title="Machine Learning — Bike Sharing Demand Forecasting",
    subtitle=(
        "Đánh giá thực nghiệm 3 mô hình hồi quy phân tán trên Apache Spark MLlib với phương pháp chia dữ liệu "
        "theo trình tự thời gian (Time-based Split 80/20: 8,708 mẫu Train / 2,178 mẫu Validation) và dự báo 6,493 mẫu Test."
    ),
)

try:
    ml_bundle = get_ml_artifacts()
except ArtifactLoadError as err:
    st.error(f"Lỗi nạp dữ liệu nguồn cho trang Machine Learning: {err}")
    st.stop()

model_df: pd.DataFrame = ml_bundle["model_comparison"]
best_meta: dict = ml_bundle["best_model_meta"]
fi_df: pd.DataFrame = ml_bundle["feature_importances"]
val_df: pd.DataFrame = ml_bundle["validation_predictions"]
test_df: pd.DataFrame = ml_bundle["test_predictions"]
kaggle_df: pd.DataFrame = ml_bundle["kaggle_submission"]
kaggle_raw_bytes: bytes = ml_bundle["kaggle_raw_bytes"]

best_val_metrics = best_meta["best_model_validation_metrics"]
split_info = best_meta["time_based_split"]
baseline_info = best_meta["baseline_mean_predictor"]
best_params = best_meta["best_model_params"]

# =============================================================================
# A. MODEL PERFORMANCE COMPARISON
# =============================================================================
st.subheader("1. So sánh Hiệu năng các Mô hình (Model Performance Comparison)")

render_kpi_row(
    [
        {
            "title": "Best Model (Selected)",
            "value": best_meta["best_model_name"].replace(" Regressor", ""),
            "unit": "Regressor",
            "description": "Mô hình đạt sai số thấp nhất và độ phù hợp R² cao nhất trên tập Validation.",
            "accent": "blue",
        },
        {
            "title": "Validation RMSE",
            "value": f"{float(best_val_metrics['rmse']):.2f}",
            "unit": f"(chính xác: {float(best_val_metrics['rmse']):.4f})",
            "description": "Root Mean Squared Error trên 2,178 bản ghi Validation (giảm 57.6% so với Baseline).",
            "accent": "emerald",
        },
        {
            "title": "Validation MAE",
            "value": f"{float(best_val_metrics['mae']):.2f}",
            "unit": f"(chính xác: {float(best_val_metrics['mae']):.4f})",
            "description": "Mean Absolute Error — sai số tuyệt đối trung bình lượt thuê/giờ.",
            "accent": "teal",
        },
        {
            "title": "Validation R²",
            "value": f"{float(best_val_metrics['r2']):.4f}",
            "unit": "(79.22%)",
            "description": "Hệ số xác định giải thích 79.22% phương sai nhu cầu thuê xe thực tế.",
            "accent": "amber",
        },
        {
            "title": "Validation RMSLE",
            "value": f"{float(best_val_metrics['rmsle']):.4f}",
            "unit": "(Kaggle metric)",
            "description": "Root Mean Squared Logarithmic Error tính trên dự đoán đã cắt âm (prediction_clipped).",
            "accent": "blue",
        },
    ]
)

st.markdown("<div style='height: 0.6rem;'></div>", unsafe_allow_html=True)

st.info(
    "📌 **Quy ước tính toán độ đo tại Milestone 5:** "
    "Các chỉ số **`RMSE`**, **`MAE`** và **`R²`** được tính trực tiếp trên giá trị dự đoán gốc (**`prediction`**) của mô hình; "
    "trong khi **`RMSLE`** được tính trên giá trị dự đoán đã cắt ngưỡng không âm (**`prediction_clipped = max(prediction, 0.0)`**) "
    "để đảm bảo hàm $\\ln(1 + \\hat{y})$ luôn xác định hợp lệ."
)

# Chuẩn bị bảng so sánh đầy đủ (kèm Baseline Mean Predictor để tham chiếu)
comparison_display_df = model_df[
    [
        "model_name",
        "rmse",
        "mae",
        "r2",
        "rmsle",
        "rmse_clipped",
        "negative_predictions_count",
        "training_time_sec",
    ]
].copy()
comparison_display_df.columns = [
    "Mô hình (Model)",
    "RMSE (Raw)",
    "MAE (Raw)",
    "R² (Raw)",
    "RMSLE (Clipped)",
    "RMSE (Clipped)",
    "Số dự đoán âm",
    "Thời gian huấn luyện (giây)",
]

st.markdown("##### Bảng tổng hợp kết quả đánh giá trên tập Validation (2,178 bản ghi)")
st.dataframe(
    comparison_display_df.style.format(
        {
            "RMSE (Raw)": "{:.4f}",
            "MAE (Raw)": "{:.4f}",
            "R² (Raw)": "{:.4f}",
            "RMSLE (Clipped)": "{:.4f}",
            "RMSE (Clipped)": "{:.4f}",
            "Số dự đoán âm": "{:d}",
            "Thời gian huấn luyện (giây)": "{:.4f}",
        }
    ),
    width="stretch",
    hide_index=True,
)
st.caption(
    f"Tham chiếu **Baseline Mean Predictor** (dự đoán hằng số bằng trung bình tập Train = `{baseline_info['train_mean_count']:.4f}` lượt/giờ): "
    f"`RMSE = {baseline_info['rmse']:.4f}`, `MAE = {baseline_info['mae']:.4f}`, `R² = {baseline_info['r2']:.4f}`."
)

# Biểu đồ Plotly so sánh 4 độ đo giữa 3 mô hình
col_m1, col_m2 = st.columns(2)

model_palette = {
    "Linear Regression": COLORS["slate"],
    "Decision Tree Regressor": COLORS["teal"],
    "Random Forest Regressor": COLORS["primary"],
}
bar_colors = [model_palette.get(name, COLORS["secondary"]) for name in model_df["model_name"]]

with col_m1:
    fig_err = go.Figure()
    fig_err.add_trace(
        go.Bar(
            name="RMSE (Càng thấp càng tốt)",
            x=model_df["model_name"],
            y=model_df["rmse"],
            text=model_df["rmse"].apply(lambda v: f"{v:.2f}"),
            textposition="outside",
            marker_color=COLORS["primary"],
            hovertemplate="<b>%{x}</b><br>RMSE: %{y:.4f} lượt/giờ<extra></extra>",
        )
    )
    fig_err.add_trace(
        go.Bar(
            name="MAE (Càng thấp càng tốt)",
            x=model_df["model_name"],
            y=model_df["mae"],
            text=model_df["mae"].apply(lambda v: f"{v:.2f}"),
            textposition="outside",
            marker_color=COLORS["teal"],
            hovertemplate="<b>%{x}</b><br>MAE: %{y:.4f} lượt/giờ<extra></extra>",
        )
    )
    fig_err.update_layout(barmode="group", yaxis_range=[0, float(model_df["rmse"].max()) * 1.18])
    apply_standard_chart_layout(
        fig_err,
        title="So sánh Sai số Tuyệt đối: RMSE và MAE (Đơn vị: Lượt thuê/giờ)",
        xaxis_title="Mô hình hồi quy (Spark MLlib)",
        yaxis_title="Sai số (Lượt thuê/giờ — Thấp hơn là tốt hơn)",
        height=390,
    )
    render_plotly_chart(fig_err, key="ml_chart_rmse_mae")

with col_m2:
    fig_r2_rmsle = go.Figure()
    fig_r2_rmsle.add_trace(
        go.Bar(
            name="R² Score (Càng cao càng tốt)",
            x=model_df["model_name"],
            y=model_df["r2"],
            text=model_df["r2"].apply(lambda v: f"{v:.4f}"),
            textposition="outside",
            marker_color=COLORS["emerald"],
            hovertemplate="<b>%{x}</b><br>R² Score: %{y:.4f}<extra></extra>",
        )
    )
    fig_r2_rmsle.add_trace(
        go.Bar(
            name="RMSLE (Càng thấp càng tốt)",
            x=model_df["model_name"],
            y=model_df["rmsle"],
            text=model_df["rmsle"].apply(lambda v: f"{v:.4f}"),
            textposition="outside",
            marker_color=COLORS["amber"],
            hovertemplate="<b>%{x}</b><br>RMSLE (Clipped): %{y:.4f}<extra></extra>",
        )
    )
    fig_r2_rmsle.update_layout(barmode="group", yaxis_range=[0, float(model_df["rmsle"].max()) * 1.22])
    apply_standard_chart_layout(
        fig_r2_rmsle,
        title="So sánh Hệ số Xác định R² và Sai số Logarit RMSLE",
        xaxis_title="Mô hình hồi quy (Spark MLlib)",
        yaxis_title="Giá trị độ đo (R² ↑ tốt hơn | RMSLE ↓ tốt hơn)",
        height=390,
    )
    render_plotly_chart(fig_r2_rmsle, key="ml_chart_r2_rmsle")

with st.expander("📖 Giải thích ý nghĩa toán học của 4 độ đo đánh giá (RMSE, MAE, R², RMSLE)", expanded=False):
    st.markdown(
        """
- **RMSE (Root Mean Squared Error):** Căn bậc hai của trung bình bình phương sai số $\\text{RMSE} = \\sqrt{\\frac{1}{n}\\sum_{i=1}^{n}(y_i - \\hat{y}_i)^2}$. Vì bình phương sai số trước khi lấy trung bình, RMSE phạt nặng các sai số lớn tại các giờ cao điểm. **Random Forest đạt RMSE thấp nhất (`99.1848`)**.
- **MAE (Mean Absolute Error):** Trung bình trị tuyệt đối sai số $\\text{MAE} = \\frac{1}{n}\\sum_{i=1}^{n}|y_i - \\hat{y}_i|$. Phản ánh độ lệch trung bình trực tiếp theo đơn vị lượt thuê/giờ (`66.3189` lượt/giờ đối với Random Forest và `65.1976` đối với Decision Tree).
- **R² (Coefficient of Determination):** Tỷ lệ phương sai của biến mục tiêu `count` được giải thích bởi mô hình $\\text{R}^2 = 1 - \\frac{\\sum (y_i - \\hat{y}_i)^2}{\\sum (y_i - \\bar{y})^2}$. **Random Forest đạt R² cao nhất (`0.7922` hay `79.22%`)**, vượt trội so với Linear Regression (`0.4291`).
- **RMSLE (Root Mean Squared Logarithmic Error):** Độ đo chính thức của cuộc thi Kaggle Bike Sharing Demand: $\\text{RMSLE} = \\sqrt{\\frac{1}{n}\\sum_{i=1}^{n}(\\ln(1 + \\hat{y}_{i,\\text{clipped}}) - \\ln(1 + y_i))^2}$. Đo lường sai số tương đối theo tỷ lệ và được tính trên `prediction_clipped = max(prediction, 0.0)`. **Random Forest đạt RMSLE tốt nhất (`0.4016`)**.
        """
    )

st.markdown("---")

# =============================================================================
# B. BEST MODEL & CONFIGURATION
# =============================================================================
st.subheader("2. Cấu hình Mô hình Tốt nhất & Thiết lập Huấn luyện (Best Model & Configuration)")

col_cfg1, col_cfg2, col_cfg3 = st.columns([1.15, 1.15, 1.0])

with col_cfg1:
    st.markdown(
        f"""
        <div class="info-panel">
            <h3>🏆 Mô hình Chiến thắng & Siêu tham số</h3>
            <ul>
                <li><strong>Tên mô hình:</strong> <code>{best_meta['best_model_name']}</code> (<code>{best_meta['best_model_key']}</code>)</li>
                <li><strong>Đường dẫn lưu trên HDFS:</strong> <code>{best_meta['hdfs_model_path']}</code></li>
                <li><strong>Số cây quyết định (numTrees):</strong> <code>{best_params['numTrees']}</code></li>
                <li><strong>Độ sâu tối đa mỗi cây (maxDepth):</strong> <code>{best_params['maxDepth']}</code></li>
                <li><strong>Số mẫu tối thiểu tại nút lá (minInstancesPerNode):</strong> <code>{best_params['minInstancesPerNode']}</code></li>
                <li><strong>Chiến lược chọn đặc trưng (featureSubsetStrategy):</strong> <code>"{best_params['featureSubsetStrategy']}"</code></li>
                <li><strong>Tỷ lệ lấy mẫu con (subsamplingRate):</strong> <code>{best_params['subsamplingRate']}</code></li>
                <li><strong>Hạt giống ngẫu nhiên (seed):</strong> <code>{best_params['seed']}</code></li>
                <li><strong>Thời gian huấn luyện Spark MLlib:</strong> <code>{float(best_val_metrics['training_time_sec']):.4f} giây</code></li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col_cfg2:
    st.markdown(
        f"""
        <div class="info-panel">
            <h3>⏱️ Phương pháp Chia dữ liệu Time-based Split (80/20)</h3>
            <ul>
                <li><strong>Nguyên tắc:</strong> Chia theo trình tự thời gian nghiêm ngặt để mô phỏng bài toán dự báo tương lai (không dùng Random Split tránh rò rỉ tương lai).</li>
                <li><strong>Tập Huấn luyện (Train — {int(split_info['train_ratio']*100)}%):</strong> <code>{int(split_info['train_rows']):,}</code> bản ghi</li>
                <li><strong>Khoảng thời gian Train:</strong> <code>{split_info['train_start']}</code> → <code>{split_info['train_end']}</code></li>
                <li><strong>Tập Kiểm định (Validation — {int(split_info['validation_ratio']*100)}%):</strong> <code>{int(split_info['validation_rows']):,}</code> bản ghi</li>
                <li><strong>Khoảng thời gian Validation:</strong> <code>{split_info['validation_start']}</code> → <code>{split_info['validation_end']}</code></li>
                <li><strong>Kiểm soát Data Leakage:</strong> Loại bỏ hoàn toàn <code>{', '.join(best_meta['excluded_leakage_columns'])}</code> và cột định danh <code>{', '.join(best_meta['excluded_non_feature_columns'])}</code>.</li>
                <li><strong>Số đặc trưng đầu vào (VectorAssembler):</strong> <code>{len(best_meta['feature_columns'])}</code> đặc trưng.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col_cfg3:
    fig_time = go.Figure(
        go.Bar(
            x=model_df["training_time_sec"],
            y=model_df["model_name"],
            orientation="h",
            text=model_df["training_time_sec"].apply(lambda v: f"{v:.2f} s"),
            textposition="outside",
            marker_color=bar_colors,
            hovertemplate="<b>%{y}</b><br>Thời gian huấn luyện: %{x:.4f} giây<extra></extra>",
        )
    )
    fig_time.update_layout(xaxis_range=[0, float(model_df["training_time_sec"].max()) * 1.25])
    apply_standard_chart_layout(
        fig_time,
        title="Thời gian Huấn luyện trên Spark Cluster (Giây)",
        xaxis_title="Thời gian thực thi (Giây)",
        yaxis_title="Mô hình",
        height=310,
        show_legend=False,
    )
    render_plotly_chart(fig_time, key="ml_chart_training_time")

st.markdown("---")

# =============================================================================
# C. FEATURE IMPORTANCE
# =============================================================================
st.subheader("3. Mức độ Quan trọng của các Đặc trưng (Feature Importance — 18 Features)")

col_fi_ctrl1, col_fi_ctrl2 = st.columns([1.2, 1.8])
with col_fi_ctrl1:
    fi_view_mode = st.radio(
        "Chế độ hiển thị biểu đồ Feature Importance:",
        options=[
            "Top 10 đặc trưng quan trọng nhất (Random Forest)",
            "So sánh Top 10: Random Forest vs Decision Tree",
            "Toàn bộ 18 đặc trưng (Random Forest)",
        ],
        index=0,
        key="ml_fi_view_mode",
    )
with col_fi_ctrl2:
    top3_sum_pct = float(fi_df.iloc[:3]["rf_importance_pct"].sum())
    top6_sum_pct = float(fi_df.iloc[:6]["rf_importance_pct"].sum())
    st.markdown(
        f"""
        <div class="insight-box">
            <strong>💡 Nhận định Feature Importance (Nguồn: <code>outputs/models/feature_importances.csv</code>):</strong><br/>
            • Ba đặc trưng khung giờ (<code>hour</code>: <strong>30.45%</strong>, <code>hour_sin</code>: <strong>18.02%</strong>, <code>hour_cos</code>: <strong>11.15%</strong>) đóng góp tổng cộng <strong>{top3_sum_pct:.2f}%</strong> mức độ quan trọng của Random Forest.<br/>
            • Tiếp theo là cảm nhận nhiệt độ <code>atemp</code> (<strong>8.96%</strong>), xu hướng năm <code>year</code> (<strong>7.70%</strong>) và nhiệt độ thực <code>temp</code> (<strong>7.35%</strong>) — Top 6 đặc trưng giải thích tới <strong>{top6_sum_pct:.2f}%</strong> quyết định phân tách cây.
        </div>
        """,
        unsafe_allow_html=True,
    )

if fi_view_mode == "Toàn bộ 18 đặc trưng (Random Forest)":
    fi_plot_df = fi_df.copy().iloc[::-1]
    chart_height = 520
    chart_title = "Mức độ Quan trọng của Toàn bộ 18 Đặc trưng — Random Forest Regressor (%)"
else:
    fi_plot_df = fi_df.head(10).copy().iloc[::-1]
    chart_height = 420
    chart_title = "Top 10 Đặc trưng Quan trọng nhất — Random Forest Regressor (Sắp xếp giảm dần)"

fig_fi = go.Figure()
fig_fi.add_trace(
    go.Bar(
        name="Random Forest Importance (%)",
        x=fi_plot_df["rf_importance_pct"],
        y=fi_plot_df["feature"],
        orientation="h",
        text=fi_plot_df["rf_importance_pct"].apply(lambda v: f"{v:.2f}%"),
        textposition="outside",
        marker_color=COLORS["primary"],
        hovertemplate="<b>Đặc trưng: %{y}</b><br>Random Forest Importance: %{x:.2f}%<extra></extra>",
    )
)

if fi_view_mode == "So sánh Top 10: Random Forest vs Decision Tree":
    fig_fi.add_trace(
        go.Bar(
            name="Decision Tree Importance (%)",
            x=fi_plot_df["dt_importance_pct"],
            y=fi_plot_df["feature"],
            orientation="h",
            text=fi_plot_df["dt_importance_pct"].apply(lambda v: f"{v:.2f}%"),
            textposition="outside",
            marker_color=COLORS["teal"],
            hovertemplate="<b>Đặc trưng: %{y}</b><br>Decision Tree Importance: %{x:.2f}%<extra></extra>",
        )
    )
    fig_fi.update_layout(barmode="group", xaxis_range=[0, max(float(fi_plot_df["dt_importance_pct"].max()), float(fi_plot_df["rf_importance_pct"].max())) * 1.18])
    chart_title = "So sánh Top 10 Đặc trưng Quan trọng nhất: Random Forest vs Decision Tree (%)"
else:
    fig_fi.update_layout(xaxis_range=[0, float(fi_plot_df["rf_importance_pct"].max()) * 1.18])

apply_standard_chart_layout(
    fig_fi,
    title=chart_title,
    xaxis_title="Tỷ trọng đóng góp Importance (%)",
    yaxis_title="Tên đặc trưng (Feature)",
    height=chart_height,
    show_legend=(fi_view_mode == "So sánh Top 10: Random Forest vs Decision Tree"),
)
render_plotly_chart(fig_fi, key="ml_chart_feature_importance")

with st.expander("📋 Xem bảng chi tiết toàn bộ 18 đặc trưng (feature_importances.csv)", expanded=False):
    fi_table = fi_df[["rank", "feature", "rf_importance", "rf_importance_pct", "dt_importance", "dt_importance_pct"]].copy()
    fi_table.columns = [
        "Hạng (Rank)",
        "Tên đặc trưng (Feature)",
        "RF Importance (Raw)",
        "RF Importance (%)",
        "DT Importance (Raw)",
        "DT Importance (%)",
    ]
    st.dataframe(
        fi_table.style.format(
            {
                "RF Importance (Raw)": "{:.6f}",
                "RF Importance (%)": "{:.2f}%",
                "DT Importance (Raw)": "{:.6f}",
                "DT Importance (%)": "{:.2f}%",
            }
        ),
        width="stretch",
        hide_index=True,
    )

st.markdown("---")

# =============================================================================
# D. ACTUAL VS PREDICTED (VALIDATION SET — 2,178 BẢN GHI)
# =============================================================================
st.subheader("4. Đối chiếu Thực tế và Dự báo trên Tập Validation (Actual vs Predicted — 2,178 mẫu)")

st.caption(
    "Nguồn dữ liệu: `outputs/models/validation_predictions.csv` (`2,178` bản ghi từ `2012-08-05 05:00:00` đến `2012-12-19 23:00:00`). "
    "Tuyệt đối không sử dụng tập Test tại mục này vì tập Test không có nhãn `actual_count`."
)

model_col_map = {
    "Random Forest Regressor (Best Model)": ("rf_prediction", "rf_residual", COLORS["primary"]),
    "Decision Tree Regressor": ("dt_prediction", "dt_residual", COLORS["teal"]),
    "Linear Regression": ("lr_prediction", "lr_residual", COLORS["amber"]),
}

val_min_date = val_df["date"].min()
val_max_date = val_df["date"].max()

col_val_f1, col_val_f2, col_val_f3 = st.columns([1.3, 1.3, 1.0])
with col_val_f1:
    selected_val_model_label = st.selectbox(
        "Chọn mô hình để phân tích Actual vs Predicted & Residuals:",
        options=list(model_col_map.keys()),
        index=0,
        key="ml_val_model_select",
    )
with col_val_f2:
    val_date_range = st.date_input(
        "Khoảng ngày hiển thị trên biểu đồ chuỗi thời gian Validation:",
        value=(val_min_date, min(val_max_date, pd.to_datetime("2012-08-19").date())),
        min_value=val_min_date,
        max_value=val_max_date,
        key="ml_val_date_range",
    )
with col_val_f3:
    overlay_all_models = st.checkbox(
        "Hiển thị đồng thời cả 3 mô hình trên Time-Series",
        value=False,
        key="ml_val_overlay_all",
    )

pred_col, res_col, model_color = model_col_map[selected_val_model_label]

# Xử lý khoảng ngày được chọn cho biểu đồ Time-Series
if isinstance(val_date_range, (tuple, list)) and len(val_date_range) == 2:
    start_d, end_d = val_date_range[0], val_date_range[1]
elif isinstance(val_date_range, (tuple, list)) and len(val_date_range) == 1:
    start_d = end_d = val_date_range[0]
else:
    start_d = end_d = val_date_range

if start_d > end_d:
    start_d, end_d = end_d, start_d

val_ts_filtered = val_df.loc[(val_df["date"] >= start_d) & (val_df["date"] <= end_d)].copy()
if val_ts_filtered.empty:
    val_ts_filtered = val_df.head(336).copy()

col_avp1, col_avp2 = st.columns([1.0, 1.25])

with col_avp1:
    max_axis_val = float(max(val_df["actual_count"].max(), val_df[pred_col].max())) * 1.05
    fig_scatter_avp = go.Figure()
    fig_scatter_avp.add_trace(
        go.Scatter(
            x=val_df["actual_count"],
            y=val_df[pred_col],
            mode="markers",
            name="Mẫu Validation (n=2,178)",
            marker={"color": model_color, "size": 5, "opacity": 0.45},
            customdata=val_df[["datetime", res_col]],
            hovertemplate=(
                "<b>Thời điểm: %{customdata[0]}</b><br>"
                "Thực tế (Actual): %{x:.0f} lượt<br>"
                "Dự báo (Predicted): %{y:.2f} lượt<br>"
                "Phần dư (Actual - Predicted): %{customdata[1]:.2f}<extra></extra>"
            ),
        )
    )
    fig_scatter_avp.add_trace(
        go.Scatter(
            x=[0, max_axis_val],
            y=[0, max_axis_val],
            mode="lines",
            name="Đường lý tưởng y = x",
            line={"color": COLORS["crimson"], "width": 2, "dash": "dash"},
            hoverinfo="skip",
        )
    )
    fig_scatter_avp.update_layout(xaxis_range=[0, max_axis_val], yaxis_range=[0, max_axis_val])
    apply_standard_chart_layout(
        fig_scatter_avp,
        title=f"Scatter Plot: Actual vs Predicted ({selected_val_model_label.split(' (')[0]})",
        xaxis_title="Số lượt thuê thực tế — Actual Rentals (lượt/giờ)",
        yaxis_title="Số lượt thuê dự báo — Predicted Rentals (lượt/giờ)",
        height=420,
    )
    render_plotly_chart(fig_scatter_avp, key="ml_chart_scatter_avp")

with col_avp2:
    fig_ts_avp = go.Figure()
    fig_ts_avp.add_trace(
        go.Scatter(
            x=val_ts_filtered["datetime_dt"],
            y=val_ts_filtered["actual_count"],
            mode="lines",
            name="Thực tế (Actual Count)",
            line={"color": "#0F172A", "width": 2.2},
            hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Thực tế (Actual): %{y:.0f} lượt<extra></extra>",
        )
    )
    if overlay_all_models:
        fig_ts_avp.add_trace(
            go.Scatter(
                x=val_ts_filtered["datetime_dt"],
                y=val_ts_filtered["rf_prediction"],
                mode="lines",
                name="Random Forest (Predicted)",
                line={"color": COLORS["primary"], "width": 1.8},
            )
        )
        fig_ts_avp.add_trace(
            go.Scatter(
                x=val_ts_filtered["datetime_dt"],
                y=val_ts_filtered["dt_prediction"],
                mode="lines",
                name="Decision Tree (Predicted)",
                line={"color": COLORS["teal"], "width": 1.5, "dash": "dot"},
            )
        )
        fig_ts_avp.add_trace(
            go.Scatter(
                x=val_ts_filtered["datetime_dt"],
                y=val_ts_filtered["lr_prediction"],
                mode="lines",
                name="Linear Regression (Predicted)",
                line={"color": COLORS["amber"], "width": 1.5, "dash": "dash"},
            )
        )
    else:
        fig_ts_avp.add_trace(
            go.Scatter(
                x=val_ts_filtered["datetime_dt"],
                y=val_ts_filtered[pred_col],
                mode="lines",
                name=f"Dự báo ({selected_val_model_label.split(' (')[0]})",
                line={"color": model_color, "width": 2.0},
                hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Dự báo (Predicted): %{y:.2f} lượt<extra></extra>",
            )
        )
    apply_standard_chart_layout(
        fig_ts_avp,
        title=f"Chuỗi thời gian: Actual vs Predicted ({start_d} → {end_d}, {len(val_ts_filtered):,} giờ)",
        xaxis_title="Thời gian quan sát (Datetime)",
        yaxis_title="Số lượt thuê xe (Lượt/giờ)",
        height=420,
    )
    render_plotly_chart(fig_ts_avp, key="ml_chart_ts_avp")

st.markdown("---")

# =============================================================================
# E. RESIDUAL ANALYSIS (PHÂN TÍCH PHẦN DƯ TRÊN TẬP VALIDATION)
# =============================================================================
st.subheader("5. Phân tích Phần dư trên Tập Validation (Residual Analysis)")

st.markdown(
    "Phần dư được tính trực tiếp trong bộ nhớ từ `validation_predictions.csv` theo công thức "
    "$\\text{Residual} = \\text{Actual} - \\text{Predicted}$ (sử dụng giá trị dự đoán gốc `prediction`)."
)

residuals_series = val_df[res_col]
mean_residual = float(residuals_series.mean())
mae_from_residual = float(residuals_series.abs().mean())
std_residual = float(residuals_series.std())
median_residual = float(residuals_series.median())
under_pred_pct = float((residuals_series > 0).mean() * 100.0)

render_kpi_row(
    [
        {
            "title": "Mean Residual (Bias)",
            "value": f"{mean_residual:+.2f}",
            "unit": "lượt/giờ",
            "description": f"Trung bình (Actual - Predicted) của {selected_val_model_label.split(' (')[0]} trên 2,178 mẫu.",
            "accent": "blue",
        },
        {
            "title": "Validation MAE (|Residual|)",
            "value": f"{mae_from_residual:.2f}",
            "unit": f"(chính xác: {mae_from_residual:.4f})",
            "description": "Trung bình trị tuyệt đối phần dư |Actual - Predicted| khớp với bảng đánh giá.",
            "accent": "emerald",
        },
        {
            "title": "Residual StdDev",
            "value": f"{std_residual:.2f}",
            "unit": "lượt/giờ",
            "description": f"Độ lệch chuẩn của phần dư (Median Residual = {median_residual:+.2f} lượt/giờ).",
            "accent": "teal",
        },
        {
            "title": "Tỷ lệ Residual > 0",
            "value": f"{under_pred_pct:.1f}%",
            "unit": f"({int((residuals_series > 0).sum()):,}/2,178 giờ)",
            "description": "Tỷ lệ mẫu có Actual > Predicted (mô hình dự báo thấp hơn nhu cầu thực tế).",
            "accent": "amber",
        },
    ]
)

st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

col_res1, col_res2 = st.columns(2)

with col_res1:
    fig_res_hist = go.Figure()
    fig_res_hist.add_trace(
        go.Histogram(
            x=residuals_series,
            nbinsx=45,
            name="Phân phối Residual",
            marker_color=model_color,
            opacity=0.85,
            hovertemplate="Khoảng Residual: %{x}<br>Số giờ: %{y}<extra></extra>",
        )
    )
    fig_res_hist.add_vline(
        x=0.0,
        line_width=2.2,
        line_dash="dash",
        line_color=COLORS["crimson"],
        annotation_text="Residual = 0",
        annotation_position="top right",
    )
    apply_standard_chart_layout(
        fig_res_hist,
        title=f"Phân phối Phần dư (Residual Distribution — {selected_val_model_label.split(' (')[0]})",
        xaxis_title="Phần dư: Residual = Actual - Predicted (Lượt/giờ)",
        yaxis_title="Tần suất (Số bản ghi giờ)",
        height=390,
        show_legend=False,
    )
    render_plotly_chart(fig_res_hist, key="ml_chart_res_hist")

with col_res2:
    fig_res_scatter = go.Figure()
    fig_res_scatter.add_trace(
        go.Scatter(
            x=val_df[pred_col],
            y=residuals_series,
            mode="markers",
            name="Residual vs Predicted",
            marker={"color": model_color, "size": 5, "opacity": 0.45},
            customdata=val_df[["datetime", "actual_count"]],
            hovertemplate=(
                "<b>Thời điểm: %{customdata[0]}</b><br>"
                "Thực tế (Actual): %{customdata[1]:.0f} lượt<br>"
                "Dự báo (Predicted): %{x:.2f} lượt<br>"
                "Residual (Actual - Predicted): %{y:+.2f} lượt<extra></extra>"
            ),
        )
    )
    fig_res_scatter.add_hline(
        y=0.0,
        line_width=2.2,
        line_dash="dash",
        line_color=COLORS["crimson"],
        annotation_text="Residual = 0 (Không sai số)",
        annotation_position="bottom right",
    )
    apply_standard_chart_layout(
        fig_res_scatter,
        title=f"Biểu đồ Phần dư theo Giá trị Dự báo (Residual vs Predicted)",
        xaxis_title="Giá trị dự báo — Predicted Rentals (Lượt/giờ)",
        yaxis_title="Phần dư — Residual = Actual - Predicted (Lượt/giờ)",
        height=390,
        show_legend=False,
    )
    render_plotly_chart(fig_res_scatter, key="ml_chart_res_scatter")

st.markdown(
    """
    <div class="insight-box">
        <strong>🔍 Giải thích ý nghĩa Phần dư ($\text{Residual} = \text{Actual} - \text{Predicted}$):</strong><br/>
        • <strong>Residual &gt; 0 ($\text{Actual} &gt; \text{Predicted}$ — Under-prediction):</strong> Nhu cầu thuê thực tế vượt mức dự báo của mô hình. Trên tập Validation (cuối năm 2012, khi tổng nhu cầu thuê xe tăng trưởng <strong>+65.41%</strong> so với năm 2011), các điểm có Residual dương lớn thường rơi vào các giờ cao điểm 08:00 và 17:00–18:00.<br/>
        • <strong>Residual &lt; 0 ($\text{Actual} &lt; \text{Predicted}$ — Over-prediction):</strong> Mô hình dự báo cao hơn nhu cầu thực tế, thường xảy ra khi thời tiết chuyển xấu đột ngột trong khung giờ ban ngày.<br/>
        • <strong>Residual $\approx 0$:</strong> Dự báo của mô hình bám sát nhu cầu thực tế; mật độ điểm tập trung dày đặc quanh đường tham chiếu $\text{Residual} = 0$.
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

# =============================================================================
# F. TEST PREDICTION EXPLORER (6,493 BẢN GHI)
# =============================================================================
st.subheader("6. Khám phá Dự báo trên Tập Test (Test Prediction Explorer — 6,493 mẫu)")

st.caption(
    "Nguồn dữ liệu: `outputs/models/test_predictions.csv` (`6,493` bản ghi dự báo cho các ngày từ ngày 20 đến cuối mỗi tháng trong 2 năm 2011–2012). "
    "Tập Test của Kaggle không có cột `count` thực tế nên không tính sai số trên tập Test."
)

render_kpi_row(
    [
        {
            "title": "Test Records",
            "value": f"{len(test_df):,}",
            "unit": "bản ghi giờ",
            "description": "Đúng 6,493 khung giờ cần dự báo trong tập test.csv của Kaggle.",
            "accent": "blue",
        },
        {
            "title": "Mean Predicted Demand",
            "value": f"{float(test_df['prediction_clipped'].mean()):.2f}",
            "unit": "lượt/giờ",
            "description": f"Dự báo trung bình trên tập Test (Median: {float(test_df['prediction_clipped'].median()):.2f} lượt/giờ).",
            "accent": "emerald",
        },
        {
            "title": "Min / Max Forecast",
            "value": f"{float(test_df['prediction_clipped'].min()):.2f} / {float(test_df['prediction_clipped'].max()):.2f}",
            "unit": "lượt/giờ",
            "description": "Khoảng giá trị dự báo nhỏ nhất và lớn nhất trên 6,493 mẫu Test.",
            "accent": "teal",
        },
        {
            "title": "Negative Predictions",
            "value": f"{int((test_df['prediction'] < 0).sum())}",
            "unit": "bản ghi",
            "description": "Random Forest Regressor luôn cho dự báo dương tự nhiên trên toàn bộ tập Test.",
            "accent": "amber",
        },
    ]
)

st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

available_months = ["Tất cả các tháng (2011-01 → 2012-12)"] + sorted(test_df["year_month"].unique().tolist())

col_tf1, col_tf2 = st.columns([1.2, 1.2])
with col_tf1:
    selected_test_month = st.selectbox(
        "Lọc theo Tháng-Năm (Year-Month) trên tập Test:",
        options=available_months,
        index=1,  # Mặc định hiển thị tháng 2011-01 rõ nét trên chuỗi thời gian, có thể chọn Tất cả
        key="ml_test_month_filter",
    )
with col_tf2:
    datetime_search_query = st.text_input(
        "Tìm kiếm theo chuỗi thời gian (ví dụ: 2012-12-24 hoặc 17:00:00):",
        value="",
        key="ml_test_datetime_search",
    ).strip()

filtered_test_df = test_df.copy()
if selected_test_month != "Tất cả các tháng (2011-01 → 2012-12)":
    filtered_test_df = filtered_test_df.loc[filtered_test_df["year_month"] == selected_test_month]
if datetime_search_query:
    filtered_test_df = filtered_test_df.loc[
        filtered_test_df["datetime"].astype(str).str.contains(datetime_search_query, case=False, na=False)
    ]

col_tc1, col_tc2 = st.columns([1.3, 1.0])

with col_tc1:
    plot_test_df = filtered_test_df if not filtered_test_df.empty else test_df.head(260)
    fig_test_ts = go.Figure()
    fig_test_ts.add_trace(
        go.Scatter(
            x=plot_test_df["datetime_dt"],
            y=plot_test_df["prediction_clipped"],
            mode="lines",
            name="Dự báo nhu cầu thuê (Test Forecast)",
            line={"color": COLORS["primary"], "width": 2.0},
            hovertemplate="<b>%{x|%Y-%m-%d %H:%M}</b><br>Dự báo: %{y:.2f} lượt/giờ<extra></extra>",
        )
    )
    apply_standard_chart_layout(
        fig_test_ts,
        title=f"Dự báo Nhu cầu Thuê xe theo Thời gian trên Tập Test ({len(plot_test_df):,} bản ghi đang hiển thị)",
        xaxis_title="Thời điểm dự báo (Datetime)",
        yaxis_title="Số lượt thuê dự báo (Lượt/giờ)",
        height=380,
        show_legend=False,
    )
    render_plotly_chart(fig_test_ts, key="ml_chart_test_ts")

with col_tc2:
    fig_test_dist = go.Figure()
    fig_test_dist.add_trace(
        go.Histogram(
            x=test_df["prediction_clipped"],
            nbinsx=40,
            name="Phân phối dự báo Test",
            marker_color=COLORS["teal"],
            opacity=0.88,
            hovertemplate="Khoảng dự báo: %{x} lượt/giờ<br>Số bản ghi: %{y}<extra></extra>",
        )
    )
    apply_standard_chart_layout(
        fig_test_dist,
        title="Phân phối Giá trị Dự báo trên Toàn bộ Tập Test (6,493 mẫu)",
        xaxis_title="Số lượt thuê dự báo (prediction_clipped — Lượt/giờ)",
        yaxis_title="Tần suất (Số bản ghi giờ)",
        height=380,
        show_legend=False,
    )
    render_plotly_chart(fig_test_dist, key="ml_chart_test_dist")

st.markdown(f"##### Bảng dữ liệu dự báo tập Test ({len(filtered_test_df):,} / 6,493 bản ghi khớp bộ lọc)")
test_preview_df = filtered_test_df[["datetime", "prediction", "prediction_clipped"]].head(200).copy()
test_preview_df.columns = ["Thời điểm (datetime)", "Dự đoán gốc (prediction)", "Dự đoán không âm (prediction_clipped)"]
st.dataframe(
    test_preview_df.style.format(
        {
            "Dự đoán gốc (prediction)": "{:.4f}",
            "Dự đoán không âm (prediction_clipped)": "{:.4f}",
        }
    ),
    width="stretch",
    hide_index=True,
)

st.markdown("---")

# =============================================================================
# G. KAGGLE SUBMISSION
# =============================================================================
st.subheader("7. Tệp Nộp bài Kaggle (Kaggle Submission Verification & Download)")

is_row_count_ok = len(kaggle_df) == 6493
is_cols_ok = list(kaggle_df.columns) == ["datetime", "count"]
null_count_total = int(kaggle_df.isna().sum().sum())
min_sub_count = int(kaggle_df["count"].min())
max_sub_count = int(kaggle_df["count"].max())

col_kag1, col_kag2 = st.columns([1.15, 1.25])

with col_kag1:
    st.markdown(
        f"""
        <div class="info-panel">
            <h3>✅ Kiểm định Hợp lệ Tệp Kaggle Submission</h3>
            <ul>
                <li><strong>Tệp nguồn:</strong> <code>outputs/models/kaggle_submission.csv</code></li>
                <li><strong>Kiểm tra Header & Tên cột:</strong> <code>{', '.join(kaggle_df.columns)}</code> ({'Hợp lệ' if is_cols_ok else 'Lỗi'})</li>
                <li><strong>Tổng số dòng dữ liệu:</strong> <code>{len(kaggle_df):,}</code> dòng ({'Đúng chuẩn 6,493 dòng' if is_row_count_ok else 'Sai số dòng'})</li>
                <li><strong>Số giá trị Null / NaN:</strong> <code>{null_count_total}</code> (Không có giá trị khuyết)</li>
                <li><strong>Miền giá trị dự báo (count):</strong> Từ <code>{min_sub_count:,}</code> đến <code>{max_sub_count:,}</code> lượt thuê/giờ (Trung bình: <code>{float(kaggle_df['count'].mean()):.2f}</code>)</li>
                <li><strong>Khoảng thời gian:</strong> <code>{kaggle_df['datetime'].iloc[0]}</code> → <code>{kaggle_df['datetime'].iloc[-1]}</code></li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.download_button(
        label="📥 Tải xuống bike_sharing_kaggle_submission.csv (6,493 dòng — Nguyên bản)",
        data=kaggle_raw_bytes,
        file_name="bike_sharing_kaggle_submission.csv",
        mime="text/csv",
        key="ml_kaggle_download_btn",
    )

with col_kag2:
    st.markdown("##### Xem trước nội dung tệp `kaggle_submission.csv` (10 dòng đầu & 5 dòng cuối)")
    preview_concat = pd.concat([kaggle_df.head(10), kaggle_df.tail(5)], axis=0).reset_index(drop=True)
    st.dataframe(preview_concat, width="stretch", hide_index=True)
