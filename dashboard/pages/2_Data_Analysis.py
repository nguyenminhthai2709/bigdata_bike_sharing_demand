"""
Trang 2: Data Analysis — Bike Sharing Demand
Trực quan hóa kết quả phân tích khám phá dữ liệu (EDA) và 8 truy vấn phân tích chuyên sâu bằng Apache Spark SQL.
Toàn bộ biểu đồ và KPI sử dụng 100% dữ liệu thực từ outputs/eda/, outputs/analytics/ và outputs/preprocessing/.
"""

from pathlib import Path
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parents[1]
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from config import COLORS
from data_loader import ArtifactLoadError, get_data_analysis_artifacts
from components.charts import apply_standard_chart_layout, render_plotly_chart
from components.kpi_cards import render_kpi_row, render_page_header

# =============================================================================
# A. HEADER & KPI CARDS
# =============================================================================
render_page_header(
    badge_text="Milestone 4 — Exploratory Data Analysis & Spark SQL Analytics",
    title="Data Analysis — Bike Sharing Demand",
    subtitle=(
        "Phân tích các yếu tố thời gian, thời tiết và đặc điểm người dùng ảnh hưởng đến nhu cầu thuê xe đạp "
        "dựa trên tập dữ liệu huấn luyện (10,886 bản ghi theo giờ trong 2 năm 2011–2012)."
    ),
)

try:
    bundle = get_data_analysis_artifacts()
except ArtifactLoadError as err:
    st.error(f"Lỗi nạp dữ liệu nguồn cho trang Data Analysis: {err}")
    st.stop()

kpis = bundle["kpis"]
q1_df = bundle["q1_hourly"]
q2_df = bundle["q2_dayofweek"]
q3_df = bundle["q3_seasonal"]
q4_df = bundle["q4_weather"]
q5_df = bundle["q5_workingday"]
q6_df = bundle["q6_monthly"]
q7_df = bundle["q7_peak_hours"]
q8_df = bundle["q8_user_behavior"]
b1_desc_df = bundle["b1_desc"]
b2_dist_df = bundle["b2_dist"]
b3_year_df = bundle["b3_year"]
b4_temp_hum_df = bundle["b4_temp_hum"]

st.subheader("1. Các Chỉ số Phân tích Tổng quan (Analytical KPIs)")

render_kpi_row(
    [
        {
            "title": "Total Rentals (Train)",
            "value": f"{kpis['total_rentals']:,}",
            "unit": "lượt thuê",
            "description": "Tổng số lượt thuê xe trong 10,886 giờ quan sát (outputs/eda/b2_target_count_stats.csv).",
            "accent": "blue",
        },
        {
            "title": "Average Rentals",
            "value": f"{kpis['avg_rentals']:.2f}",
            "unit": "lượt/giờ",
            "description": "Số lượt thuê trung bình mỗi giờ (Median: 145 lượt/giờ, StdDev: 181.14).",
            "accent": "emerald",
        },
        {
            "title": "Peak Hour",
            "value": kpis["peak_hour_str"],
            "unit": f"({kpis['peak_hour_avg']:.2f}/giờ)",
            "description": "Khung giờ có nhu cầu thuê trung bình cao nhất trong ngày (q1_hourly_demand.csv).",
            "accent": "amber",
        },
        {
            "title": "Highest Seasonal Demand",
            "value": kpis["highest_season_name"],
            "unit": f"({kpis['highest_season_avg']:.2f}/giờ)",
            "description": "Mùa Thu dẫn đầu về nhu cầu thuê trung bình (q3_seasonal_demand.csv).",
            "accent": "teal",
        },
        {
            "title": "Registered Share (Working Days)",
            "value": f"{kpis['reg_share_workingday']:.2f}%",
            "unit": "lượt thuê",
            "description": "Tỷ trọng lượt thuê của thành viên đăng ký vào ngày làm việc (q5_workingday_comparison.csv).",
            "accent": "blue",
        },
    ]
)

st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)

# =============================================================================
# BỘ LỌC TƯƠNG TÁC (INTERACTIVE FILTERS)
# =============================================================================
with st.expander("🎛️ Bộ lọc Tương tác (Interactive Filters — Áp dụng cho các bảng dữ liệu tương ứng)", expanded=True):
    st.caption(
        "Bộ lọc chỉ cắt lát (filter) trên các bảng kết quả tổng hợp có sẵn trường tương ứng (`hour`, `workingday`, "
        "`season_name`, `weather_condition`), tuyệt đối không tự nội suy hoặc làm sai lệch kết quả thống kê gốc."
    )
    f_col1, f_col2, f_col3, f_col4 = st.columns(4)
    with f_col1:
        hour_range = st.slider(
            "Khoảng giờ trong ngày (Hour)",
            min_value=0,
            max_value=23,
            value=(0, 23),
            step=1,
            help="Lọc khung giờ hiển thị cho các biểu đồ theo giờ (q1, q8).",
        )
    with f_col2:
        day_type_filter = st.selectbox(
            "Loại ngày (Working Day / Non-working Day)",
            options=["Tất cả", "Working Day (1)", "Non-Working Day (0)"],
            index=0,
            help="Lọc đường biểu diễn theo loại ngày trên biểu đồ q8 và bảng q7.",
        )
    with f_col3:
        all_seasons = list(q3_df["season_name"].unique())
        selected_seasons = st.multiselect(
            "Mùa trong năm (Season)",
            options=all_seasons,
            default=all_seasons,
            help="Lọc các mùa hiển thị trên biểu đồ Mùa (q3).",
        )
    with f_col4:
        all_weathers = list(q4_df["weather_condition"].unique())
        selected_weathers = st.multiselect(
            "Điều kiện thời tiết (Weather)",
            options=all_weathers,
            default=all_weathers,
            help="Lọc nhóm thời tiết hiển thị trên biểu đồ Thời tiết (q4).",
        )

# Áp dụng bộ lọc an toàn (nếu người dùng bỏ chọn hết multiselect thì giữ nguyên đầy đủ để tránh biểu đồ rỗng)
q1_filtered = q1_df.loc[
    (q1_df["hour"] >= hour_range[0]) & (q1_df["hour"] <= hour_range[1])
].copy()

q8_filtered = q8_df.loc[
    (q8_df["hour"] >= hour_range[0]) & (q8_df["hour"] <= hour_range[1])
].copy()
if day_type_filter == "Working Day (1)":
    q8_filtered = q8_filtered.loc[q8_filtered["workingday"] == 1].copy()
elif day_type_filter == "Non-Working Day (0)":
    q8_filtered = q8_filtered.loc[q8_filtered["workingday"] == 0].copy()

q3_filtered = (
    q3_df.loc[q3_df["season_name"].isin(selected_seasons)].copy()
    if selected_seasons
    else q3_df.copy()
)
q4_filtered = (
    q4_df.loc[q4_df["weather_condition"].isin(selected_weathers)].copy()
    if selected_weathers
    else q4_df.copy()
)

st.markdown("---")

# =============================================================================
# B. HOURLY DEMAND ANALYSIS
# =============================================================================
st.subheader("2. Phân tích Nhu cầu Thuê xe theo Giờ (Hourly Demand Analysis)")
st.caption(
    "📂 **Nguồn dữ liệu:** `outputs/analytics/q1_hourly_demand.csv`, `outputs/analytics/q8_user_behavior_casual_vs_registered.csv`, "
    "`outputs/analytics/q7_top10_peak_hours.csv`."
)

hr_col1, hr_col2 = st.columns(2)

with hr_col1:
    hourly_metric_mode = st.radio(
        "Chọn đại lượng hiển thị cho Biểu đồ 24 giờ (q1):",
        options=[
            "Hourly Average (Lượt thuê trung bình / giờ)",
            "Total Rentals (Tổng lượt thuê tích lũy)",
        ],
        horizontal=True,
        key="hourly_metric_mode",
    )

    if hourly_metric_mode.startswith("Hourly Average"):
        fig_q1 = go.Figure()
        fig_q1.add_trace(
            go.Scatter(
                x=q1_filtered["hour"],
                y=q1_filtered["avg_total_demand"],
                mode="lines+markers",
                name="Mean Demand (avg_total_demand)",
                line={"color": COLORS["secondary"], "width": 3},
                marker={"size": 7},
                hovertemplate="Giờ %{x}:00<br>Trung bình: %{y:.2f} lượt/giờ<extra></extra>",
            )
        )
        fig_q1.add_trace(
            go.Scatter(
                x=q1_filtered["hour"],
                y=q1_filtered["median_total_demand"],
                mode="lines+markers",
                name="Median Demand (median_total_demand)",
                line={"color": COLORS["teal"], "width": 2, "dash": "dash"},
                marker={"size": 6},
                hovertemplate="Giờ %{x}:00<br>Trung vị: %{y:.0f} lượt/giờ<extra></extra>",
            )
        )
        apply_standard_chart_layout(
            fig_q1,
            title="Nhu cầu Thuê xe Trung bình & Trung vị theo 24 Giờ (Hourly Average)",
            xaxis_title="Giờ trong ngày (0h – 23h)",
            yaxis_title="Số lượt thuê trung bình (lượt/giờ)",
            height=410,
        )
    else:
        fig_q1 = go.Figure()
        fig_q1.add_trace(
            go.Bar(
                x=q1_filtered["hour"],
                y=q1_filtered["total_rentals"],
                name="Total Rentals (total_rentals)",
                marker_color=COLORS["primary"],
                hovertemplate="Giờ %{x}:00<br>Tổng lượt thuê: %{y:,} lượt<br>Số bản ghi: %{customdata} giờ<extra></extra>",
                customdata=q1_filtered["total_records"],
            )
        )
        apply_standard_chart_layout(
            fig_q1,
            title="Tổng Lượt Thuê Xe Tích lũy theo 24 Giờ (Total Rentals)",
            xaxis_title="Giờ trong ngày (0h – 23h)",
            yaxis_title="Tổng số lượt thuê (lượt)",
            height=410,
        )

    fig_q1.update_xaxes(dtick=1)
    render_plotly_chart(fig_q1, key="chart_q1_hourly")

with hr_col2:
    st.markdown("<div style='height: 2.35rem;'></div>", unsafe_allow_html=True)
    q8_plot_df = q8_filtered.copy()
    q8_plot_df["day_label"] = q8_plot_df["workingday"].map(
        {1: "Working Day (Ngày làm việc)", 0: "Non-working Day (Ngày nghỉ/Cuối tuần)"}
    )

    fig_q8_compare = go.Figure()
    for w_val, label, color in [
        (1, "Working Day (Ngày làm việc)", COLORS["secondary"]),
        (0, "Non-working Day (Ngày nghỉ)", COLORS["amber"]),
    ]:
        sub = q8_plot_df.loc[q8_plot_df["workingday"] == w_val]
        if not sub.empty:
            fig_q8_compare.add_trace(
                go.Scatter(
                    x=sub["hour"],
                    y=sub["avg_total"],
                    mode="lines+markers",
                    name=label,
                    line={"color": color, "width": 3},
                    marker={"size": 7},
                    hovertemplate=f"<b>{label}</b><br>Giờ %{{x}}:00<br>Trung bình: %{{y:.2f}} lượt/giờ<extra></extra>",
                )
            )

    apply_standard_chart_layout(
        fig_q8_compare,
        title="So sánh Nhu cầu Trung bình theo Giờ: Working Day vs Non-working Day",
        xaxis_title="Giờ trong ngày (0h – 23h)",
        yaxis_title="Nhu cầu trung bình (avg_total — lượt/giờ)",
        height=410,
    )
    fig_q8_compare.update_xaxes(dtick=1)
    render_plotly_chart(fig_q8_compare, key="chart_q8_workingday_hourly")

st.markdown(
    """
    <div class="info-panel">
        <strong>📌 Nhận xét thực nghiệm theo khung giờ (đối chiếu <code>q1_hourly_demand.csv</code> & <code>q8_user_behavior_casual_vs_registered.csv</code>):</strong>
        <ul style="margin: 0.35rem 0 0 1.1rem; color: #334155; font-size: 0.9rem;">
            <li><strong>Trên toàn tập Train (<code>q1</code>):</strong> Đỉnh cao nhất rơi vào <strong>17:00</strong> (trung bình <strong>468.77 lượt/giờ</strong>, tổng <code>213,757</code> lượt), tiếp theo là <strong>18:00</strong> (<strong>430.86 lượt/giờ</strong>, tổng <code>196,472</code> lượt) và đỉnh buổi sáng <strong>08:00</strong> (<strong>362.77 lượt/giờ</strong>, tổng <code>165,060</code> lượt). Mức thấp nhất vào lúc <strong>04:00 sáng</strong> (chỉ <strong>6.41 lượt/giờ</strong>, tổng <code>2,832</code> lượt).</li>
            <li><strong>Sự đối lập giữa Working Day và Non-working Day (<code>q8</code>):</strong> Ngày làm việc có <strong>2 đỉnh nhọn (bimodal)</strong> vào giờ đi làm <strong>08:00 (479.95 lượt/giờ)</strong> và tan tầm <strong>17:00 (529.21 lượt/giờ)</strong> – <strong>18:00 (495.49 lượt/giờ)</strong>; ngược lại, ngày nghỉ có dạng <strong>1 đỉnh thoải (unimodal)</strong> từ 12:00 đến 16:00 (cao nhất lúc <strong>13:00 đạt 387.82 lượt/giờ</strong>).</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander("📋 Xem Bảng Top 10 Khung Giờ Cao Điểm Nhất (`outputs/analytics/q7_top10_peak_hours.csv`)", expanded=False):
    q7_display = q7_df.copy()
    if day_type_filter == "Working Day (1)":
        q7_display = q7_display.loc[q7_display["day_category"] == "Working Day"]
    elif day_type_filter == "Non-Working Day (0)":
        q7_display = q7_display.loc[q7_display["day_category"] == "Non-Working Day"]
    st.dataframe(q7_display, width="stretch", hide_index=True)

st.markdown("---")

# =============================================================================
# C. WEEKLY AND SEASONAL ANALYSIS
# =============================================================================
st.subheader("3. Phân tích theo Ngày trong Tuần và theo Mùa (Weekly & Seasonal Analysis)")
st.caption(
    "📂 **Nguồn dữ liệu:** `outputs/analytics/q2_dayofweek_demand.csv` (đã sắp xếp đúng thứ tự `Monday → Sunday`), "
    "`outputs/analytics/q3_seasonal_demand.csv`."
)

wk_col1, wk_col2, wk_col3 = st.columns(3)

with wk_col1:
    fig_q2_avg = go.Figure(
        go.Bar(
            x=q2_df["day_name"],
            y=q2_df["avg_total_demand"],
            text=q2_df["avg_total_demand"].apply(lambda v: f"{v:.2f}"),
            textposition="outside",
            marker_color=[
                COLORS["emerald"] if d == "Friday" else (COLORS["amber"] if w == 1 else COLORS["secondary"])
                for d, w in zip(q2_df["day_name"], q2_df["is_weekend"])
            ],
            hovertemplate="<b>%{x}</b><br>Trung bình: %{y:.2f} lượt/giờ<br>Số bản ghi: %{customdata} giờ<extra></extra>",
            customdata=q2_df["total_records"],
        )
    )
    apply_standard_chart_layout(
        fig_q2_avg,
        title="1. Nhu cầu Trung bình theo Thứ (Monday–Sunday)",
        xaxis_title="Ngày trong tuần",
        yaxis_title="Trung bình (avg_total_demand — lượt/giờ)",
        height=390,
        show_legend=False,
    )
    fig_q2_avg.update_yaxes(range=[0, 235])
    render_plotly_chart(fig_q2_avg, key="chart_q2_avg")

with wk_col2:
    fig_q2_total = go.Figure(
        go.Bar(
            x=q2_df["day_name"],
            y=q2_df["total_rentals"],
            text=q2_df["total_rentals"].apply(lambda v: f"{v:,}"),
            textposition="outside",
            marker_color=[
                COLORS["emerald"] if d == "Saturday" else (COLORS["amber"] if w == 1 else COLORS["primary"])
                for d, w in zip(q2_df["day_name"], q2_df["is_weekend"])
            ],
            hovertemplate="<b>%{x}</b><br>Tổng lượt thuê: %{y:,} lượt<br>Số giờ quan sát: %{customdata} giờ<extra></extra>",
            customdata=q2_df["total_records"],
        )
    )
    apply_standard_chart_layout(
        fig_q2_total,
        title="2. Tổng Lượt Thuê theo Thứ (Total Rentals)",
        xaxis_title="Ngày trong tuần",
        yaxis_title="Tổng lượt thuê (total_rentals — lượt)",
        height=390,
        show_legend=False,
    )
    fig_q2_total.update_yaxes(range=[0, 365000])
    render_plotly_chart(fig_q2_total, key="chart_q2_total")

with wk_col3:
    season_colors = {
        "Spring": "#0D9488",
        "Summer": "#2563EB",
        "Fall": "#16A34A",
        "Winter": "#64748B",
    }
    fig_q3 = go.Figure(
        go.Bar(
            x=q3_filtered["season_name"],
            y=q3_filtered["avg_total_demand"],
            text=q3_filtered["avg_total_demand"].apply(lambda v: f"{v:.2f}"),
            textposition="outside",
            marker_color=[season_colors.get(s, COLORS["secondary"]) for s in q3_filtered["season_name"]],
            customdata=q3_filtered[["total_rentals", "avg_temp_c", "avg_humidity_pct"]],
            hovertemplate=(
                "<b>Mùa %{x}</b><br>"
                "Trung bình: %{y:.2f} lượt/giờ<br>"
                "Tổng lượt thuê: %{customdata[0]:,} lượt<br>"
                "Nhiệt độ TB: %{customdata[1]:.2f}°C | Độ ẩm TB: %{customdata[2]:.2f}%<extra></extra>"
            ),
        )
    )
    apply_standard_chart_layout(
        fig_q3,
        title="3. Nhu cầu Thuê xe Trung bình theo Mùa",
        xaxis_title="Mùa trong năm (Season)",
        yaxis_title="Trung bình (avg_total_demand — lượt/giờ)",
        height=390,
        show_legend=False,
    )
    fig_q3.update_yaxes(range=[0, 275])
    render_plotly_chart(fig_q3, key="chart_q3_season")

st.markdown(
    """
    <div class="info-panel">
        <strong>📌 Nhận xét thực nghiệm theo Thứ trong tuần & Mùa (<code>q2_dayofweek_demand.csv</code> & <code>q3_seasonal_demand.csv</code>):</strong>
        <ul style="margin: 0.35rem 0 0 1.1rem; color: #334155; font-size: 0.9rem;">
            <li><strong>Theo ngày trong tuần:</strong> <strong>Friday</strong> đạt nhu cầu trung bình theo giờ cao nhất (<strong>197.84 lượt/giờ</strong> trên <code>1,529</code> giờ), trong khi <strong>Saturday</strong> đạt tổng lượt thuê tích lũy cao nhất (<strong>311,518 lượt</strong> trên <code>1,584</code> giờ, trung bình <code>196.67 lượt/giờ</code>). <strong>Sunday</strong> thấp nhất (<code>180.84 lượt/giờ</code>, tổng <code>285,546</code> lượt).</li>
            <li><strong>Theo mùa:</strong> <strong>Fall (Mùa Thu)</strong> dẫn đầu với <strong>234.42 lượt/giờ</strong> (tổng <code>640,662</code> lượt, nhiệt độ TB <code>28.79°C</code>), theo sau là <strong>Summer (215.25 lượt/giờ)</strong> và <strong>Winter (198.99 lượt/giờ)</strong>. <strong>Spring (Mùa Xuân)</strong> có nhu cầu thấp nhất (<strong>116.34 lượt/giờ</strong>, nhiệt độ TB <code>12.53°C</code>).</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

# =============================================================================
# D. WEATHER IMPACT ANALYSIS & E. TEMPERATURE AND HUMIDITY ANALYSIS
# =============================================================================
st.subheader("4. Ảnh hưởng của Thời tiết, Nhiệt độ và Độ ẩm (Weather, Temperature & Humidity)")
st.caption(
    "📂 **Nguồn dữ liệu:** `outputs/analytics/q4_weather_demand.csv`, `outputs/eda/b4_demand_by_weather.csv`, "
    "`outputs/eda/b4_demand_by_temp_humidity.csv` và đối chiếu nhãn từ `docs/DATA_DICTIONARY.md`."
)

wt_col1, wt_col2 = st.columns(2)

with wt_col1:
    fig_q4 = go.Figure(
        go.Bar(
            x=q4_filtered["weather_condition"],
            y=q4_filtered["avg_total_demand"],
            text=[
                f"{val:.2f} (n={rec:,})"
                for val, rec in zip(q4_filtered["avg_total_demand"], q4_filtered["total_records"])
            ],
            textposition="outside",
            marker_color=[
                COLORS["emerald"] if w == 1 else (COLORS["secondary"] if w == 2 else (COLORS["amber"] if w == 3 else COLORS["crimson"]))
                for w in q4_filtered["weather"]
            ],
            customdata=q4_filtered[["total_records", "total_rentals", "avg_temp_c", "avg_humidity_pct"]],
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Trung bình: %{y:.2f} lượt/giờ<br>"
                "Số giờ quan sát (n): %{customdata[0]:,} bản ghi<br>"
                "Tổng lượt thuê: %{customdata[1]:,} lượt<br>"
                "Nhiệt độ TB: %{customdata[2]:.2f}°C | Độ ẩm TB: %{customdata[3]:.2f}%<extra></extra>"
            ),
        )
    )
    apply_standard_chart_layout(
        fig_q4,
        title="Nhu cầu Thuê xe Trung bình theo Điều kiện Thời tiết (q4)",
        xaxis_title="Nhóm thời tiết (Theo Data Dictionary: Mức 1 → Mức 4)",
        yaxis_title="Trung bình (avg_total_demand — lượt/giờ)",
        height=420,
        show_legend=False,
    )
    fig_q4.update_yaxes(range=[0, 245])
    render_plotly_chart(fig_q4, key="chart_q4_weather")

with wt_col2:
    # Kiểm tra và dựng Heatmap từ b4_demand_by_temp_humidity.csv (4 nhóm temp_range x 3 nhóm humidity_level)
    temp_order = [
        "1. Cold (<10C)",
        "2. Mild (10-20C)",
        "3. Warm (20-30C)",
        "4. Hot (>=30C)",
    ]
    hum_order = [
        "Low (<40%)",
        "Moderate (40-75%)",
        "High (>75%)",
    ]

    pivot_avg = (
        b4_temp_hum_df.pivot(index="temp_range", columns="humidity_level", values="avg_count")
        .reindex(index=temp_order, columns=hum_order)
    )
    pivot_rec = (
        b4_temp_hum_df.pivot(index="temp_range", columns="humidity_level", values="records")
        .reindex(index=temp_order, columns=hum_order)
    )

    text_matrix = [
        [
            f"{pivot_avg.loc[t, h]:.2f}<br>(n={int(pivot_rec.loc[t, h])})"
            for h in hum_order
        ]
        for t in temp_order
    ]

    fig_heatmap = go.Figure(
        data=go.Heatmap(
            z=pivot_avg.values,
            x=hum_order,
            y=temp_order,
            text=text_matrix,
            texttemplate="%{text}",
            textfont={"size": 12},
            colorscale="Blues",
            colorbar={"title": "Lượt/giờ"},
            hovertemplate=(
                "Nhiệt độ: <b>%{y}</b><br>"
                "Độ ẩm: <b>%{x}</b><br>"
                "Nhu cầu trung bình: <b>%{z:.2f} lượt/giờ</b><extra></extra>"
            ),
        )
    )
    apply_standard_chart_layout(
        fig_heatmap,
        title="Heatmap Nhu cầu Trung bình theo Nhiệt độ & Độ ẩm (b4)",
        xaxis_title="Mức độ ẩm (humidity_level)",
        yaxis_title="Khoảng nhiệt độ (temp_range)",
        height=420,
        show_legend=False,
    )
    render_plotly_chart(fig_heatmap, key="chart_b4_heatmap")

st.markdown(
    """
    <div class="info-panel">
        <strong>📌 Nhận xét Thời tiết, Nhiệt độ & Độ ẩm (<code>q4_weather_demand.csv</code> & <code>b4_demand_by_temp_humidity.csv</code>):</strong>
        <ul style="margin: 0.35rem 0 0 1.1rem; color: #334155; font-size: 0.9rem;">
            <li><strong>Ảnh hưởng của thời tiết:</strong> Thời tiết quang đãng <strong>Clear / Partly Cloudy (Mức 1)</strong> có nhu cầu cao nhất (<strong>205.24 lượt/giờ</strong> trên <code>7,192</code> giờ), giảm xuống <strong>178.96 lượt/giờ</strong> khi có sương mù <strong>Mist / Cloudy (Mức 2)</strong> và giảm mạnh còn <strong>118.85 lượt/giờ</strong> khi có mưa/tuyết nhẹ <strong>Light Snow / Light Rain (Mức 3)</strong>. <em>Lưu ý: Nhóm <strong>Mức 4 (Heavy Rain / Ice Pellets)</strong> chỉ có đúng <strong>1 bản ghi duy nhất</strong> (<code>164.0 lượt</code> vào giờ tan tầm 18:00 ngày 09/01/2012) nên không mang tính đại diện thống kê.</em></li>
            <li><strong>Tương tác Nhiệt độ × Độ ẩm:</strong> Vùng thời tiết lý tưởng nhất là <strong>Warm (20–30°C) kết hợp Độ ẩm thấp Low (&lt;40%)</strong> đạt tới <strong>390.03 lượt/giờ</strong> (<code>518</code> giờ), tiếp theo là <strong>Hot (&ge;30°C) + Low (&lt;40%)</strong> đạt <strong>369.48 lượt/giờ</strong>. Ngược lại, trời lạnh <strong>Cold (&lt;10°C) + Độ ẩm cao High (&gt;75%)</strong> có nhu cầu thấp nhất, chỉ <strong>65.61 lượt/giờ</strong>.</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

# =============================================================================
# F. USER SEGMENTATION (CASUAL VS REGISTERED)
# =============================================================================
st.subheader("5. Phân khúc Hành vi Người dùng: Casual vs Registered (User Segmentation)")
st.caption(
    "📂 **Nguồn dữ liệu:** `outputs/analytics/q5_workingday_comparison.csv`, "
    "`outputs/analytics/q8_user_behavior_casual_vs_registered.csv`."
)

seg_col1, seg_col2 = st.columns(2)

with seg_col1:
    fig_q5 = go.Figure()
    fig_q5.add_trace(
        go.Bar(
            name="Registered Share (%)",
            x=q5_df["day_category"],
            y=q5_df["registered_share_pct"],
            text=[
                f"{pct:.2f}% ({avg_v:.2f}/h)"
                for pct, avg_v in zip(q5_df["registered_share_pct"], q5_df["avg_registered"])
            ],
            textposition="inside",
            marker_color=COLORS["secondary"],
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Tỷ trọng Registered: %{y:.2f}%<br>"
                "Trung bình Registered: %{customdata:.2f} lượt/giờ<extra></extra>"
            ),
            customdata=q5_df["avg_registered"],
        )
    )
    fig_q5.add_trace(
        go.Bar(
            name="Casual Share (%)",
            x=q5_df["day_category"],
            y=q5_df["casual_share_pct"],
            text=[
                f"{pct:.2f}% ({avg_v:.2f}/h)"
                for pct, avg_v in zip(q5_df["casual_share_pct"], q5_df["avg_casual"])
            ],
            textposition="inside",
            marker_color=COLORS["amber"],
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Tỷ trọng Casual: %{y:.2f}%<br>"
                "Trung bình Casual: %{customdata:.2f} lượt/giờ<extra></extra>"
            ),
            customdata=q5_df["avg_casual"],
        )
    )
    fig_q5.update_layout(barmode="stack")
    apply_standard_chart_layout(
        fig_q5,
        title="Tỷ trọng Lượt thuê Casual vs Registered theo Loại Ngày (q5)",
        xaxis_title="Loại ngày (Working Day vs Non-Working Day)",
        yaxis_title="Tỷ trọng số lượt thuê (%)",
        height=410,
    )
    fig_q5.update_yaxes(range=[0, 108])
    render_plotly_chart(fig_q5, key="chart_q5_share")

with seg_col2:
    fig_q8_seg = go.Figure()
    for w_val, day_tag, dash_style in [
        (1, "Working Day", "solid"),
        (0, "Non-working Day", "dot"),
    ]:
        sub = q8_filtered.loc[q8_filtered["workingday"] == w_val]
        if not sub.empty:
            fig_q8_seg.add_trace(
                go.Scatter(
                    x=sub["hour"],
                    y=sub["avg_registered"],
                    mode="lines+markers",
                    name=f"Registered — {day_tag}",
                    line={"color": COLORS["secondary"], "width": 2.5, "dash": dash_style},
                    marker={"size": 6},
                    hovertemplate=f"<b>Registered ({day_tag})</b><br>Giờ %{{x}}:00: %{{y:.2f}} lượt/giờ<extra></extra>",
                )
            )
            fig_q8_seg.add_trace(
                go.Scatter(
                    x=sub["hour"],
                    y=sub["avg_casual"],
                    mode="lines+markers",
                    name=f"Casual — {day_tag}",
                    line={"color": COLORS["amber"], "width": 2.5, "dash": dash_style},
                    marker={"size": 6},
                    hovertemplate=f"<b>Casual ({day_tag})</b><br>Giờ %{{x}}:00: %{{y:.2f}} lượt/giờ<extra></extra>",
                )
            )

    apply_standard_chart_layout(
        fig_q8_seg,
        title="Nhu cầu theo Giờ của Nhóm Registered và Casual (q8)",
        xaxis_title="Giờ trong ngày (0h – 23h)",
        yaxis_title="Số lượt thuê trung bình (lượt/giờ)",
        height=410,
    )
    fig_q8_seg.update_xaxes(dtick=1)
    render_plotly_chart(fig_q8_seg, key="chart_q8_user_seg")

st.markdown(
    """
    <div class="env-notice">
        <strong>📌 Chú thích phương pháp & Kết quả phân khúc người dùng:</strong>
        Các tỷ lệ phần trăm trong <code>q5_workingday_comparison.csv</code> là <strong>tỷ trọng số lượt thuê xe (rental volume share)</strong>,
        <em>không phải tỷ lệ số lượng người dùng duy nhất (unique users)</em>:
        <br>• <strong>Working Day (Ngày làm việc):</strong> Nhóm <strong>Registered chiếm 86.99%</strong> (<code>167.90 lượt/giờ</code>), nhóm <strong>Casual chiếm 13.01%</strong> (<code>25.11 lượt/giờ</code>).
        <br>• <strong>Non-Working Day (Cuối tuần/Nghỉ lễ):</strong> Tỷ trọng nhóm <strong>Casual tăng vọt lên 31.46%</strong> (<code>59.31 lượt/giờ</code> — tăng gấp 2.36 lần ngày thường), trong khi nhóm <strong>Registered chiếm 68.54%</strong> (<code>129.20 lượt/giờ</code>).
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

# =============================================================================
# G. YEARLY GROWTH ANALYSIS
# =============================================================================
st.subheader("6. Phân tích Tăng trưởng theo Năm & Xu hướng 24 Tháng (Yearly Growth Analysis)")
st.caption(
    "📂 **Nguồn dữ liệu:** `outputs/eda/b3_demand_by_year.csv`, `outputs/analytics/q6_monthly_yearly_trend.csv`."
)

yr_col1, yr_col2 = st.columns([1, 2])

with yr_col1:
    y2011_avg = float(b3_year_df.loc[b3_year_df["year"] == 2011, "avg_count"].iloc[0])
    y2012_avg = float(b3_year_df.loc[b3_year_df["year"] == 2012, "avg_count"].iloc[0])
    y2011_tot = int(b3_year_df.loc[b3_year_df["year"] == 2011, "total_count"].iloc[0])
    y2012_tot = int(b3_year_df.loc[b3_year_df["year"] == 2012, "total_count"].iloc[0])
    growth_pct = round(((y2012_avg - y2011_avg) / y2011_avg) * 100.0, 2)

    fig_year = go.Figure()
    fig_year.add_trace(
        go.Bar(
            x=b3_year_df["year"].astype(str),
            y=b3_year_df["avg_count"],
            text=[
                f"{avg_v:.2f} lượt/giờ" if yr == 2011 else f"{avg_v:.2f} lượt/giờ (+{growth_pct:.2f}%)"
                for yr, avg_v in zip(b3_year_df["year"], b3_year_df["avg_count"])
            ],
            textposition="outside",
            marker_color=[COLORS["secondary"], COLORS["emerald"]],
            customdata=b3_year_df[["total_count", "records", "avg_casual", "avg_registered"]],
            hovertemplate=(
                "<b>Năm %{x}</b><br>"
                "Trung bình: %{y:.2f} lượt/giờ<br>"
                "Tổng lượt thuê: %{customdata[0]:,} lượt (%{customdata[1]:,} giờ)<br>"
                "Registered TB: %{customdata[3]:.2f} | Casual TB: %{customdata[2]:.2f}<extra></extra>"
            ),
        )
    )
    apply_standard_chart_layout(
        fig_year,
        title=f"Tăng trưởng Nhu cầu Trung bình Năm 2011 vs 2012 (+{growth_pct:.2f}%)",
        xaxis_title="Năm quan sát (Year)",
        yaxis_title="Trung bình (avg_count — lượt/giờ)",
        height=400,
        show_legend=False,
    )
    fig_year.update_yaxes(range=[0, 280])
    render_plotly_chart(fig_year, key="chart_b3_yearly")

with yr_col2:
    fig_q6 = go.Figure()
    fig_q6.add_trace(
        go.Bar(
            x=q6_df["year_month"],
            y=q6_df["avg_total_demand"],
            name="Nhu cầu TB (avg_total_demand)",
            marker_color=[
                COLORS["secondary"] if yr == 2011 else COLORS["emerald"]
                for yr in q6_df["year"]
            ],
            customdata=q6_df[["total_rentals", "avg_temp_c", "avg_registered", "avg_casual"]],
            hovertemplate=(
                "<b>Tháng %{x}</b><br>"
                "Nhu cầu TB: %{y:.2f} lượt/giờ<br>"
                "Tổng lượt thuê: %{customdata[0]:,} lượt<br>"
                "Nhiệt độ TB: %{customdata[1]:.2f}°C<br>"
                "Registered TB: %{customdata[2]:.2f} | Casual TB: %{customdata[3]:.2f}<extra></extra>"
            ),
        )
    )
    fig_q6.add_trace(
        go.Scatter(
            x=q6_df["year_month"],
            y=q6_df["avg_temp_c"] * 8.0,
            mode="lines+markers",
            name="Xu hướng Nhiệt độ (avg_temp_c × 8)",
            line={"color": COLORS["amber"], "width": 2.5},
            customdata=q6_df["avg_temp_c"],
            hovertemplate="Tháng %{x}<br>Nhiệt độ TB: %{customdata:.2f}°C<extra></extra>",
        )
    )
    apply_standard_chart_layout(
        fig_q6,
        title="Xu hướng Nhu cầu Thuê xe Trung bình qua 24 Tháng (2011-01 → 2012-12) (q6)",
        xaxis_title="Tháng - Năm (year_month)",
        yaxis_title="Nhu cầu trung bình (lượt/giờ)",
        height=400,
    )
    render_plotly_chart(fig_q6, key="chart_q6_monthly")

st.markdown(
    f"""
    <div class="info-panel">
        <strong>📌 Chú thích & Nhận xét Tăng trưởng theo Năm (<code>b3_demand_by_year.csv</code> & <code>q6_monthly_yearly_trend.csv</code>):</strong>
        <ul style="margin: 0.35rem 0 0 1.1rem; color: #334155; font-size: 0.9rem;">
            <li><strong>So sánh 2011 vs 2012:</strong> Nhu cầu trung bình theo giờ tăng từ <strong>{y2011_avg:.2f} lượt/giờ</strong> trong năm 2011 (tổng <code>{y2011_tot:,}</code> lượt trên <code>5,422</code> giờ) lên <strong>{y2012_avg:.2f} lượt/giờ</strong> trong năm 2012 (tổng <code>{y2012_tot:,}</code> lượt trên <code>5,464</code> giờ), tương ứng mức tăng trưởng <strong>+{growth_pct:.2f}%</strong>.</li>
            <li><strong>Lưu ý phương pháp luận:</strong> Đây là so sánh trung bình trên dữ liệu quan sát của hai năm trong tập Train (gồm các ngày từ mùng 1 đến 19 hàng tháng). Sự dịch chuyển mặt bằng nhu cầu mạnh mẽ từ năm 2011 sang 2012 chính là lý do dự án bắt buộc sử dụng đặc trưng <code>year</code> và phương pháp chia dữ liệu theo thời gian (Time-based Split) ở Milestone 5.</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander("📊 Xem thêm: Phân bố Phân khúc Nhu cầu (`b2_target_count_distribution.csv`) & Thống kê Mô tả (`b1_descriptive_statistics.csv`)", expanded=False):
    ex_col1, ex_col2 = st.columns([1, 1.4])
    with ex_col1:
        st.markdown("**Phân bố 5 mức nhu cầu (`b2_target_count_distribution.csv`):**")
        st.dataframe(b2_dist_df, width="stretch", hide_index=True)
    with ex_col2:
        st.markdown("**Thống kê mô tả 11 biến số (`b1_descriptive_statistics.csv`):**")
        st.dataframe(b1_desc_df, width="stretch", hide_index=True)
