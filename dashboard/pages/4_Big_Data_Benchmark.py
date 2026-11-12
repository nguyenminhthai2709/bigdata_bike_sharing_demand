"""
Trang 4: Big Data Benchmark (1x, 10x, 50x, 100x Scaling)
Trực quan hóa kết quả đánh giá khả năng mở rộng dữ liệu (lên tới 1,088,600 dòng),
so sánh lưu trữ CSV vs Parquet Snappy, hiệu năng 5 phép toán xử lý phân tán của Apache Spark,
tối ưu hóa số lượng Partition (1, 2, 4, 8) và mức sử dụng tài nguyên hệ thống từ outputs/benchmark/.
"""

from pathlib import Path
import sys
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parents[1]
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from config import CLUSTER_CONFIG_SPEC, COLORS
from data_loader import ArtifactLoadError, get_benchmark_artifacts
from components.charts import apply_standard_chart_layout, render_plotly_chart
from components.kpi_cards import render_kpi_row, render_page_header

# =============================================================================
# HEADER & NẠP ARTIFACTS MILESTONE 6
# =============================================================================
render_page_header(
    badge_text="Milestone 6 — Big Data Scaling & Distributed Performance Benchmark",
    title="Big Data Benchmark — Scaling, Storage & Partition Optimization",
    subtitle=(
        "Đánh giá thực nghiệm hiệu năng lưu trữ HDFS (CSV vs Parquet Snappy), thời gian thực thi và thông lượng "
        "của 5 phép toán Spark ở các quy mô 1x (10,886 dòng) → 100x (1,088,600 dòng) và tối ưu hóa Partition (1, 2, 4, 8)."
    ),
)

try:
    bm_bundle = get_benchmark_artifacts()
except ArtifactLoadError as err:
    st.error(f"Lỗi nạp dữ liệu nguồn cho trang Big Data Benchmark: {err}")
    st.stop()

scaling_df: pd.DataFrame = bm_bundle["scaling"]
storage_df: pd.DataFrame = bm_bundle["storage"]
processing_df: pd.DataFrame = bm_bundle["processing"]
partition_df: pd.DataFrame = bm_bundle["partition"]
summary_json: dict = bm_bundle["summary_json"]
kpis: dict = bm_bundle["kpis"]
env_info: dict = summary_json.get("environment", {})

# =============================================================================
# A. BENCHMARK OVERVIEW (KPI CARDS)
# =============================================================================
st.subheader("1. Tổng quan Kết quả Thực nghiệm Big Data (Benchmark Overview)")

render_kpi_row(
    [
        {
            "title": "Maximum Dataset Size",
            "value": f"{kpis['max_rows']:,}",
            "unit": "rows",
            "description": "Quy mô tập dữ liệu lớn nhất được kiểm thử trên HDFS & Spark (20 cột đặc trưng).",
            "accent": "blue",
        },
        {
            "title": "Maximum Scale",
            "value": kpis["max_scale_label"],
            "unit": "(1x → 100x)",
            "description": "Nhân bản có kiểm soát từ 10,886 dòng gốc lên gấp 100 lần (1,088,600 dòng).",
            "accent": "emerald",
        },
        {
            "title": "CSV Size at 100x",
            "value": f"{kpis['csv_mb_100x']:.2f}",
            "unit": "MB",
            "description": f"Dung lượng lưu trữ CSV không nén ở quy mô 100x (chính xác: {kpis['csv_mb_100x']:.4f} MB).",
            "accent": "amber",
        },
        {
            "title": "Parquet Size at 100x",
            "value": f"{kpis['pq_mb_100x']:.2f}",
            "unit": "MB",
            "description": f"Dung lượng lưu trữ Parquet Snappy ở quy mô 100x (chính xác: {kpis['pq_mb_100x']:.4f} MB).",
            "accent": "teal",
        },
        {
            "title": "Storage Reduction",
            "value": f"{kpis['storage_reduction_pct']:.2f}%",
            "unit": f"({kpis['compression_ratio_100x']:.2f}x nhỏ hơn)",
            "description": "Tỷ lệ tiết kiệm dung lượng HDFS khi chuyển từ CSV sang Parquet Snappy ở 100x.",
            "accent": "blue",
        },
        {
            "title": "Read + Aggregation Speedup",
            "value": f"{kpis['read_agg_speedup_100x']:.2f}x",
            "unit": "nhanh hơn",
            "description": (
                f"Parquet ({kpis['pq_read_agg_100x']:.2f}s) so với CSV ({kpis['csv_read_agg_100x']:.2f}s) ở 100x "
                f"(Read + Count tăng tốc {kpis['read_cnt_speedup_100x']:.2f}x)."
            ),
            "accent": "emerald",
        },
    ]
)

st.markdown("<div style='height: 0.6rem;'></div>", unsafe_allow_html=True)

st.info(
    "📌 **Lưu ý về bản chất dữ liệu Benchmark:** Tập dữ liệu huấn luyện gốc (`1x`) có **10,886 dòng**. "
    "Các bộ dữ liệu mở rộng **`10x` (108,860 dòng)**, **`50x` (544,300 dòng)** và **`100x` (1,088,600 dòng)** "
    "là dữ liệu benchmark tổng hợp có kiểm soát (controlled synthetic scaling với nhiễu thời tiết cực nhỏ và dịch chuyển nhãn thời gian) "
    "nhằm đánh giá khả năng mở rộng lưu trữ HDFS và xử lý phân tán của Apache Spark, không phải dữ liệu quan sát thực tế mới thu thập."
)

st.markdown("---")

# =============================================================================
# B. DATA SCALING ANALYSIS (SCALING_RESULTS.CSV)
# =============================================================================
st.subheader("2. Phân tích Khả năng Mở rộng theo Quy mô Dữ liệu (Data Scaling Analysis)")

all_scales = scaling_df["scale_label"].tolist()

col_sc_f1, col_sc_f2 = st.columns([1.3, 1.3])
with col_sc_f1:
    selected_scaling_labels = st.multiselect(
        "Chọn quy mô dữ liệu hiển thị (Scale Filter):",
        options=all_scales,
        default=all_scales,
        key="bm_scaling_scale_filter",
    )
with col_sc_f2:
    scaling_time_metric_mode = st.radio(
        "Chỉ số thời gian xử lý trên biểu đồ Scaling:",
        options=[
            "Hiển thị cả Tổng thời gian 5 phép toán & Trung bình 1 phép toán",
            "Tổng thời gian 5 phép toán (total_5ops_processing_time_sec)",
            "Thời gian trung bình 1 phép toán (avg_op_processing_time_sec)",
        ],
        index=0,
        key="bm_scaling_time_mode",
    )

if not selected_scaling_labels:
    selected_scaling_labels = all_scales

sc_filtered = scaling_df.loc[scaling_df["scale_label"].isin(selected_scaling_labels)].copy()

col_sc1, col_sc2, col_sc3 = st.columns(3)

with col_sc1:
    fig_sc_time = go.Figure()
    if scaling_time_metric_mode in (
        "Hiển thị cả Tổng thời gian 5 phép toán & Trung bình 1 phép toán",
        "Tổng thời gian 5 phép toán (total_5ops_processing_time_sec)",
    ):
        fig_sc_time.add_trace(
            go.Scatter(
                x=sc_filtered["scale_label"],
                y=sc_filtered["total_5ops_processing_time_sec"],
                mode="lines+markers+text",
                name="Tổng 5 phép toán (s)",
                text=sc_filtered["total_5ops_processing_time_sec"].apply(lambda v: f"{v:.2f}s"),
                textposition="top center",
                line={"color": COLORS["primary"], "width": 3},
                marker={"size": 9},
                customdata=sc_filtered[["num_rows", "num_partitions"]],
                hovertemplate=(
                    "<b>Quy mô: %{x}</b> (%{customdata[0]:,} dòng | %{customdata[1]} partitions)<br>"
                    "Tổng thời gian 5 phép toán: %{y:.4f} giây<extra></extra>"
                ),
            )
        )
    if scaling_time_metric_mode in (
        "Hiển thị cả Tổng thời gian 5 phép toán & Trung bình 1 phép toán",
        "Thời gian trung bình 1 phép toán (avg_op_processing_time_sec)",
    ):
        fig_sc_time.add_trace(
            go.Scatter(
                x=sc_filtered["scale_label"],
                y=sc_filtered["avg_op_processing_time_sec"],
                mode="lines+markers+text",
                name="Trung bình 1 phép toán (s)",
                text=sc_filtered["avg_op_processing_time_sec"].apply(lambda v: f"{v:.2f}s"),
                textposition="bottom center",
                line={"color": COLORS["teal"], "width": 2.5, "dash": "dash"},
                marker={"size": 8},
                customdata=sc_filtered[["num_rows", "num_partitions"]],
                hovertemplate=(
                    "<b>Quy mô: %{x}</b> (%{customdata[0]:,} dòng | %{customdata[1]} partitions)<br>"
                    "Trung bình 1 phép toán: %{y:.4f} giây<extra></extra>"
                ),
            )
        )
    max_y_sc_time = float(sc_filtered["total_5ops_processing_time_sec"].max()) * 1.22
    fig_sc_time.update_layout(yaxis_range=[0, max_y_sc_time])
    apply_standard_chart_layout(
        fig_sc_time,
        title="Dataset Scale vs Processing Time (Giây)",
        xaxis_title="Quy mô dữ liệu (Scale Factor)",
        yaxis_title="Thời gian xử lý (Giây)",
        height=390,
    )
    render_plotly_chart(fig_sc_time, key="bm_chart_scale_time")

with col_sc2:
    fig_sc_tp = go.Figure()
    fig_sc_tp.add_trace(
        go.Bar(
            x=sc_filtered["scale_label"],
            y=sc_filtered["avg_processing_throughput_rows_per_sec"],
            name="Thông lượng TB (rows/sec)",
            text=sc_filtered["avg_processing_throughput_rows_per_sec"].apply(lambda v: f"{v:,.0f}"),
            textposition="outside",
            marker_color=COLORS["emerald"],
            customdata=sc_filtered[["num_rows", "avg_op_processing_time_sec", "total_5ops_processing_time_sec"]],
            hovertemplate=(
                "<b>Quy mô: %{x}</b> (%{customdata[0]:,} dòng)<br>"
                "Thông lượng TB 5 phép toán: %{y:,.2f} dòng/giây<br>"
                "Thời gian TB 1 phép toán: %{customdata[1]:.4f}s (Tổng 5 phép: %{customdata[2]:.4f}s)<extra></extra>"
            ),
        )
    )
    fig_sc_tp.update_layout(yaxis_range=[0, float(sc_filtered["avg_processing_throughput_rows_per_sec"].max()) * 1.22])
    apply_standard_chart_layout(
        fig_sc_tp,
        title="Dataset Scale vs Throughput (Dòng/giây)",
        xaxis_title="Quy mô dữ liệu (Scale Factor)",
        yaxis_title="Thông lượng (Rows / sec)",
        height=390,
        show_legend=False,
    )
    render_plotly_chart(fig_sc_tp, key="bm_chart_scale_tp")

with col_sc3:
    fig_sc_storage = go.Figure()
    fig_sc_storage.add_trace(
        go.Bar(
            name="CSV (Uncompressed)",
            x=sc_filtered["scale_label"],
            y=sc_filtered["dataset_csv_size_mb"],
            text=sc_filtered["dataset_csv_size_mb"].apply(lambda v: f"{v:.2f} MB"),
            textposition="outside",
            marker_color=COLORS["amber"],
            customdata=sc_filtered[["num_rows", "csv_to_parquet_size_ratio"]],
            hovertemplate=(
                "<b>Quy mô: %{x}</b> (%{customdata[0]:,} dòng)<br>"
                "Dung lượng CSV: %{y:.4f} MB<br>"
                "Tỷ lệ nén CSV/Parquet: %{customdata[1]:.2f}x<extra></extra>"
            ),
        )
    )
    fig_sc_storage.add_trace(
        go.Bar(
            name="Parquet (Snappy)",
            x=sc_filtered["scale_label"],
            y=sc_filtered["dataset_parquet_size_mb"],
            text=sc_filtered["dataset_parquet_size_mb"].apply(lambda v: f"{v:.2f} MB"),
            textposition="outside",
            marker_color=COLORS["primary"],
            customdata=sc_filtered[["num_rows", "csv_to_parquet_size_ratio"]],
            hovertemplate=(
                "<b>Quy mô: %{x}</b> (%{customdata[0]:,} dòng)<br>"
                "Dung lượng Parquet Snappy: %{y:.4f} MB<br>"
                "Tỷ lệ nén CSV/Parquet: %{customdata[1]:.2f}x<extra></extra>"
            ),
        )
    )
    fig_sc_storage.update_layout(
        barmode="group",
        yaxis_range=[0, float(sc_filtered["dataset_csv_size_mb"].max()) * 1.22],
    )
    apply_standard_chart_layout(
        fig_sc_storage,
        title="Dataset Scale vs Storage Size (MB)",
        xaxis_title="Quy mô dữ liệu (Scale Factor)",
        yaxis_title="Dung lượng lưu trữ HDFS (MB)",
        height=390,
    )
    render_plotly_chart(fig_sc_storage, key="bm_chart_scale_storage")

st.markdown(
    """
    <div class="insight-box">
        <strong>🔍 Phân tích Thực nghiệm từ <code>scaling_results.csv</code> (Tránh suy diễn tuyến tính):</strong><br/>
        • <strong>Sự khác biệt giữa Thời gian xử lý và Thông lượng (Throughput):</strong> Với mỗi phép toán $i$, thông lượng được tính theo công thức $\\text{Throughput}_i = \\frac{\\text{num\\_rows}}{\\text{avg\\_processing\\_time\\_sec}_i}$ (dòng/giây). Trong <code>scaling_results.csv</code>, <code>total_5ops_processing_time_sec</code> là tổng thời gian trung bình của cả 5 phép toán (<code>avg_op_processing_time_sec = total / 5</code>), còn <code>avg_processing_throughput_rows_per_sec</code> là trung bình cộng thông lượng của 5 phép toán tại quy mô đó.<br/>
        • <strong>Hiện tượng khởi động JVM/Catalyst ở 1x:</strong> Quy mô <code>1x</code> (10,886 dòng) mất tổng cộng <strong>16.3879s</strong> (trung bình <strong>3.2776s</strong>/phép toán, thông lượng <strong>5,355.24 rows/s</strong>) — chậm hơn cả <code>10x</code> (<strong>3.5003s</strong>) và <code>50x</code> (<strong>8.5353s</strong>) do chi phí khởi tạo Spark DAG, biên dịch mã bytecode Whole-Stage Codegen và cấp phát bộ nhớ ở lượt chạy đầu tiên.<br/>
        • <strong>Điểm bão hòa ở 50x và áp lực Shuffle/GC ở 100x:</strong> Thông lượng đạt đỉnh ở <code>50x</code> (<strong>747,565.49 rows/s</strong>, tổng thời gian <strong>8.5353s</strong>) khi dữ liệu đủ lớn để khấu hao chi phí lập lịch nhưng vẫn nằm gọn trong bộ nhớ Executor (1024 MiB). Khi tăng lên <code>100x</code> (1,088,600 dòng), tổng thời gian tăng lên <strong>28.7421s</strong> và thông lượng giảm xuống <strong>440,647.12 rows/s</strong> do chi phí trộn dữ liệu (shuffle) của phép <code>Sort</code> (11.7692s) và <code>Join</code> (8.9701s) trên Worker 2 CPU cores.
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander("📋 Xem bảng dữ liệu gốc `outputs/benchmark/scaling_results.csv` (4 quy mô)", expanded=False):
    st.dataframe(sc_filtered, width="stretch", hide_index=True)

st.markdown("---")

# =============================================================================
# C. CSV VS PARQUET STORAGE BENCHMARK (STORAGE_COMPARISON.CSV)
# =============================================================================
st.subheader("3. So sánh Lưu trữ & Truy vấn: CSV vs Parquet Snappy (Storage Benchmark)")

st.info(
    "⚙️ **Chế độ Cache thực nghiệm (`cache_regime`):** **`Spark-Uncached / Warm OS Page Cache`** (lặp lại **`num_runs = 3`** lần cho mỗi cấu hình). "
    "Trước mỗi lần đọc, lệnh `spark.catalog.clearCache()` được gọi để xóa hoàn toàn bộ nhớ đệm trong Spark, "
    "nhưng tệp khối dữ liệu trên DataNode vẫn hưởng lợi từ OS Page Cache của nhân Linux/WSL2 (không gọi đây là Cold Disk Cache Benchmark)."
)

col_st_f1, col_st_f2 = st.columns([1.2, 1.4])
with col_st_f1:
    selected_storage_scale = st.selectbox(
        "Chọn quy mô để xem KPI đối chiếu chi tiết CSV vs Parquet:",
        options=all_scales,
        index=3,  # Mặc định chọn 100x
        key="bm_storage_scale_select",
    )
with col_st_f2:
    selected_storage_op_focus = st.selectbox(
        "Chọn thao tác I/O trọng tâm để phân tích:",
        options=[
            "Tất cả thao tác (Write, Read + Count, Read + Aggregation)",
            "Write Time (avg_write_time_sec)",
            "Read + Count (avg_read_count_time_sec)",
            "Read + Aggregation (avg_read_agg_time_sec)",
        ],
        index=0,
        key="bm_storage_op_select",
    )

st_csv_sel = storage_df.loc[
    (storage_df["scale_label"] == selected_storage_scale) & (storage_df["format"] == "CSV")
].iloc[0]
st_pq_sel = storage_df.loc[
    (storage_df["scale_label"] == selected_storage_scale) & (storage_df["format"] == "PARQUET")
].iloc[0]

sel_csv_mb = float(st_csv_sel["storage_size_mb"])
sel_pq_mb = float(st_pq_sel["storage_size_mb"])
sel_comp_ratio = float(st_csv_sel["storage_size_bytes"]) / float(st_pq_sel["storage_size_bytes"])
sel_reduction_pct = (1.0 - float(st_pq_sel["storage_size_bytes"]) / float(st_csv_sel["storage_size_bytes"])) * 100.0
sel_write_ratio = float(st_csv_sel["avg_write_time_sec"]) / float(st_pq_sel["avg_write_time_sec"])
sel_read_cnt_speedup = float(st_csv_sel["avg_read_count_time_sec"]) / float(st_pq_sel["avg_read_count_time_sec"])
sel_read_agg_speedup = float(st_csv_sel["avg_read_agg_time_sec"]) / float(st_pq_sel["avg_read_agg_time_sec"])

render_kpi_row(
    [
        {
            "title": f"Storage Size ({selected_storage_scale})",
            "value": f"{sel_pq_mb:.2f} MB",
            "unit": f"vs {sel_csv_mb:.2f} MB (CSV)",
            "description": f"Parquet tiết kiệm {sel_reduction_pct:.2f}% dung lượng (Compression Ratio: {sel_comp_ratio:.2f}x).",
            "accent": "blue",
        },
        {
            "title": f"Write Time ({selected_storage_scale})",
            "value": f"{float(st_pq_sel['avg_write_time_sec']):.2f}s",
            "unit": f"vs {float(st_csv_sel['avg_write_time_sec']):.2f}s (CSV)",
            "description": "Trung bình 3 lần ghi (Parquet tốn thêm CPU để mã hóa cột & nén Snappy).",
            "accent": "amber",
        },
        {
            "title": f"Read + Count ({selected_storage_scale})",
            "value": f"{float(st_pq_sel['avg_read_count_time_sec']):.4f}s",
            "unit": f"({sel_read_cnt_speedup:.2f}x vs CSV {float(st_csv_sel['avg_read_count_time_sec']):.4f}s)",
            "description": "Parquet đọc trực tiếp thống kê số dòng từ footer metadata cực nhanh.",
            "accent": "teal",
        },
        {
            "title": f"Read + Aggregation ({selected_storage_scale})",
            "value": f"{float(st_pq_sel['avg_read_agg_time_sec']):.4f}s",
            "unit": f"({sel_read_agg_speedup:.2f}x vs CSV {float(st_csv_sel['avg_read_agg_time_sec']):.4f}s)",
            "description": "Parquet chỉ đọc các cột cần thiết (Column Pruning) thay vì quét toàn bộ văn bản CSV.",
            "accent": "emerald",
        },
    ]
)

st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

st_csv_all = storage_df.loc[storage_df["format"] == "CSV"].sort_values("scale_factor")
st_pq_all = storage_df.loc[storage_df["format"] == "PARQUET"].sort_values("scale_factor")

col_st1, col_st2 = st.columns(2)

with col_st1:
    fig_st_size = go.Figure()
    fig_st_size.add_trace(
        go.Bar(
            name="CSV (none)",
            x=st_csv_all["scale_label"],
            y=st_csv_all["storage_size_mb"],
            text=st_csv_all["storage_size_mb"].apply(lambda v: f"{v:.2f} MB"),
            textposition="outside",
            marker_color=COLORS["amber"],
            customdata=st_csv_all[["num_rows", "storage_size_bytes"]],
            hovertemplate="<b>CSV — Quy mô %{x}</b> (%{customdata[0]:,} dòng)<br>Dung lượng: %{y:.4f} MB (%{customdata[1]:,} bytes)<extra></extra>",
        )
    )
    fig_st_size.add_trace(
        go.Bar(
            name="PARQUET (snappy)",
            x=st_pq_all["scale_label"],
            y=st_pq_all["storage_size_mb"],
            text=st_pq_all["storage_size_mb"].apply(lambda v: f"{v:.2f} MB"),
            textposition="outside",
            marker_color=COLORS["primary"],
            customdata=st_pq_all[["num_rows", "storage_size_bytes"]],
            hovertemplate="<b>PARQUET — Quy mô %{x}</b> (%{customdata[0]:,} dòng)<br>Dung lượng: %{y:.4f} MB (%{customdata[1]:,} bytes)<extra></extra>",
        )
    )
    fig_st_size.update_layout(barmode="group", yaxis_range=[0, float(st_csv_all["storage_size_mb"].max()) * 1.22])
    apply_standard_chart_layout(
        fig_st_size,
        title="1. CSV vs Parquet Storage Size trên HDFS (MB)",
        xaxis_title="Quy mô dữ liệu (Scale Factor)",
        yaxis_title="Dung lượng lưu trữ (MB)",
        height=380,
    )
    render_plotly_chart(fig_st_size, key="bm_chart_st_size")

with col_st2:
    fig_st_write = go.Figure()
    fig_st_write.add_trace(
        go.Bar(
            name="CSV Write Time (avg 3 runs)",
            x=st_csv_all["scale_label"],
            y=st_csv_all["avg_write_time_sec"],
            text=st_csv_all["avg_write_time_sec"].apply(lambda v: f"{v:.2f}s"),
            textposition="outside",
            marker_color=COLORS["amber"],
            customdata=st_csv_all[["std_write_time_sec", "write_throughput_rows_per_sec"]],
            hovertemplate="<b>CSV Write — %{x}</b><br>Trung bình 3 lần: %{y:.4f}s (±%{customdata[0]:.4f}s)<br>Thông lượng ghi: %{customdata[1]:,.2f} rows/s<extra></extra>",
        )
    )
    fig_st_write.add_trace(
        go.Bar(
            name="Parquet Write Time (avg 3 runs)",
            x=st_pq_all["scale_label"],
            y=st_pq_all["avg_write_time_sec"],
            text=st_pq_all["avg_write_time_sec"].apply(lambda v: f"{v:.2f}s"),
            textposition="outside",
            marker_color=COLORS["primary"],
            customdata=st_pq_all[["std_write_time_sec", "write_throughput_rows_per_sec"]],
            hovertemplate="<b>Parquet Write — %{x}</b><br>Trung bình 3 lần: %{y:.4f}s (±%{customdata[0]:.4f}s)<br>Thông lượng ghi: %{customdata[1]:,.2f} rows/s<extra></extra>",
        )
    )
    fig_st_write.update_layout(barmode="group", yaxis_range=[0, float(st_pq_all["avg_write_time_sec"].max()) * 1.22])
    apply_standard_chart_layout(
        fig_st_write,
        title="2. CSV vs Parquet Write Time (Trung bình 3 lần chạy — Giây)",
        xaxis_title="Quy mô dữ liệu (Scale Factor)",
        yaxis_title="Thời gian ghi HDFS (Giây)",
        height=380,
    )
    render_plotly_chart(fig_st_write, key="bm_chart_st_write")

col_st3, col_st4 = st.columns(2)

with col_st3:
    fig_st_rc = go.Figure()
    fig_st_rc.add_trace(
        go.Bar(
            name="CSV Read + Count",
            x=st_csv_all["scale_label"],
            y=st_csv_all["avg_read_count_time_sec"],
            text=st_csv_all["avg_read_count_time_sec"].apply(lambda v: f"{v:.2f}s"),
            textposition="outside",
            marker_color=COLORS["amber"],
            customdata=st_csv_all[["std_read_count_time_sec", "read_count_throughput_rows_per_sec"]],
            hovertemplate="<b>CSV Read + Count — %{x}</b><br>Trung bình 3 lần: %{y:.4f}s (±%{customdata[0]:.4f}s)<br>Thông lượng: %{customdata[1]:,.2f} rows/s<extra></extra>",
        )
    )
    fig_st_rc.add_trace(
        go.Bar(
            name="Parquet Read + Count",
            x=st_pq_all["scale_label"],
            y=st_pq_all["avg_read_count_time_sec"],
            text=st_pq_all["avg_read_count_time_sec"].apply(lambda v: f"{v:.2f}s"),
            textposition="outside",
            marker_color=COLORS["teal"],
            customdata=st_pq_all[["std_read_count_time_sec", "read_count_throughput_rows_per_sec"]],
            hovertemplate="<b>Parquet Read + Count — %{x}</b><br>Trung bình 3 lần: %{y:.4f}s (±%{customdata[0]:.4f}s)<br>Thông lượng: %{customdata[1]:,.2f} rows/s<extra></extra>",
        )
    )
    fig_st_rc.update_layout(barmode="group", yaxis_range=[0, float(st_csv_all["avg_read_count_time_sec"].max()) * 1.22])
    apply_standard_chart_layout(
        fig_st_rc,
        title="3. CSV vs Parquet Read + Count Time (Trung bình 3 lần chạy — Giây)",
        xaxis_title="Quy mô dữ liệu (Scale Factor)",
        yaxis_title="Thời gian Read + Count (Giây)",
        height=380,
    )
    render_plotly_chart(fig_st_rc, key="bm_chart_st_read_count")

with col_st4:
    fig_st_ra = go.Figure()
    fig_st_ra.add_trace(
        go.Bar(
            name="CSV Read + Aggregation",
            x=st_csv_all["scale_label"],
            y=st_csv_all["avg_read_agg_time_sec"],
            text=st_csv_all["avg_read_agg_time_sec"].apply(lambda v: f"{v:.2f}s"),
            textposition="outside",
            marker_color=COLORS["amber"],
            customdata=st_csv_all[["std_read_agg_time_sec", "read_agg_throughput_rows_per_sec"]],
            hovertemplate="<b>CSV Read + Aggregation — %{x}</b><br>Trung bình 3 lần: %{y:.4f}s (±%{customdata[0]:.4f}s)<br>Thông lượng: %{customdata[1]:,.2f} rows/s<extra></extra>",
        )
    )
    fig_st_ra.add_trace(
        go.Bar(
            name="Parquet Read + Aggregation",
            x=st_pq_all["scale_label"],
            y=st_pq_all["avg_read_agg_time_sec"],
            text=st_pq_all["avg_read_agg_time_sec"].apply(lambda v: f"{v:.2f}s"),
            textposition="outside",
            marker_color=COLORS["emerald"],
            customdata=st_pq_all[["std_read_agg_time_sec", "read_agg_throughput_rows_per_sec"]],
            hovertemplate="<b>Parquet Read + Aggregation — %{x}</b><br>Trung bình 3 lần: %{y:.4f}s (±%{customdata[0]:.4f}s)<br>Thông lượng: %{customdata[1]:,.2f} rows/s<extra></extra>",
        )
    )
    fig_st_ra.update_layout(barmode="group", yaxis_range=[0, float(st_csv_all["avg_read_agg_time_sec"].max()) * 1.22])
    apply_standard_chart_layout(
        fig_st_ra,
        title="4. CSV vs Parquet Read + Aggregation Time (Trung bình 3 lần chạy — Giây)",
        xaxis_title="Quy mô dữ liệu (Scale Factor)",
        yaxis_title="Thời gian Read + Aggregation (Giây)",
        height=380,
    )
    render_plotly_chart(fig_st_ra, key="bm_chart_st_read_agg")

with st.expander("📋 Xem bảng đối chiếu đầy đủ `outputs/benchmark/storage_comparison.csv` (8 cấu hình)", expanded=False):
    st.dataframe(storage_df, width="stretch", hide_index=True)

st.markdown("---")

# =============================================================================
# D. SPARK PROCESSING PERFORMANCE (PROCESSING_RESULTS.CSV)
# =============================================================================
st.subheader("4. Hiệu năng 5 Phép toán Xử lý Phân tán trên Spark (Spark Processing Performance)")

st.caption(
    "Nguồn dữ liệu: `outputs/benchmark/processing_results.csv` (`20` dòng = 4 quy mô × 5 phép toán: `Filter`, `GroupBy`, `Aggregation`, `Sort`, `Join`). "
    "Mỗi cấu hình được thực thi lặp lại **`num_runs = 3`** lần; `avg_processing_time_sec` là trung bình cộng của `run1_time_sec`, `run2_time_sec`, `run3_time_sec`."
)

all_ops = ["Filter", "GroupBy", "Aggregation", "Sort", "Join"]

col_pr_f1, col_pr_f2, col_pr_f3 = st.columns([1.2, 1.3, 1.0])
with col_pr_f1:
    selected_proc_scales = st.multiselect(
        "Lọc theo Quy mô Dữ liệu (Dataset Scale):",
        options=all_scales,
        default=all_scales,
        key="bm_proc_scales_filter",
    )
with col_pr_f2:
    selected_proc_ops = st.multiselect(
        "Lọc theo Phép xử lý Spark (Processing Operation):",
        options=all_ops,
        default=all_ops,
        key="bm_proc_ops_filter",
    )
with col_pr_f3:
    focus_proc_scale = st.selectbox(
        "Quy mô trọng tâm cho Biểu đồ 4 (So sánh 5 phép toán):",
        options=all_scales,
        index=3,  # Mặc định 100x
        key="bm_proc_focus_scale",
    )

if not selected_proc_scales:
    selected_proc_scales = all_scales
if not selected_proc_ops:
    selected_proc_ops = all_ops

proc_filtered = processing_df.loc[
    processing_df["scale_label"].isin(selected_proc_scales)
    & processing_df["operation"].isin(selected_proc_ops)
].copy()

proc_focus_df = processing_df.loc[processing_df["scale_label"] == focus_proc_scale].copy()

op_colors = {
    "Filter": COLORS["primary"],
    "GroupBy": COLORS["teal"],
    "Aggregation": COLORS["emerald"],
    "Sort": COLORS["crimson"],
    "Join": COLORS["amber"],
}
scale_colors = {
    "1x": COLORS["slate"],
    "10x": COLORS["teal"],
    "50x": COLORS["emerald"],
    "100x": COLORS["primary"],
}

col_pr1, col_pr2 = st.columns(2)

with col_pr1:
    fig_pr_by_op = go.Figure()
    for sc_lbl in selected_proc_scales:
        sub_df = proc_filtered.loc[proc_filtered["scale_label"] == sc_lbl]
        fig_pr_by_op.add_trace(
            go.Bar(
                name=f"Scale {sc_lbl}",
                x=sub_df["operation"],
                y=sub_df["avg_processing_time_sec"],
                text=sub_df["avg_processing_time_sec"].apply(lambda v: f"{v:.2f}s"),
                textposition="outside",
                marker_color=scale_colors.get(sc_lbl, COLORS["secondary"]),
                customdata=sub_df[["input_rows", "output_rows", "throughput_rows_per_sec", "cpu_usage_pct"]],
                hovertemplate=(
                    f"<b>Quy mô {sc_lbl} — Phép toán %{{x}}</b><br>"
                    "Thời gian TB (3 runs): %{y:.4f}s<br>"
                    "Số dòng vào/ra: %{customdata[0]:,} → %{customdata[1]:,}<br>"
                    "Thông lượng: %{customdata[2]:,.2f} rows/s | CPU: %{customdata[3]:.1f}%<extra></extra>"
                ),
            )
        )
    fig_pr_by_op.update_layout(
        barmode="group",
        yaxis_range=[0, float(proc_filtered["avg_processing_time_sec"].max()) * 1.22],
    )
    apply_standard_chart_layout(
        fig_pr_by_op,
        title="1. Processing Time by Operation (Trung bình 3 lần chạy — Giây)",
        xaxis_title="Phép xử lý Spark (Operation)",
        yaxis_title="Thời gian xử lý trung bình (Giây)",
        height=400,
    )
    render_plotly_chart(fig_pr_by_op, key="bm_chart_proc_by_op")

with col_pr2:
    fig_pr_by_scale = go.Figure()
    for op_name in selected_proc_ops:
        sub_df = proc_filtered.loc[proc_filtered["operation"] == op_name].sort_values("scale_factor")
        fig_pr_by_scale.add_trace(
            go.Scatter(
                name=op_name,
                x=sub_df["scale_label"],
                y=sub_df["avg_processing_time_sec"],
                mode="lines+markers",
                line={"color": op_colors.get(op_name, COLORS["primary"]), "width": 2.5},
                marker={"size": 8},
                customdata=sub_df[["input_rows", "std_processing_time_sec", "throughput_rows_per_sec"]],
                hovertemplate=(
                    f"<b>{op_name} — Quy mô %{{x}}</b> (%{{customdata[0]:,}} dòng)<br>"
                    "Thời gian TB (3 runs): %{y:.4f}s (±%{customdata[1]:.4f}s)<br>"
                    "Thông lượng: %{customdata[2]:,.2f} rows/s<extra></extra>"
                ),
            )
        )
    apply_standard_chart_layout(
        fig_pr_by_scale,
        title="2. Processing Time by Dataset Scale (Theo từng phép xử lý — Giây)",
        xaxis_title="Quy mô dữ liệu (Dataset Scale)",
        yaxis_title="Thời gian xử lý trung bình (Giây)",
        height=400,
    )
    render_plotly_chart(fig_pr_by_scale, key="bm_chart_proc_by_scale")

col_pr3, col_pr4 = st.columns(2)

with col_pr3:
    fig_pr_tp = go.Figure()
    for op_name in selected_proc_ops:
        sub_df = proc_filtered.loc[proc_filtered["operation"] == op_name].sort_values("scale_factor")
        fig_pr_tp.add_trace(
            go.Scatter(
                name=op_name,
                x=sub_df["scale_label"],
                y=sub_df["throughput_rows_per_sec"],
                mode="lines+markers",
                line={"color": op_colors.get(op_name, COLORS["primary"]), "width": 2.5},
                marker={"size": 8},
                customdata=sub_df[["input_rows", "avg_processing_time_sec"]],
                hovertemplate=(
                    f"<b>{op_name} — Quy mô %{{x}}</b> (%{{customdata[0]:,}} dòng)<br>"
                    "Thông lượng: %{y:,.2f} rows/s<br>"
                    "Thời gian TB: %{customdata[1]:.4f}s<extra></extra>"
                ),
            )
        )
    apply_standard_chart_layout(
        fig_pr_tp,
        title="3. Throughput by Dataset Scale (Dòng/giây theo từng phép toán)",
        xaxis_title="Quy mô dữ liệu (Dataset Scale)",
        yaxis_title="Thông lượng xử lý (Rows / sec)",
        height=400,
    )
    render_plotly_chart(fig_pr_tp, key="bm_chart_proc_tp")

with col_pr4:
    fig_pr_focus = go.Figure()
    focus_colors = [op_colors.get(op, COLORS["primary"]) for op in proc_focus_df["operation"]]
    fig_pr_focus.add_trace(
        go.Bar(
            x=proc_focus_df["operation"],
            y=proc_focus_df["avg_processing_time_sec"],
            text=proc_focus_df["avg_processing_time_sec"].apply(lambda v: f"{v:.4f}s"),
            textposition="outside",
            marker_color=focus_colors,
            customdata=proc_focus_df[
                [
                    "throughput_rows_per_sec",
                    "cpu_usage_pct",
                    "driver_container_mem_mb",
                    "run1_time_sec",
                    "run2_time_sec",
                    "run3_time_sec",
                ]
            ],
            hovertemplate=(
                f"<b>Quy mô {focus_proc_scale} — %{{x}}</b><br>"
                "Thời gian TB (3 runs): %{y:.4f}s (Run1: %{customdata[3]:.2f}s | Run2: %{customdata[4]:.2f}s | Run3: %{customdata[5]:.2f}s)<br>"
                "Thông lượng: %{customdata[0]:,.2f} rows/s<br>"
                "CPU: %{customdata[1]:.2f}% | Driver RAM: %{customdata[2]:.2f} MB<extra></extra>"
            ),
        )
    )
    fig_pr_focus.update_layout(yaxis_range=[0, float(proc_focus_df["avg_processing_time_sec"].max()) * 1.25])
    apply_standard_chart_layout(
        fig_pr_focus,
        title=f"4. So sánh 5 Phép xử lý tại Quy mô {focus_proc_scale} ({int(proc_focus_df['input_rows'].iloc[0]):,} dòng)",
        xaxis_title="Phép xử lý Spark (Operation)",
        yaxis_title="Thời gian xử lý trung bình (Giây)",
        height=400,
        show_legend=False,
    )
    render_plotly_chart(fig_pr_focus, key="bm_chart_proc_focus")

with st.expander("📋 Xem bảng dữ liệu chi tiết `outputs/benchmark/processing_results.csv` (20 dòng)", expanded=False):
    st.dataframe(proc_filtered, width="stretch", hide_index=True)

st.markdown("---")

# =============================================================================
# E. PARTITION OPTIMIZATION (PARTITION_RESULTS.CSV)
# =============================================================================
st.subheader("5. Tối ưu hóa Số lượng Partition trên Spark (Partition Optimization: 1, 2, 4, 8 Partitions)")

st.caption(
    "Nguồn dữ liệu: `outputs/benchmark/partition_results.csv` (`12` cấu hình = 3 quy mô `10x`, `50x`, `100x` × 4 mức `1, 2, 4, 8` partitions, mỗi cấu hình chạy lặp `3` lần)."
)

part_scales = ["10x", "50x", "100x"]
all_part_counts = [1, 2, 4, 8]

col_pt_f1, col_pt_f2 = st.columns([1.2, 1.3])
with col_pt_f1:
    selected_part_scales = st.multiselect(
        "Lọc theo Quy mô trong thử nghiệm Partition (Scale):",
        options=part_scales,
        default=part_scales,
        key="bm_part_scales_filter",
    )
with col_pt_f2:
    selected_part_counts = st.multiselect(
        "Lọc theo Số lượng Partition (Number of Partitions):",
        options=all_part_counts,
        default=all_part_counts,
        key="bm_part_counts_filter",
    )

if not selected_part_scales:
    selected_part_scales = part_scales
if not selected_part_counts:
    selected_part_counts = all_part_counts

part_filtered = partition_df.loc[
    partition_df["scale_label"].isin(selected_part_scales)
    & partition_df["target_partitions"].isin(selected_part_counts)
].copy()
part_filtered["partition_label"] = part_filtered["target_partitions"].apply(lambda p: f"{p} Partition{'s' if p > 1 else ''}")

col_pt1, col_pt2, col_pt3 = st.columns(3)

with col_pt1:
    fig_pt_time = go.Figure()
    for sc_lbl in selected_part_scales:
        sub_df = part_filtered.loc[part_filtered["scale_label"] == sc_lbl].sort_values("target_partitions")
        fig_pt_time.add_trace(
            go.Bar(
                name=f"Scale {sc_lbl}",
                x=sub_df["partition_label"],
                y=sub_df["avg_processing_time_sec"],
                text=sub_df["avg_processing_time_sec"].apply(lambda v: f"{v:.2f}s"),
                textposition="outside",
                marker_color=scale_colors.get(sc_lbl, COLORS["primary"]),
                customdata=sub_df[["num_rows", "std_processing_time_sec", "throughput_rows_per_sec", "repartition_cache_time_sec"]],
                hovertemplate=(
                    f"<b>Quy mô {sc_lbl} — %{{x}}</b> (%{{customdata[0]:,}} dòng)<br>"
                    "Thời gian xử lý TB (3 runs): %{y:.4f}s (±%{customdata[1]:.4f}s)<br>"
                    "Thông lượng: %{customdata[2]:,.2f} rows/s<br>"
                    "Thời gian Repartition + Cache: %{customdata[3]:.2f}s<extra></extra>"
                ),
            )
        )
    fig_pt_time.update_layout(
        barmode="group",
        yaxis_range=[0, float(part_filtered["avg_processing_time_sec"].max()) * 1.22],
    )
    apply_standard_chart_layout(
        fig_pt_time,
        title="1. Partitions vs Processing Time (Giây)",
        xaxis_title="Số lượng Partition (target_partitions)",
        yaxis_title="Thời gian xử lý trung bình (Giây)",
        height=390,
    )
    render_plotly_chart(fig_pt_time, key="bm_chart_part_time")

with col_pt2:
    fig_pt_tp = go.Figure()
    for sc_lbl in selected_part_scales:
        sub_df = part_filtered.loc[part_filtered["scale_label"] == sc_lbl].sort_values("target_partitions")
        fig_pt_tp.add_trace(
            go.Bar(
                name=f"Scale {sc_lbl}",
                x=sub_df["partition_label"],
                y=sub_df["throughput_rows_per_sec"],
                text=sub_df["throughput_rows_per_sec"].apply(lambda v: f"{v/1000:.0f}k"),
                textposition="outside",
                marker_color=scale_colors.get(sc_lbl, COLORS["primary"]),
                customdata=sub_df[["num_rows", "avg_processing_time_sec"]],
                hovertemplate=(
                    f"<b>Quy mô {sc_lbl} — %{{x}}</b> (%{{customdata[0]:,}} dòng)<br>"
                    "Thông lượng: %{y:,.2f} rows/s<br>"
                    "Thời gian TB: %{customdata[1]:.4f}s<extra></extra>"
                ),
            )
        )
    fig_pt_tp.update_layout(
        barmode="group",
        yaxis_range=[0, float(part_filtered["throughput_rows_per_sec"].max()) * 1.22],
    )
    apply_standard_chart_layout(
        fig_pt_tp,
        title="2. Partitions vs Throughput (Dòng/giây)",
        xaxis_title="Số lượng Partition (target_partitions)",
        yaxis_title="Thông lượng xử lý (Rows / sec)",
        height=390,
    )
    render_plotly_chart(fig_pt_tp, key="bm_chart_part_tp")

with col_pt3:
    fig_pt_cpu = go.Figure()
    for sc_lbl in selected_part_scales:
        sub_df = part_filtered.loc[part_filtered["scale_label"] == sc_lbl].sort_values("target_partitions")
        fig_pt_cpu.add_trace(
            go.Scatter(
                name=f"Scale {sc_lbl}",
                x=sub_df["partition_label"],
                y=sub_df["cpu_usage_pct"],
                mode="lines+markers+text",
                text=sub_df["cpu_usage_pct"].apply(lambda v: f"{v:.1f}%"),
                textposition="top center",
                line={"color": scale_colors.get(sc_lbl, COLORS["primary"]), "width": 2.5},
                marker={"size": 9},
                customdata=sub_df[["driver_container_mem_mb", "avg_processing_time_sec"]],
                hovertemplate=(
                    f"<b>Quy mô {sc_lbl} — %{{x}}</b><br>"
                    "CPU Usage: %{y:.2f}%<br>"
                    "Driver RAM: %{customdata[0]:.2f} MB | Thời gian: %{customdata[1]:.4f}s<extra></extra>"
                ),
            )
        )
    fig_pt_cpu.update_layout(yaxis_range=[0, 105])
    apply_standard_chart_layout(
        fig_pt_cpu,
        title="3. CPU Usage by Partition (%)",
        xaxis_title="Số lượng Partition (target_partitions)",
        yaxis_title="Mức sử dụng CPU (%)",
        height=390,
    )
    render_plotly_chart(fig_pt_cpu, key="bm_chart_part_cpu")

st.markdown(
    """
    <div class="insight-box">
        <strong>💡 Giải thích Kết quả Tối ưu hóa Partition trên Worker 2 CPU Cores:</strong><br/>
        • <strong>Tại quy mô 10x và 50x:</strong> Cấu hình <strong>4 partitions</strong> (gấp <code>2x</code> số nhân CPU của Worker) đạt thời gian nhanh nhất và thông lượng cao nhất: <strong>0.5280s</strong> (<strong>206,174 rows/s</strong>, CPU 83.99%) ở <code>10x</code> và <strong>0.9043s</strong> (<strong>601,902 rows/s</strong>, CPU 75.77%) ở <code>50x</code>.<br/>
        • <strong>Tại quy mô 100x (1,088,600 dòng):</strong> Khi dữ liệu đã được <code>repartition().cache()</code> trong bộ nhớ, <strong>1 partition</strong> đạt thời gian thực thi thuần thấp nhất (<strong>0.9749s</strong>) do không phát sinh trao đổi shuffle liên phân vùng; trong số các cấu hình đa phân vùng, <strong>4 partitions</strong> dẫn đầu với <strong>1.3309s</strong> (<strong>817,943 rows/s</strong>, CPU 83.51%), còn <strong>2 partitions</strong> bị kéo trung bình lên <strong>9.9614s</strong> do hiện tượng dừng thu gom rác JVM (GC pause) ở lượt chạy đầu tiên (<code>run1 = 20.5834s</code>, trong khi <code>run2 = 2.5360s</code>).<br/>
        • <strong>Vì sao 8 partitions chậm hơn trong các trường hợp đã ghi nhận?</strong> Với 1 Spark Worker chỉ có <strong>2 CPU cores</strong>, việc chia thành 8 partitions khiến chỉ tối đa 2 tasks được thực thi song song tại một thời điểm; 6 tasks còn lại phải xếp hàng chờ kèm theo chi phí lập lịch task (task scheduling), tuần tự hóa (serialization) và trộn dữ liệu (shuffle) lớn hơn. Kết quả này minh chứng rằng <strong>tăng số partition không luôn cải thiện hiệu năng, và không có một con số cố định nào tối ưu cho mọi quy mô hay mọi cụm máy</strong>.
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander("📋 Xem bảng dữ liệu gốc `outputs/benchmark/partition_results.csv` (12 cấu hình)", expanded=False):
    st.dataframe(part_filtered.drop(columns=["partition_label"]), width="stretch", hide_index=True)

st.markdown("---")

# =============================================================================
# F. RESOURCE UTILIZATION (CPU & MEMORY USAGE)
# =============================================================================
st.subheader("6. Phân tích Mức sử dụng Tài nguyên Hệ thống (Resource Utilization: CPU & Memory)")

st.caption(
    "Dữ liệu tài nguyên được ghi nhận trực tiếp theo từng cấu hình trong `processing_results.csv` và `scaling_results.csv` "
    "(`cpu_usage_pct`, `driver_container_mem_mb`, `memory_usage_mb`). Không tạo biểu đồ chuỗi thời gian CPU/RAM giả."
)

col_res_u1, col_res_u2 = st.columns(2)

with col_res_u1:
    fig_res_scatter = go.Figure()
    for op_name in all_ops:
        sub_df = processing_df.loc[processing_df["operation"] == op_name]
        # Kích thước điểm tỷ lệ với căn bậc hai của thông lượng để dễ quan sát
        marker_sizes = (sub_df["throughput_rows_per_sec"] ** 0.5) / 28.0
        marker_sizes = marker_sizes.clip(lower=10.0, upper=36.0)
        fig_res_scatter.add_trace(
            go.Scatter(
                name=op_name,
                x=sub_df["cpu_usage_pct"],
                y=sub_df["avg_processing_time_sec"],
                mode="markers+text",
                text=sub_df["scale_label"],
                textposition="top center",
                marker={
                    "color": op_colors.get(op_name, COLORS["primary"]),
                    "size": marker_sizes,
                    "opacity": 0.82,
                    "line": {"width": 1.2, "color": "#FFFFFF"},
                },
                customdata=sub_df[["scale_label", "throughput_rows_per_sec", "driver_container_mem_mb"]],
                hovertemplate=(
                    f"<b>{op_name} — Quy mô %{{customdata[0]}}</b><br>"
                    "CPU Usage: %{x:.2f}%<br>"
                    "Thời gian xử lý TB: %{y:.4f}s<br>"
                    "Thông lượng: %{customdata[1]:,.2f} rows/s<br>"
                    "Driver Container RAM: %{customdata[2]:.2f} MB<extra></extra>"
                ),
            )
        )
    apply_standard_chart_layout(
        fig_res_scatter,
        title="Tương quan CPU Usage (%) vs Thời gian Xử lý (Kích thước điểm ~ Throughput)",
        xaxis_title="Mức sử dụng CPU trung bình — cpu_usage_pct (%)",
        yaxis_title="Thời gian xử lý trung bình — avg_processing_time_sec (Giây)",
        height=400,
    )
    render_plotly_chart(fig_res_scatter, key="bm_chart_res_scatter")

with col_res_u2:
    fig_res_mem = go.Figure()
    fig_res_mem.add_trace(
        go.Bar(
            name="Memory Usage (MB)",
            x=scaling_df["scale_label"],
            y=scaling_df["memory_usage_mb"],
            text=scaling_df["memory_usage_mb"].apply(lambda v: f"{v:.1f} MB"),
            textposition="outside",
            marker_color=COLORS["primary"],
            customdata=scaling_df[["num_rows", "cpu_usage_pct"]],
            hovertemplate=(
                "<b>Quy mô %{x}</b> (%{customdata[0]:,} dòng)<br>"
                "Bộ nhớ sử dụng TB: %{y:.2f} MB<br>"
                "CPU Usage TB: %{customdata[1]:.2f}%<extra></extra>"
            ),
        )
    )
    fig_res_mem.add_trace(
        go.Scatter(
            name="CPU Usage TB (%)",
            x=scaling_df["scale_label"],
            y=scaling_df["cpu_usage_pct"] * 10.0,  # Quy đổi hiển thị song song hoặc dùng hover rõ ràng
            mode="lines+markers+text",
            text=scaling_df["cpu_usage_pct"].apply(lambda v: f"CPU: {v:.1f}%"),
            textposition="bottom center",
            line={"color": COLORS["emerald"], "width": 2.5, "dash": "dot"},
            marker={"size": 8},
            customdata=scaling_df["cpu_usage_pct"],
            hovertemplate="<b>Quy mô %{x}</b><br>CPU Usage TB: %{customdata:.2f}%<extra></extra>",
        )
    )
    fig_res_mem.update_layout(yaxis_range=[0, float(scaling_df["memory_usage_mb"].max()) * 1.22])
    apply_standard_chart_layout(
        fig_res_mem,
        title="Mức sử dụng Bộ nhớ (MB) & CPU Trung bình (%) theo Quy mô Dữ liệu",
        xaxis_title="Quy mô dữ liệu (Scale Factor)",
        yaxis_title="Bộ nhớ Container (MB)",
        height=400,
    )
    render_plotly_chart(fig_res_mem, key="bm_chart_res_mem")

st.markdown("---")

# =============================================================================
# G. EXPERIMENTAL ENVIRONMENT & METHODOLOGY
# =============================================================================
st.subheader("7. Môi trường Thực nghiệm & Phương pháp Đo kiểm (Experimental Environment & Methodology)")

col_env1, col_env2 = st.columns(2)

with col_env1:
    st.markdown(
        f"""
        <div class="info-panel">
            <h3>🖥️ Cấu hình Cụm Containerized Hadoop & Spark</h3>
            <ul>
                <li><strong>Mô hình triển khai:</strong> <code>{env_info.get('deployment_mode', CLUSTER_CONFIG_SPEC['deployment_mode'])}</code></li>
                <li><strong>Hệ điều hành & Hạ tầng Host:</strong> <code>{env_info.get('os', 'Windows 11 + Docker Desktop WSL2 (16 GB Host RAM)')}</code></li>
                <li><strong>Phiên bản Apache Spark:</strong> <code>{env_info.get('spark_version', '3.5.6')}</code> (Bitnami Spark Standalone)</li>
                <li><strong>Phiên bản Apache Hadoop:</strong> <code>{CLUSTER_CONFIG_SPEC['hadoop_version']}</code></li>
                <li><strong>Tô-pô tính toán:</strong> <code>1 Spark Master</code> + <code>1 Spark Worker</code></li>
                <li><strong>Tài nguyên Spark Worker:</strong> <code>{env_info.get('worker_cores', 2)} CPU Cores</code> | <code>{env_info.get('worker_memory_mb', 2048)} MB RAM</code></li>
                <li><strong>Bộ nhớ Spark Executor:</strong> <code>{env_info.get('executor_memory_mb', 1024)} MiB</code></li>
                <li><strong>Cấu hình lưu trữ HDFS:</strong> <code>1 NameNode + 1 DataNode</code> (<code>dfs.replication = {env_info.get('hdfs_replication', 1)}</code>)</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col_env2:
    st.markdown(
        """
        <div class="info-panel">
            <h3>📐 Phương pháp Thực nghiệm & Các Giới hạn Học thuật</h3>
            <ul>
                <li><strong>Định dạng lưu trữ đối chiếu:</strong> <code>CSV (uncompressed)</code> vs <code>Parquet (Snappy columnar compression)</code>.</li>
                <li><strong>Số lần lặp lại đo kiểm (Repetitions):</strong> <code>3 lần chạy / cấu hình</code> (ghi nhận từng lượt <code>run1</code>, <code>run2</code>, <code>run3</code>, giá trị trung bình <code>avg</code> và độ lệch chuẩn <code>std</code>).</li>
                <li><strong>Chế độ bộ nhớ đệm I/O:</strong> <code>Spark-Uncached / Warm OS Page Cache</code> — gọi <code>spark.catalog.clearCache()</code> trước mỗi lần đọc.</li>
                <li><strong>Bản chất môi trường một máy vật lý:</strong> Toàn bộ các container chạy chung trên một máy chủ vật lý (Single-Host Containerized Cluster), không phải cụm nhiều máy chủ vật lý tách biệt qua mạng LAN.</li>
                <li><strong>Bản chất dữ liệu mở rộng (10x–100x):</strong> Là dữ liệu tổng hợp có kiểm soát từ 10,886 dòng gốc nhằm đánh giá hành vi mở rộng của động cơ thực thi Spark và định dạng Parquet.</li>
                <li><strong>Các yếu tố ảnh hưởng kết quả:</strong> Tài nguyên CPU/RAM giới hạn của Worker (2 cores, 1 GiB Executor), cơ chế JIT/Codegen warm-up của JVM, OS Page Cache và chi phí shuffle theo từng loại workload.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )
