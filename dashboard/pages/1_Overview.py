"""
Trang 1: Overview — Tổng quan dự án, Dataset, 6 KPI cốt lõi, Kiến trúc Hệ thống và Tiến độ Milestone.
Toàn bộ số liệu được đọc trực tiếp từ artifacts của Milestone 4, 5, 6 qua data_loader.py.
"""

from pathlib import Path
import sys
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parents[1]
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from config import CLUSTER_CONFIG_SPEC, PROJECT_INFO
from data_loader import ArtifactLoadError, get_overview_kpis
from components.kpi_cards import render_kpi_row, render_page_header

# =============================================================================
# A. HEADER
# =============================================================================
render_page_header(
    badge_text=PROJECT_INFO["subtitle"],
    title=PROJECT_INFO["title"],
    subtitle=(
        f"{PROJECT_INFO['vietnamese_title']} — Hệ thống lưu trữ phân tán trên Apache Hadoop HDFS, "
        "tiền xử lý, phân tích truy vấn Spark SQL, huấn luyện mô hình dự báo bằng Apache Spark MLlib "
        "và đánh giá khả năng mở rộng dữ liệu quy mô lớn."
    ),
)

# Đọc dữ liệu KPI tổng hợp trực tiếp từ các tệp CSV/JSON nguồn (không hardcode kết quả)
try:
    kpis = get_overview_kpis()
except ArtifactLoadError as err:
    st.error(f"Lỗi nạp dữ liệu nguồn cho trang Overview: {err}")
    st.stop()

# =============================================================================
# B. DATASET OVERVIEW
# =============================================================================
st.subheader("1. Tổng quan Bộ dữ liệu (Dataset Overview)")

ds_col1, ds_col2, ds_col3, ds_col4 = st.columns(4)
with ds_col1:
    st.metric(
        label="Dataset",
        value=PROJECT_INFO["dataset_name"],
        help="Bộ dữ liệu chuẩn từ Kaggle ghi nhận nhu cầu thuê xe đạp theo giờ trong 2 năm 2011–2012.",
    )
with ds_col2:
    st.metric(
        label="Training Records",
        value=f"{kpis['train_rows']:,}",
        delta=f"{kpis['train_cols']} cột gốc → {kpis['num_features']} features",
        delta_color="off",
    )
with ds_col3:
    st.metric(
        label="Test Records",
        value=f"{kpis['test_rows']:,}",
        delta=f"{kpis['test_cols']} cột gốc → {kpis['num_features']} features",
        delta_color="off",
    )
with ds_col4:
    st.metric(
        label="Target Variable",
        value=kpis["target_column"],
        delta=f"Tổng {kpis['total_rentals']:,} lượt thuê (Train)",
        delta_color="off",
    )

st.markdown("<div style='height: 0.4rem;'></div>", unsafe_allow_html=True)

# =============================================================================
# C. KPI CARDS (6 KPI CHÍNH ĐỐI CHIẾU TỪ MILESTONE 5 & 6)
# =============================================================================
st.subheader("2. Các Chỉ số Kết quả Cốt lõi (Key Performance Indicators)")

render_kpi_row(
    [
        {
            "title": "1. Best Model",
            "value": kpis["best_model_name"],
            "unit": "",
            "description": f"{kpis['best_model_full_name']} (30 trees, maxDepth=8) đạt sai số thấp nhất trên Validation.",
            "accent": "emerald",
        },
        {
            "title": "2. Validation RMSE",
            "value": f"{kpis['val_rmse']:.4f}",
            "unit": "lượt/giờ",
            "description": "Sai số bình phương trung bình căn bậc hai trên tập Validation (Time-based Split 20%).",
            "accent": "blue",
        },
        {
            "title": "3. Validation MAE",
            "value": f"{kpis['val_mae']:.4f}",
            "unit": "lượt/giờ",
            "description": "Sai số tuyệt đối trung bình trên 2,178 bản ghi Validation (2012-08-05 → 2012-12-19).",
            "accent": "blue",
        },
    ]
)

st.markdown("<div style='height: 0.65rem;'></div>", unsafe_allow_html=True)

render_kpi_row(
    [
        {
            "title": "4. Validation R²",
            "value": f"{kpis['val_r2']:.4f}",
            "unit": f"(RMSLE: {kpis['val_rmsle']:.4f})",
            "description": "Giải thích 79.22% phương sai nhu cầu thuê xe thực tế trên tập Validation.",
            "accent": "emerald",
        },
        {
            "title": "5. Parquet Storage Reduction",
            "value": f"{kpis['parquet_reduction_pct']:.2f}%",
            "unit": "tiết kiệm",
            "description": (
                f"Tại quy mô 100x (1,088,600 dòng): giảm từ {kpis['csv_mb_100x']:.2f} MB (CSV) "
                f"xuống {kpis['pq_mb_100x']:.2f} MB (Parquet Snappy)."
            ),
            "accent": "teal",
        },
        {
            "title": "6. Parquet Compression Ratio",
            "value": f"{kpis['compression_ratio']:.2f}x",
            "unit": "CSV / Parquet",
            "description": "Tỷ số dung lượng lưu trữ CSV so với Parquet nén Snappy tại quy mô 100x trên HDFS.",
            "accent": "amber",
        },
    ]
)

st.markdown("<div style='height: 1.1rem;'></div>", unsafe_allow_html=True)

# =============================================================================
# D. SYSTEM ARCHITECTURE
# =============================================================================
st.subheader("3. Kiến trúc Hệ thống Phân tán (System Architecture)")

st.markdown(
    f"""
    <div class="env-notice">
        <strong>📌 Ghi chú Môi trường Thực nghiệm:</strong> Hệ thống được triển khai theo mô hình
        <strong>{CLUSTER_CONFIG_SPEC['deployment_mode']}</strong> gồm <strong>{CLUSTER_CONFIG_SPEC['spark_topology']}</strong>
        (<strong>{CLUSTER_CONFIG_SPEC['worker_cores']}</strong>, <strong>{CLUSTER_CONFIG_SPEC['worker_ram']}</strong>)
        kết nối với <strong>Hadoop HDFS ({CLUSTER_CONFIG_SPEC['hdfs_replication']})</strong> trên cùng một máy chủ vật lý
        thông qua Docker WSL2 — <em>không phải cụm nhiều máy chủ vật lý (multi-node physical cluster)</em>.
    </div>
    """,
    unsafe_allow_html=True,
)

pipeline_steps = [
    ("Bước 1", "Kaggle Dataset", "train.csv (10,886) & test.csv (6,493)"),
    ("Bước 2", "HDFS Storage", "Lưu trữ phân tán tại /bike-sharing/raw/ (rep=1)"),
    ("Bước 3", "Apache Spark Processing", "Làm sạch, kiểm tra 0 Null, 0 Duplicate, Spark SQL"),
    ("Bước 4", "Feature Engineering", "Trích xuất 18 features, mã hóa chu kỳ, chống Leakage"),
    ("Bước 5", "MLlib Model Training", "Time-based Split 80/20: LR, Decision Tree, Random Forest"),
    ("Bước 6", "Prediction & Evaluation", "Đánh giá RMSE/MAE/R²/RMSLE & dự báo 6,493 dòng Test"),
    ("Bước 7", "Benchmark (1x–100x)", "Đo kiểm CSV vs Parquet, 5 phép xử lý & Partition 1–8"),
    ("Bước 8", "Streamlit Dashboard", "Trực quan hóa kết quả từ outputs/ (@st.cache_data)"),
]

row1_cols = st.columns(4)
for col, (num, title, detail) in zip(row1_cols, pipeline_steps[:4]):
    with col:
        st.markdown(
            f"""
            <div class="arch-step">
                <span class="step-num">{num}</span>
                <div class="step-title">{title}</div>
                <div class="step-detail">{detail}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown("<div style='text-align: center; color: #64748B; font-size: 0.85rem; margin: 0.35rem 0;'>▼ Luồng xử lý tiếp theo ▼</div>", unsafe_allow_html=True)

row2_cols = st.columns(4)
for col, (num, title, detail) in zip(row2_cols, pipeline_steps[4:]):
    with col:
        st.markdown(
            f"""
            <div class="arch-step">
                <span class="step-num">{num}</span>
                <div class="step-title">{title}</div>
                <div class="step-detail">{detail}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

with st.expander("Xem Sơ đồ Luồng Kiến trúc Chi tiết (Architecture Flowchart)", expanded=False):
    st.markdown(
        """
        ```text
        [1. Kaggle Dataset (CSV)]
                   │
                   ▼
        [2. Hadoop HDFS Storage (/bike-sharing/raw, replication=1)]
                   │
                   ▼
        [3. Apache Spark Processing (Data Cleaning, Validation & 8 Spark SQL Queries)]
                   │
                   ▼
        [4. Feature Engineering (18 Input Features, Cyclical Encoding, Leakage Control)]
                   │
                   ├───────────────────────────────────────────────┐
                   ▼                                               ▼
        [5. Spark MLlib Model Training]                 [7. Big Data Scaling Benchmark]
            (Linear Regression, Decision Tree,              (1x, 10x, 50x, 100x up to 1.09M rows;
             Random Forest on Time-based Split)              CSV vs Parquet; Partitions 1, 2, 4, 8)
                   │                                               │
                   ▼                                               │
        [6. Prediction & Evaluation]                               │
            (Best Model: Random Forest, RMSE=99.1848;              │
             Test Predictions & Kaggle Submission)                 │
                   │                                               │
                   └───────────────────────┬───────────────────────┘
                                           ▼
                      [8. Streamlit Interactive Dashboard (Port 8501)]
        ```
        """
    )

st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)

# =============================================================================
# E. PROJECT MILESTONES
# =============================================================================
st.subheader("4. Tiến độ Thực hiện Dự án (Project Milestones)")

milestones_data = [
    (
        "Milestone 2: HDFS Deployment",
        "Triển khai Hadoop NameNode & DataNode (replication=1), tải thành công bộ dữ liệu gốc lên /bike-sharing/raw/.",
        "Hoàn thành",
        True,
    ),
    (
        "Milestone 3: Spark Cluster Setup",
        "Triển khai Spark Standalone (1 Master + 1 Worker), xác minh tích hợp đọc/ghi Parquet và Spark SQL trên HDFS.",
        "Hoàn thành",
        True,
    ),
    (
        "Milestone 4: EDA & Feature Engineering",
        "Khám phá dữ liệu (EDA), tiền xử lý, trích xuất 18 đặc trưng chống Data Leakage và thực thi 8 truy vấn Spark SQL.",
        "Hoàn thành",
        True,
    ),
    (
        "Milestone 5: Machine Learning",
        "Huấn luyện Linear Regression, Decision Tree, Random Forest bằng Spark MLlib (Time-based Split 80/20) và tạo dự báo Test.",
        "Hoàn thành",
        True,
    ),
    (
        "Milestone 6: Scaling & Performance Benchmark",
        "Mở rộng dữ liệu có kiểm soát (1x, 10x, 50x, 100x — 1,088,600 dòng), đo kiểm CSV vs Parquet, 5 phép xử lý và Partition 1–8.",
        "Hoàn thành",
        True,
    ),
    (
        "Milestone 7: Dashboard & Project Finalization",
        "Xây dựng hoàn chỉnh ứng dụng Streamlit Dashboard 4 trang (Overview, Data Analysis, Machine Learning, Big Data Benchmark) và hoàn tất kiểm thử tích hợp.",
        "Hoàn thành",
        True,
    ),
]

for m_title, m_desc, m_status, is_done in milestones_data:
    badge_cls = "status-badge-done" if is_done else "status-badge-active"
    icon = "✅" if is_done else "🔄"
    st.markdown(
        f"""
        <div class="milestone-row">
            <div>
                <div style="font-weight: 700; color: #0F172A; font-size: 0.95rem;">{icon} {m_title}</div>
                <div style="color: #475569; font-size: 0.84rem; margin-top: 0.15rem;">{m_desc}</div>
            </div>
            <span class="{badge_cls}">{m_status}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

