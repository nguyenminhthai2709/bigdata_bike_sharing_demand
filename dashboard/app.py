"""
Điểm khởi chạy chính (Main Entrypoint) của ứng dụng Streamlit:
Bike Sharing Demand Analysis & Forecasting Dashboard
"""

from pathlib import Path
import sys
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parent
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from config import ASSETS_DIR, PROJECT_INFO
from components.sidebar import render_sidebar_Metadata

st.set_page_config(
    page_title=PROJECT_INFO["title"],
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Nạp CSS giao diện
css_file = ASSETS_DIR / "style.css"
if css_file.exists():
    st.markdown(f"<style>{css_file.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

# Định nghĩa 4 trang điều hướng bằng st.Page & st.navigation (Streamlit 1.52.2)
overview_page = st.Page(
    "pages/1_Overview.py",
    title="1. Overview",
    icon="🏠",
    default=True,
)
analysis_page = st.Page(
    "pages/2_Data_Analysis.py",
    title="2. Data Analysis (EDA & SQL)",
    icon="📈",
)
ml_page = st.Page(
    "pages/3_Machine_Learning.py",
    title="3. Machine Learning (MLlib)",
    icon="🤖",
)
benchmark_page = st.Page(
    "pages/4_Big_Data_Benchmark.py",
    title="4. Big Data Benchmark",
    icon="⚡",
)

with st.sidebar:
    st.markdown(f"## 🚲 {PROJECT_INFO['title']}")
    st.caption(PROJECT_INFO["subtitle"])

nav = st.navigation(
    {
        "Điều hướng Dashboard": [
            overview_page,
            analysis_page,
            ml_page,
            benchmark_page,
        ]
    }
)

# Hiển thị thông tin Dataset và môi trường Spark ở phần dưới Sidebar
render_sidebar_Metadata()

# Thực thi trang được chọn
nav.run()

