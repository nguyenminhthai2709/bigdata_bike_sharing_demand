"""
Thành phần Sidebar hiển thị thông tin dự án, thông tin dataset và cấu hình thực nghiệm Spark/HDFS.
"""

import streamlit as st
from config import CLUSTER_CONFIG_SPEC, PROJECT_INFO


def render_sidebar_Metadata() -> None:
    """Hiển thị thông tin ngữ cảnh dự án, dataset và cấu hình cụm thực nghiệm ở Sidebar."""
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 📊 Thông tin Dataset")
        st.caption(
            f"**Nguồn:** {PROJECT_INFO['dataset_name']}\n\n"
            f"- **Train set:** `10,886` dòng (`20` cột features)\n"
            f"- **Test set:** `6,493` dòng (`19` cột features)\n"
            f"- **Biến mục tiêu:** `{PROJECT_INFO['target_column']}`\n"
            f"- **Kiểm soát Leakage:** Đã loại bỏ `casual`, `registered`"
        )

        st.markdown("### ⚙️ Cấu hình Cụm Thực nghiệm")
        st.caption(
            "*(Thông số cấu hình thực nghiệm đã ghi nhận — không phải giám sát realtime)*\n\n"
            f"- **Chế độ:** `{CLUSTER_CONFIG_SPEC['deployment_mode']}`\n"
            f"- **Cụm Spark:** `{CLUSTER_CONFIG_SPEC['spark_topology']}`\n"
            f"- **CPU Worker:** `{CLUSTER_CONFIG_SPEC['worker_cores']}`\n"
            f"- **RAM Worker:** `{CLUSTER_CONFIG_SPEC['worker_ram']}`\n"
            f"- **Lưu trữ HDFS:** `{CLUSTER_CONFIG_SPEC['hdfs_replication']}`"
        )
        st.markdown("---")
        st.caption("© 2026 Big Data & Apache Spark MLlib Project")

