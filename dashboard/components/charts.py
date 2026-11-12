"""
Các hàm tiện ích chuẩn hóa biểu đồ Plotly cho toàn bộ Dashboard.
Đảm bảo thống nhất tiêu đề, đơn vị, chú thích, tooltip và kích thước hiển thị.
"""

import plotly.graph_objects as go
import streamlit as st
from config import COLORS


def apply_standard_chart_layout(
    fig: go.Figure,
    title: str,
    xaxis_title: str,
    yaxis_title: str,
    height: int = 400,
    show_legend: bool = True,
) -> go.Figure:
    """Áp dụng layout chuẩn hóa, chuyên nghiệp cho đối tượng biểu đồ Plotly."""
    fig.update_layout(
        title={
            "text": title,
            "x": 0.0,
            "xanchor": "left",
            "font": {"size": 16, "color": "#0F172A", "family": "Segoe UI, Inter, Arial, sans-serif"},
        },
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        template="plotly_white",
        height=height,
        margin={"l": 50, "r": 30, "t": 60, "b": 50},
        font={"family": "Segoe UI, Inter, Arial, sans-serif", "size": 12, "color": "#334155"},
        showlegend=show_legend,
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1.0,
        },
        hoverlabel={"bgcolor": "#0F172A", "font_size": 12, "font_family": "Segoe UI, Arial"},
    )
    fig.update_xaxes(showgrid=False, linecolor=COLORS["border"])
    fig.update_yaxes(showgrid=True, gridcolor="#F1F5F9", linecolor=COLORS["border"])
    return fig


def render_plotly_chart(fig: go.Figure, key: str | None = None) -> None:
    """Hiển thị biểu đồ Plotly thích ứng toàn chiều rộng vùng chứa."""
    st.plotly_chart(fig, width="stretch", key=key)

