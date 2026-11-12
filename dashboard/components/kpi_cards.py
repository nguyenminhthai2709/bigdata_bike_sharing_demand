"""
Thành phần hiển thị thẻ chỉ số KPI (KPI Cards) và Header Banner có thể tái sử dụng.
"""

from typing import Any
import streamlit as st


def render_page_header(badge_text: str, title: str, subtitle: str) -> None:
    """Hiển thị phần Header Banner chuẩn hóa ở đầu mỗi trang."""
    st.markdown(
        f"""
        <div class="dashboard-header">
            <div class="badge">{badge_text}</div>
            <h1>{title}</h1>
            <p class="subtitle">{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_card(
    title: str,
    value: str | int | float,
    unit: str = "",
    description: str = "",
    accent: str = "blue",
) -> None:
    """Hiển thị một thẻ KPI đơn lẻ với tiêu đề, giá trị, đơn vị và mô tả ngắn."""
    accent_class = {
        "blue": "",
        "emerald": "accent-emerald",
        "teal": "accent-teal",
        "amber": "accent-amber",
    }.get(accent, "")

    unit_html = f'<span class="kpi-unit">{unit}</span>' if unit else ""
    st.markdown(
        f"""
        <div class="kpi-card {accent_class}">
            <div>
                <div class="kpi-title">{title}</div>
                <div class="kpi-value">{value}{unit_html}</div>
            </div>
            <div class="kpi-desc">{description}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_row(items: list[dict[str, Any]]) -> None:
    """
    Hiển thị một hàng gồm nhiều KPI cards bằng st.columns.
    Mỗi phần tử trong items gồm: title, value, unit (tùy chọn), description (tùy chọn), accent (tùy chọn).
    """
    if not items:
        return
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        with col:
            render_kpi_card(
                title=item["title"],
                value=item["value"],
                unit=item.get("unit", ""),
                description=item.get("description", ""),
                accent=item.get("accent", "blue"),
            )

