"""
Milestone 6 - Giai đoạn F: Tổng hợp kết quả Benchmark & Xuất biểu đồ trực quan
Đề tài: Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark

Nội dung thực hiện:
1. Đọc kết quả từ Giai đoạn B, C, D, E:
   - generation_metadata.json / scaling_generation_summary.csv
   - storage_comparison.csv / storage_benchmark.json
   - processing_results.csv / partition_results.csv / processing_and_partition_benchmark.json
2. Tổng hợp bảng chỉ số tổng thể theo từng quy mô (1x, 10x, 50x, 100x) vào:
   - scaling_results.csv (chứa đầy đủ: Dataset size, Number of rows, Number of partitions,
     Storage size CSV/Parquet, Write time, Read time, Processing time, Throughput, CPU usage, Memory usage)
   - storage_comparison.csv
   - processing_results.csv
   - partition_results.csv
   - benchmark_summary.json
3. Tạo 5 biểu đồ SVG trực quan dựa trên 100% số liệu thực nghiệm thực tế:
   - charts/1_scale_vs_processing_time.svg
   - charts/2_scale_vs_throughput.svg
   - charts/3_csv_vs_parquet_storage_size.svg
   - charts/4_csv_vs_parquet_read_write_time.svg
   - charts/5_partitions_vs_processing_time.svg
"""

import csv
import json
import os
import shutil
from datetime import datetime, timezone


def render_grouped_bar_svg(
    filepath,
    title,
    subtitle,
    categories,
    series_list,
    y_label,
    width=860,
    height=480,
):
    """
    Vẽ biểu đồ cột ghép (Grouped Bar Chart) chuẩn SVG sắc nét từ dữ liệu thực nghiệm.
    series_list: list of dicts [{"name": str, "values": list[float], "color": str}]
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    margin_top = 80
    margin_bottom = 75
    margin_left = 90
    margin_right = 40
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    all_vals = [v for s in series_list for v in s["values"]]
    max_val = max(all_vals) * 1.15 if all_vals and max(all_vals) > 0 else 1.0

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}">',
        '<style>',
        '  .bg { fill: #ffffff; }',
        '  .title { font: bold 18px "Segoe UI", Arial, sans-serif; fill: #1e293b; }',
        '  .subtitle { font: 13px "Segoe UI", Arial, sans-serif; fill: #64748b; }',
        '  .axis-label { font: bold 13px "Segoe UI", Arial, sans-serif; fill: #334155; }',
        '  .tick-label { font: 12px "Segoe UI", Arial, sans-serif; fill: #475569; }',
        '  .val-label { font: bold 11px "Segoe UI", Arial, sans-serif; fill: #1e293b; }',
        '  .grid { stroke: #e2e8f0; stroke-width: 1; stroke-dasharray: 4,4; }',
        '  .axis { stroke: #94a3b8; stroke-width: 1.5; }',
        '  .legend { font: 12px "Segoe UI", Arial, sans-serif; fill: #334155; }',
        '</style>',
        f'<rect class="bg" width="{width}" height="{height}" rx="8" stroke="#cbd5e1" stroke-width="1"/>',
        f'<text x="{width/2}" y="32" text-anchor="middle" class="title">{title}</text>',
        f'<text x="{width/2}" y="54" text-anchor="middle" class="subtitle">{subtitle}</text>',
    ]

    # Grid lines & Y ticks
    num_ticks = 5
    for i in range(num_ticks + 1):
        val = max_val * i / num_ticks
        y = margin_top + plot_h - (plot_h * i / num_ticks)
        svg.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{margin_left + plot_w}" y2="{y:.1f}" class="grid"/>')
        lbl = f"{val:,.0f}" if max_val >= 100 else f"{val:.2f}"
        svg.append(f'<text x="{margin_left - 10}" y="{y + 4:.1f}" text-anchor="end" class="tick-label">{lbl}</text>')

    # Y axis label
    svg.append(
        f'<text x="24" y="{margin_top + plot_h/2}" text-anchor="middle" '
        f'transform="rotate(-90, 24, {margin_top + plot_h/2})" class="axis-label">{y_label}</text>'
    )

    # Axes
    svg.append(
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" class="axis"/>'
    )
    svg.append(
        f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}" class="axis"/>'
    )

    n_cats = len(categories)
    n_series = len(series_list)
    group_w = plot_w / max(n_cats, 1)
    bar_w = min(48.0, (group_w * 0.72) / max(n_series, 1))
    total_bars_w = bar_w * n_series

    for c_idx, cat in enumerate(categories):
        group_center = margin_left + (c_idx + 0.5) * group_w
        start_x = group_center - total_bars_w / 2.0
        svg.append(
            f'<text x="{group_center:.1f}" y="{margin_top + plot_h + 24}" text-anchor="middle" class="axis-label">{cat}</text>'
        )

        for s_idx, s in enumerate(series_list):
            v = s["values"][c_idx]
            bh = (v / max_val) * plot_h if max_val > 0 else 0
            bx = start_x + s_idx * bar_w
            by = margin_top + plot_h - bh
            svg.append(
                f'<rect x="{bx + 2:.1f}" y="{by:.1f}" width="{max(bar_w - 4, 4):.1f}" height="{bh:.1f}" '
                f'fill="{s["color"]}" rx="3"/>'
            )
            v_str = f"{v:,.0f}" if v >= 100 else (f"{v:.2f}" if v >= 1 else f"{v:.3f}")
            svg.append(
                f'<text x="{bx + bar_w/2:.1f}" y="{by - 6:.1f}" text-anchor="middle" class="val-label">{v_str}</text>'
            )

    # Legend
    leg_y = height - 22
    leg_start_x = width / 2 - (n_series * 135) / 2
    for s_idx, s in enumerate(series_list):
        lx = leg_start_x + s_idx * 145
        svg.append(f'<rect x="{lx:.1f}" y="{leg_y - 11}" width="14" height="14" fill="{s["color"]}" rx="2"/>')
        svg.append(f'<text x="{lx + 20:.1f}" y="{leg_y}" class="legend">{s["name"]}</text>')

    svg.append("</svg>")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(svg))
    print(f"  [SAVED CHART SVG] {filepath}")


def main():
    print("=" * 95)
    print("MILESTONE 6 - GIAI ĐOẠN F: TỔNG HỢP KẾT QUẢ BENCHMARK & TẠO BIỂU ĐỒ")
    print("=" * 95)

    # Tự động xác định thư mục nguồn (chạy trong container hoặc chạy trực tiếp trên host)
    staging_dir = "/opt/spark-jobs/_staging_outputs/benchmark"
    if not os.path.exists(staging_dir):
        staging_dir = os.path.abspath("outputs/benchmark")

    with open(f"{staging_dir}/generation_metadata.json", "r", encoding="utf-8") as f:
        gen_meta = json.load(f)
    with open(f"{staging_dir}/storage_benchmark.json", "r", encoding="utf-8") as f:
        storage_meta = json.load(f)
    with open(f"{staging_dir}/processing_and_partition_benchmark.json", "r", encoding="utf-8") as f:
        proc_part_meta = json.load(f)

    gen_scales = {r["scale_factor"]: r for r in gen_meta["scales"]}
    storage_rows = storage_meta["summary"]
    proc_rows = proc_part_meta["processing_results"]
    part_rows = proc_part_meta["partition_results"]

    # Xây dựng bảng tổng hợp scaling_results.csv theo từng quy mô (1x, 10x, 50x, 100x)
    scaling_results = []
    for scale in [1, 10, 50, 100]:
        g = gen_scales[scale]
        csv_st = next(r for r in storage_rows if r["scale_factor"] == scale and r["format"] == "CSV")
        pq_st = next(r for r in storage_rows if r["scale_factor"] == scale and r["format"] == "PARQUET")
        p_ops = [r for r in proc_rows if r["scale_factor"] == scale]

        total_proc_time = round(sum(r["avg_processing_time_sec"] for r in p_ops), 4)
        avg_proc_time = round(total_proc_time / len(p_ops), 4)
        avg_proc_throughput = round(sum(r["throughput_rows_per_sec"] for r in p_ops) / len(p_ops), 2)
        avg_cpu = round(sum(r["cpu_usage_pct"] for r in p_ops) / len(p_ops), 2)
        avg_mem = round(sum(r["driver_container_mem_mb"] for r in p_ops) / len(p_ops), 2)

        op_time_map = {r["operation"]: r["avg_processing_time_sec"] for r in p_ops}
        op_tp_map = {r["operation"]: r["throughput_rows_per_sec"] for r in p_ops}

        compression_ratio = (
            round(csv_st["storage_size_bytes"] / pq_st["storage_size_bytes"], 2)
            if pq_st["storage_size_bytes"] > 0
            else 0.0
        )

        scaling_results.append(
            {
                "scale_label": f"{scale}x",
                "scale_factor": scale,
                "num_rows": g["num_rows"],
                "num_columns": g["num_columns"],
                "num_partitions": g["default_partitions"],
                "dataset_parquet_size_mb": pq_st["storage_size_mb"],
                "dataset_csv_size_mb": csv_st["storage_size_mb"],
                "csv_to_parquet_size_ratio": compression_ratio,
                "parquet_write_time_sec": pq_st["avg_write_time_sec"],
                "csv_write_time_sec": csv_st["avg_write_time_sec"],
                "parquet_read_count_time_sec": pq_st["avg_read_count_time_sec"],
                "csv_read_count_time_sec": csv_st["avg_read_count_time_sec"],
                "parquet_read_agg_time_sec": pq_st["avg_read_agg_time_sec"],
                "csv_read_agg_time_sec": csv_st["avg_read_agg_time_sec"],
                "filter_time_sec": op_time_map["Filter"],
                "groupby_time_sec": op_time_map["GroupBy"],
                "aggregation_time_sec": op_time_map["Aggregation"],
                "sort_time_sec": op_time_map["Sort"],
                "join_time_sec": op_time_map["Join"],
                "total_5ops_processing_time_sec": total_proc_time,
                "avg_op_processing_time_sec": avg_proc_time,
                "avg_processing_throughput_rows_per_sec": avg_proc_throughput,
                "filter_throughput_rows_per_sec": op_tp_map["Filter"],
                "aggregation_throughput_rows_per_sec": op_tp_map["Aggregation"],
                "sort_throughput_rows_per_sec": op_tp_map["Sort"],
                "join_throughput_rows_per_sec": op_tp_map["Join"],
                "cpu_usage_pct": avg_cpu,
                "memory_usage_mb": avg_mem,
            }
        )

    scaling_csv_path = f"{staging_dir}/scaling_results.csv"
    with open(scaling_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(scaling_results[0].keys()))
        writer.writeheader()
        for r in scaling_results:
            writer.writerow(r)
    print(f"[SAVED] {scaling_csv_path}")

    # Tạo benchmark_summary.json tổng hợp toàn bộ Milestone 6
    summary_payload = {
        "project": "Large-Scale Bike Sharing Demand Analysis and Forecasting using Apache Spark",
        "milestone": "Milestone 6 - Big Data Scaling & Distributed Performance Benchmark",
        "environment": {
            "deployment_mode": "Single-machine containerized Spark Standalone (1 Master + 1 Worker)",
            "spark_version": "3.5.6",
            "worker_cores": 2,
            "worker_memory_mb": 2048,
            "executor_memory_mb": 1024,
            "hdfs_replication": 1,
            "os": "Windows 11 + Docker Desktop WSL2 (16 GB Host RAM)",
        },
        "scaling_results": scaling_results,
        "storage_comparison": storage_rows,
        "processing_results": proc_rows,
        "partition_results": part_rows,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    summary_json_path = f"{staging_dir}/benchmark_summary.json"
    with open(summary_json_path, "w", encoding="utf-8") as jf:
        json.dump(summary_payload, jf, indent=2, ensure_ascii=False)
    print(f"[SAVED] {summary_json_path}")

    # Tạo 5 biểu đồ SVG trực quan
    charts_dir = f"{staging_dir}/charts"
    scales_lbl = [f"{r['scale_label']} ({r['num_rows']:,} rows)" for r in scaling_results]

    # Chart 1: Dataset Scale vs Processing Time
    render_grouped_bar_svg(
        filepath=f"{charts_dir}/1_scale_vs_processing_time.svg",
        title="Biểu đồ 1: Quy mô Dữ liệu (Dataset Scale) vs Thời gian Xử lý Spark",
        subtitle="Thời gian thực thi trung bình (giây) của 5 phép toán Spark trên các quy mô 1x, 10x, 50x, 100x",
        categories=scales_lbl,
        series_list=[
            {"name": "Filter", "values": [r["filter_time_sec"] for r in scaling_results], "color": "#2563eb"},
            {"name": "GroupBy", "values": [r["groupby_time_sec"] for r in scaling_results], "color": "#0891b2"},
            {"name": "Aggregation", "values": [r["aggregation_time_sec"] for r in scaling_results], "color": "#16a34a"},
            {"name": "Sort", "values": [r["sort_time_sec"] for r in scaling_results], "color": "#ea580c"},
            {"name": "Join", "values": [r["join_time_sec"] for r in scaling_results], "color": "#7c3aed"},
        ],
        y_label="Thời gian xử lý (Giây - càng thấp càng tốt)",
    )

    # Chart 2: Dataset Scale vs Throughput
    render_grouped_bar_svg(
        filepath=f"{charts_dir}/2_scale_vs_throughput.svg",
        title="Biểu đồ 2: Quy mô Dữ liệu (Dataset Scale) vs Thông lượng (Throughput)",
        subtitle="Thông lượng xử lý (Rows / Second) theo quy mô dữ liệu 1x -> 100x",
        categories=scales_lbl,
        series_list=[
            {
                "name": "Filter (rows/s)",
                "values": [r["filter_throughput_rows_per_sec"] for r in scaling_results],
                "color": "#2563eb",
            },
            {
                "name": "Aggregation (rows/s)",
                "values": [r["aggregation_throughput_rows_per_sec"] for r in scaling_results],
                "color": "#16a34a",
            },
            {
                "name": "Sort (rows/s)",
                "values": [r["sort_throughput_rows_per_sec"] for r in scaling_results],
                "color": "#ea580c",
            },
            {
                "name": "Join (rows/s)",
                "values": [r["join_throughput_rows_per_sec"] for r in scaling_results],
                "color": "#7c3aed",
            },
        ],
        y_label="Thông lượng (Rows / Second - càng cao càng tốt)",
    )

    # Chart 3: CSV vs Parquet Storage Size
    render_grouped_bar_svg(
        filepath=f"{charts_dir}/3_csv_vs_parquet_storage_size.svg",
        title="Biểu đồ 3: So sánh Dung lượng Lưu trữ HDFS — CSV vs Parquet (Snappy)",
        subtitle="Dung lượng lưu trữ thực tế trên HDFS (MB) ở các quy mô 1x, 10x, 50x, 100x",
        categories=scales_lbl,
        series_list=[
            {"name": "CSV (Uncompressed)", "values": [r["dataset_csv_size_mb"] for r in scaling_results], "color": "#dc2626"},
            {
                "name": "Parquet (Snappy)",
                "values": [r["dataset_parquet_size_mb"] for r in scaling_results],
                "color": "#059669",
            },
        ],
        y_label="Dung lượng lưu trữ (MB - càng thấp càng tốt)",
    )

    # Chart 4: CSV vs Parquet Read/Write Time (ở các quy mô 1x, 10x, 50x, 100x)
    render_grouped_bar_svg(
        filepath=f"{charts_dir}/4_csv_vs_parquet_read_write_time.svg",
        title="Biểu đồ 4: So sánh Thời gian Ghi & Đọc (Aggregation) — CSV vs Parquet",
        subtitle="Thời gian trung bình (giây) qua 3 lần chạy trên từng quy mô dữ liệu",
        categories=scales_lbl,
        series_list=[
            {"name": "CSV Write", "values": [r["csv_write_time_sec"] for r in scaling_results], "color": "#ef4444"},
            {"name": "Parquet Write", "values": [r["parquet_write_time_sec"] for r in scaling_results], "color": "#10b981"},
            {"name": "CSV Read+Agg", "values": [r["csv_read_agg_time_sec"] for r in scaling_results], "color": "#f97316"},
            {
                "name": "Parquet Read+Agg",
                "values": [r["parquet_read_agg_time_sec"] for r in scaling_results],
                "color": "#0284c7",
            },
        ],
        y_label="Thời gian thực thi (Giây - càng thấp càng tốt)",
    )

    # Chart 5: Number of Partitions vs Processing Time
    part_cats = ["1 Partition", "2 Partitions", "4 Partitions", "8 Partitions"]
    part_series = []
    colors_p = {10: "#2563eb", 50: "#16a34a", 100: "#9333ea"}
    for sc in [10, 50, 100]:
        vals = [
            next(
                r["avg_processing_time_sec"]
                for r in part_rows
                if r["scale_factor"] == sc and r["actual_df_partitions"] == p
            )
            for p in [1, 2, 4, 8]
        ]
        part_series.append({"name": f"Scale {sc}x ({sc*10886:,} rows)", "values": vals, "color": colors_p[sc]})

    render_grouped_bar_svg(
        filepath=f"{charts_dir}/5_partitions_vs_processing_time.svg",
        title="Biểu đồ 5: Số lượng Partition (1, 2, 4, 8) vs Thời gian Xử lý Spark",
        subtitle="Đánh giá trên cụm 1 Worker (2 CPU Cores) với các quy mô 10x, 50x, 100x",
        categories=part_cats,
        series_list=part_series,
        y_label="Thời gian xử lý trung bình (Giây)",
    )

    print("=" * 95)
    print("HOÀN TẤT GIAI ĐOẠN F (SUMMARIZE BENCHMARK & GENERATE CHARTS) THÀNH CÔNG!")
    print("=" * 95)


if __name__ == "__main__":
    main()

