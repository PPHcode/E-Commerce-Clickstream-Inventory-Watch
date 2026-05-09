"""
Top-5 most-viewed products of the day - formatted text summary.

Output: <out_path>/top5_products_<date>.txt
"""

from __future__ import annotations

import os

from batch_io import load_events_for_date


def run(raw_path: str, out_path: str, logical_date: str) -> None:
    df = load_events_for_date(raw_path, logical_date)
    views = df[df["event_type"] == "view"]

    top5 = (
        views.groupby(["product_id", "category"])
             .size()
             .reset_index(name="view_count")
             .sort_values("view_count", ascending=False)
             .head(5)
    )

    os.makedirs(out_path, exist_ok=True)
    out_file = os.path.join(out_path, f"top5_products_{logical_date}.txt")

    lines = [
        "================================================================",
        f"  Top 5 Most-Viewed Products  -  {logical_date}",
        "================================================================",
        f"{'Rank':<6}{'Product':<10}{'Category':<16}{'Views':>10}",
        "-" * 64,
    ]
    for rank, (_, row) in enumerate(top5.iterrows(), start=1):
        lines.append(
            f"{rank:<6}{row['product_id']:<10}{row['category']:<16}"
            f"{int(row['view_count']):>10,}"
        )
    lines.append("=" * 64)

    with open(out_file, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))

    print(f"[top5_products] Wrote -> {out_file}")
    print("\n".join(lines))
