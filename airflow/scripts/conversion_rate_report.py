"""
Conversion-rate report: Purchases / Views per product category.

Outputs:
  - <out_path>/conversion_rates_<date>.csv
  - <out_path>/conversion_rates_<date>.png   (bar chart)
"""

from __future__ import annotations

import os

import matplotlib
matplotlib.use("Agg")  # headless container
import matplotlib.pyplot as plt

from batch_io import load_events_for_date


def run(raw_path: str, out_path: str, logical_date: str) -> None:
    df = load_events_for_date(raw_path, logical_date)

    pivot = (
        df.groupby(["category", "event_type"])
          .size()
          .unstack(fill_value=0)
          .reset_index()
    )
    for col in ("view", "add_to_cart", "purchase"):
        if col not in pivot.columns:
            pivot[col] = 0

    pivot["conversion_rate"] = pivot.apply(
        lambda r: (r["purchase"] / r["view"]) if r["view"] > 0 else 0.0,
        axis=1,
    )
    pivot = pivot.rename(columns={
        "view": "views",
        "add_to_cart": "cart_adds",
        "purchase": "purchases",
    })
    pivot = pivot[["category", "views", "cart_adds", "purchases", "conversion_rate"]]
    pivot = pivot.sort_values("conversion_rate", ascending=False)

    os.makedirs(out_path, exist_ok=True)
    csv_file = os.path.join(out_path, f"conversion_rates_{logical_date}.csv")
    png_file = os.path.join(out_path, f"conversion_rates_{logical_date}.png")
    pivot.to_csv(csv_file, index=False)

    # ----- chart
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(pivot["category"], pivot["conversion_rate"] * 100)
    ax.set_title(f"Conversion Rate by Category - {logical_date}")
    ax.set_xlabel("Category")
    ax.set_ylabel("Conversion Rate (%)")
    ax.grid(axis="y", linestyle=":", alpha=0.6)
    for i, v in enumerate(pivot["conversion_rate"] * 100):
        ax.text(i, v, f"{v:.1f}%", ha="center", va="bottom", fontsize=9)
    plt.xticks(rotation=20)
    plt.tight_layout()
    fig.savefig(png_file, dpi=140)
    plt.close(fig)

    print(f"[conversion_rate] Wrote -> {csv_file}")
    print(f"[conversion_rate] Wrote -> {png_file}")
    print(pivot.to_string(index=False))
