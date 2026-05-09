"""
Daily user segmentation: Window Shoppers vs Buyers
---------------------------------------------------
A user is classified as a BUYER if they made >= 1 purchase that day,
otherwise WINDOW_SHOPPER.

Output: <out_path>/user_segments_<date>.csv with columns
        user_id, views, cart_adds, purchases, segment
"""

from __future__ import annotations

import os

import pandas as pd

from batch_io import load_events_for_date


def run(raw_path: str, out_path: str, logical_date: str) -> None:
    df = load_events_for_date(raw_path, logical_date)

    counts = (
        df.groupby(["user_id", "event_type"])
          .size()
          .unstack(fill_value=0)
          .reset_index()
    )
    for col in ("view", "add_to_cart", "purchase"):
        if col not in counts.columns:
            counts[col] = 0

    counts = counts.rename(columns={
        "view": "views",
        "add_to_cart": "cart_adds",
        "purchase": "purchases",
    })
    counts["segment"] = counts["purchases"].apply(
        lambda p: "BUYER" if p >= 1 else "WINDOW_SHOPPER"
    )
    counts = counts[["user_id", "views", "cart_adds", "purchases", "segment"]]

    os.makedirs(out_path, exist_ok=True)
    out_file = os.path.join(out_path, f"user_segments_{logical_date}.csv")
    counts.to_csv(out_file, index=False)

    summary = counts["segment"].value_counts().to_dict()
    print(f"[segment_users] Wrote {len(counts)} rows -> {out_file}")
    print(f"[segment_users] Summary: {summary}")
