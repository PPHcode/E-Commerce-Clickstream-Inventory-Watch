"""Shared helpers for Airflow batch tasks."""

from __future__ import annotations

import glob
import os
from datetime import date, datetime, timedelta

import pandas as pd


def load_events_for_date(raw_path: str, target_date: str) -> pd.DataFrame:
    """
    Load all events whose event_date partition matches `target_date`
    (YYYY-MM-DD). If nothing is found for that exact day, fall back to
    the last available partition so the demo still produces output.
    """
    partition = os.path.join(raw_path, f"event_date={target_date}")
    if not os.path.isdir(partition):
        # Fallback: use the most recently-written partition we can find.
        candidates = sorted(glob.glob(os.path.join(raw_path, "event_date=*")))
        if not candidates:
            raise FileNotFoundError(
                f"No raw event partitions under {raw_path}. "
                f"Has the Spark streaming job written any data yet?"
            )
        partition = candidates[-1]
        print(f"[batch] No data for {target_date}, falling back to {partition}")

    files = glob.glob(os.path.join(partition, "*.parquet"))
    if not files:
        raise FileNotFoundError(f"Partition {partition} contains no parquet files")

    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    print(f"[batch] Loaded {len(df):,} events from {partition}")
    return df


def previous_day(logical_date: str) -> str:
    """Return YYYY-MM-DD for the day before `logical_date`."""
    d = datetime.strptime(logical_date, "%Y-%m-%d").date()
    return (d - timedelta(days=1)).isoformat()
