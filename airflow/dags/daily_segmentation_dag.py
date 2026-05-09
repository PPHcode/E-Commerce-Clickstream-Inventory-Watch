"""
Daily Segmentation DAG - Batch Layer
-------------------------------------
Reads the previous day's raw clickstream events (Parquet, written by
the Spark streaming job) and produces three artefacts in /opt/reports:

  1. user_segments_<date>.csv         (Window Shoppers vs Buyers)
  2. top5_products_<date>.txt         (formatted summary)
  3. conversion_rates_<date>.csv      (and a PNG bar chart)

The DAG is scheduled daily at 00:30 UTC. It uses Airflow's logical
"data interval" so each run aggregates events for that interval's
date - this is how Airflow turns event-time-stamped data into batch
slices without needing the wall clock of the worker.
"""

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make /opt/airflow/scripts importable
sys.path.insert(0, "/opt/airflow/scripts")

from segment_users import run as run_segment           # noqa: E402
from top_products_report import run as run_top5        # noqa: E402
from conversion_rate_report import run as run_convrate # noqa: E402
from build_pdf_report import run as run_build_pdf      # noqa: E402

DEFAULT_ARGS = {
    "owner": "abda-mini-project",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_user_segmentation",
    description="Daily batch: segment users, top-5 products, conversion rates",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="30 0 * * *",         # 00:30 UTC every day
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "abda"],
) as dag:

    raw_path = "/opt/data/raw_events"
    reports_path = "/opt/reports"
    os.makedirs(reports_path, exist_ok=True)

    segment_users = PythonOperator(
        task_id="segment_users",
        python_callable=run_segment,
        op_kwargs={
            "raw_path": raw_path,
            "out_path": reports_path,
            "logical_date": "{{ ds }}",
        },
    )

    top5_products = PythonOperator(
        task_id="top5_products_report",
        python_callable=run_top5,
        op_kwargs={
            "raw_path": raw_path,
            "out_path": reports_path,
            "logical_date": "{{ ds }}",
        },
    )

    conversion_rates = PythonOperator(
        task_id="conversion_rate_report",
        python_callable=run_convrate,
        op_kwargs={
            "raw_path": raw_path,
            "out_path": reports_path,
            "logical_date": "{{ ds }}",
        },
    )

    build_pdf = PythonOperator(
        task_id="build_pdf_report",
        python_callable=run_build_pdf,
        op_kwargs={
            "out_path": reports_path,
            "logical_date": "{{ ds }}",
        },
    )

    # The three independent reports run in parallel; once all three
    # have produced their CSV/TXT/PNG outputs, build_pdf bundles them
    # into a single Analyzed Report PDF.
    [segment_users, top5_products, conversion_rates] >> build_pdf
