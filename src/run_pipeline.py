#!/usr/bin/env python3
"""Run the full medallion pipeline end to end and print a layer-by-layer summary.

Usage:
    python3 data/generate_orders.py   # build synthetic raw data (once)
    PYTHONPATH=src python3 src/run_pipeline.py
"""
import shutil
from pathlib import Path

from bronze_ingest import run as bronze
from gold_aggregates import run as gold
from silver_cleanse import run as cleanse
from silver_scd2 import run as scd2
from spark_session import get_spark


def main():
    wh = Path("warehouse")
    if wh.exists():                       # fresh run every time
        shutil.rmtree(wh)
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    n_bronze = bronze(spark)
    n_silver, n_quar = cleanse(spark)
    n_cur, n_hist = scd2(spark)
    n_daily, n_ltv = gold(spark)

    print("\n==== medallion summary ====")
    print(f"bronze_orders          : {n_bronze:>6} raw events (schema drift absorbed)")
    print(f"silver_orders          : {n_silver:>6} clean  | quarantine_orders: {n_quar:>4}")
    print(f"silver_dim_customers   : {n_cur:>6} current | {n_hist:>4} SCD2 history rows")
    print(f"gold_daily_revenue     : {n_daily:>6} rows")
    print(f"gold_customer_ltv      : {n_ltv:>6} customers")
    spark.stop()


if __name__ == "__main__":
    main()
