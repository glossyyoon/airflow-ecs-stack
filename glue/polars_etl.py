"""Glue Python Shell 5.0 entry — Polars based ETL.

Args (passed by Airflow GlueJobOperator.script_args):
  --input   s3://<raw>/dt=YYYY-MM-DD/
  --output  s3://<curated>/dt=YYYY-MM-DD/
  --run_id  Airflow run id (for log correlation)

Transformation:
  1. Read all parquet files under --input
  2. Derive revenue = quantity * unit_price
  3. Aggregate by (country, product_id):
       order_count       = number of rows
       distinct_users    = unique user_id count
       total_quantity    = sum(quantity)
       total_revenue     = sum(revenue)
       avg_unit_price    = mean(unit_price)
  4. Write the aggregate as a single Snappy-compressed Parquet file
  5. Drop a 0-byte _SUCCESS marker
"""
from __future__ import annotations

import argparse
import sys
from urllib.parse import urlparse

import boto3
import polars as pl


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--run_id", required=True)
    return p.parse_args(argv)


def put_success_marker(s3_uri: str) -> None:
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/").rstrip("/") + "/_SUCCESS"
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=b"")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    print(f"[polars-etl] run_id={args.run_id}", flush=True)
    print(f"[polars-etl] input ={args.input}", flush=True)
    print(f"[polars-etl] output={args.output}", flush=True)

    raw = pl.scan_parquet(args.input + "*.parquet")

    aggregated = (
        raw
        .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("revenue"))
        .group_by(["country", "product_id"])
        .agg(
            pl.len().alias("order_count"),
            pl.col("user_id").n_unique().alias("distinct_users"),
            pl.col("quantity").sum().alias("total_quantity"),
            pl.col("revenue").sum().round(2).alias("total_revenue"),
            pl.col("unit_price").mean().round(2).alias("avg_unit_price"),
        )
        .sort(["country", "total_revenue"], descending=[False, True])
    )

    out_file = args.output.rstrip("/") + "/orders_by_country_product.parquet"
    aggregated.sink_parquet(out_file, compression="snappy")

    # Quick observability: collect a small sample for the log.
    sample = aggregated.head(10).collect()
    print(f"[polars-etl] wrote {out_file}", flush=True)
    print(f"[polars-etl] sample of aggregated output:\n{sample}", flush=True)

    put_success_marker(args.output)
    print(f"[polars-etl] wrote _SUCCESS marker under {args.output}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
