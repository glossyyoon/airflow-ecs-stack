"""Pretty-print the curated aggregate that the Glue job wrote.

Usage:
    python scripts/inspect_curated.py \
        --bucket airflow-ecs-curated-857988933565 \
        --date 2026-05-27
"""
from __future__ import annotations

import argparse
import io
import sys

import boto3
import polars as pl


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--date", required=True)
    p.add_argument("--region", default="ap-northeast-2")
    args = p.parse_args(argv)

    s3 = boto3.client("s3", region_name=args.region)
    prefix = f"dt={args.date}/"
    listing = s3.list_objects_v2(Bucket=args.bucket, Prefix=prefix).get("Contents", [])
    print(f"objects under s3://{args.bucket}/{prefix}:")
    for obj in listing:
        print(f"  {obj['Key']:60s} {obj['Size']:>10d}B")

    parquet_keys = [o["Key"] for o in listing if o["Key"].endswith(".parquet")]
    if not parquet_keys:
        print("no parquet output found", file=sys.stderr)
        return 1

    body = s3.get_object(Bucket=args.bucket, Key=parquet_keys[0])["Body"].read()
    df = pl.read_parquet(io.BytesIO(body))

    print()
    print(f"schema:\n{df.schema}")
    print()
    print(f"rows: {df.height}")
    print()
    print("first 20:")
    print(df.head(20))
    print()
    print("per-country totals:")
    print(
        df.group_by("country").agg(
            pl.col("order_count").sum().alias("orders"),
            pl.col("total_revenue").sum().round(2).alias("revenue"),
        ).sort("revenue", descending=True)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
