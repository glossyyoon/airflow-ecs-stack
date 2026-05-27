"""Generate mock e-commerce order parquet and upload it to the raw S3 bucket.

Usage:
    python scripts/generate_mock_data.py \
        --bucket airflow-ecs-raw-857988933565 \
        --date 2026-05-27 \
        --rows 50000

Writes:
    s3://<bucket>/dt=<date>/orders-0000.parquet
    s3://<bucket>/dt=<date>/_SUCCESS
"""
from __future__ import annotations

import argparse
import io
import random
import sys
from datetime import datetime, timedelta, timezone

import boto3
import polars as pl


COUNTRIES = ["KR", "US", "JP", "DE", "FR", "GB", "SG", "AU"]
PRODUCTS = [f"SKU-{i:04d}" for i in range(1, 51)]


def build_dataframe(rows: int, dt: str, seed: int) -> pl.DataFrame:
    random.seed(seed)
    day_start = datetime.fromisoformat(dt).replace(tzinfo=timezone.utc)

    return pl.DataFrame(
        {
            "event_time": [
                day_start + timedelta(seconds=random.randint(0, 86_399))
                for _ in range(rows)
            ],
            "user_id":    [f"u-{random.randint(1, 10_000):06d}" for _ in range(rows)],
            "product_id": [random.choice(PRODUCTS)              for _ in range(rows)],
            "quantity":   [random.randint(1, 5)                 for _ in range(rows)],
            "unit_price": [round(random.uniform(2.0, 199.0), 2) for _ in range(rows)],
            "country":    [random.choice(COUNTRIES)             for _ in range(rows)],
        }
    )


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True, help="raw S3 bucket name")
    p.add_argument("--date", required=True, help="partition date, e.g. 2026-05-27")
    p.add_argument("--rows", type=int, default=50_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--region", default="ap-northeast-2")
    args = p.parse_args(argv)

    df = build_dataframe(args.rows, args.date, args.seed)
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)

    s3 = boto3.client("s3", region_name=args.region)
    key_parquet = f"dt={args.date}/orders-0000.parquet"
    key_success = f"dt={args.date}/_SUCCESS"

    s3.put_object(Bucket=args.bucket, Key=key_parquet, Body=buf.getvalue())
    s3.put_object(Bucket=args.bucket, Key=key_success, Body=b"")

    print(f"uploaded s3://{args.bucket}/{key_parquet}  ({df.height} rows, {buf.tell()} bytes)")
    print(f"uploaded s3://{args.bucket}/{key_success}")
    print()
    print("preview:")
    print(df.head(5))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
